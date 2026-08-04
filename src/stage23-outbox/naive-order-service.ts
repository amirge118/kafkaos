import { Pool } from "pg";
import { Partitioners } from "kafkajs";
import { kafka } from "../shared/kafka";
import { Order } from "../shared/types";

// The dual-write problem, made real: two independent systems (Postgres,
// Kafka), no shared transaction between them. Commit the DB write, THEN
// publish to Kafka — a crash between those two steps loses the publish
// forever. Nothing links "this DB row" to "did its event ever go out,"
// so nothing can even detect the loss afterward, let alone retry it.
const pool = new Pool({ host: "localhost", port: 5433, user: "kafkaos", password: "kafkaos", database: "kafkaos" });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

const crashAfterOrderId = process.env.CRASH_AFTER_ORDER_ID;

const orders: Order[] = [
  { orderId: "order-naive-1", customerId: "cust-A", items: [{ sku: "sku-100", qty: 1 }], total: 20, status: "created" },
  { orderId: "order-naive-2", customerId: "cust-B", items: [{ sku: "sku-200", qty: 1 }], total: 40, status: "created" },
  { orderId: "order-naive-3", customerId: "cust-C", items: [{ sku: "sku-300", qty: 1 }], total: 60, status: "created" },
];

async function run() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS orders_naive (
      order_id TEXT PRIMARY KEY,
      customer_id TEXT NOT NULL,
      total NUMERIC NOT NULL,
      status TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
  await producer.connect();

  for (const order of orders) {
    // Step 1: the real business write. Commits immediately — this order
    // now genuinely exists, permanently, regardless of what happens next.
    await pool.query(`INSERT INTO orders_naive (order_id, customer_id, total, status) VALUES ($1, $2, $3, $4)`, [
      order.orderId,
      order.customerId,
      order.total,
      order.status,
    ]);
    console.log(`[naive-order-service] DB commit: ${order.orderId}`);

    if (order.orderId === crashAfterOrderId) {
      console.log(`[CRASH] DB already committed for ${order.orderId}, exiting BEFORE publishing to Kafka`);
      process.exit(1);
    }

    // Step 2: publish to Kafka — a completely separate system, with no
    // transactional link back to step 1 at all.
    await producer.send({
      topic: "orders-naive-events",
      messages: [{ key: order.orderId, value: JSON.stringify({ type: "OrderCreated", order }) }],
    });
    console.log(`[naive-order-service] Kafka publish: ${order.orderId}`);
  }

  await producer.disconnect();
  await pool.end();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
