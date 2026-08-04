import { Pool } from "pg";
import { Order } from "../shared/types";

// The fix: the business write and the "event to publish" are written
// together, in the SAME transaction — either both commit, or neither
// does. Nothing about *publishing to Kafka* happens in this process at
// all anymore; that's Debezium's job, reading Postgres's write-ahead log
// independently, whenever it runs. There is no code path here that can
// "commit the DB write but never publish" — publishing isn't something
// this service does or can forget to do.
const pool = new Pool({ host: "localhost", port: 5433, user: "kafkaos", password: "kafkaos", database: "kafkaos" });

const crashAfterOrderId = process.env.CRASH_AFTER_ORDER_ID;

const orders: Order[] = [
  { orderId: "order-outbox-1", customerId: "cust-A", items: [{ sku: "sku-100", qty: 1 }], total: 20, status: "created" },
  { orderId: "order-outbox-2", customerId: "cust-B", items: [{ sku: "sku-200", qty: 1 }], total: 40, status: "created" },
  { orderId: "order-outbox-3", customerId: "cust-C", items: [{ sku: "sku-300", qty: 1 }], total: 60, status: "created" },
];

async function run() {
  for (const order of orders) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(`INSERT INTO orders_outbox_demo (order_id, customer_id, total, status) VALUES ($1, $2, $3, $4)`, [
        order.orderId,
        order.customerId,
        order.total,
        order.status,
      ]);
      await client.query(
        `INSERT INTO outbox (aggregatetype, aggregateid, type, payload) VALUES ($1, $2, $3, $4)`,
        ["order", order.orderId, "OrderCreated", JSON.stringify({ type: "OrderCreated", order })]
      );
      await client.query("COMMIT");
      console.log(`[outbox-order-service] committed order + outbox row: ${order.orderId}`);
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }

    if (order.orderId === crashAfterOrderId) {
      console.log(`[CRASH] DB transaction already committed for ${order.orderId} (order row + outbox row, together) — exiting now`);
      process.exit(1);
    }
  }

  await pool.end();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
