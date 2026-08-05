import { Partitioners } from "kafkajs";
import { kafka } from "../../shared/kafka";
import { Order } from "../../shared/types";

// Deliberately mirrors Stage 5's exact seed pattern (same 6 orders, same
// order-1/order-2 resend) — this stage is a direct rerun of that
// experiment, not a new one, so the "before" and "after" are genuinely
// comparable.
const topic = process.env.TOPIC ?? "orders-idempotent-demo";

const orders: Order[] = [
  { orderId: "order-1", customerId: "cust-A", items: [{ sku: "sku-100", qty: 2 }], total: 39.98, status: "created" },
  { orderId: "order-2", customerId: "cust-B", items: [{ sku: "sku-200", qty: 1 }], total: 15.0, status: "created" },
  { orderId: "order-3", customerId: "cust-C", items: [{ sku: "sku-300", qty: 3 }], total: 89.97, status: "created" },
  { orderId: "order-4", customerId: "cust-A", items: [{ sku: "sku-100", qty: 1 }], total: 19.99, status: "created" },
  { orderId: "order-5", customerId: "cust-D", items: [{ sku: "sku-400", qty: 5 }], total: 250.0, status: "created" },
  { orderId: "order-6", customerId: "cust-B", items: [{ sku: "sku-200", qty: 2 }], total: 30.0, status: "created" },
];

async function run() {
  const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();

  for (const order of orders) {
    await producer.send({ topic, messages: [{ key: order.orderId, value: JSON.stringify(order) }] });
  }
  for (const orderId of ["order-1", "order-2"]) {
    const order = orders.find((o) => o.orderId === orderId)!;
    await producer.send({
      topic,
      messages: [{ key: order.orderId, value: JSON.stringify({ ...order, status: "updated" }) }],
    });
  }

  console.log(`seeded 8 messages to "${topic}" (order-1..order-6, then order-1/order-2 resent as updates)`);
  await producer.disconnect();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
