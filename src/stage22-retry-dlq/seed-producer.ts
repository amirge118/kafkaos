import { Partitioners } from "kafkajs";
import { kafka } from "../shared/kafka";
import { Order } from "../shared/types";

const topic = process.env.TOPIC ?? "orders-resilience-demo";

const orders: Order[] = [
  { orderId: "order-normal-1", customerId: "cust-A", items: [{ sku: "sku-100", qty: 1 }], total: 19.99, status: "created" },
  // Fails on attempts 1-2, succeeds on attempt 3 — within MAX_ATTEMPTS=3, so
  // this should recover via retry, never reach the DLQ.
  { orderId: "order-transient-fail-3", customerId: "cust-B", items: [{ sku: "sku-200", qty: 1 }], total: 45.0, status: "created" },
  // Always fails — exhausts every retry, must land in the DLQ.
  { orderId: "order-permanent-fail-1", customerId: "cust-C", items: [{ sku: "sku-300", qty: 2 }], total: 99.99, status: "created" },
];

async function run() {
  const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();

  for (const order of orders) {
    await producer.send({ topic, messages: [{ key: order.orderId, value: JSON.stringify(order) }] });
    console.log(`seeded ${order.orderId}`);
  }

  await producer.disconnect();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
