import { Partitioners } from "kafkajs";
import { kafka } from "../shared/kafka";
import { Order } from "../shared/types";

// Explicitly opt into kafkajs's default partitioner (murmur2 hash of the key,
// matching the Java client). kafkajs warns at startup if this isn't set
// explicitly, since it changed the default in v2.0.0 and wants existing
// consumers of the library to make the choice consciously.
const topic = process.env.TOPIC ?? "orders";

const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});

const orders: Order[] = [
  { orderId: "order-1", customerId: "cust-A", items: [{ sku: "sku-100", qty: 2 }], total: 39.98, status: "created" },
  { orderId: "order-2", customerId: "cust-B", items: [{ sku: "sku-200", qty: 1 }], total: 15.0, status: "created" },
  { orderId: "order-3", customerId: "cust-C", items: [{ sku: "sku-300", qty: 3 }], total: 89.97, status: "created" },
  { orderId: "order-4", customerId: "cust-A", items: [{ sku: "sku-100", qty: 1 }], total: 19.99, status: "created" },
  { orderId: "order-5", customerId: "cust-D", items: [{ sku: "sku-400", qty: 5 }], total: 250.0, status: "created" },
  { orderId: "order-6", customerId: "cust-B", items: [{ sku: "sku-200", qty: 2 }], total: 30.0, status: "created" },
];

async function run() {
  await producer.connect();

  for (const order of orders) {
    const result = await producer.send({
      topic,
      messages: [{ key: order.orderId, value: JSON.stringify(order) }],
    });
    console.log(`sent ${order.orderId} -> partition ${result[0].partition}`);
  }

  // Resend the same keys to prove that a given key always lands on the same
  // partition (this is what "ordering per key" relies on).
  for (const orderId of ["order-1", "order-2"]) {
    const order = orders.find((o) => o.orderId === orderId)!;
    const result = await producer.send({
      topic,
      messages: [{ key: order.orderId, value: JSON.stringify({ ...order, status: "updated" }) }],
    });
    console.log(`resent ${order.orderId} -> partition ${result[0].partition}`);
  }

  await producer.disconnect();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
