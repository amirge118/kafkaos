import { Partitioners } from "kafkajs";
import { kafka } from "../../shared/kafka";
import { Order } from "../../shared/types";

// Deterministic by order ID, matching Stages 22/23's convention:
//   "order-happy-*"            -> payment succeeds, inventory succeeds, ships
//   "order-payment-declined-*" -> payment fails outright (Stage 6's existing case)
//   "order-out-of-stock-*"     -> payment succeeds, but inventory reservation
//                                 fails afterward -- this is the case Stage 6
//                                 never handled: the compensating refund path.
const topic = process.env.TOPIC ?? "orders-saga";

const orders: Order[] = [
  { orderId: "order-happy-1", customerId: "cust-A", items: [{ sku: "sku-100", qty: 1 }], total: 25, status: "created" },
  { orderId: "order-payment-declined-1", customerId: "cust-B", items: [{ sku: "sku-200", qty: 1 }], total: 40, status: "created" },
  { orderId: "order-out-of-stock-1", customerId: "cust-C", items: [{ sku: "sku-300", qty: 1 }], total: 60, status: "created" },
];

async function run() {
  const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();

  for (const order of orders) {
    await producer.send({ topic, messages: [{ key: order.orderId, value: JSON.stringify(order) }] });
    console.log(`[order-producer-saga] sent ${order.orderId}`);
  }

  await producer.disconnect();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
