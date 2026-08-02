import { Partitioners } from "kafkajs";
import { kafka } from "../shared/kafka";
import { Order, Payment } from "../shared/types";
import { safeParseJson } from "../shared/util";

// orders -> payments
// Every order that arrives gets a (simulated) payment attempt. In a real
// system this would call a payment gateway; here we just decide succeeded vs
// failed deterministically-ish so the downstream services have something
// interesting to filter on.
const consumer = kafka.consumer({ groupId: "payment-service" });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

function simulatePayment(order: Order): Payment {
  // ~85% succeed; a few fail so inventory-service has something to skip.
  const status = Math.random() < 0.85 ? "succeeded" : "failed";
  return {
    orderId: order.orderId,
    items: order.items,
    amount: order.total,
    status,
    processedAt: new Date().toISOString(),
  };
}

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: "orders", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const order = safeParseJson<Order>(message.value?.toString(), "payment-service");
      if (!order) return;

      const payment = simulatePayment(order);

      await producer.send({
        topic: "payments",
        messages: [{ key: payment.orderId, value: JSON.stringify(payment) }],
      });

      console.log(`[payment-service] order=${order.orderId} -> payment ${payment.status}`);
    },
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
