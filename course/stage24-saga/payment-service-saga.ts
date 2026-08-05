import { Partitioners } from "kafkajs";
import { kafka } from "../../shared/kafka";
import { Order, Payment } from "../../shared/types";
import { Refund } from "./types";
import { safeParseJson } from "../../shared/util";

// Choreography saga, part 1 of 2: this service owns the payment step AND
// the compensating refund step — it reacts to two independent topics, with
// no central process telling it "now do the refund." That's the defining
// property of choreography: every participant subscribes to whatever
// events matter to it and decides what to do on its own.
const consumer = kafka.consumer({ groupId: "payment-service-saga" });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

function simulatePayment(order: Order): Payment {
  const status = order.orderId.includes("payment-declined") ? "failed" : "succeeded";
  return { orderId: order.orderId, items: order.items, amount: order.total, status, processedAt: new Date().toISOString() };
}

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: "orders-saga", fromBeginning: true });
  await consumer.subscribe({ topic: "payment-compensation-saga", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (topic === "orders-saga") {
        const order = safeParseJson<Order>(message.value?.toString(), "payment-service-saga");
        if (!order) return;

        const payment = simulatePayment(order);
        await producer.send({ topic: "payments-saga", messages: [{ key: payment.orderId, value: JSON.stringify(payment) }] });
        console.log(`[payment-service-saga] order=${order.orderId} -> payment ${payment.status}`);
        return;
      }

      // topic === "payment-compensation-saga": this order's payment
      // succeeded, but a *later* step (inventory) failed and needs it
      // reversed. This service never initiated that decision — it's
      // purely reacting to an event another service published.
      const request = safeParseJson<{ orderId: string; reason: string }>(message.value?.toString(), "payment-service-saga");
      if (!request) return;

      const refund: Refund = { orderId: request.orderId, reason: request.reason, status: "issued", issuedAt: new Date().toISOString() };
      await producer.send({
        topic: "payments-saga",
        messages: [{ key: refund.orderId, value: JSON.stringify({ orderId: refund.orderId, items: [], amount: 0, status: "refunded", processedAt: refund.issuedAt, reason: refund.reason }) }],
      });
      console.log(`[payment-service-saga] order=${request.orderId} -> REFUNDED (reason: ${request.reason})`);
    },
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
