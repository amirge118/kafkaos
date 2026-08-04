import { Partitioners } from "kafkajs";
import { kafka } from "../shared/kafka";
import { Payment, InventoryReservation } from "../shared/types";
import { safeParseJson } from "../shared/util";

// Choreography saga, part 2: the compensating action Stage 6 never had.
// Stage 6's inventory-service just skipped a failed payment and moved on;
// this one additionally handles the case Stage 6 never modeled at all —
// payment succeeded, but *this* step fails, and the payment now needs to
// be reversed. This service doesn't call payment-service directly or wait
// for a reply; it just publishes what happened and trusts something else
// is listening — the other defining property of choreography.
const consumer = kafka.consumer({ groupId: "inventory-service-saga" });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: "payments-saga", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const payment = safeParseJson<Payment>(message.value?.toString(), "inventory-service-saga");
      if (!payment) return;

      // "refunded" events also flow through payments-saga (part of the
      // same topic's history) — must not be reprocessed as a fresh
      // successful payment, or this would loop the compensation forever.
      if (payment.status !== "succeeded") {
        console.log(`[inventory-service-saga] order=${payment.orderId} skipped (payment ${payment.status})`);
        return;
      }

      if (payment.orderId.includes("out-of-stock")) {
        await producer.send({
          topic: "payment-compensation-saga",
          messages: [{ key: payment.orderId, value: JSON.stringify({ orderId: payment.orderId, reason: "inventory reservation failed: out of stock" }) }],
        });
        console.log(`[inventory-service-saga] order=${payment.orderId} -> RESERVATION FAILED, requesting compensation`);
        return;
      }

      const reservation: InventoryReservation = { orderId: payment.orderId, items: payment.items, reserved: true, reservedAt: new Date().toISOString() };
      await producer.send({ topic: "inventory-saga", messages: [{ key: reservation.orderId, value: JSON.stringify(reservation) }] });
      console.log(`[inventory-service-saga] order=${payment.orderId} -> reserved`);
    },
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
