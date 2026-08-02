import { Partitioners } from "kafkajs";
import { kafka } from "../shared/kafka";
import { Payment, InventoryReservation } from "../shared/types";
import { safeParseJson } from "../shared/util";

// payments -> inventory
// Only reacts to *successful* payments — a failed payment never gets
// inventory reserved for it, which is exactly the point of chaining events
// instead of processing everything unconditionally.
//
// SLOW_MS adds an artificial delay per message, to make a deliberate
// bottleneck: watch what happens to `payments` lag (this service falls
// behind) versus `orders`/`payment-service` (completely unaffected, since
// Kafka decouples each stage's pace via the topic acting as a buffer).
const slowMs = Number(process.env.SLOW_MS ?? 0);

const consumer = kafka.consumer({ groupId: "inventory-service" });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: "payments", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const payment = safeParseJson<Payment>(message.value?.toString(), "inventory-service");
      if (!payment) return;

      if (payment.status !== "succeeded") {
        console.log(`[inventory-service] order=${payment.orderId} skipped (payment ${payment.status})`);
        return;
      }

      if (slowMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, slowMs));
      }

      const reservation: InventoryReservation = {
        orderId: payment.orderId,
        items: payment.items,
        reserved: true,
        reservedAt: new Date().toISOString(),
      };

      await producer.send({
        topic: "inventory",
        messages: [{ key: reservation.orderId, value: JSON.stringify(reservation) }],
      });

      console.log(`[inventory-service] order=${payment.orderId} -> reserved`);
    },
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
