import { Partitioners } from "kafkajs";
import { kafka } from "../shared/kafka";
import { InventoryReservation, Shipment } from "../shared/types";
import { safeParseJson } from "../shared/util";

// End of the happy path — unchanged in intent from Stage 6. Never sees
// orders that hit the compensation path at all, since inventory-service
// only publishes here on a successful reservation.
const consumer = kafka.consumer({ groupId: "shipping-service-saga" });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: "inventory-saga", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const reservation = safeParseJson<InventoryReservation>(message.value?.toString(), "shipping-service-saga");
      if (!reservation) return;

      const shipment: Shipment = { orderId: reservation.orderId, trackingId: `TRK-${reservation.orderId}-${Date.now()}`, shippedAt: new Date().toISOString() };
      await producer.send({ topic: "shipping-saga", messages: [{ key: shipment.orderId, value: JSON.stringify(shipment) }] });
      console.log(`[shipping-service-saga] order=${reservation.orderId} -> shipped, tracking=${shipment.trackingId}`);
    },
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
