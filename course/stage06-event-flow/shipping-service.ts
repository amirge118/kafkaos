import { Partitioners } from "kafkajs";
import { kafka } from "../../shared/kafka";
import { InventoryReservation, Shipment } from "../../shared/types";
import { safeParseJson } from "../../shared/util";

// inventory -> shipping
// End of the chain: once inventory is reserved, create a shipment.
const consumer = kafka.consumer({ groupId: "shipping-service" });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: "inventory", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const reservation = safeParseJson<InventoryReservation>(message.value?.toString(), "shipping-service");
      if (!reservation) return;

      const shipment: Shipment = {
        orderId: reservation.orderId,
        trackingId: `TRK-${reservation.orderId}-${Date.now()}`,
        shippedAt: new Date().toISOString(),
      };

      await producer.send({
        topic: "shipping",
        messages: [{ key: shipment.orderId, value: JSON.stringify(shipment) }],
      });

      console.log(`[shipping-service] order=${reservation.orderId} -> shipped, tracking=${shipment.trackingId}`);
    },
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
