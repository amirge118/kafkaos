import { Partitioners } from "kafkajs";
import { context, SpanStatusCode } from "@opentelemetry/api";
import { kafka } from "../shared/kafka";
import { initTracing, injectTraceHeaders, extractTraceContext } from "../shared/tracing";
import { InventoryReservation, Shipment } from "../shared/types";
import { safeParseJson } from "../shared/util";

const tracer = initTracing("shipping-service");

const consumer = kafka.consumer({ groupId: "shipping-service-traced" });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: "inventory", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const reservation = safeParseJson<InventoryReservation>(message.value?.toString(), "shipping-service-traced");
      if (!reservation) return;

      const parentCtx = extractTraceContext(message.headers);

      await tracer.startActiveSpan("shipping.create", {}, parentCtx, async (span) => {
        span.setAttribute("order.id", reservation.orderId);

        const shipment: Shipment = {
          orderId: reservation.orderId,
          trackingId: `TRK-${reservation.orderId}-${Date.now()}`,
          shippedAt: new Date().toISOString(),
        };
        span.setAttribute("shipment.trackingId", shipment.trackingId);

        // End of the chain — no outgoing message, so no further header
        // injection needed; this span is the last leaf in the trace.
        const headers = injectTraceHeaders(context.active());
        await producer.send({
          topic: "shipping",
          messages: [{ key: shipment.orderId, value: JSON.stringify(shipment), headers }],
        });

        console.log(`[shipping-service-traced] order=${reservation.orderId} -> shipped, tracking=${shipment.trackingId}`);
        span.setStatus({ code: SpanStatusCode.OK });
        span.end();
      });
    },
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
