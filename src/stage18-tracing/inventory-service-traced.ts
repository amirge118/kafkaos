import { Partitioners } from "kafkajs";
import { context, SpanStatusCode } from "@opentelemetry/api";
import { kafka } from "../shared/kafka";
import { initTracing, injectTraceHeaders, extractTraceContext } from "../shared/tracing";
import { Payment, InventoryReservation } from "../shared/types";
import { safeParseJson } from "../shared/util";

const tracer = initTracing("inventory-service");

const consumer = kafka.consumer({ groupId: "inventory-service-traced" });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: "payments", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const payment = safeParseJson<Payment>(message.value?.toString(), "inventory-service-traced");
      if (!payment) return;

      const parentCtx = extractTraceContext(message.headers);

      await tracer.startActiveSpan("inventory.reserve", {}, parentCtx, async (span) => {
        span.setAttribute("order.id", payment.orderId);

        if (payment.status !== "succeeded") {
          span.setAttribute("inventory.skipped", true);
          span.setStatus({ code: SpanStatusCode.OK, message: "payment not succeeded, skipped" });
          span.end();
          console.log(`[inventory-service-traced] order=${payment.orderId} skipped (payment ${payment.status})`);
          return;
        }

        const reservation: InventoryReservation = {
          orderId: payment.orderId,
          items: payment.items,
          reserved: true,
          reservedAt: new Date().toISOString(),
        };

        const headers = injectTraceHeaders(context.active());
        await producer.send({
          topic: "inventory",
          messages: [{ key: reservation.orderId, value: JSON.stringify(reservation), headers }],
        });

        console.log(`[inventory-service-traced] order=${payment.orderId} -> reserved`);
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
