import { Partitioners } from "kafkajs";
import { context, SpanStatusCode } from "@opentelemetry/api";
import { kafka } from "../shared/kafka";
import { initTracing, injectTraceHeaders, extractTraceContext } from "../shared/tracing";
import { Order, Payment } from "../shared/types";
import { safeParseJson } from "../shared/util";

const tracer = initTracing("payment-service");

// A distinct consumer group from Stage 6's plain `payment-service` — this
// one reprocesses `orders` from the beginning independently, including
// years of pre-Stage-18 backlog with no trace headers at all (handled
// gracefully by extractTraceContext's fallback to a fresh root context).
const consumer = kafka.consumer({ groupId: "payment-service-traced" });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

function simulatePayment(order: Order): Payment {
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
      const order = safeParseJson<Order>(message.value?.toString(), "payment-service-traced");
      if (!order) return;

      // Continue the trace the producer started, instead of beginning a
      // new, disconnected one — this is the actual mechanism this stage
      // is about: reading `traceparent` back out of the message headers.
      const parentCtx = extractTraceContext(message.headers);

      await tracer.startActiveSpan("payment.process", {}, parentCtx, async (span) => {
        span.setAttribute("order.id", order.orderId);

        const payment = simulatePayment(order);
        span.setAttribute("payment.status", payment.status);

        const headers = injectTraceHeaders(context.active());
        await producer.send({
          topic: "payments",
          messages: [{ key: payment.orderId, value: JSON.stringify(payment), headers }],
        });

        console.log(`[payment-service-traced] order=${order.orderId} -> payment ${payment.status}`);
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
