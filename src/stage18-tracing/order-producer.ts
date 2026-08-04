import { Partitioners } from "kafkajs";
import { context, SpanStatusCode } from "@opentelemetry/api";
import { kafka } from "../shared/kafka";
import { initTracing, injectTraceHeaders, shutdownTracing } from "../shared/tracing";
import { Order } from "../shared/types";

const tracer = initTracing("order-producer");
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

const orders: Order[] = [
  {
    orderId: process.env.ORDER_ID ?? "order-otel-demo",
    customerId: "cust-otel",
    items: [{ sku: "sku-100", qty: 1 }],
    total: 42.0,
    status: "created",
  },
];

async function run() {
  await producer.connect();

  for (const order of orders) {
    // The root span for this entire order's journey — every downstream
    // service's span (payment, inventory, shipping) will attach as a
    // descendant of this one, purely via what gets written into the
    // Kafka message's own headers below.
    await tracer.startActiveSpan(`order.create`, async (span) => {
      span.setAttribute("order.id", order.orderId);
      span.setAttribute("order.total", order.total);

      const headers = injectTraceHeaders(context.active());

      await producer.send({
        topic: "orders",
        messages: [{ key: order.orderId, value: JSON.stringify(order), headers }],
      });

      console.log(`[order-producer] sent ${order.orderId}, trace headers:`, headers);
      span.setStatus({ code: SpanStatusCode.OK });
      span.end();
    });
  }

  await producer.disconnect();
  // Without this, the BatchSpanProcessor's pending export never fires —
  // this script exits before its own timer, and the root span silently
  // never reaches Jaeger at all (see shutdownTracing's own comment).
  await shutdownTracing();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
