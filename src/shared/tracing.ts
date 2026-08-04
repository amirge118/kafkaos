import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { ATTR_SERVICE_NAME } from "@opentelemetry/semantic-conventions";
import { AsyncHooksContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { context, propagation, trace, type Context } from "@opentelemetry/api";
import type { IHeaders } from "kafkajs";

// Kafka has no built-in equivalent of an HTTP request header carrying trace
// context automatically — a message consumed by inventory-service has no
// inherent link back to the order-producer or payment-service that touched
// it earlier. This is that link, built by hand: the W3C `traceparent`
// format gets written into the Kafka message's own headers by the
// producer, and read back out by the next consumer, exactly the way an
// HTTP client/server pair would use a `traceparent` request header —
// except here there's no framework doing it for us.
let activeProvider: NodeTracerProvider | undefined;

export function initTracing(serviceName: string) {
  const provider = new NodeTracerProvider({
    resource: resourceFromAttributes({ [ATTR_SERVICE_NAME]: serviceName }),
    spanProcessors: [
      new BatchSpanProcessor(
        new OTLPTraceExporter({ url: process.env.OTLP_URL ?? "http://localhost:4318/v1/traces" })
      ),
    ],
  });
  activeProvider = provider;

  const contextManager = new AsyncHooksContextManager();
  contextManager.enable();
  context.setGlobalContextManager(contextManager);

  provider.register();
  propagation.setGlobalPropagator(new W3CTraceContextPropagator());

  return trace.getTracer(serviceName);
}

// BatchSpanProcessor buffers finished spans and exports them on a timer
// (or once the buffer fills) — fine for a long-running consumer service,
// which stays alive long enough for that timer to fire naturally. A
// short-lived script (order-producer, here) can exit before the first
// export ever happens, silently dropping every span it created — this is
// what that first run actually did (see NOTES.md Stage 18: 3 spans in
// Jaeger instead of 4, root span missing). Call this before exiting.
export async function shutdownTracing(): Promise<void> {
  await activeProvider?.shutdown();
}

// Turns the active (or given) trace context into a plain string carrier —
// propagation.inject() writes `traceparent` (and `tracestate`, if any) into
// it — which then gets attached as literal Kafka message headers alongside
// the business payload.
export function injectTraceHeaders(ctx: Context = context.active()): Record<string, string> {
  const carrier: Record<string, string> = {};
  propagation.inject(ctx, carrier);
  return carrier;
}

// The inverse, run by the next consumer in the chain: reconstructs an OTel
// Context from whatever `traceparent` header arrived on the message, so a
// span created here attaches as a child of the producer's span instead of
// starting a brand new, disconnected trace. Falls back to a fresh root
// context for messages with no trace headers at all (e.g. this project's
// pre-Stage-18 backlog).
export function extractTraceContext(headers: IHeaders | undefined): Context {
  if (!headers) return context.active();
  const carrier: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    if (value !== undefined) carrier[key] = value.toString();
  }
  return propagation.extract(context.active(), carrier);
}

// Stage 25 (Orderweave): the outbox pattern breaks header-based propagation
// above. Once a service writes to Postgres and Debezium — not our own
// process — is what physically produces the resulting Kafka message, our
// code never gets a chance to set that message's *headers* at all. The fix
// is the same carrier object, just embedded as a field inside the outbox
// row's own JSON payload instead of a Kafka header, so it survives the
// DB-commit-then-CDC-republish hop and comes out the other side unchanged.
export function injectTraceContextIntoPayload(ctx: Context = context.active()): Record<string, string> {
  return injectTraceHeaders(ctx);
}

export function extractTraceContextFromPayload(traceContext: Record<string, string> | undefined): Context {
  if (!traceContext) return context.active();
  return propagation.extract(context.active(), traceContext);
}
