import * as http from "http";
import { Counter, Registry } from "prom-client";

// Generalizes Stage 22's exact metrics.ts pattern (same prom-client shape,
// same /metrics-over-plain-http approach) across all four Orderweave
// services instead of one. `service` is a label, not a separate registry
// per service, so Grafana can compare services on one panel.
export const registry = new Registry();

export const eventsProcessedTotal = new Counter({
  name: "orderweave_events_processed_total",
  help: "Events processed, labeled by service and outcome (applied/duplicate)",
  labelNames: ["service", "outcome"] as const,
  registers: [registry],
});

export const retryAttemptsTotal = new Counter({
  name: "orderweave_retry_attempts_total",
  help: "Downstream call attempts, labeled by service and outcome (success/retry/exhausted)",
  labelNames: ["service", "outcome"] as const,
  registers: [registry],
});

export const dlqMessagesTotal = new Counter({
  name: "orderweave_dlq_messages_total",
  help: "Messages routed to a DLQ after exhausting retries",
  labelNames: ["service"] as const,
  registers: [registry],
});

export const compensationsTotal = new Counter({
  name: "orderweave_compensations_total",
  help: "Compensating actions issued (e.g. refunds after a downstream failure)",
  labelNames: ["service"] as const,
  registers: [registry],
});

export function startMetricsServer(port: number): http.Server {
  const server = http.createServer(async (req, res) => {
    if (req.url === "/metrics") {
      res.setHeader("Content-Type", registry.contentType);
      res.end(await registry.metrics());
      return;
    }
    res.writeHead(404).end();
  });
  server.listen(port, () => console.log(`[metrics] listening on :${port}/metrics`));
  return server;
}
