import * as http from "http";
import { Counter, Registry } from "prom-client";

// A real Prometheus /metrics endpoint, scraped by the Stage 19
// infrastructure already running — this is what makes the DLQ alert in
// this stage a genuine metrics-driven alert instead of a log line someone
// has to be watching, the exact contrast the roadmap draws with Stage 8's
// Connect-level DLQ (which only logged malformed messages).
export const registry = new Registry();

export const retryAttemptsTotal = new Counter({
  name: "payment_retry_attempts_total",
  help: "Downstream call attempts, labeled by outcome",
  labelNames: ["outcome"] as const, // "success" | "retry" | "exhausted"
  registers: [registry],
});

export const dlqMessagesTotal = new Counter({
  name: "payment_dlq_messages_total",
  help: "Messages routed to the DLQ after exhausting retries",
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
