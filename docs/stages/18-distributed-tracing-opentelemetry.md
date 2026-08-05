# Stage 18 — Distributed tracing with OpenTelemetry

**Goal**: Extend Stage 6's `orders → payments → inventory → shipping` pipeline (four independent services connected only by Kafka topics) with real distributed tracing, so one order's journey shows up as a single connected trace instead of four disconnected sets of logs — propagated by hand through Kafka message headers, since Kafka has no built-in equivalent of an HTTP request header for this.

**What was built**: Added Jaeger (`jaegertracing/all-in-one`) to `docker-compose.yml`, chosen deliberately for its small footprint given this project's recurring memory ceiling. `shared/tracing.ts` provides `injectTraceHeaders()` (turns the current span into a W3C `traceparent` Kafka message header) and `extractTraceContext()` (reconstructs the parent context from an incoming message so a new span attaches as a child). Built a parallel traced version of Stage 6's four services in `course/stage18-tracing/`, each extracting the incoming context, starting a child span, and injecting a new context into whatever it produces downstream. See [course/stage18-tracing/](../../course/stage18-tracing/).

**The real finding**: The first traced run produced only 3 spans instead of 4 — the root `order.create` span silently vanished because `BatchSpanProcessor` buffers spans on a timer, and the one-shot producer script exited before the timer fired. Fixed with an explicit `shutdownTracing()` flush before exit, after which Jaeger's own API confirmed the correct 4-span chain: `order.create → payment.process → inventory.reserve → shipping.create`. Jaeger's measured footprint: **9.86 MiB**.

**Full story**: [NOTES.md → Stage 18](../../NOTES.md#stage-18--distributed-tracing-with-opentelemetry)
