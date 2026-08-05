# Stage 22 — Retry policies & DLQ with alerting

**Goal**: a real application-level resilience pattern for a downstream failure — retry with backoff a bounded number of times, then route to a dead-letter topic and fire a real, metrics-driven alert, not just log it. Deliberately built right after Stage 21: retrying is only safe because the consumer is already idempotent, otherwise a retry is just a slower way to reproduce Stage 5's duplicate-effect problem.

**What was built**: `flaky-downstream.ts` simulates an unreliable dependency, failing deterministically by order-ID convention (`order-transient-fail-N`, `order-permanent-fail`), not randomly. `resilient-payment-service.ts` retries up to `MAX_ATTEMPTS=3` with exponential backoff, reusing Stage 21's idempotent-write pattern verbatim for the success path, and routes exhausted orders to a DLQ topic carrying full failure context (reason, attempt count, original offset). A Prometheus + Alertmanager rule fires on DLQ events. See [course/stage22-retry-dlq/](../../course/stage22-retry-dlq/).

**The real finding**: the first alert attempt silently fired and expired between polls — not a bug, a real tuning miss: `evaluation_interval` defaulted to a full minute, barely overlapping the rule's 2-minute lookback window for a single one-off metric spike. Fixed with `evaluation_interval: 5s` and a wider `increase(...[5m])` lookback, then verified `firing` independently at both Prometheus's `/api/v1/alerts` and Alertmanager's `/api/v2/alerts`.

**Full story**: [NOTES.md → Stage 22](../../NOTES.md#stage-22--retry-policies--dlq-with-alerting)
