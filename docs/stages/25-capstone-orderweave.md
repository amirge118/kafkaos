# Stage 25 — The Capstone: Orderweave

**Goal**: assemble four independently-proven patterns — idempotent consumption (21), the transactional outbox (23), retry/DLQ (22), and saga compensation (24) — into one coherent, named system wired into the real `orders → payments → inventory → shipping` pipeline, with Stage 18/19's tracing and monitoring wrapped around it for real, then prove it at scale: millions of messages, a real mid-burst crash, and a live analytics sink.

**What was built**: four production-shaped services (`order-service`, `payment-service`, `inventory-service`, `shipping-service`) combining all four patterns, a Grafana dashboard and Prometheus alert, a real `kill -9` incident demo, a batched-commit load-test variant proving 2,000,000 messages end-to-end, and a ClickHouse analytics sink. This is the project's main deliverable — see [`../../ARCHITECTURE.md`](../../ARCHITECTURE.md) for the full, self-contained write-up, and [orderweave/](../../orderweave/) for the code.

**The real finding**: strict one-Postgres-transaction-per-message throughput measured at ~4-5 messages/sec — over four days to reach 2,000,000 messages at that rate. Switching to batched commits (the same "batching is the dominant throughput lever" finding from Stage 13, applied to the database side) processed all 2,000,000 messages in 132 Postgres transactions, fully drained in 181 seconds, with exact 1:1 correctness verified independently across Postgres, Kafka, and ClickHouse.

**Full story**: [NOTES.md → Stage 25](../../NOTES.md#stage-25--the-capstone-orderweave) · [ARCHITECTURE.md](../../ARCHITECTURE.md)
