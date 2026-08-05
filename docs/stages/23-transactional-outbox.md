# Stage 23 — Transactional Outbox Pattern

**Goal**: solve the dual-write problem — a service that needs to update its own database *and* reliably publish a Kafka event as one atomic unit, when Postgres and Kafka share no transaction. Proved the naive approach loses events for real, then proved the outbox pattern (with Debezium doing the actual publishing via CDC) survives the identical crash.

**What was built**: `naive-order-service.ts` commits to Postgres, then separately calls `producer.send()` — crashed deliberately right after the DB commit. `outbox-order-service.ts` fixes it: the business row and an `outbox` row (columns matching Debezium's `EventRouter` SMT defaults) are written in one Postgres transaction, and Debezium — reading the write-ahead log via logical replication, entirely independent of the application process — is what actually publishes to Kafka. See [course/stage23-outbox/](../../course/stage23-outbox/).

**The real finding**: crashing both versions at the identical point (right after the second order's DB commit) produced different real outcomes — the naive version's `order-naive-2` exists in Postgres forever with its Kafka event permanently gone, while the outbox version's `order-outbox-2` arrived in Kafka anyway, because Debezium had already seen the committed WAL transaction before the crash and doesn't depend on the application process staying alive.

**Full story**: [NOTES.md → Stage 23](../../NOTES.md#stage-23--transactional-outbox-pattern)
