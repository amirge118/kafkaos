# Stage 9 — Stream processing with ksqlDB

**Goal**: Do real stateful stream processing — a join across two topics, a windowed aggregation, instant point lookups — using declarative SQL instead of hand-writing consumer logic with local state in TypeScript.

**What was built**: Added `ksqldb-server` (`localhost:8088`) and a persistent `ksqldb-cli` container, wired into Kafka UI, plus `course/stage09-ksqldb/statements.sql` recording every `CREATE STREAM`/`CREATE TABLE` used. Built streams over all four existing topics (`orders`, `payments`, `inventory`, `shipping`, including nested `items` as `ARRAY<STRUCT<...>>`), a windowed stream-stream join between `orders` and `payments`, a tumbling-window aggregation of payments per minute, and a `LATEST_BY_OFFSET` table demonstrating pull vs. push queries. See [course/stage09-ksqldb/](../../course/stage09-ksqldb/).

**The real finding**: A `WITHIN (1 HOURS, 1 HOURS)` join on `orders`/`payments` only matched 2 of the historical orders because `order-1`'s payment was processed **68.9 hours** after the order was created (`payment-service` didn't exist until Stage 6) — proven by widening the window to `WITHIN (5 DAYS, 5 DAYS)`, which immediately surfaced the missing matches. A separate tumbling window also caught **15 succeeded payments totaling ~$909.87** in a single minute-bucket, the moment `payment-service` first blew through its 3-day backlog.

**Full story**: [NOTES.md → Stage 9](../../NOTES.md#stage-9--stream-processing-with-ksqldb)
