# Stage 21 — Idempotent consumers & deduplication

**Goal**: Opens Part 3 (Distributed Data Patterns). Rerun Stage 5's at-least-once crash experiment exactly (same 8 seeded messages, same crash point after processing message 3, before committing) but with a real dedup layer, and prove the identical Kafka-level duplicate delivery now produces zero duplicate effect rather than a theoretical claim.

**What was built**: `schema.sql` defines `processed_events` (dedup ledger, primary key `(topic, partition, "offset")` — delivery identity, not business identity, so legitimate resends of `order-1`/`order-2` at new offsets aren't wrongly swallowed) and `orders_processed` (the side effect, with an `effect_count` column). `idempotent-consumer.ts` keeps Stage 5's exact at-least-once ordering but wraps the dedup check and side effect in a single Postgres transaction: insert into `processed_events` first, and if the unique constraint fails, roll back and skip the effect entirely. Also fixed a real environment bug where a native Mac Postgres on port 5432 was silently intercepting connections meant for the Docker container, requiring a remap to port 5433. See [src/stage21-idempotent-consumer/](../../src/stage21-idempotent-consumer/).

**The real finding**: After the rerun crash and restart, direct SQL against `orders_processed` showed `order-3` (redelivered at offset 2) with `effect_count = 1`, while `order-1` and `order-2` (legitimate resends) correctly show `effect_count = 2` each; `processed_events` contained exactly 8 rows despite offset 2 being delivered to `eachMessage` twice.

**Full story**: [NOTES.md → Stage 21](../../NOTES.md#stage-21--idempotent-consumers--deduplication)
