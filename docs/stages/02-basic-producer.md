# Stage 2 — Basic producer

**Goal**: Write a real producer in TypeScript, send keyed JSON order events to
`orders`, and directly observe the key → partition mapping (and that it's
*consistent* for a given key, not random per send).

**What was built**: A shared `Kafka` client (`src/shared/kafka.ts`) and
`Order` type (`src/shared/types.ts`), plus `producer.ts`, which connects a
`kafkajs` producer (explicitly configured with `Partitioners.DefaultPartitioner`
to avoid kafkajs's v2.0.0 default-partitioner warning) and sends 6 distinct
orders keyed by `orderId`, then resends `order-1` and `order-2` with an
updated status to test whether the same key maps to the same partition on a
second, independent send. Results were independently verified by reading
straight off the broker with `kafka-console-consumer.sh`. See
[src/stage02-producer/](../../src/stage02-producer/).

**The real finding**: `order-1` landed on partition 1 both times and `order-2`
landed on partition 0 both times — same key produced the same partition
deterministically, confirmed independently via the console consumer, which
also showed messages arriving within each partition in exact send order.

**Full story**: [NOTES.md → Stage 2](../../NOTES.md#stage-2--basic-producer)
