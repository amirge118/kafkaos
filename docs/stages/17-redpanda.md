# Stage 17 — Redpanda

**Goal**: Compare the project's real Kafka broker against Redpanda — a completely different implementation (C++, thread-per-core, single binary, no JVM) of the same wire protocol — by rerunning Stage 13's exact load tests unmodified, and check directly whether Kafka 4.0's KRaft mode already closes the operational-simplicity gap Redpanda was built to exploit.

**What was built**: Added a `BROKERS` env var to `shared/kafka.ts`, replacing the hardcoded broker address, so every producer/consumer/script from all 16 prior stages could point at Redpanda (`localhost:9195`) with zero application-logic changes. Reran Stage 13's `load-producer.ts`/`load-consumer.ts` byte-for-byte against Redpanda across the same batch-size and `acks` configurations used in Stage 13, and compared both throughput numbers and operational characteristics (memory, startup time, process model). No dedicated `course/` directory for this stage — the change lives in `shared/kafka.ts` and a Redpanda service added to the root compose file. See [docker-compose.yml](../../docker-compose.yml).

**The real finding**: Redpanda used **345.7 MiB** of memory vs Kafka's **1.065 GiB** (~3x less) and started in **~0.65s** vs Kafka's **~15.4s** (~23x faster) — but raw throughput was in the same order of magnitude for both (e.g. `BATCH_SIZE=2000`: 108,932 vs 110,375 msgs/sec). KRaft closed the ZooKeeper-coordination gap but not the JVM-vs-native resource/startup gap — those are two different gaps.

**Full story**: [NOTES.md → Stage 17](../../NOTES.md#stage-17--redpanda)
