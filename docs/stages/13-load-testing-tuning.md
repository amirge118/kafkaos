# Stage 13 — Load testing & producer/consumer tuning

**Goal**: Opens Part 2 (Scale) — move past "does the feature work" to "what actually happens to throughput as volume grows, and does tuning `batch.size`, `linger.ms`, `compression.type`, and `acks` make a measured difference, not a theoretical one."

**What was built**: A configurable load producer/consumer against a new 6-partition `load-test` topic, run on the project's single-broker cluster. Since kafkajs has no native `batch.size`/`linger.ms`, `load-producer.ts` implements the accumulate-then-flush batching policy by hand. Real gzip/lz4/zstd compression codecs were wired in (including a hand-fixed LZ4 frame encoder after a third-party library bug caused broker-side rejections), and batch size, acks, and linger were each benchmarked in isolation. See [course/stage13-load-testing/](../../course/stage13-load-testing/).

**The real finding**: Batch size was the single biggest lever measured — throughput went from 2,318 msgs/sec at `BATCH_SIZE=1` to 108,932 msgs/sec at `BATCH_SIZE=2000`, roughly a 47x improvement on identical hardware and payload.

**Full story**: [NOTES.md → Stage 13](../../NOTES.md#stage-13--load-testing--producerconsumer-tuning)
