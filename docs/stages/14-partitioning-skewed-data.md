# Stage 14 — Partitioning strategy under skewed data

**Goal**: Prove the counter-intuitive claim that adding more partitions does not fix a single hot key, then fix it for real with key salting, and measure both on the project's own cluster.

**What was built**: A producer generating deliberately skewed traffic (one hot key gets 80% of messages, the rest spread across 5000 normal keys), a distribution checker reading real per-partition counts from the broker via the admin client, and a `simulate-partitions.ts` tool that uses kafkajs's own internal `murmur2` hash to cheaply sweep partition-count/salt-bucket combinations without producing real traffic. See [src/stage14-partitioning/](../../src/stage14-partitioning/).

**The real finding**: Quadrupling partitions from 6 to 24 barely moved the hot key's worst-case share (83.2% to 80.7%), but key salting with 64 buckets against 6 partitions brought it down to 19.7% — and the simulator's predictions matched real-broker measurements to within a few tenths of a percent (e.g. simulated 19.6% vs measured 19.7%).

**Full story**: [NOTES.md → Stage 14](../../NOTES.md#stage-14--partitioning-strategy-under-skewed-data)
