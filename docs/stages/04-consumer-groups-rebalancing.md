# Stage 4 — Consumer groups & rebalancing

**Goal**: Run several consumer instances in the same group at once, watch
Kafka split the 3 `orders` partitions across them as membership changes, and
see what actually happens — not what's assumed to happen — when a member
dies.

**What was built**: `consumer.ts` was extended with an `INSTANCE_ID` log tag,
a configurable `SESSION_TIMEOUT_MS` (default 10000ms, lowered from Kafka's
30s default), and a `GROUP_JOIN` event listener logging partition assignment
on every (re)join. Each instance was launched as its own OS process with
output redirected to its own log file, to simulate independent service
instances. Two experiments were run: scaling up membership from 1 to 3
instances, and hard-killing (`kill -9`) a member mid-run to observe failure
detection. See
[src/stage03-04-consumer-groups/](../../src/stage03-04-consumer-groups/).

**The real finding**: After `kill -9` on instance B2 at 11:53:14 (with
`sessionTimeoutMs=10000`), the broker did not evict it or rebalance partition
2 for over four minutes — B2's stale session was only reaped at 11:58:03 as a
side effect of A2's own unrelated graceful shutdown at 11:57:53, not by the
session-timeout watchdog firing independently.

**Full story**: [NOTES.md → Stage 4](../../NOTES.md#stage-4--consumer-groups--rebalancing)
