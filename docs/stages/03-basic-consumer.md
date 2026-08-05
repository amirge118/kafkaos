# Stage 3 — Basic consumer

**Goal**: Write a consumer, understand what an offset actually controls, and
see the difference between auto-commit and manual commit directly — not just
read about it.

**What was built**: A configurable `kafkajs` consumer (`consumer.ts`) driven
by env vars (`GROUP_ID`, `AUTO_COMMIT`, `RUN_MS`), subscribed with
`fromBeginning: true`. A series of experiments compared a fresh group's first
run against a repeat run, checked consumer lag via
`kafka-consumer-groups.sh --describe` before and after producing a second
batch, and compared an auto-commit group against a separate manual-commit
group (`order-processor-manual`) that committed explicitly after every
message. See
[course/stage03-04-consumer-groups/](../../course/stage03-04-consumer-groups/).

**The real finding**: After producing a second batch, `order-processor`
(auto-commit, not re-run) showed lag of 4/2/2 across the three partitions,
while `order-processor-manual`, having read all 16 messages from scratch and
committed after each one, showed `0:8/8 lag0  1:4/4 lag0  2:4/4 lag0` —
each group's progress is tracked completely independently.

**Full story**: [NOTES.md → Stage 3](../../NOTES.md#stage-3--basic-consumer)
