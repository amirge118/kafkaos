# Stage 10 — Transactions / exactly-once semantics

**Goal**: Close the gap Stage 5 deliberately left open — a crash between "do the work" and "commit the offset" can duplicate the output even under at-least-once delivery. Kafka's transactional API ties the produce and the offset commit into one atomic unit, and this stage proves it holds up under a real crash, not just in theory.

**What was built**: `transactional-processor.ts`, which consumes the same deterministic 8-message `orders-crash-demo` topic from Stage 5 and, for each message, wraps the produce and the consumer offset commit in a single Kafka transaction (`transaction.send()` + `transaction.sendOffsets()` + `transaction.commit()`) using a stable `transactionalId`. Ran two experiments: a clean full run, and a run configured to crash mid-transaction right after producing for `order-3` but before committing. See [course/stage10-transactions/](../../course/stage10-transactions/).

**The real finding**: After the crash-and-restart, `read_committed` consumers saw `order-3` **exactly once** while `read_uncommitted` consumers saw **both** the aborted attempt and the committed retry (`sourceOffset=2` appearing twice) — proving transactions make the aborted write's effect invisible via isolation level rather than deleting it. A clean run also revealed committed transactions write invisible control records: the output topic's raw offset count was **16** for only 8 processed messages.

**Full story**: [NOTES.md → Stage 10](../../NOTES.md#stage-10--transactions--exactly-once-semantics)
