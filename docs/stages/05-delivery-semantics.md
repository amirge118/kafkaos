# Stage 5 — Delivery semantics

**Goal**: Turn "at-most-once / at-least-once / exactly-once" from vocabulary
into something actually watched happen — a real lost message, a real
duplicate, a real `acks` latency difference, and the real boundary of what an
idempotent producer protects against.

**What was built**: `producer-acks.ts` (sends N messages at a configurable
`acks` level, timing the batch), `producer-idempotence.ts` (an
`idempotent: true` producer sending the same logical order via two
independent `send()` calls), and `consumer-semantics.ts` (a consumer that
either commits-then-works or works-then-commits depending on `MODE`, and can
be told to crash mid-run via `CRASH_AFTER`, with every unit of work appended
to a side-effect log file). New topics `orders-semantics` and
`orders-crash-demo` were seeded for these experiments. See
[src/stage05-delivery-semantics/](../../src/stage05-delivery-semantics/).

**The real finding**: In the at-most-once crash test (commit before work,
crash right after committing offset 3 for `order-3`), the final side-effect
log had 7 entries for 8 messages with offset 2 permanently missing — the
work for `order-3` never ran and Kafka had no way to know. In the
at-least-once test (work before commit, same crash point), the final log had
9 entries for 8 messages, with `order-3` processed twice by two different
process IDs.

**Full story**: [NOTES.md → Stage 5](../../NOTES.md#stage-5--delivery-semantics)
