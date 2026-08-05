# Stage 24 — Saga Pattern

**Goal**: Stage 6's `orders → payments → inventory → shipping` chain has always been a choreography-style saga, but was missing the half that makes it a *real* saga: compensation. `inventory-service` used to just skip a failed payment; it never reversed one that had already succeeded when the next step failed. Built that missing compensating path, then built the same saga a second way — orchestration — to make the trade-off concrete instead of just described.

**What was built**: a new `order-out-of-stock-*` scenario (payment succeeds, inventory reservation fails afterward). Choreography: `inventory-service-saga.ts` publishes a compensation request on reservation failure; `payment-service-saga.ts` reacts independently and issues a refund, with an explicit `status !== "succeeded"` guard preventing it from reprocessing its own refund event as a new payment. Orchestration: `order-saga-orchestrator.ts` implements the identical logic as one linear `runSaga()` function, publishing an audit event at every step. See [course/stage24-saga/](../../course/stage24-saga/).

**The real finding**: both implementations produced byte-for-byte identical outcomes for all three scenarios — the real difference was operational, not correctness. Reconstructing "what happened to this order" from the choreography version required correlating three separate topics' independent logs; the orchestration version's single `saga-orchestrator-audit` topic, filtered to one `orderId`, gave the complete ordered story from one `kafka-console-consumer` read.

**Full story**: [NOTES.md → Stage 24](../../NOTES.md#stage-24--saga-pattern)
