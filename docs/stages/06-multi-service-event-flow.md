# Stage 6 — Multi-service event flow

**Goal**: Build an actual chain of independent services that react to each other's events — `orders` → `payments` → `inventory` → `shipping` — and watch what event-driven decoupling really buys you: filtering, backpressure isolation, and (unplanned, but very real) resilience to bad data.

**What was built**: Three new topics (`payments`, `inventory`, `shipping`) and three chained services — `payment-service`, `inventory-service`, and `shipping-service` — each consuming the previous stage's topic and producing to the next. Payments carry `items` forward so `inventory-service` doesn't need to look up the original order (event-carried state transfer), and `inventory-service` filters out failed payments by only reacting to `status === "succeeded"`. Services live in [src/stage06-event-flow/](../../src/stage06-event-flow/).

**The real finding**: A stray `"heyyy"` string (a non-JSON message sent manually via Kafka UI) crashed `payment-service` outright — `KafkaJSNonRetriableError: Unexpected token 'h', "heyyy" is not valid JSON` — a real poison-pill failure, fixed with a `safeParseJson()` helper. The final funnel across the chain narrowed from `orders: 17` to `payments: 16` to `inventory: 15` to `shipping: 15`, visibly showing filtering and the poison pill's effect at each stage.

**Full story**: [NOTES.md → Stage 6](../../NOTES.md#stage-6--multi-service-event-flow)
