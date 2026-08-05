# Stage 7 — Schema Registry

**Goal**: Replace "JSON we hope matches the TypeScript interface" with an actually-enforced schema, using Confluent Schema Registry — and directly watch it accept a compatible change and reject an incompatible one, rather than just reading about compatibility rules.

**What was built**: Added a `schema-registry` service (`confluentinc/cp-schema-registry:7.6.1`, `localhost:8081`) wired into Kafka UI, plus `course/stage07-schema-registry/` containing three Avro schemas for `Order` (v1, a compatible v2 adding `discountCode`, and a deliberately breaking v3 adding `shippingAddress` with no default), a shared registry client, and Avro producer/consumer scripts. Also hardened `producer-avro.ts` with a `try/catch` around `registry.encode()` after discovering an unhandled schema-mismatch error would kill the whole batch mid-run. See [course/stage07-schema-registry/](../../course/stage07-schema-registry/).

**The real finding**: Avro-encoded messages came out at **52B versus 117B for equivalent JSON** (~55% smaller), and attempting to register the breaking v3 schema was rejected with an **HTTP 409**: `"READER_FIELD_MISSING_DEFAULT_VALUE"` naming the exact offending field (`shippingAddress`) — confirmed the subject stayed at only versions `[1, 2]` afterward.

**Full story**: [NOTES.md → Stage 7](../../NOTES.md#stage-7--schema-registry)
