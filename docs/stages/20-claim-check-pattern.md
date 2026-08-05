# Stage 20 — Large payloads & the claim-check pattern (bonus)

**Goal**: Prove, not assert, why huge messages hurt — both throughput degradation as messages grow and a hard wall at some point — then implement the standard fix: store the payload somewhere built for large blobs and let Kafka carry only a small reference to it.

**What was built**: Escalated payload size through Stage 13's load producer to prove degradation and find the hard limit, then added `minio` (a real S3-compatible object store) to `docker-compose.yml`. `blob-store.ts` wraps the real AWS SDK pointed at MinIO. `claim-check-producer.ts` uploads the large payload, computes a SHA-256 checksum, and sends Kafka only a small JSON reference; `claim-check-consumer.ts` reads the reference, fetches the blob only when needed, and verifies the checksum. A runtime type guard (`isAttachmentReference()`) was added after discovering a wrong-shaped message could stall the consumer indefinitely. See [course/stage20-claim-check/](../../course/stage20-claim-check/).

**The real finding**: At `SIZE=2,000,000` (2MB) the broker rejected the produce request outright with `MESSAGE_TOO_LARGE` (confirmed `message.max.bytes=1048588` on the actual broker). The claim-check producer then sent 5 attachments of 10,000,000B each — 10x that rejected size — with **no error**, because Kafka never saw more than a ~282-byte reference message.

**Full story**: [NOTES.md → Stage 20](../../NOTES.md#stage-20--large-payloads--the-claim-check-pattern-bonus)
