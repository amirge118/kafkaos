# Stage 0 — Environment setup

**Goal**: Get a local Kafka cluster running that we can see into, and a Node/TS
project wired up to talk to it — no Kafka-specific code yet, just the foundation.

**What was built**: A `docker-compose.yml` with two containers — `kafka`
(`apache/kafka:3.8.0`) running in KRaft mode (single node as both broker and
controller, no ZooKeeper), with three listeners (`PLAINTEXT`, `CONTROLLER`,
`PLAINTEXT_HOST`) and replication factor 1 everywhere since there's only one
broker; and `kafka-ui` for browsing topics/partitions/consumer groups at
`localhost:8080`. Alongside it, a minimal Node.js + TypeScript scaffold
(`package.json`/`tsconfig.json`) with `kafkajs` installed and a placeholder
`src/index.ts` to prove the build pipeline works. See
[docker-compose.yml](../../docker-compose.yml).

**The real finding**: `docker compose ps` showed `kafka` as `healthy` and
`kafka-ui` as `Up`, and `curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080`
returned `200` — Kafka UI was reachable, showing the `kafkaos-local` cluster
with zero topics since nothing had produced to it yet.

**Full story**: [NOTES.md → Stage 0](../../NOTES.md#stage-0--environment-setup)
