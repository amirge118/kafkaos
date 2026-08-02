# kafkaos

A hands-on, staged Apache Kafka learning project — an e-commerce order flow
(`orders → payments → inventory → shipping`) built incrementally to explore
Kafka end to end, with every stage verified by actually running it, breaking
it, and reading the real output rather than assuming the theory holds.

Every command, every experiment, and every unplanned discovery along the way
is documented in **[`NOTES.md`](./NOTES.md)** — a full running journal of the
project, stage by stage.

## What this covers

Twelve stages, each building on the last:

| Stage | Topic |
|---|---|
| 0 | Environment setup (Docker Compose, KRaft, Kafka UI) |
| 1 | Topics & partitions (on-disk structure, replication basics) |
| 2 | Basic producer (key-based partitioning) |
| 3 | Basic consumer (offsets, auto vs. manual commit) |
| 4 | Consumer groups & rebalancing (live, with real failure timing) |
| 5 | Delivery semantics (at-most/at-least-once, `acks`, idempotent producer) |
| 6 | Multi-service event flow (chained producer/consumer services, poison pills) |
| 7 | Schema Registry (Avro, schema evolution, compatibility enforcement) |
| 8 | Kafka Connect (JDBC + file sink/source connectors, Postgres) |
| 9 | Stream processing with ksqlDB (joins, windowed aggregations) |
| 10 | Transactions / exactly-once semantics |
| 11 | Monitoring & operations (a real 3-broker cluster, leader election, ISR) |
| 12 | Failure testing (deliberate broker/consumer failures, guarantee verification) |

## Stack

- **Apache Kafka** (KRaft mode, no ZooKeeper) — single-broker cluster for
  Stages 0–10, plus an independent 3-broker cluster for Stages 11–12
- **Node.js + TypeScript** (`kafkajs`) for all producers/consumers/services
- **Confluent Schema Registry** — Avro schemas and schema evolution
- **Kafka Connect** — JDBC (Postgres) and file-based sink/source connectors
- **ksqlDB** — streaming SQL over Kafka topics
- **Kafka UI**, **Adminer** — visual inspection, no CLI required
- Everything runs via **Docker Compose**

## Running it

```bash
docker compose up -d
npm install
```

Then see `NOTES.md` for the full stage-by-stage walkthrough — each stage's
section documents exactly what to run and what to expect.

## Project layout

```
src/
  shared/                       # Kafka client, shared types, utilities
  stage02-producer/
  stage03-04-consumer-groups/
  stage05-delivery-semantics/
  stage06-event-flow/
  stage07-schema-registry/
  stage08-kafka-connect/
  stage09-ksqldb/
  stage10-transactions/
docker-compose.yml               # every service used across all 12 stages
NOTES.md                         # the full journal — start here
```
