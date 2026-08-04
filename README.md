# kafkaos

A hands-on, 25-stage Apache Kafka learning project, built around one
running example — an e-commerce order flow
(`orders → payments → inventory → shipping`) — taken from "what is a
partition" all the way to a production-shaped distributed system proven
at 2,000,000 messages. Every stage is verified by actually running it,
breaking it, and reading the real output, not by assuming the theory
holds.

The capstone, **[Orderweave](./ARCHITECTURE.md)**, assembles four of the
patterns learned along the way — idempotent consumption, the
transactional outbox, retry-with-DLQ, and saga compensation — into one
coherent system, wires real tracing/monitoring around it, and proves it
end-to-end at scale: a real mid-burst crash recovered with zero
duplicate effects, 2,000,000 orders processed with exact 1:1 correctness
across three independently-checked systems, and a live ClickHouse
analytics sink. **[Read the full write-up →](./ARCHITECTURE.md)**

Want the whole journey instead of just the destination?
**[Browse it stage by stage →](./docs/stages/README.md)** — a short,
skimmable page per stage. The complete, unabridged chronological journal
— every command, every false start, every real number — lives in
**[`NOTES.md`](./NOTES.md)**.

## What this covers

26 stages across four parts, each building on the last:

| Stage | Topic |
|---|---|
| 0 | Environment setup (Docker Compose, KRaft, Kafka UI) |
| 1 | Topics & partitions (on-disk structure, replication basics) |
| 2 | Basic producer (key-based partitioning) |
| 3 | Basic consumer (offsets, auto vs. manual commit) |
| 4 | Consumer groups & rebalancing (live, with real failure timing) |
| 5 | Delivery semantics (at-most/at-least/exactly-once, `acks`, idempotent producer) |
| 6 | Multi-service event flow (chained producer/consumer services, poison pills) |
| 7 | Schema Registry (Avro, schema evolution, compatibility enforcement) |
| 8 | Kafka Connect (JDBC + file sink/source connectors, Postgres) |
| 9 | Stream processing with ksqlDB (joins, windowed aggregations) |
| 10 | Transactions / exactly-once semantics |
| 11 | Monitoring & operations (a real 3-broker cluster, leader election, ISR) |
| 12 | Failure testing (deliberate broker/consumer failures, guarantee verification) |
| 13 | Load testing & producer/consumer tuning (real throughput, `batch.size`, `compression.type`) |
| 14 | Partitioning strategy under skewed data (hot keys, key salting) |
| 15 | Kafka → ClickHouse (a real analytics pipeline for millions of events) |
| 16 | Scaling stateful processing (ksqlDB state redistribution under load) |
| 17 | Redpanda (same code, different broker — API vs. implementation) |
| 18 | Distributed tracing with OpenTelemetry |
| 19 | Prometheus + Grafana monitoring (fleet-level lag/throughput/ISR dashboards) |
| 20 | Large payloads & the claim-check pattern |
| 21 | Idempotent consumers & deduplication |
| 22 | Retry policies & DLQ with alerting |
| 23 | Transactional Outbox Pattern |
| 24 | Saga Pattern (choreography vs. orchestration) |
| 25 | **The Capstone — Orderweave** (see [`ARCHITECTURE.md`](./ARCHITECTURE.md)) |

## Stack

- **Apache Kafka** (KRaft mode, no ZooKeeper) — single-broker cluster for
  most stages, plus an independent 3-broker cluster for Stages 11–12
- **Node.js + TypeScript** (`kafkajs`) for all producers/consumers/services
- **Confluent Schema Registry** — Avro schemas and schema evolution
- **Kafka Connect** — JDBC (Postgres), file-based sink/source connectors,
  and Debezium's Postgres CDC connector for the transactional outbox
  pattern (Stages 23 and 25)
- **ksqlDB** — streaming SQL over Kafka topics
- **ClickHouse** — OLAP analytics store, wired to Kafka via the Kafka
  table engine + materialized view pattern (Stages 15 and 25)
- **Redpanda** — a wire-protocol-compatible alternative broker, run
  temporarily for Stage 17's comparison (`docker compose up -d redpanda`)
- **OpenTelemetry + Jaeger** — distributed tracing, propagated through
  Kafka message headers (Stage 18) or embedded in outbox payloads when
  headers aren't available (Stage 25)
- **Prometheus + Grafana** (via `kafka-exporter`) — fleet-level lag,
  throughput, and ISR-health dashboards, provisioned as code (Stage 19)
- **MinIO** — S3-compatible object storage for the claim-check pattern
  (Stage 20)
- **Alertmanager** — real, verifiable alerts for DLQ and consumer-lag
  events, driven by Stage 19's Prometheus (Stages 22 and 25)
- **Kafka UI**, **Adminer** — visual inspection, no CLI required
- Everything runs via **Docker Compose**

## Running it

```bash
docker compose up -d
npm install
```

Then either dive straight into the capstone
(**[`ARCHITECTURE.md`](./ARCHITECTURE.md)** has its own one-command
walkthrough), or work through the stages in order — see
**[`docs/stages/`](./docs/stages/README.md)** for a short page per stage,
or **[`NOTES.md`](./NOTES.md)** for the complete journal.

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
  stage13-load-testing/
  stage14-partitioning/
  stage15-clickhouse/
  stage16-scaling-ksqldb/
  stage18-tracing/
  stage19-monitoring/
  stage20-claim-check/
  stage21-idempotent-consumer/
  stage22-retry-dlq/
  stage23-outbox/
  stage24-saga/
  stage25-capstone/             # Orderweave — see ARCHITECTURE.md
docs/
  stages/                       # short, skimmable per-stage pages
docker-compose.yml               # every service used across every stage
ARCHITECTURE.md                  # Orderweave: the capstone, self-contained
NOTES.md                         # the full journal — every stage, in order
```

(Stages 0, 1, 11, 12, and 17 don't have their own `src/` directory —
they're pure infrastructure/CLI exercises, or reuse Stage 6's services
against a different broker/cluster shape. See their pages in
[`docs/stages/`](./docs/stages/README.md) for exactly what was run.)
