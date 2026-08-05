# Orderweave

The capstone of [kafkaos](../README.md) — an `orders → payment →
inventory → shipping` pipeline that weaves together idempotent
consumption, the transactional outbox, retry-with-DLQ, and saga
compensation into one production-shaped system, proven at 2,000,000
messages.

For the full write-up — architecture diagram, the four guarantees and
how to verify each one, the incident walkthrough, the load test numbers,
and every real failure hit along the way — see
**[`../ARCHITECTURE.md`](../ARCHITECTURE.md)**. This file is just the
quick start.

## Layout

```
services/       # order-service, payment-service, inventory-service, shipping-service
shared/         # orderweave-local helpers (outbox parsing, metrics, flaky-downstream)
db/             # Postgres schema + ClickHouse schema/parse SQL
connect/        # Debezium outbox connector config + apply script
monitoring/     # Prometheus alert rule + Grafana dashboard
scripts/        # load-test producer, ClickHouse apply script
```

## Quick start

Requires `docker compose up -d` already running from the repo root.

```bash
npm install                                                       # from repo root
docker exec -i postgres psql -U kafkaos -d kafkaos < db/postgres/schema.sql
./connect/apply-connectors.sh

# four terminals, one service each, from the repo root:
npm run orderweave:order
npm run orderweave:payment
npm run orderweave:inventory
npm run orderweave:shipping
```

Grafana (`localhost:3000`) → **Orderweave Overview** dashboard.

## ClickHouse sink

```bash
./scripts/apply-clickhouse.sh
```

Two phases (raw ingest, then a controlled batch parse) — pauses between
them on purpose. See `../ARCHITECTURE.md` for why.

## Load test

```bash
COUNT=2000000 BATCH_SIZE=2000 npm run orderweave:load-test   # from repo root
npm run orderweave:payment-batched                           # instead of orderweave:payment
```
