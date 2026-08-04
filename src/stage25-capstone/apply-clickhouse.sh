#!/usr/bin/env bash
# Applies Orderweave's ClickHouse sink in its two deliberate phases (see
# clickhouse-schema.sql for why they're split): first the cheap raw-byte
# Kafka-engine backfill, then a separate, controllable batch parse. Mirrors
# Stage 15's schema.sql convention (declarative SQL, applied via
# clickhouse-client), extended into two explicit steps for this pipeline's
# higher message volume.
set -euo pipefail
cd "$(dirname "$0")"

echo "Phase 1: raw ingestion (Kafka engine -> MergeTree, no parsing)..."
docker exec -i clickhouse clickhouse-client --password kafkaos --multiquery < clickhouse-schema.sql

echo ""
echo "Waiting for the raw backfill to settle (row counts stop climbing) before parsing."
echo "Check manually: docker exec clickhouse clickhouse-client --password kafkaos --query \"SELECT count() FROM kafkaos.orderweave_orders_raw\""
echo ""
read -p "Press enter once orderweave_orders_raw / orderweave_payments_raw counts have stabilized... "

echo "Phase 2: batch parse (one-time INSERT ... SELECT, not a live transform)..."
docker exec -i clickhouse clickhouse-client --password kafkaos --multiquery < clickhouse-parse.sql

echo "Done. Row counts:"
docker exec clickhouse clickhouse-client --password kafkaos --query "SELECT 'orderweave_orders', count() FROM kafkaos.orderweave_orders UNION ALL SELECT 'orderweave_payments', count() FROM kafkaos.orderweave_payments"
