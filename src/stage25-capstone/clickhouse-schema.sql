-- Stage 25 (Orderweave): ClickHouse analytics sink for `orders`/`payments`.
--
-- Deliberate deviation from the plan's first draft (which named a Kafka
-- Connect sink connector, copied from Stage 15's
-- clickhouse-analytics-sink.json): these two topics are Debezium
-- EventRouter output, so their message *value* is the outbox `payload`
-- JSONB column's content — and that column is written via our own
-- JSON.stringify() into a JSONB field, so Postgres stores a JSON *string*,
-- not a JSON object (the double-JSON-encoding quirk documented in
-- outbox-message.ts). The Connect sink's default insert path expects one
-- JSON *object* per record; a raw JSON string doesn't fit that shape.
-- ClickHouse's native Kafka engine has a purpose-built answer — reuses
-- Stage 15's OTHER already-proven mechanism (Kafka engine table ->
-- Materialized View -> MergeTree).
--
-- Three real, worked-through failures getting here (same evidentiary
-- standard as Stage 15's three):
--   1. `kafka_auto_offset_reset` is rejected as an unknown per-table
--      SETTINGS key on this ClickHouse version (24.8.14) — worked around
--      by pre-seeding each consumer group's committed offset via
--      kafka-consumer-groups.sh --execute before creating these tables.
--   2. `kafka_format = 'JSONAsString'` — despite its name — requires the
--      top-level JSON value to be an object (`Code: 117: JSON object must
--      begin with '{'`); our wire bytes are a JSON *string* at the top
--      level. Fixed by switching to `kafka_format = 'RawBLOB'`, which
--      stores each message verbatim with zero validation.
--   3. Backfilling ~4.1M pre-existing messages (Stage 6 onward) through a
--      Kafka engine table with an attached materialized view that does
--      the JSON-unwrap/extract inline OOM-killed the container twice in a
--      row, even with RawBLOB (no exceptions) and ample headroom (~4.3GiB
--      free at crash time) — the live per-row JSONExtract/concat
--      transform under a multi-million-row streaming backfill is
--      genuinely heavier than a straight byte copy. Fixed by splitting
--      into two phases: ingest raw bytes only here (cheap, minimal
--      per-row work), then parse via a separate, one-time, controllable
--      batch `INSERT ... SELECT` run manually after the backfill
--      completes (see apply-clickhouse.sh) instead of live-transforming
--      every row inline during the highest-volume phase.

CREATE TABLE IF NOT EXISTS kafkaos.orderweave_orders_queue
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'orders',
    kafka_group_name = 'clickhouse-orderweave-orders',
    kafka_format = 'RawBLOB',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS kafkaos.orderweave_payments_queue
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'payments',
    kafka_group_name = 'clickhouse-orderweave-payments',
    kafka_format = 'RawBLOB',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS kafkaos.orderweave_orders_raw
(
    raw String
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS kafkaos.orderweave_payments_raw
(
    raw String
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE MATERIALIZED VIEW IF NOT EXISTS kafkaos.orderweave_orders_raw_mv
TO kafkaos.orderweave_orders_raw
AS SELECT raw FROM kafkaos.orderweave_orders_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS kafkaos.orderweave_payments_raw_mv
TO kafkaos.orderweave_payments_raw
AS SELECT raw FROM kafkaos.orderweave_payments_queue;

-- Final parsed tables + the batch parse step itself (run manually, once
-- the raw backfill above is confirmed complete and stable) live in
-- clickhouse-parse.sql — kept separate so the two phases can never be
-- accidentally re-merged into one live-streaming step again.
