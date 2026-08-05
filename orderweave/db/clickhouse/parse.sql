-- Phase 2 of the ClickHouse sink (see db/clickhouse/schema.sql for why this
-- is split out): parses the raw double-JSON-encoded bytes already safely
-- landed in orderweave_orders_raw/orderweave_payments_raw into typed
-- columns, as one controlled batch query — run manually, after the Kafka
-- engine backfill is confirmed complete (row counts stop climbing).

CREATE TABLE IF NOT EXISTS kafkaos.orderweave_orders
(
    order_id    String,
    customer_id String,
    total       Float64,
    status      String
)
ENGINE = MergeTree
ORDER BY (order_id);

CREATE TABLE IF NOT EXISTS kafkaos.orderweave_payments
(
    order_id String,
    amount   Float64,
    status   String
)
ENGINE = MergeTree
ORDER BY (order_id);

-- Old Stage 6 messages (same topic names, pre-outbox flat JSON shape)
-- fail this extraction gracefully to empty order_id — filtered out here
-- rather than left to pollute the analytical tables.
INSERT INTO kafkaos.orderweave_orders
SELECT
    JSONExtractString(JSONExtractString(concat('{"v":', raw, '}'), 'v'), 'order', 'orderId')    AS order_id,
    JSONExtractString(JSONExtractString(concat('{"v":', raw, '}'), 'v'), 'order', 'customerId') AS customer_id,
    JSONExtractFloat(JSONExtractString(concat('{"v":', raw, '}'), 'v'), 'order', 'total')        AS total,
    JSONExtractString(JSONExtractString(concat('{"v":', raw, '}'), 'v'), 'order', 'status')      AS status
FROM kafkaos.orderweave_orders_raw
WHERE JSONExtractString(JSONExtractString(concat('{"v":', raw, '}'), 'v'), 'order', 'orderId') != '';

INSERT INTO kafkaos.orderweave_payments
SELECT
    JSONExtractString(JSONExtractString(concat('{"v":', raw, '}'), 'v'), 'orderId') AS order_id,
    JSONExtractFloat(JSONExtractString(concat('{"v":', raw, '}'), 'v'), 'amount')   AS amount,
    JSONExtractString(JSONExtractString(concat('{"v":', raw, '}'), 'v'), 'status')  AS status
FROM kafkaos.orderweave_payments_raw
WHERE JSONExtractString(JSONExtractString(concat('{"v":', raw, '}'), 'v'), 'orderId') != '';
