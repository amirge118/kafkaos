-- Stage 9 — ksqlDB statements, in the order they should be applied.
-- Apply with:
--   docker exec -i ksqldb-cli ksql http://ksqldb-server:8088 < statements.sql
-- (run from src/stage09-ksqldb/, or adjust the path)

-- ============================================================
-- 1. Streams over the existing event topics from Stages 2 and 6.
--    Plain JSON, matching how those topics were actually produced —
--    no dependency on the Stage 7 schema registry here.
-- ============================================================

CREATE STREAM orders_stream (
  orderId VARCHAR KEY,
  customerId VARCHAR,
  items ARRAY<STRUCT<sku VARCHAR, qty INT>>,
  total DOUBLE,
  status VARCHAR
) WITH (
  KAFKA_TOPIC='orders',
  VALUE_FORMAT='JSON'
);

CREATE STREAM payments_stream (
  orderId VARCHAR KEY,
  items ARRAY<STRUCT<sku VARCHAR, qty INT>>,
  amount DOUBLE,
  status VARCHAR,
  processedAt VARCHAR
) WITH (
  KAFKA_TOPIC='payments',
  VALUE_FORMAT='JSON'
);

CREATE STREAM inventory_stream (
  orderId VARCHAR KEY,
  items ARRAY<STRUCT<sku VARCHAR, qty INT>>,
  reserved BOOLEAN,
  reservedAt VARCHAR
) WITH (
  KAFKA_TOPIC='inventory',
  VALUE_FORMAT='JSON'
);

CREATE STREAM shipping_stream (
  orderId VARCHAR KEY,
  trackingId VARCHAR,
  shippedAt VARCHAR
) WITH (
  KAFKA_TOPIC='shipping',
  VALUE_FORMAT='JSON'
);

-- ============================================================
-- 2. Stream-stream join: orders + payments, correlated by orderId.
--
--    IMPORTANT: WITHIN is an event-time window, not "ever both
--    happened." Our orders (Stage 2) and their payments (only
--    produced once payment-service actually ran, in Stage 6, up to
--    ~69 hours later) are often much further apart than 1 hour — see
--    NOTES.md for the full explanation. A 1-hour window is realistic
--    for a live, continuously-running system; it deliberately misses
--    our own historical backlog, which is expected, not a bug.
-- ============================================================

CREATE STREAM order_payment_joined AS
SELECT
  o.orderId AS orderId,
  o.customerId AS customerId,
  o.total AS orderTotal,
  p.status AS paymentStatus,
  p.processedAt AS paymentProcessedAt
FROM orders_stream o
INNER JOIN payments_stream p
WITHIN (1 HOURS, 1 HOURS) GRACE PERIOD 1 MINUTES
ON o.orderId = p.orderId
EMIT CHANGES;

-- ============================================================
-- 3. Windowed aggregation: payment count + total amount per status,
--    bucketed into 1-minute tumbling windows.
-- ============================================================

CREATE TABLE payments_per_minute AS
SELECT
  status,
  COUNT(*) AS payment_count,
  SUM(amount) AS total_amount
FROM payments_stream
WINDOW TUMBLING (SIZE 1 MINUTES)
GROUP BY status
EMIT CHANGES;

-- ============================================================
-- 4. Non-windowed materialized table: latest payment status per
--    order. Supports instant PULL queries (point lookups), unlike
--    everything above which only supports PUSH queries (EMIT CHANGES,
--    streams forever until you stop it).
--
--    Example pull query (run separately, not part of this script):
--      SELECT orderId, status, amount FROM latest_payment_status
--      WHERE orderId = 'order-1';
-- ============================================================

CREATE TABLE latest_payment_status AS
SELECT
  orderId,
  LATEST_BY_OFFSET(status) AS status,
  LATEST_BY_OFFSET(amount) AS amount
FROM payments_stream
GROUP BY orderId
EMIT CHANGES;
