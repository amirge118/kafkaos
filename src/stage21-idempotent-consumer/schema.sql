-- Stage 21: the idempotent-consumer dedup layer.
--
-- `processed_events` is keyed by (topic, partition, offset) — a specific
-- Kafka *delivery*, not a business key like order_id. This distinction
-- matters: Stage 5 deliberately resent order-1/order-2 as legitimate new
-- messages at new offsets (an "update"), and those must NOT be treated as
-- duplicates. Deduping by delivery coordinates catches true Kafka
-- redelivery (the same offset, reprocessed after a crash) without
-- accidentally collapsing legitimate repeat business events.
CREATE TABLE IF NOT EXISTS processed_events (
  topic       TEXT NOT NULL,
  partition   INT NOT NULL,
  "offset"    BIGINT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (topic, partition, "offset")
);

-- The actual side effect (stands in for a real downstream write — e.g. an
-- order status update). effect_count exists purely to make "did this run
-- twice" directly observable: incremented only on the code path that
-- actually performs the effect, which the dedup check (above) prevents
-- from running a second time for the same delivery.
CREATE TABLE IF NOT EXISTS orders_processed (
  order_id           TEXT PRIMARY KEY,
  status             TEXT NOT NULL,
  effect_count       INT NOT NULL DEFAULT 1,
  first_processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
