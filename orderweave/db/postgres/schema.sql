-- Stage 25 (Orderweave): unified schema for the assembled system.
--
-- processed_events is the exact same table from Stage 21/22 (reused
-- verbatim — its PK is (topic, partition, "offset"), so there's no
-- collision risk sharing it across stages; this is now the THIRD stage to
-- reuse it unmodified, real proof it's genuinely general-purpose
-- infrastructure, not a one-stage throwaway).
CREATE TABLE IF NOT EXISTS processed_events (
  topic       TEXT NOT NULL,
  partition   INT NOT NULL,
  "offset"    BIGINT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (topic, partition, "offset")
);

-- Deliberately NOT the same physical table as Stage 23's `outbox` — same
-- column shape (matching Debezium's EventRouter SMT defaults exactly), but
-- a separate table (`capstone_outbox`). Two independent Debezium
-- connectors watching one shared table would cross-contaminate: Stage 23's
-- demo re-run would republish Orderweave's live rows, and Orderweave's
-- connector would replay Stage 23's old demo rows into live topics. This
-- keeps Stage 23's demo and Orderweave fully isolated while still proving
-- the same reusable *shape*.
CREATE TABLE IF NOT EXISTS capstone_outbox (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregatetype TEXT NOT NULL,
  aggregateid   TEXT NOT NULL,
  type          TEXT NOT NULL,
  payload       JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  order_id    TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  total       NUMERIC NOT NULL,
  status      TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS payments (
  order_id       TEXT PRIMARY KEY,
  amount         NUMERIC NOT NULL,
  status         TEXT NOT NULL,
  retry_attempts INT NOT NULL DEFAULT 1,
  processed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_reservations (
  order_id    TEXT PRIMARY KEY,
  reserved    BOOLEAN NOT NULL,
  reserved_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shipments (
  order_id    TEXT PRIMARY KEY,
  tracking_id TEXT NOT NULL,
  shipped_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS refunds (
  order_id  TEXT PRIMARY KEY,
  reason    TEXT NOT NULL,
  status    TEXT NOT NULL,
  issued_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
