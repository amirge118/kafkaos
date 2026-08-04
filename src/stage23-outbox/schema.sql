-- Stage 23: the transactional outbox.
--
-- Column names match Debezium's outbox-event-router SMT defaults exactly
-- (aggregatetype, aggregateid, type, payload) — no extra SMT config
-- needed to remap field names, the simplest version of the pattern.
CREATE TABLE IF NOT EXISTS orders_outbox_demo (
  order_id    TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  total       NUMERIC NOT NULL,
  status      TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregatetype TEXT NOT NULL,
  aggregateid   TEXT NOT NULL,
  type          TEXT NOT NULL,
  payload       JSONB NOT NULL
);
