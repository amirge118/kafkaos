-- Stage 22 reuses Stage 21's `processed_events` dedup ledger as-is (same
-- (topic, partition, offset) primary key) — proof it's genuinely reusable
-- infrastructure, not a one-stage throwaway. Only the side-effect table is
-- new here.
CREATE TABLE IF NOT EXISTS processed_events (
  topic       TEXT NOT NULL,
  partition   INT NOT NULL,
  "offset"    BIGINT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (topic, partition, "offset")
);

CREATE TABLE IF NOT EXISTS payments_processed (
  order_id    TEXT PRIMARY KEY,
  status      TEXT NOT NULL,
  attempts    INT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
