-- Stage 15: the canonical ClickHouse + Kafka ingestion pattern.
--
-- Three objects, each with a distinct job:
--   1. `analytics_events_queue` (Kafka table engine) — not a real table, a
--      *view* over the Kafka topic itself. Reading it consumes from Kafka.
--      It stores nothing on disk; querying it directly is a one-shot,
--      destructive read (each row is consumed once, like a normal Kafka
--      consumer). This alone is not the ingestion mechanism.
--   2. `analytics_events` (MergeTree) — the real, persistent, columnar
--      storage table this stage's analytical queries actually run against.
--   3. `analytics_events_mv` (Materialized View) — the glue: ClickHouse
--      runs this SELECT continuously in the background against the Kafka
--      engine table and INSERTs every result block into `analytics_events`.
--      This is what makes ingestion actually happen — without an attached
--      materialized view, the Kafka engine table just sits there unread.

CREATE TABLE IF NOT EXISTS kafkaos.analytics_events_queue
(
    event_id    String,
    event_time  UInt64,  -- unix epoch millis; converted to DateTime64 in the MV below
    event_type  LowCardinality(String),
    user_id     String,
    product_id  String,
    category    LowCardinality(String),
    country     LowCardinality(String),
    amount      Float64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'analytics-events',
    kafka_group_name = 'clickhouse-analytics-events',
    kafka_format = 'JSONEachRow',
    -- Matches the topic's 6 partitions — each consumer thread ClickHouse
    -- spins up for this table can claim its own partition, so ingestion
    -- itself can fan out in parallel instead of bottlenecking on one thread.
    kafka_num_consumers = 6,
    kafka_skip_broken_messages = 100;

CREATE TABLE IF NOT EXISTS kafkaos.analytics_events
(
    event_id    String,
    event_time  DateTime64(3),
    event_date  Date MATERIALIZED toDate(event_time),
    event_type  LowCardinality(String),
    user_id     String,
    product_id  String,
    category    LowCardinality(String),
    country     LowCardinality(String),
    amount      Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
-- Ordered for the queries this stage actually runs: aggregations grouped
-- by category/event_type first, time-range-filtered second.
ORDER BY (category, event_type, event_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS kafkaos.analytics_events_mv
TO kafkaos.analytics_events
AS SELECT
    event_id,
    fromUnixTimestamp64Milli(event_time) AS event_time,
    event_type,
    user_id,
    product_id,
    category,
    country,
    amount
FROM kafkaos.analytics_events_queue;
