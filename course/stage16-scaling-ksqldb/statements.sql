-- Stage 16: co-partitioning demo.
--
-- Two brand-new topics, deliberately created with MISMATCHED partition
-- counts (clicks-hot: 3, sessions-hot: 5 initially), to prove ksqlDB
-- refuses to join streams that aren't co-partitioned rather than silently
-- producing wrong results. Run against a running ksqldb-server
-- (localhost:8088) after creating the topics:
--
--   docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
--     --topic clicks-hot --partitions 3 --replication-factor 1 \
--     --bootstrap-server localhost:9092
--   docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
--     --topic sessions-hot --partitions 3 --replication-factor 1 \
--     --bootstrap-server localhost:9092
--
-- (partitions=3 on both, matching — the mismatched 3-vs-5 attempt and its
-- exact rejection error are recorded in NOTES.md Stage 16, not re-run here)

CREATE STREAM clicks_stream (userId VARCHAR, page VARCHAR)
WITH (KAFKA_TOPIC='clicks-hot', VALUE_FORMAT='JSON');

CREATE STREAM sessions_stream (userId VARCHAR, device VARCHAR)
WITH (KAFKA_TOPIC='sessions-hot', VALUE_FORMAT='JSON');

CREATE STREAM clicks_sessions_joined AS
SELECT
  c.userId,
  c.page,
  s.device
FROM clicks_stream c
INNER JOIN sessions_stream s
WITHIN 1 HOURS
ON c.userId = s.userId
EMIT CHANGES;
