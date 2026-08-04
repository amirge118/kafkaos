# Stage 16 — Scaling stateful processing

**Goal**: Extend Stage 9's ksqlDB work to see how RocksDB-backed state actually gets distributed and rebalanced as processing instances are added or removed under real load, and prove co-partitioning requirements get sharper once processing is genuinely distributed across multiple instances.

**What was built**: A real 3-node ksqlDB cluster (`ksqldb-server`, `ksqldb-server-2`, `ksqldb-server-3`) sharing one `KSQL_KSQL_SERVICE_ID`, brought up one at a time after an OOM kill from running everything simultaneously on this machine's memory ceiling. Partition ownership was verified via `kafka-consumer-groups.sh --describe` and `/clusterStatus` before and after scaling, an instance was killed mid-processing to test failover and RocksDB state recovery from Kafka changelog topics, and a join across mismatched-partition-count topics was attempted to test co-partitioning enforcement. See [src/stage16-scaling-ksqldb/](../../src/stage16-scaling-ksqldb/).

**The real finding**: After killing `ksqldb-server-3` outright, a fresh pull query against `LATEST_PAYMENT_STATUS` for a payment produced after failover returned the correct result immediately (`order-post-failover-2 | succeeded | 123.45`), proving the surviving host rebuilt RocksDB state from the changelog topic with no manual intervention.

**Full story**: [NOTES.md → Stage 16](../../NOTES.md#stage-16--scaling-stateful-processing)
