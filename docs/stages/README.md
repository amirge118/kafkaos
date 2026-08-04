# Stage by stage

A short, skimmable page per stage — goal, what was built, the single most
interesting real number, and a link back to [`NOTES.md`](../../NOTES.md)
for the complete story (including the false starts and dead ends, which
live only there). Start anywhere; each page is self-contained.

| Stage | Title | Hook |
|---|---|---|
| [00](./00-environment-setup.md) | Environment setup | KRaft mode, no ZooKeeper — the whole cluster is one container |
| [01](./01-topics-partitions.md) | Topics & partitions | A partition is a real directory of segment files, not an abstraction |
| [02](./02-basic-producer.md) | Basic producer | Key-based partitioning, verified by checking which partition a key actually lands on |
| [03](./03-basic-consumer.md) | Basic consumer | Manual vs. auto commit, watched through real lag numbers |
| [04](./04-consumer-groups-rebalancing.md) | Consumer groups & rebalancing | A dead consumer sat undetected for 4+ minutes past a 10s session timeout |
| [05](./05-delivery-semantics.md) | Delivery semantics | At-most-once silently drops a message; at-least-once silently duplicates one — both proven with a real crash |
| [06](./06-multi-service-event-flow.md) | Multi-service event flow | A stray non-JSON message crashed a live service — a real poison pill |
| [07](./07-schema-registry.md) | Schema Registry | Avro payloads at 52B vs. JSON's 117B, and a schema-evolution rejection caught before it shipped |
| [08](./08-kafka-connect.md) | Kafka Connect | JDBC + file connectors, and a DLQ for malformed messages Connect itself manages |
| [09](./09-ksqldb-stream-processing.md) | Stream processing with ksqlDB | Windowed joins over real payment data, streaming SQL instead of hand-written consumers |
| [10](./10-transactions-exactly-once.md) | Transactions / exactly-once semantics | `read_committed` vs. `read_uncommitted` — the same topic, two different answers |
| [11](./11-monitoring-operations.md) | Monitoring & operations | Graceful failover: 2ms. Ungraceful: ~7.5s. Measured, not assumed |
| [12](./12-failure-testing.md) | Failure testing | A stuck high watermark, frozen until a dead follower rejoined the ISR |
| [13](./13-load-testing-tuning.md) | Load testing & producer/consumer tuning | Batching alone: 2,318 → 108,932 msgs/sec, ~47x |
| [14](./14-partitioning-skewed-data.md) | Partitioning strategy under skewed data | More partitions barely touched a hot key; salting actually fixed it |
| [15](./15-kafka-clickhouse.md) | Kafka → ClickHouse | The same 2M rows, 49x faster analytical queries than Postgres |
| [16](./16-scaling-stateful-processing.md) | Scaling stateful processing | Killed a ksqlDB node mid-query — state rebuilt from the changelog, correctly |
| [17](./17-redpanda.md) | Redpanda | Same `kafkajs` code, zero changes, ~3x less memory, ~23x faster startup |
| [18](./18-distributed-tracing-opentelemetry.md) | Distributed tracing with OpenTelemetry | One order, traced as a single connected span chain across four services |
| [19](./19-prometheus-grafana-monitoring.md) | Prometheus + Grafana monitoring | A deliberately engineered lag of 4,000, confirmed exactly at every layer |
| [20](./20-claim-check-pattern.md) | Large payloads & the claim-check pattern | A real `MESSAGE_TOO_LARGE` broker rejection, then fixed with S3-style claim checks |
| [21](./21-idempotent-consumers.md) | Idempotent consumers & deduplication | The identical redelivered message — one produces a real duplicate, one doesn't |
| [22](./22-retry-dlq-alerting.md) | Retry policies & DLQ with alerting | An alert that silently fired and expired between polls — a real tuning lesson, not a bug |
| [23](./23-transactional-outbox.md) | Transactional Outbox Pattern | The identical crash, two implementations — one loses an event forever, one doesn't |
| [24](./24-saga-pattern.md) | Saga Pattern | Choreography vs. orchestration — byte-for-byte identical outcomes, completely different operational shape |
| [25](./25-capstone-orderweave.md) | **The Capstone — Orderweave** | 2,000,000 messages, 132 transactions, zero duplicates, verified three ways |

See **[`../../ARCHITECTURE.md`](../../ARCHITECTURE.md)** for the capstone's
full, self-contained write-up, or **[`../../NOTES.md`](../../NOTES.md)**
for the complete chronological journal this index is condensed from.
