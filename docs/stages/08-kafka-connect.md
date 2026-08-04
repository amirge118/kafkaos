# Stage 8 — Kafka Connect

**Goal**: See how Kafka integrates with outside systems declaratively — a JSON config posted to a REST API — instead of hand-writing a producer or consumer for every integration. Built two built-in file connectors (to validate Connect itself) and a real JDBC sink/source pair against Postgres.

**What was built**: Added a `kafka-connect` service (distributed mode, REST API on `localhost:8083`, JDBC connector installed at startup) and a `postgres` service, plus `src/stage08-kafka-connect/` with a deliberately flat Avro schema and seed script for a `payments-flat` topic. Configured a file sink (`orders` → `/data/orders-sink.txt`), a file source (`/data/source-input.txt` → `manual-events`), a JDBC sink (`payments-flat` → a Postgres table, auto-created from the Avro schema), and a JDBC source (a Postgres table polled into `pg-manual_source_items`). Live connector configs were pulled back via the REST API and saved as JSON files under [src/stage08-kafka-connect/](../../src/stage08-kafka-connect/) so they're re-creatable, not just one-off curl commands.

**The real finding**: With `errors.tolerance: all` and a dead-letter queue configured on the file sink, **16 of 17** messages from `orders` landed in `/data/orders-sink.txt` and exactly **1** (the `heyyy` poison pill) was routed to `orders-file-sink-dlq` instead of crashing the task — Connect's framework-level answer to the same poison-pill problem seen in Stages 6 and 7.

**Full story**: [NOTES.md → Stage 8](../../NOTES.md#stage-8--kafka-connect)
