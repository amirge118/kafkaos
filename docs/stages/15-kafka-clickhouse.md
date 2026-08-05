# Stage 15 — Kafka → ClickHouse

**Goal**: The Part 2 centerpiece — a real analytics pipeline for millions of events, standing up ClickHouse via the canonical Kafka Engine table → Materialized View → MergeTree pattern, pushing millions of synthetic e-commerce events through it, and comparing against a Kafka Connect ClickHouse sink connector.

**What was built**: A funnel-shaped synthetic event producer (view → add_to_cart → purchase, weighted 70/22/8) feeding an `analytics-events` topic, plus `schema.sql` defining the three-object ClickHouse ingestion pipeline (Kafka engine view, MergeTree storage table, materialized view gluing them together). The same 2 million rows were also loaded into Postgres for a direct query-performance and storage comparison, and the official ClickHouse Kafka Connect sink connector was set up as an alternative ingestion path. See [course/stage15-clickhouse/](../../course/stage15-clickhouse/).

**The real finding**: Identical analytical queries over the same 2,000,000 rows ran dramatically faster in ClickHouse than Postgres — revenue-by-category was 8ms vs 392ms (~49x), and storage was 103.28 MiB in ClickHouse vs 247 MB in Postgres (~2.4x smaller).

**Full story**: [NOTES.md → Stage 15](../../NOTES.md#stage-15--kafka--clickhouse)
