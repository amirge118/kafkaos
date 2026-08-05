# Stage 19 — Prometheus + Grafana monitoring

**Goal**: Build the piece Stage 11 deliberately left undone — Stage 11 checked lag and ISR by hand, on demand. This stage builds continuous, fleet-level monitoring: metrics scraped on a schedule, stored as a time series, and visualized on a dashboard that stays useful after the terminal closes, covering every topic and consumer group at once rather than one request's journey (Stage 18's job).

**What was built**: Used `kafka-exporter` (a small Go binary that talks the plain Kafka protocol) instead of the standard JMX Prometheus javaagent approach, deliberately avoiding another JVM-agent injection given prior OOM problems. Everything was provisioned as files rather than clicked together: `prometheus.yml` scrape config, Grafana datasource and dashboard-provisioning YAML, and a dashboard JSON (`kafkaos-overview.json`) covering consumer lag, under-replicated partitions, and per-topic throughput, all mounted read-only via `docker-compose.yml`. See [course/stage19-monitoring/](../../course/stage19-monitoring/).

**The real finding**: Verified with a deliberately engineered, known lag of 4,000 (produced 6,000 messages, consumed only 2,000) traced through every hop: kafka-exporter's raw metric present, Prometheus query returned exactly `4000`, and Grafana's own datasource proxy returned `4146` (the 4,000 plus 146 pre-existing lag from other groups). The combined stack (kafka-exporter + Prometheus + Grafana) used **~157 MiB** (15.3 + 50.8 + 91.5 MiB).

**Full story**: [NOTES.md → Stage 19](../../NOTES.md#stage-19--prometheus--grafana-monitoring)
