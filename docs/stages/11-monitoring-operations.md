# Stage 11 — Monitoring & operations

**Goal**: Make replication factor mean something for the first time — stand up a real multi-broker cluster, watch leader election and ISR shrink/recover under an actual killed broker, and connect consumer lag to what "monitoring" actually means operationally.

**What was built**: A separate, independent 3-broker KRaft cluster (`kafka-b1`, `kafka-b2`, `kafka-b3`, own `CLUSTER_ID` and controller quorum) was added directly in the root `docker-compose.yml`, kept apart from the main single-broker cluster used since Stage 0 to avoid reconfiguring an already-formed quorum. A topic was created with RF=3, a broker was killed with `docker stop` (graceful) and later `docker kill` (ungraceful) to compare failover timing, and consumer lag was reframed as the same production alerting signal used since Stage 3. See [docker-compose.yml](../../docker-compose.yml).

**The real finding**: Graceful shutdown (`docker stop`, SIGTERM) triggered leader failover in **two milliseconds** ("13:43:12.993" broker marked in controlled shutdown, "13:43:12.995" new leader elected), while an ungraceful `docker kill` (SIGKILL) took **~7.5 seconds** for the controller to fence the dead broker — matching `broker.session.timeout.ms` (9s) rather than Stage 4's 4.5-minute consumer-heartbeat delay.

**Full story**: [NOTES.md → Stage 11](../../NOTES.md#stage-11--monitoring--operations)
