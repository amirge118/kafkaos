# Stage 12 — Failure testing (final stage)

**Goal**: The capstone of the original 12-stage run — deliberately stress-test the claims from Stages 4, 5, 10, and 11 under real adversarial conditions, including the `acks=1` vs `acks=all` experiment deferred back in Stage 5 until RF>1 was available.

**What was built**: Using the Stage 11 3-broker cluster, this stage first discovered that pausing 2 of 3 combined broker+controller nodes freezes controller-quorum majority and stalls the *entire* cluster, not just replication — forcing all later experiments to take down only 1 node at a time. It then set `min.insync.replicas=3` on a topic and killed one follower to compare `acks=all` and `acks=1` behavior against the same broken ISR, and verified whether "acknowledged" writes were actually consumer-visible. See [docker-compose.yml](../../docker-compose.yml).

**The real finding**: With ISR reduced to 2 of 3 replicas, `acks=1` writes were reported as `SUCCESS` (offset 11) by the producer but the consumer high watermark stayed stuck at offset 9 — three "successfully acknowledged" messages were invisible to every consumer until the dead follower rejoined ISR, at which point the offset jumped from 9 straight to 12.

**Full story**: [NOTES.md → Stage 12](../../NOTES.md#stage-12--failure-testing-final-stage)
