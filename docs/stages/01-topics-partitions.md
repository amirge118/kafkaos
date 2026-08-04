# Stage 1 — Topics & partitions

**Goal**: Create our first real topic and understand what a "partition" concretely
is — both at the metadata level (what the cluster reports) and at the storage level
(what actually exists on disk inside the broker container).

**What was built**: The `orders` topic was created with 3 partitions and
replication factor 1 via `kafka-topics.sh --create`, then inspected with
`--describe` and `--list`. Its on-disk layout was examined directly inside the
broker container — each partition is a directory (`orders-0`, `orders-1`,
`orders-2`) under `/tmp/kafka-logs` containing `.log`, `.index`, and
`.timeindex` segment files plus a `leader-epoch-checkpoint`. A deliberate
attempt to create a topic with replication factor 2 on the single-broker
cluster was made to confirm the failure mode. See
[docker-compose.yml](../../docker-compose.yml).

**The real finding**: Creating `test-rf-demo` with `--replication-factor 2` on
the single-broker cluster failed cleanly with: `Error: Unable to replicate the
partition 2 time(s): The target replication factor of 2 cannot be reached
because only 1 broker(s) are registered.` — no topic was created.

**Full story**: [NOTES.md → Stage 1](../../NOTES.md#stage-1--topics--partitions)
