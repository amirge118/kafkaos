import { Kafka } from "kafkajs";

// Stage 17: the only change needed to point every existing producer/
// consumer/service at Redpanda instead of Kafka is this connection
// target — none of the actual application code changes at all, since
// Redpanda speaks the same wire protocol. Defaults to our real Kafka
// broker; set BROKERS=localhost:9195 to run the exact same scripts
// against Redpanda instead.
export const kafka = new Kafka({
  clientId: "kafkaos",
  brokers: (process.env.BROKERS ?? "localhost:9092").split(","),
});
