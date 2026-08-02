import { Kafka } from "kafkajs";

export const kafka = new Kafka({
  clientId: "kafkaos",
  brokers: ["localhost:9092"],
});
