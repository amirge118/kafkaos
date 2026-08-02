import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";

export const registry = new SchemaRegistry({ host: "http://localhost:8081" });
