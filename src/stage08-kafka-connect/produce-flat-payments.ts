import { SchemaRegistry, SchemaType } from "@kafkajs/confluent-schema-registry";
import * as fs from "fs";
import * as path from "path";
import { kafka } from "../shared/kafka";

// Deliberately flat (no nested arrays/records) — the JDBC sink connector
// maps Avro fields to SQL columns 1:1 and doesn't handle nested structures
// without extra Single Message Transforms. Keeping this schema flat isolates
// "does the JDBC sink work at all" from "how do you flatten nested data",
// which is a separate problem for another day.
const registry = new SchemaRegistry({ host: "http://localhost:8081" });

const producer = kafka.producer();

const payments = [
  { orderId: "order-flat-1", amount: 39.98, status: "succeeded", processedAt: new Date().toISOString() },
  { orderId: "order-flat-2", amount: 15.0, status: "succeeded", processedAt: new Date().toISOString() },
  { orderId: "order-flat-3", amount: 89.97, status: "failed", processedAt: new Date().toISOString() },
  { orderId: "order-flat-4", amount: 100.97, status: "succeeded", processedAt: new Date().toISOString() },

];

async function run() {
  const schema = fs.readFileSync(path.join(__dirname, "schemas", "payment-flat-v1.avsc"), "utf-8");
  const { id } = await registry.register({ type: SchemaType.AVRO, schema }, { subject: "payments-flat-value" });
  console.log(`registered payment-flat-v1.avsc -> schema id=${id}`);

  await producer.connect();

  for (const payment of payments) {
    const encodedValue = await registry.encode(id, payment);
    const result = await producer.send({
      topic: "payments-flat",
      messages: [{ key: payment.orderId, value: encodedValue }],
    });
    console.log(`sent ${payment.orderId} -> partition ${result[0].partition}`);
  }

  await producer.disconnect();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
