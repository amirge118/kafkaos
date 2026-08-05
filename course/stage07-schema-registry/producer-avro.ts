import * as fs from "fs";
import * as path from "path";
import { SchemaType } from "@kafkajs/confluent-schema-registry";
import { registry } from "./registry";
import { kafka } from "../../shared/kafka";
import { Order } from "../../shared/types";

// Registers schemas/order-v1.avsc under subject "orders-avro-value" (the
// Confluent naming convention: <topic>-value for the value schema, <topic>-key
// for the key schema if one exists). Registering an already-registered
// (identical) schema is idempotent — it just returns the existing id.
const SCHEMA_FILE = process.env.SCHEMA_FILE ?? "order-v1.avsc";

const producer = kafka.producer();

const orders: (Order & { discountCode?: string | null })[] = [
  { orderId: "order-avro-1", customerId: "cust-A", items: [{ sku: "sku-100", qty: 2 }], total: 39.98, status: "created" },
  { orderId: "order-avro-2", customerId: "cust-B", items: [{ sku: "sku-200", qty: 1 }], total: 15.0, status: "created" },
  { orderId: "order-avro-3", customerId: "cust-C", items: [{ sku: "sku-300", qty: 3 }], total: 89.97, status: "created" },
];

// When testing schema evolution (order-v2.avsc), give the orders a
// discountCode so the new field actually carries real data instead of just
// falling back to its default.
if (SCHEMA_FILE === "order-v2.avsc") {
  orders.forEach((o, i) => {
    o.orderId = `order-avro-v2-${i + 1}`;
    o.discountCode = i === 0 ? "SUMMER10" : null;
  });
}

async function run() {
  const schema = fs.readFileSync(path.join(__dirname, "schemas", SCHEMA_FILE), "utf-8");
  const { id } = await registry.register({ type: SchemaType.AVRO, schema }, { subject: "orders-avro-value" });
  console.log(`using schema ${SCHEMA_FILE} -> registry id=${id}`);

  await producer.connect();

  for (const order of orders) {
    const jsonBytes = Buffer.byteLength(JSON.stringify(order));

    let encodedValue: Buffer;
    try {
      encodedValue = await registry.encode(id, order);
    } catch (err) {
      console.error(`skipping ${order.orderId}: doesn't fit schema ${SCHEMA_FILE} (${(err as Error).message})`);
      continue;
    }

    const result = await producer.send({
      topic: "orders-avro",
      messages: [{ key: order.orderId, value: encodedValue }],
    });

    console.log(
      `sent ${order.orderId} -> partition ${result[0].partition} | avro=${encodedValue.length}B vs json=${jsonBytes}B`
    );
  }

  await producer.disconnect();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
