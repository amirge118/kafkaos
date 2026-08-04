import { Partitioners } from "kafkajs";
import { kafka } from "../shared/kafka";
import { Order } from "../shared/types";
import { safeParseJson } from "../shared/util";

// The orchestration-style comparison. One process explicitly sequences
// every step AND every compensation for a given order — no other service
// independently decides anything. Contrast this file directly against the
// three separate saga-*.ts files: there, "what happens next" is implicit,
// scattered across whichever service happens to be subscribed to which
// topic; here, the entire sequence — happy path and compensation both —
// is one linear, readable function.
const AUDIT_TOPIC = "saga-orchestrator-audit";

const consumer = kafka.consumer({ groupId: "order-saga-orchestrator" });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

async function audit(orderId: string, step: string, detail: Record<string, unknown> = {}) {
  await producer.send({
    topic: AUDIT_TOPIC,
    messages: [{ key: orderId, value: JSON.stringify({ orderId, step, ...detail, at: new Date().toISOString() }) }],
  });
  console.log(`[orchestrator] order=${orderId} step=${step}`, detail);
}

async function attemptPayment(order: Order): Promise<"succeeded" | "failed"> {
  return order.orderId.includes("payment-declined") ? "failed" : "succeeded";
}

async function attemptInventoryReservation(order: Order): Promise<"reserved" | "out-of-stock"> {
  return order.orderId.includes("out-of-stock") ? "out-of-stock" : "reserved";
}

// The orchestrator explicitly drives the whole saga, step by step,
// including deciding when and how to compensate — this function IS the
// saga definition, in one place, instead of being implied by which
// services happen to subscribe to which topics.
async function runSaga(order: Order) {
  const paymentResult = await attemptPayment(order);
  await audit(order.orderId, "payment", { result: paymentResult });
  if (paymentResult === "failed") {
    await audit(order.orderId, "saga-ended", { outcome: "payment declined, no further steps" });
    return;
  }

  const inventoryResult = await attemptInventoryReservation(order);
  await audit(order.orderId, "inventory", { result: inventoryResult });
  if (inventoryResult === "out-of-stock") {
    // The compensation decision is made right here, explicitly, by the
    // same process that made every other decision for this saga — not
    // discovered by some other service reacting to an event.
    await audit(order.orderId, "compensate-payment", { reason: "inventory reservation failed: out of stock" });
    await audit(order.orderId, "saga-ended", { outcome: "compensated: payment refunded after inventory failure" });
    return;
  }

  await audit(order.orderId, "shipment", { trackingId: `TRK-${order.orderId}-${Date.now()}` });
  await audit(order.orderId, "saga-ended", { outcome: "completed" });
}

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: "orders-orchestrated", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const order = safeParseJson<Order>(message.value?.toString(), "order-saga-orchestrator");
      if (!order) return;
      await runSaga(order);
    },
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
