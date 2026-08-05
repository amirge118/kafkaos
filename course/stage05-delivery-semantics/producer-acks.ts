import { Partitioners } from "kafkajs";
import { kafka } from "../../shared/kafka";

// acks: 0 = don't wait for any broker confirmation (fire-and-forget)
//       1 = wait for the partition leader to write it (default)
//      -1 = wait for all in-sync replicas (strongest guarantee, needs RF>1 to matter)
const acks = Number(process.env.ACKS ?? 1) as -1 | 0 | 1;
const count = Number(process.env.COUNT ?? 20);

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

async function run() {
  await producer.connect();

  const start = Date.now();
  for (let i = 0; i < count; i++) {
    await producer.send({
      topic: "orders-semantics",
      acks,
      messages: [{ key: `acks-test-${i}`, value: JSON.stringify({ i, acks }) }],
    });
  }
  const elapsedMs = Date.now() - start;

  console.log(`acks=${acks}: sent ${count} messages in ${elapsedMs}ms (${(elapsedMs / count).toFixed(2)}ms/msg)`);

  await producer.disconnect();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
