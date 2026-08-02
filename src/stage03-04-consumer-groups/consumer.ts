// Written in Stage 3 (basic consumer: offsets, auto/manual commit) and
// extended in Stage 4 (INSTANCE_ID, SESSION_TIMEOUT_MS, GROUP_JOIN logging)
// for the consumer-group rebalancing experiments — same file, two stages.
import { kafka } from "../shared/kafka";

const groupId = process.env.GROUP_ID ?? "order-processor";
const autoCommit = process.env.AUTO_COMMIT !== "false";
const runForMs = Number(process.env.RUN_MS ?? 5000);
const instanceId = process.env.INSTANCE_ID ?? "default";
// Default kafkajs sessionTimeout is 30s, meaning a killed instance can take up
// to 30s to be noticed. Lowered here so rebalances are visible without a long
// wait during this exercise; a real service would tune this based on how fast
// it needs to detect failures vs. how much heartbeat noise it can tolerate.
const sessionTimeout = Number(process.env.SESSION_TIMEOUT_MS ?? 10000);

const consumer = kafka.consumer({ groupId, sessionTimeout });

consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
  console.log(
    `[instance=${instanceId} group=${groupId}] (re)joined — assigned partitions:`,
    JSON.stringify(payload.memberAssignment)
  );
});

async function run() {
  await consumer.connect();
  // fromBeginning only matters the *first* time this group ever reads this
  // topic. Once the group has committed offsets, Kafka resumes from those
  // instead, regardless of what fromBeginning says.
  await consumer.subscribe({ topic: "orders", fromBeginning: true });

  let count = 0;

  await consumer.run({
    autoCommit,
    eachMessage: async ({ topic, partition, message }) => {
      count++;
      console.log(
        `[instance=${instanceId} group=${groupId} autoCommit=${autoCommit}] partition=${partition} offset=${message.offset} key=${message.key?.toString()} value=${message.value?.toString()}`
      );

      if (!autoCommit) {
        const nextOffset = (BigInt(message.offset) + 1n).toString();
        await consumer.commitOffsets([{ topic, partition, offset: nextOffset }]);
        console.log(`  -> manually committed partition=${partition} offset=${nextOffset}`);
      }
    },
  });

  setTimeout(async () => {
    console.log(`\n[instance=${instanceId} group=${groupId}] consumed ${count} message(s) this run, disconnecting`);
    await consumer.disconnect();
    process.exit(0);
  }, runForMs);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
