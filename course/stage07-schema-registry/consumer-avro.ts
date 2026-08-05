import { kafka } from "../../shared/kafka";
import { registry } from "./registry";

// Notice this file never imports or references a specific .avsc schema file.
// registry.decode() reads the 4-byte schema id embedded in every message's
// wire format and fetches (and caches) whichever schema version that
// particular message was written with — automatically, per message. That's
// what lets this same unmodified code correctly decode both v1 and v2
// messages later in this stage.
const groupId = process.env.GROUP_ID ?? "avro-consumer";
const runForMs = Number(process.env.RUN_MS ?? 8000);

const consumer = kafka.consumer({ groupId });

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: "orders-avro", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message, partition }) => {
      const decoded = await registry.decode(message.value!);
      console.log(
        `partition=${partition} offset=${message.offset} key=${message.key?.toString()} decoded=${JSON.stringify(decoded)}`
      );
    },
  });

  setTimeout(async () => {
    await consumer.disconnect();
    process.exit(0);
  }, runForMs);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
