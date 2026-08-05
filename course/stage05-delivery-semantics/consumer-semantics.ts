import * as fs from "fs";
import { kafka } from "../../shared/kafka";

// Makes the trade-off between at-most-once and at-least-once concrete by
// actually crashing mid-stream and inspecting a side-effect log afterward,
// instead of just describing the trade-off.
//
// MODE=at-most-once  -> commit the offset, THEN do the "work" (append to LOG_FILE).
//                       Crashing between those two steps loses the work: the
//                       offset says "done", but the side effect never happened.
// MODE=at-least-once -> do the "work" FIRST, then commit the offset.
//                       Crashing between those two steps reprocesses the
//                       message on restart: the side effect happens twice.
const groupId = process.env.GROUP_ID ?? "semantics-demo";
const mode = process.env.MODE ?? "at-least-once"; // "at-most-once" | "at-least-once"
const topic = process.env.TOPIC ?? "orders-crash-demo";
const crashAfter = process.env.CRASH_AFTER ? Number(process.env.CRASH_AFTER) : undefined;
const logFile = process.env.LOG_FILE ?? "/tmp/kafkaos-semantics.log";

const consumer = kafka.consumer({ groupId });

function doWork(key: string, offset: string) {
  fs.appendFileSync(logFile, `pid=${process.pid} mode=${mode} key=${key} offset=${offset}\n`);
}

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  let processedCount = 0;

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic: t, partition, message }) => {
      processedCount++;
      const key = message.key?.toString() ?? "(none)";
      const nextOffset = (BigInt(message.offset) + 1n).toString();

      if (mode === "at-most-once") {
        await consumer.commitOffsets([{ topic: t, partition, offset: nextOffset }]);
        if (processedCount === crashAfter) {
          console.log(`[CRASH] committed offset=${nextOffset} for key=${key}, exiting BEFORE doing the work`);
          process.exit(1);
        }
        doWork(key, message.offset);
      } else {
        doWork(key, message.offset);
        if (processedCount === crashAfter) {
          console.log(`[CRASH] did the work for key=${key}, exiting BEFORE committing offset=${nextOffset}`);
          process.exit(1);
        }
        await consumer.commitOffsets([{ topic: t, partition, offset: nextOffset }]);
      }

      console.log(`[mode=${mode}] processed key=${key} offset=${message.offset}`);
    },
  });

  setTimeout(async () => {
    console.log(`[mode=${mode}] finished run, ${processedCount} message(s) processed, disconnecting`);
    await consumer.disconnect();
    process.exit(0);
  }, Number(process.env.RUN_MS ?? 8000));
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
