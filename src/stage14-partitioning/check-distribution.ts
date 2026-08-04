import { kafka } from "../shared/kafka";

const TOPIC = process.env.TOPIC ?? "hot-key-test";
// The practical guideline from this stage's research: no single partition
// should carry more than ~20% of total traffic, or it becomes a fixed
// bottleneck no amount of consumer-side scaling can work around (a
// partition is only ever read by one consumer within a group at a time).
const HEALTHY_MAX_SHARE = Number(process.env.HEALTHY_MAX_SHARE ?? 0.2);

async function run() {
  const admin = kafka.admin();
  await admin.connect();

  const offsets = await admin.fetchTopicOffsets(TOPIC);
  const counts = offsets
    .map((o) => ({ partition: o.partition, count: Number(o.high) - Number(o.low) }))
    .sort((a, b) => a.partition - b.partition);

  const total = counts.reduce((sum, c) => sum + c.count, 0);

  console.log(`--- partition distribution for "${TOPIC}" (${counts.length} partitions, ${total} messages) ---`);
  for (const c of counts) {
    const share = total > 0 ? c.count / total : 0;
    const bar = "#".repeat(Math.round(share * 50));
    const flag = share > HEALTHY_MAX_SHARE ? "  <-- over the ~20% guideline" : "";
    console.log(`  partition ${String(c.partition).padStart(2)}: ${String(c.count).padStart(6)} (${(share * 100).toFixed(1)}%) ${bar}${flag}`);
  }

  const worst = counts.reduce((a, b) => (b.count > a.count ? b : a));
  console.log(`worst partition: ${worst.partition} carries ${((worst.count / total) * 100).toFixed(1)}% of all traffic`);

  await admin.disconnect();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
