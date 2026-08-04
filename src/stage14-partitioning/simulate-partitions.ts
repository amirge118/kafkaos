// The "simulate partition assignment before committing to a key" half of
// this stage's methodology — instead of producing 50,000 real messages to a
// real broker every time to test a candidate SALT_BUCKETS value (as the
// rest of this stage did to get verified, ground-truth numbers), this does
// the same murmur2 % numPartitions math kafkajs's DefaultPartitioner uses
// internally, in memory, so a key/salting design can be sanity-checked in
// milliseconds. Reaches into a kafkajs internal module (not part of its
// public API) deliberately: the whole point is bit-for-bit the same hash
// real production traffic would get, not a reimplementation that could
// silently drift from it.
// eslint-disable-next-line @typescript-eslint/no-var-requires
const murmur2 = require("kafkajs/src/producer/partitioners/default/murmur2") as (key: string) => number;

function toPositive(x: number): number {
  return x & 0x7fffffff;
}

function partitionFor(key: string, numPartitions: number): number {
  return toPositive(murmur2(key)) % numPartitions;
}

function worstPartitionShare(
  hotKey: string,
  hotKeyShare: number,
  saltBuckets: number,
  numPartitions: number,
  normalKeyCardinality = 5000
): number {
  const counts = new Array(numPartitions).fill(0);
  const trials = 200_000; // large enough to average out RNG noise for a stable estimate

  for (let i = 0; i < trials; i++) {
    let key: string;
    if (Math.random() < hotKeyShare) {
      key = saltBuckets > 0 ? `${hotKey}#${i % saltBuckets}` : hotKey;
    } else {
      key = `customer-${i % normalKeyCardinality}`;
    }
    counts[partitionFor(key, numPartitions)]++;
  }

  return Math.max(...counts) / trials;
}

const HOT_KEY_SHARE = 0.8;
const HEALTHY_MAX_SHARE = 0.2;

console.log(`--- partition-assignment simulation (HOT_KEY_SHARE=${HOT_KEY_SHARE}, no real broker involved) ---`);
console.log("partitions | salt buckets | worst-partition share");
for (const numPartitions of [6, 12, 24]) {
  for (const saltBuckets of [0, 4, 8, 16, 32, 64]) {
    const share = worstPartitionShare("customer-VIP", HOT_KEY_SHARE, saltBuckets, numPartitions);
    const flag = share > HEALTHY_MAX_SHARE ? "  <-- over ~20%" : "  OK";
    console.log(`${String(numPartitions).padStart(10)} | ${String(saltBuckets || "off").padStart(12)} | ${(share * 100).toFixed(1)}%${flag}`);
  }
}
