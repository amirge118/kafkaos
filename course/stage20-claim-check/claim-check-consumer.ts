import { createHash } from "crypto";
import { kafka } from "../../shared/kafka";
import { downloadBlob } from "./blob-store";
import { safeParseJson } from "../../shared/util";

const TOPIC = process.env.TOPIC ?? "attachment-events";

interface AttachmentReference {
  eventId: string;
  bucket: string;
  key: string;
  sizeBytes: number;
  checksum: string;
  contentType: string;
}

// safeParseJson only proves the message is *valid JSON* — it says nothing
// about whether it's the *right shape*. `<AttachmentReference>` on its own
// is just a TypeScript type annotation, erased at runtime; it doesn't
// reject anything. This is the actual runtime check: any message missing
// one of the fields the "redeem" step depends on is treated as not a
// claim-check reference at all, not as a broken one. Confirmed live (see
// NOTES.md Stage 20) that skipping this check means a same-shaped-but-
// wrong message (a plain order JSON landing on this topic) doesn't just
// fail once — kafkajs retries a throwing eachMessage handler on the same
// offset forever, permanently stalling the partition. The exact same
// poison-pill failure mode as Stage 6, just triggered by a differently-
// shaped valid JSON object instead of malformed JSON.
function isAttachmentReference(value: unknown): value is AttachmentReference {
  if (typeof value !== "object" || value === null) return false;
  const v = value as Record<string, unknown>;
  return (
    typeof v.eventId === "string" &&
    typeof v.bucket === "string" &&
    typeof v.key === "string" &&
    typeof v.sizeBytes === "number" &&
    typeof v.checksum === "string" &&
    typeof v.contentType === "string"
  );
}

async function run() {
  const consumer = kafka.consumer({ groupId: "claim-check-consumer" });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const parsed = safeParseJson<unknown>(message.value?.toString(), "claim-check-consumer");
      if (!parsed) return;

      if (!isAttachmentReference(parsed)) {
        console.log(
          `[claim-check-consumer] skipping non-claim-check message at offset=${message.offset} ` +
            `(valid JSON, wrong shape — not a claim-check reference): ${JSON.stringify(parsed).slice(0, 120)}`
        );
        return;
      }
      const ref = parsed;

      // This is the "redeem the claim check" step — the consumer only
      // pulls the actual bytes when it actually needs them, from whatever
      // system is actually built to serve large blobs efficiently, not
      // from Kafka.
      const blob = await downloadBlob(ref.key);
      const actualChecksum = createHash("sha256").update(blob).digest("hex");
      const ok = actualChecksum === ref.checksum && blob.length === ref.sizeBytes;

      console.log(
        `[claim-check-consumer] ${ref.eventId} -> fetched ${blob.length.toLocaleString()}B from ${ref.bucket}/${ref.key} ` +
          `-- integrity ${ok ? "OK" : "MISMATCH"}`
      );
    },
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
