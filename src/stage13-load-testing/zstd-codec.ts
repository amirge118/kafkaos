import { compress, decompress } from "@mongodb-js/zstd";
import { codecStats } from "./codec-stats";

// kafkajs has no built-in ZSTD support (only gzip ships in the library
// itself) — this registers a custom codec via kafkajs's CompressionCodecs
// extension point, matching the shape kafkajs expects: a zero-arg factory
// returning { compress(encoder), decompress(buffer) }.
export function ZstdCodec() {
  return {
    async compress(encoder: { buffer: Buffer }) {
      const start = Date.now();
      const out = await compress(encoder.buffer);
      codecStats.compressMs += Date.now() - start;
      codecStats.rawBytes += encoder.buffer.length;
      codecStats.compressedBytes += out.length;
      return out;
    },
    async decompress(buffer: Buffer) {
      return decompress(buffer);
    },
  };
}
