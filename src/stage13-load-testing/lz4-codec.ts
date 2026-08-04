import { codecStats } from "./codec-stats";

// lz4js ships no type declarations, and ts-node (unlike a full `tsc`
// project compile) doesn't pick up a sibling ambient .d.ts unless
// ts-node.files=true is set — required via `require` with a manual type
// instead of fighting that.
const lz4 = require("lz4js") as {
  compressBound(n: number): number;
  compressBlock(src: Buffer, dst: Uint8Array, sIndex: number, sLength: number, hashTable: Uint32Array): number;
  makeBuffer(size: number): Uint8Array;
  decompress(input: Buffer | Uint8Array): Uint8Array;
};
const util = require("lz4js/util.js") as {
  writeU32(b: Uint8Array, n: number, x: number): void;
};
const xxhash = require("lz4js/xxh32.js") as {
  hash(seed: number, b: Uint8Array, offset: number, length: number): number;
};

// lz4js's own compressFrame() reuses one shared hash table across the whole
// frame and never sets the LZ4 frame spec's block-independence bit — real
// blocks end up dependent on each other's history, and the frame header
// dishonestly claims otherwise (there's even a dead `fdBlockIndep = 0x20`
// constant in its source, defined but never used). Kafka's broker validates
// this and hard-rejects it: "Dependent block stream is unsupported" (see
// NOTES.md Stage 13). This reimplements just the frame-writing loop with a
// fresh hash table per block (making blocks genuinely independent) and sets
// the flag correctly — everything else (block compression itself, and
// decompression) reuses lz4js as-is.
const MAGIC_NUM = 0x184d2204;
const FD_VERSION = 0x40;
const FD_BLOCK_INDEPENDENCE = 0x20;
const BS_DEFAULT = 7;
const BS_SHIFT = 4;
const MAX_BLOCK_SIZE = 0x400000; // bsMap[7] in lz4js

function compressIndependentFrame(src: Buffer): Buffer {
  const dst = new Uint8Array(lz4.compressBound(src.length) + 16);
  let dIndex = 0;

  util.writeU32(dst, dIndex, MAGIC_NUM);
  dIndex += 4;
  dst[dIndex] = FD_VERSION | FD_BLOCK_INDEPENDENCE;
  dst[dIndex + 1] = BS_DEFAULT << BS_SHIFT;
  dst[dIndex + 2] = xxhash.hash(0, dst, dIndex, 2) >> 8;
  dIndex += 3;

  const blockBuf = lz4.makeBuffer(MAX_BLOCK_SIZE + 1024);
  let sIndex = 0;
  let remaining = src.length;

  while (remaining > 0) {
    const blockSize = Math.min(remaining, MAX_BLOCK_SIZE);
    // Fresh table per block, instead of lz4js's one-per-frame table — this
    // is the actual fix: no block can reference bytes from another block.
    const hashTable = new Uint32Array(1 << 16);
    const compSize = lz4.compressBlock(src, blockBuf, sIndex, blockSize, hashTable);

    if (compSize === 0 || compSize > blockSize) {
      util.writeU32(dst, dIndex, 0x80000000 | blockSize);
      dIndex += 4;
      for (let z = sIndex + blockSize; sIndex < z; ) dst[dIndex++] = src[sIndex++];
    } else {
      util.writeU32(dst, dIndex, compSize);
      dIndex += 4;
      for (let j = 0; j < compSize; ) dst[dIndex++] = blockBuf[j++];
      sIndex += blockSize;
    }
    remaining -= blockSize;
  }

  util.writeU32(dst, dIndex, 0);
  dIndex += 4;
  return Buffer.from(dst.slice(0, dIndex));
}

export function Lz4Codec() {
  return {
    async compress(encoder: { buffer: Buffer }) {
      const start = Date.now();
      const out = compressIndependentFrame(encoder.buffer);
      codecStats.compressMs += Date.now() - start;
      codecStats.rawBytes += encoder.buffer.length;
      codecStats.compressedBytes += out.length;
      return out;
    },
    async decompress(buffer: Buffer) {
      return Buffer.from(lz4.decompress(buffer));
    },
  };
}
