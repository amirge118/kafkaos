// kafkajs's built-in compression codecs (gzip) don't expose byte counts or
// timing, so this tracks it ourselves for whichever codec is active — used
// to report real compression ratio and CPU cost per algorithm, not just
// throughput.
export const codecStats = {
  rawBytes: 0,
  compressedBytes: 0,
  compressMs: 0,
};

export function resetCodecStats(): void {
  codecStats.rawBytes = 0;
  codecStats.compressedBytes = 0;
  codecStats.compressMs = 0;
}
