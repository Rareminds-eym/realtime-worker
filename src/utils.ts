/**
 * Pure utility functions for the realtime-worker.
 *
 * Separated from index.ts so they can be unit-tested without
 * importing `cloudflare:workers` (which is only available at runtime).
 */

/** Total number of DO partitions in the hash ring */
export const TOTAL_PARTITIONS = 10;

/**
 * Deterministic hash of a userId to a partition index (0–9).
 * Uses the Java-style 31-multiplier string hash for even distribution.
 */
export function getPartitionId(userId: string): number {
  let hash = 0;
  for (let i = 0; i < userId.length; i++) {
    hash = (Math.imul(31, hash) + userId.charCodeAt(i)) | 0;
  }
  return Math.abs(hash) % TOTAL_PARTITIONS;
}
