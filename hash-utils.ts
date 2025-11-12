// Shared hashing utilities for consistent shard routing
import CRC32 from "crc-32";

/**
 * Get shard index for a partition key using consistent hashing
 * Uses CRC32 for better distribution than naive string hashing
 */
export function getShardIndex(partitionKey: string, shardCount: number): number {
  // Use CRC32 for good distribution characteristics
  const hash = CRC32.str(partitionKey);
  // CRC32 can be negative, convert to unsigned 32-bit
  const unsignedHash = hash >>> 0;
  return unsignedHash % shardCount;
}
