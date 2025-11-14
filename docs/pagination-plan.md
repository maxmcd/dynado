# Streaming Pagination Plan

## Problem
- `Router.scan()` currently fans out across every shard and materializes the entire table in memory before trimming to the client-specified `Limit`. This wastes memory and latency on large tables and cannot serve unbounded datasets.
- `Router.query()` already routes to a single shard when a partition key is provided, but the router still buffers whole shard results before paginating, so descending pagination and exclusive start keys behave inconsistently.

## Goals
- Keep memory usage bounded by `O(shardCount * chunkSize)` rather than table size.
- Preserve DynamoDB semantics for `Limit`, `LastEvaluatedKey`, `ExclusiveStartKey`, and `ScanIndexForward`.
- Avoid touching shards that are not required (e.g., `Query` with a specific partition key).

## Proposal
1. **Shard-local chunks**: extend `Shard.scanTable()` (and the range-query helper) to accept `{ limit, exclusiveStartKey }` and return up to `limit + 1` rows plus the shard’s next cursor. Shards continue to own ordering by sort key so the router does not sort big arrays.
2. **K-way merge in the router**: maintain a min/max heap keyed by `(partitionKey, sortKey)` (or only sort key for scans without partition). Seed the heap with the first chunk from each shard; pop the smallest item, append to the response, and lazily pull the next row from the same shard (fetching another chunk only when its buffer empties). This yields streaming pagination without full materialization.
3. **Composite continuation token**: return a `LastEvaluatedKey` object that stores the router’s global frontier plus each shard’s internal cursor. On resume, rebuild the heap by asking each shard for rows starting after its saved cursor, ensuring deterministic continuation even if some shards exhausted earlier than others.
4. **Query shortcut**: when the client specifies a partition key, skip the heap and proxy the shard’s native pagination response (the shard already knows item order). This keeps the implementation simple for the common query path.

## Open Questions
- Chunk size default (e.g., 25–100 items) vs. client `Limit`; should we tune dynamically based on requested limit?
- Representation of the composite continuation token (`LastEvaluatedKey` vs. custom header) so AWS SDKs can keep paginating transparently.
- Whether scans should preserve a strictly deterministic interleaving across shards or allow looser ordering when no sort key exists.

## Validation Plan
- Add regression tests that scan large tables and assert memory stays bounded (e.g., track peak RSS or count of buffered items).
- Ensure `ExclusiveStartKey` + descending queries advance correctly by resuming from the router-provided continuation token.
- Verify multi-shard tables paginate consistently by inserting interleaved rows across shards and checking that merged results are sorted and lossless.
