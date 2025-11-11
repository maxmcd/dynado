// TransactionCoordinator: Implements DynamoDB's Two-Phase Commit protocol
// In DO architecture, this would be a pool of Durable Objects

import { Database } from "bun:sqlite";
import type {
  TransactWriteItem,
  TransactGetItem,
  DynamoDBItem,
  TransactionRecord,
  CancellationReason,
  PrepareRequest,
  PrepareResponse,
  CommitRequest,
  ReleaseRequest,
} from "./types.ts";
import { TransactionCancelledError } from "./types.ts";
import {
  generateTransactionId,
  generateTransactionTimestamp,
  buildCancellationReasons,
} from "./transaction-protocol.ts";
import type { Shard } from "./shard.ts";
import type { MetadataStore } from "./metadata-store.ts";
import * as fs from "fs";

interface IdempotencyCacheEntry {
  timestamp: number;
  result: any;
}

export class TransactionCoordinator {
  private db: Database;
  private idempotencyCache = new Map<string, IdempotencyCacheEntry>();
  private readonly CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutes

  constructor(dataDir: string) {
    // Create data directory if it doesn't exist
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }

    this.db = new Database(`${dataDir}/coordinator.db`);

    // Create transaction ledger table
    this.db.run(`
      CREATE TABLE IF NOT EXISTS transaction_ledger (
        transaction_id TEXT PRIMARY KEY,
        state TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        client_request_token TEXT,
        items TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        completed_at INTEGER,
        cancellation_reasons TEXT
      )
    `);

    // Index for cleanup queries
    this.db.run(`
      CREATE INDEX IF NOT EXISTS idx_ledger_completed
      ON transaction_ledger(completed_at)
    `);

    // Periodically clean up old transactions and cache
    setInterval(() => this.cleanup(), 60 * 1000); // Every minute
  }

  // Execute TransactWriteItems using 2PC protocol
  async transactWrite(
    items: TransactWriteItem[],
    shards: Shard[],
    metadataStore: MetadataStore,
    clientRequestToken?: string
  ): Promise<void> {
    // Validate
    if (!items || items.length === 0) {
      throw new Error("TransactItems cannot be empty");
    }
    if (items.length > 100) {
      throw new Error("Transaction cannot contain more than 100 items");
    }

    // Check idempotency cache
    if (clientRequestToken) {
      this.cleanIdempotencyCache();
      const cached = this.idempotencyCache.get(clientRequestToken);
      if (cached) {
        return cached.result;
      }
    }

    const transactionId = generateTransactionId();
    const timestamp = generateTransactionTimestamp();

    // Record transaction as PREPARING
    this.recordTransaction({
      transactionId,
      state: "PREPARING",
      timestamp,
      clientRequestToken,
      items,
      createdAt: Date.now(),
    });

    try {
      // Group items by shard and prepare requests
      const shardOperations = await this.groupItemsByShards(
        items,
        shards,
        metadataStore,
        transactionId,
        timestamp
      );

      // PHASE 1: PREPARE
      // Send prepare requests to all involved shards in parallel
      const preparePromises = shardOperations.map(async (op) => {
        const shard = shards[op.shardIndex];
        if (!shard) {
          throw new Error(`Shard ${op.shardIndex} not found`);
        }
        return await shard.prepare(op.prepareRequest);
      });

      const prepareResponses = await Promise.all(preparePromises);

      // Check if all shards accepted
      const allAccepted = prepareResponses.every((r) => r.accepted);

      if (!allAccepted) {
        // Find first failure
        const failureIndex = prepareResponses.findIndex((r) => !r.accepted);
        const failedResponse = prepareResponses[failureIndex];

        if (!failedResponse) {
          throw new Error("Failed to find failure response");
        }

        // Build cancellation reason
        const reason: CancellationReason = {
          Code: failedResponse.reason || "Unknown",
          Message: failedResponse.message || "The conditional request failed",
        };

        if (failedResponse.item) {
          reason.Item = failedResponse.item;
        }

        const cancellationReasons = buildCancellationReasons(
          items.length,
          failureIndex,
          reason
        );

        // Update ledger
        this.updateTransactionState(
          transactionId,
          "CANCELLED",
          cancellationReasons
        );

        // RELEASE: Clean up locks on all shards
        await this.releaseAllShards(shardOperations, shards, transactionId);

        throw new TransactionCancelledError(
          "Transaction cancelled, please refer cancellation reasons for specific reasons",
          cancellationReasons
        );
      }

      // All shards accepted - proceed to Phase 2
      this.updateTransactionState(transactionId, "COMMITTING");

      // PHASE 2: COMMIT
      // This phase MUST complete - retry indefinitely on failure
      await this.commitAllShards(shardOperations, shards, transactionId, timestamp);

      // Mark as committed
      this.updateTransactionState(transactionId, "COMMITTED");

      // Cache result for idempotency
      if (clientRequestToken) {
        this.idempotencyCache.set(clientRequestToken, {
          timestamp: Date.now(),
          result: undefined,
        });
      }
    } catch (error: any) {
      // If error is TransactionCancelledError, it's already handled
      if (error instanceof TransactionCancelledError) {
        throw error;
      }

      // For other errors during Phase 1, try to release locks
      try {
        const shardOperations = await this.groupItemsByShards(
          items,
          shards,
          metadataStore,
          transactionId,
          timestamp
        );
        await this.releaseAllShards(shardOperations, shards, transactionId);
      } catch (releaseError) {
        // Ignore release errors
      }

      throw error;
    }
  }

  // Execute TransactGetItems
  async transactGet(
    items: TransactGetItem[],
    shards: Shard[],
    metadataStore: MetadataStore
  ): Promise<Array<DynamoDBItem | null>> {
    if (!items || items.length === 0) {
      throw new Error("TransactItems cannot be empty");
    }
    if (items.length > 100) {
      throw new Error("Transaction cannot contain more than 100 items");
    }

    // For read transactions, we just read from each shard
    // In a real implementation, we'd use snapshot isolation
    const results: Array<DynamoDBItem | null> = [];

    for (const item of items) {
      const partitionKey = metadataStore.getPartitionKeyValue(
        item.tableName,
        item.key
      );
      const shardIndex = this.getShardIndex(partitionKey, shards.length);
      const shard = shards[shardIndex];

      if (!shard) {
        throw new Error(`Shard ${shardIndex} not found`);
      }

      const keyString = this.getKeyString(item.key);
      const dbItem = await shard.getItem(item.tableName, keyString);

      if (dbItem) {
        // Apply projection expression if provided
        let resultItem = dbItem;
        if (item.projectionExpression) {
          resultItem = this.applyProjection(
            dbItem,
            item.projectionExpression,
            item.expressionAttributeNames
          );
        }
        results.push(resultItem);
      } else {
        results.push(null);
      }
    }

    return results;
  }

  // Helper: Group items by which shard they belong to
  private async groupItemsByShards(
    items: TransactWriteItem[],
    shards: Shard[],
    metadataStore: MetadataStore,
    transactionId: string,
    timestamp: number
  ): Promise<
    Array<{
      shardIndex: number;
      prepareRequest: PrepareRequest;
      commitRequest: CommitRequest;
      releaseKey: DynamoDBItem;
    }>
  > {
    const operations: Array<{
      shardIndex: number;
      prepareRequest: PrepareRequest;
      commitRequest: CommitRequest;
      releaseKey: DynamoDBItem;
    }> = [];

    for (const item of items) {
      let tableName: string;
      let key: DynamoDBItem;
      let operation: "Put" | "Update" | "Delete" | "ConditionCheck";
      let itemData: DynamoDBItem | undefined;
      let updateExpression: string | undefined;
      let conditionExpression: string | undefined;
      let expressionAttributeNames: Record<string, string> | undefined;
      let expressionAttributeValues: Record<string, any> | undefined;
      let returnValuesOnConditionCheckFailure: "ALL_OLD" | "NONE" | undefined;

      if (item.Put) {
        operation = "Put";
        tableName = item.Put.tableName;
        key = metadataStore.extractKey(tableName, item.Put.item);
        itemData = item.Put.item;
        conditionExpression = item.Put.conditionExpression;
        expressionAttributeNames = item.Put.expressionAttributeNames;
        expressionAttributeValues = item.Put.expressionAttributeValues;
        returnValuesOnConditionCheckFailure =
          item.Put.returnValuesOnConditionCheckFailure;
      } else if (item.Update) {
        operation = "Update";
        tableName = item.Update.tableName;
        key = item.Update.key;
        updateExpression = item.Update.updateExpression;
        conditionExpression = item.Update.conditionExpression;
        expressionAttributeNames = item.Update.expressionAttributeNames;
        expressionAttributeValues = item.Update.expressionAttributeValues;
        returnValuesOnConditionCheckFailure =
          item.Update.returnValuesOnConditionCheckFailure;
      } else if (item.Delete) {
        operation = "Delete";
        tableName = item.Delete.tableName;
        key = item.Delete.key;
        conditionExpression = item.Delete.conditionExpression;
        expressionAttributeNames = item.Delete.expressionAttributeNames;
        expressionAttributeValues = item.Delete.expressionAttributeValues;
        returnValuesOnConditionCheckFailure =
          item.Delete.returnValuesOnConditionCheckFailure;
      } else if (item.ConditionCheck) {
        operation = "ConditionCheck";
        tableName = item.ConditionCheck.tableName;
        key = item.ConditionCheck.key;
        conditionExpression = item.ConditionCheck.conditionExpression;
        expressionAttributeNames = item.ConditionCheck.expressionAttributeNames;
        expressionAttributeValues = item.ConditionCheck.expressionAttributeValues;
        returnValuesOnConditionCheckFailure =
          item.ConditionCheck.returnValuesOnConditionCheckFailure;
      } else {
        throw new Error("Invalid transaction item");
      }

      // Determine which shard this item belongs to
      const partitionKey = metadataStore.getPartitionKeyValue(tableName, key);
      const shardIndex = this.getShardIndex(partitionKey, shards.length);

      const prepareRequest: PrepareRequest = {
        transactionId,
        timestamp,
        tableName,
        operation,
        key,
        item: itemData,
        updateExpression,
        conditionExpression,
        expressionAttributeNames,
        expressionAttributeValues,
        returnValuesOnConditionCheckFailure,
      };

      const commitRequest: CommitRequest = {
        transactionId,
        timestamp,
        tableName,
        operation,
        key,
        item: itemData,
        updateExpression,
        expressionAttributeNames,
        expressionAttributeValues,
      };

      operations.push({
        shardIndex,
        prepareRequest,
        commitRequest,
        releaseKey: key,
      });
    }

    return operations;
  }

  // Phase 2: Commit on all shards with retry
  private async commitAllShards(
    operations: Array<{
      shardIndex: number;
      commitRequest: CommitRequest;
    }>,
    shards: Shard[],
    transactionId: string,
    timestamp: number
  ): Promise<void> {
    // Commit operations must succeed - retry with exponential backoff
    const MAX_RETRIES = 10;
    const INITIAL_DELAY = 100;

    for (const op of operations) {
      const shard = shards[op.shardIndex];
      if (!shard) {
        throw new Error(`Shard ${op.shardIndex} not found`);
      }

      let retries = 0;
      let delay = INITIAL_DELAY;

      while (true) {
        try {
          await shard.commit(op.commitRequest);
          break; // Success
        } catch (error: any) {
          retries++;
          if (retries >= MAX_RETRIES) {
            // In production, this would be handled by a recovery manager
            // For now, we log and continue retrying
            console.error(
              `Failed to commit on shard ${op.shardIndex} after ${retries} retries:`,
              error
            );
          }

          // Exponential backoff
          await new Promise((resolve) => setTimeout(resolve, delay));
          delay = Math.min(delay * 2, 5000);
        }
      }
    }
  }

  // Release locks on all shards
  private async releaseAllShards(
    operations: Array<{
      shardIndex: number;
      prepareRequest: PrepareRequest;
      releaseKey: DynamoDBItem;
    }>,
    shards: Shard[],
    transactionId: string
  ): Promise<void> {
    // Group release requests by shard
    const releaseByShardMap = new Map<
      number,
      { tableName: string; keys: DynamoDBItem[] }
    >();

    for (const op of operations) {
      if (!releaseByShardMap.has(op.shardIndex)) {
        releaseByShardMap.set(op.shardIndex, {
          tableName: op.prepareRequest.tableName,
          keys: [],
        });
      }
      releaseByShardMap.get(op.shardIndex)!.keys.push(op.releaseKey);
    }

    // Send release requests in parallel
    const releasePromises = Array.from(releaseByShardMap.entries()).map(
      async ([shardIndex, data]) => {
        const shard = shards[shardIndex];
        if (!shard) return;

        const releaseRequest: ReleaseRequest = {
          transactionId,
          tableName: data.tableName,
          keys: data.keys,
        };

        try {
          await shard.release(releaseRequest);
        } catch (error) {
          // Ignore release errors (best effort)
          console.error(`Failed to release on shard ${shardIndex}:`, error);
        }
      }
    );

    await Promise.all(releasePromises);
  }

  // Transaction ledger operations

  private recordTransaction(record: TransactionRecord): void {
    this.db.run(
      `INSERT INTO transaction_ledger
       (transaction_id, state, timestamp, client_request_token, items, created_at, completed_at, cancellation_reasons)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        record.transactionId,
        record.state,
        record.timestamp,
        record.clientRequestToken || null,
        JSON.stringify(record.items),
        record.createdAt,
        record.completedAt || null,
        record.cancellationReasons
          ? JSON.stringify(record.cancellationReasons)
          : null,
      ]
    );
  }

  private updateTransactionState(
    transactionId: string,
    state: "PREPARING" | "COMMITTING" | "COMMITTED" | "CANCELLED",
    cancellationReasons?: CancellationReason[]
  ): void {
    const completedAt = ["COMMITTED", "CANCELLED"].includes(state)
      ? Date.now()
      : null;

    this.db.run(
      `UPDATE transaction_ledger
       SET state = ?, completed_at = ?, cancellation_reasons = ?
       WHERE transaction_id = ?`,
      [
        state,
        completedAt,
        cancellationReasons ? JSON.stringify(cancellationReasons) : null,
        transactionId,
      ]
    );
  }

  // Helper methods

  private getShardIndex(partitionKey: string, shardCount: number): number {
    // Use Web Crypto API for consistent hashing
    // For now, use a simple hash
    let hash = 0;
    for (let i = 0; i < partitionKey.length; i++) {
      hash = (hash << 5) - hash + partitionKey.charCodeAt(i);
      hash = hash & hash; // Convert to 32-bit integer
    }
    return Math.abs(hash) % shardCount;
  }

  private getKeyString(key: any): string {
    const keyAttrs = Object.keys(key).sort();
    return keyAttrs.map((attr) => JSON.stringify(key[attr])).join("#");
  }

  private applyProjection(
    item: DynamoDBItem,
    projectionExpression: string,
    expressionAttributeNames?: Record<string, string>
  ): DynamoDBItem {
    const attrs = projectionExpression.split(",").map((a) => a.trim());
    const projected: DynamoDBItem = {};

    for (const attr of attrs) {
      let attrName = attr;
      if (expressionAttributeNames && attr.startsWith("#")) {
        const resolved = expressionAttributeNames[attr];
        if (resolved) attrName = resolved;
      }
      if (attrName && attrName in item) {
        projected[attrName] = item[attrName];
      }
    }

    return projected;
  }

  private cleanIdempotencyCache(): void {
    const cutoff = Date.now() - this.CACHE_TTL_MS;
    for (const [token, entry] of this.idempotencyCache.entries()) {
      if (entry.timestamp < cutoff) {
        this.idempotencyCache.delete(token);
      }
    }
  }

  private cleanup(): void {
    // Clean up old completed transactions (keep for 10 minutes)
    const cutoff = Date.now() - this.CACHE_TTL_MS;
    this.db.run(
      "DELETE FROM transaction_ledger WHERE completed_at IS NOT NULL AND completed_at < ?",
      [cutoff]
    );

    // Clean idempotency cache
    this.cleanIdempotencyCache();
  }

  close() {
    this.db.close();
  }
}
