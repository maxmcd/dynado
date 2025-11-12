// Shard: Single storage node implementing DynamoDB's 2PC protocol
// In DO architecture, each instance would be a separate Durable Object

import { Database } from "bun:sqlite";
import type {
  DynamoDBItem,
  PrepareRequest,
  PrepareResponse,
  CommitRequest,
  ReleaseRequest,
} from "./types.ts";
import {
  evaluateConditionExpression,
  applyUpdateExpressionToItem,
} from "./expression-parser/index.ts";

export class Shard {
  private db: Database;
  private shardIndex: number;

  constructor(dbPath: string, shardIndex: number) {
    this.db = new Database(dbPath);
    this.shardIndex = shardIndex;

    // Create items table with transaction metadata fields
    this.db.run(`
      CREATE TABLE IF NOT EXISTS items (
        table_name TEXT NOT NULL,
        partition_key TEXT NOT NULL,
        sort_key TEXT NOT NULL DEFAULT '',
        item_data TEXT NOT NULL,
        ongoing_transaction_id TEXT,
        last_update_timestamp INTEGER NOT NULL DEFAULT 0,
        lsn INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (table_name, partition_key, sort_key)
      )
    `);

    // Index for table scans
    this.db.run(
      `CREATE INDEX IF NOT EXISTS idx_items_table ON items(table_name)`
    );

    // Index for range queries on sort key
    this.db.run(
      `CREATE INDEX IF NOT EXISTS idx_items_range ON items(table_name, partition_key, sort_key)`
    );
  }

  // Phase 1 of 2PC: Prepare
  async prepare(req: PrepareRequest): Promise<PrepareResponse> {
    // Serialize request to simulate DO boundary
    req = JSON.parse(JSON.stringify(req));

    const partitionKey = req.partitionKeyValue;
    const sortKey = req.sortKeyValue;

    // Read current item - empty string means no sort key
    const query = `SELECT item_data, ongoing_transaction_id, last_update_timestamp, lsn
                   FROM items
                   WHERE table_name = ? AND partition_key = ? AND sort_key = ?`;

    const result = this.db
      .query(query)
      .get(req.tableName, partitionKey, sortKey) as any;

    let currentItem: DynamoDBItem | null = null;
    let currentLsn = 0;
    let currentTimestamp = 0;
    let ongoingTxId: string | null = null;

    if (result) {
      currentItem = JSON.parse(result.item_data);
      currentLsn = result.lsn;
      currentTimestamp = result.last_update_timestamp;
      ongoingTxId = result.ongoing_transaction_id;
    }

    // Validate timestamp ordering (DynamoDB's serialization mechanism)
    if (req.timestamp <= currentTimestamp) {
      return {
        accepted: false,
        reason: "TimestampConflict",
        message: `Transaction timestamp ${req.timestamp} is not greater than item's last update ${currentTimestamp}`,
      };
    }

    // Check for conflicting transaction
    if (ongoingTxId && ongoingTxId !== req.transactionId) {
      return {
        accepted: false,
        reason: "TransactionConflict",
        message: `Item is locked by transaction ${ongoingTxId}`,
      };
    }

    // Evaluate condition expression
    const conditionPassed = evaluateConditionExpression(
      currentItem,
      req.conditionExpression,
      req.expressionAttributeNames,
      req.expressionAttributeValues
    );

    if (!conditionPassed) {
      const response: PrepareResponse = {
        accepted: false,
        reason: "ConditionalCheckFailed",
        message: "The conditional request failed",
      };

      // Return old item if requested
      if (req.returnValuesOnConditionCheckFailure === "ALL_OLD") {
        response.item = currentItem || undefined;
      }

      return response;
    }

    // Lock the item for this transaction
    if (currentItem) {
      // Update existing item's lock
      this.db.run(
        `UPDATE items
         SET ongoing_transaction_id = ?
         WHERE table_name = ? AND partition_key = ? AND sort_key = ?`,
        [req.transactionId, req.tableName, partitionKey, sortKey]
      );
    } else {
      // For new items (Put operation), create placeholder with lock
      const placeholderItem = { ...req.key };
      this.db.run(
        `INSERT INTO items
         (table_name, partition_key, sort_key, item_data, ongoing_transaction_id, last_update_timestamp, lsn)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [
          req.tableName,
          partitionKey,
          sortKey,
          JSON.stringify(placeholderItem),
          req.transactionId,
          0,
          0,
        ]
      );
    }

    return {
      accepted: true,
      lsn: currentLsn,
    };
  }

  // Phase 2 of 2PC: Commit
  async commit(req: CommitRequest): Promise<void> {
    // Serialize request to simulate DO boundary
    req = JSON.parse(JSON.stringify(req));

    const partitionKey = req.partitionKeyValue;
    const sortKey = req.sortKeyValue;

    if (req.operation === "ConditionCheck") {
      // Just release the lock, no actual write
      this.db.run(
        `UPDATE items
         SET ongoing_transaction_id = NULL
         WHERE table_name = ? AND partition_key = ? AND sort_key = ? AND ongoing_transaction_id = ?`,
        [req.tableName, partitionKey, sortKey, req.transactionId]
      );
      return;
    }

    if (req.operation === "Delete") {
      // Delete the item
      this.db.run(
        `DELETE FROM items
         WHERE table_name = ? AND partition_key = ? AND sort_key = ? AND ongoing_transaction_id = ?`,
        [req.tableName, partitionKey, sortKey, req.transactionId]
      );
      return;
    }

    // For Put and Update operations
    let finalItem: DynamoDBItem;

    if (req.operation === "Put") {
      finalItem = req.item!;
    } else {
      // Update operation - apply update expression
      const result = this.db
        .query(
          `SELECT item_data, lsn
         FROM items
         WHERE table_name = ? AND partition_key = ? AND sort_key = ?`
        )
        .get(req.tableName, partitionKey, sortKey) as any;

      let currentItem = result ? JSON.parse(result.item_data) : { ...req.key };

      // Apply update expression
      finalItem = this.applyUpdateExpression(
        currentItem,
        req.updateExpression!,
        req.expressionAttributeNames,
        req.expressionAttributeValues
      );
    }

    // Get current LSN to increment
    const result = this.db
      .query(
        `SELECT lsn FROM items WHERE table_name = ? AND partition_key = ? AND sort_key = ?`
      )
      .get(req.tableName, partitionKey, sortKey) as any;

    const newLsn = result ? result.lsn + 1 : 1;

    // Write item with updated metadata
    this.db.run(
      `INSERT OR REPLACE INTO items
       (table_name, partition_key, sort_key, item_data, ongoing_transaction_id, last_update_timestamp, lsn)
       VALUES (?, ?, ?, ?, NULL, ?, ?)`,
      [
        req.tableName,
        partitionKey,
        sortKey,
        JSON.stringify(finalItem),
        req.timestamp,
        newLsn,
      ]
    );
  }

  // Release: Clean up transaction lock on abort
  async release(req: ReleaseRequest): Promise<void> {
    // Serialize request to simulate DO boundary
    req = JSON.parse(JSON.stringify(req));

    for (const keyValue of req.keyValues) {
      const partitionKey = keyValue.partitionKeyValue;
      const sortKey = keyValue.sortKeyValue;

      // Check if item was a placeholder (created during prepare)
      const result = this.db
        .query(
          `SELECT lsn FROM items WHERE table_name = ? AND partition_key = ? AND sort_key = ?`
        )
        .get(req.tableName, partitionKey, sortKey) as any;

      if (result && result.lsn === 0) {
        // Placeholder item - delete it
        this.db.run(
          `DELETE FROM items
           WHERE table_name = ? AND partition_key = ? AND sort_key = ? AND ongoing_transaction_id = ?`,
          [req.tableName, partitionKey, sortKey, req.transactionId]
        );
      } else {
        // Real item - just clear the lock
        this.db.run(
          `UPDATE items
           SET ongoing_transaction_id = NULL
           WHERE table_name = ? AND partition_key = ? AND sort_key = ? AND ongoing_transaction_id = ?`,
          [req.tableName, partitionKey, sortKey, req.transactionId]
        );
      }
    }
  }

  // Regular operations (non-transactional)

  async putItem(
    tableName: string,
    partitionKey: string,
    sortKey: string,
    item: DynamoDBItem
  ) {
    const itemData = JSON.stringify(item);
    // For non-transactional operations, use timestamp=0
    // This allows transactional writes (which use monotonically increasing timestamps > 0)
    // to always succeed over non-transactional writes, ensuring proper 2PC semantics
    // NOTE: This is a simplification - in production DynamoDB, all writes use timestamps
    const timestamp = 0;

    // Get current LSN for this item (if it exists)
    const currentLsnResult = this.db
      .query(
        `SELECT lsn FROM items WHERE table_name = ? AND partition_key = ? AND sort_key = ?`
      )
      .get(tableName, partitionKey, sortKey) as any;
    const newLsn = currentLsnResult ? currentLsnResult.lsn + 1 : 1;

    this.db.run(
      `INSERT OR REPLACE INTO items
       (table_name, partition_key, sort_key, item_data, ongoing_transaction_id, last_update_timestamp, lsn)
       VALUES (?, ?, ?, ?, NULL, ?, ?)`,
      [tableName, partitionKey, sortKey, itemData, timestamp, newLsn]
    );
  }

  async getItem(
    tableName: string,
    partitionKey: string,
    sortKey: string
  ): Promise<DynamoDBItem | null> {
    const result = this.db
      .query(
        `SELECT item_data FROM items WHERE table_name = ? AND partition_key = ? AND sort_key = ?`
      )
      .get(tableName, partitionKey, sortKey) as any;

    return result ? JSON.parse(result.item_data) : null;
  }

  async deleteItem(
    tableName: string,
    partitionKey: string,
    sortKey: string
  ): Promise<DynamoDBItem | null> {
    const item = await this.getItem(tableName, partitionKey, sortKey);
    if (!item) return null;

    this.db.run(
      "DELETE FROM items WHERE table_name = ? AND partition_key = ? AND sort_key = ?",
      [tableName, partitionKey, sortKey]
    );

    return item;
  }

  async scanTable(tableName: string): Promise<DynamoDBItem[]> {
    const results = this.db
      .query("SELECT item_data FROM items WHERE table_name = ?")
      .all(tableName) as any[];

    return results.map((r: any) => JSON.parse(r.item_data));
  }

  async getItemCount(tableName: string): Promise<number> {
    const result = this.db
      .query("SELECT COUNT(*) as count FROM items WHERE table_name = ?")
      .get(tableName) as any;

    return result.count;
  }

  async deleteAllTableItems(tableName: string): Promise<void> {
    this.db.run("DELETE FROM items WHERE table_name = ?", [tableName]);
  }

  async query(
    tableName: string,
    partitionKeyValue: string,
    sortKeyCondition?: import("./types.ts").SortKeyCondition,
    limit?: number,
    scanIndexForward: boolean = true,
    exclusiveStartKey?: { partitionKeyValue: string; sortKeyValue: string }
  ): Promise<{
    items: DynamoDBItem[];
    lastEvaluatedKey?: { partitionKeyValue: string; sortKeyValue: string };
    count: number;
    scannedCount: number;
  }> {
    // Build SQL query based on sort key condition
    let sql = `SELECT item_data, sort_key FROM items WHERE table_name = ? AND partition_key = ?`;
    const params: any[] = [tableName, partitionKeyValue];

    // Add sort key condition if provided
    if (sortKeyCondition) {
      switch (sortKeyCondition.operator) {
        case "=":
          sql += ` AND sort_key = ?`;
          params.push(JSON.stringify(sortKeyCondition.value));
          break;
        case "<":
          sql += ` AND sort_key < ?`;
          params.push(JSON.stringify(sortKeyCondition.value));
          break;
        case ">":
          sql += ` AND sort_key > ?`;
          params.push(JSON.stringify(sortKeyCondition.value));
          break;
        case "<=":
          sql += ` AND sort_key <= ?`;
          params.push(JSON.stringify(sortKeyCondition.value));
          break;
        case ">=":
          sql += ` AND sort_key >= ?`;
          params.push(JSON.stringify(sortKeyCondition.value));
          break;
        case "BETWEEN":
          sql += ` AND sort_key BETWEEN ? AND ?`;
          params.push(JSON.stringify(sortKeyCondition.value));
          params.push(JSON.stringify(sortKeyCondition.value2));
          break;
        case "begins_with":
          // For begins_with, we use LIKE with the value as prefix
          // Since sort keys are JSON.stringify'd, we need to match the stringified version
          // For example, {S: "PROD"} becomes {"S":"PROD"}
          // We want to match any sort key where the inner string value starts with "PROD"
          const valueStr = JSON.stringify(sortKeyCondition.value);
          // Parse the value to get the actual string content
          // For {S: "PROD"}, we want to extract "PROD" and create pattern {"S":"PROD%
          const valueObj = sortKeyCondition.value;
          if (valueObj.S !== undefined) {
            // String type - extract the string value
            const prefix = valueObj.S;
            // Build pattern: {"S":"PREFIX% to match {"S":"PREFIX-001"}, etc.
            const likePattern = `{"S":"${prefix}%`;
            sql += ` AND sort_key LIKE ?`;
            params.push(likePattern);
          } else {
            // Fallback for other types
            const likePattern = valueStr.slice(0, -1) + "%";
            sql += ` AND sort_key LIKE ?`;
            params.push(likePattern);
          }
          break;
      }
    }

    // Handle pagination with exclusiveStartKey
    if (exclusiveStartKey) {
      sql += ` AND sort_key > ?`;
      params.push(exclusiveStartKey.sortKeyValue);
    }

    // Add sort order
    sql += ` ORDER BY sort_key ${scanIndexForward ? "ASC" : "DESC"}`;

    // Add limit (fetch one extra to determine if there are more results)
    const fetchLimit = limit ? limit + 1 : undefined;
    if (fetchLimit) {
      sql += ` LIMIT ?`;
      params.push(fetchLimit);
    }

    // Execute query
    const results = this.db.query(sql).all(...params) as any[];

    // Parse results
    const items: DynamoDBItem[] = [];
    let hasMore = false;

    for (let i = 0; i < results.length; i++) {
      if (limit && i >= limit) {
        hasMore = true;
        break;
      }
      items.push(JSON.parse(results[i].item_data));
    }

    // Determine last evaluated key for pagination
    let lastEvaluatedKey:
      | { partitionKeyValue: string; sortKeyValue: string }
      | undefined;
    if (hasMore && items.length > 0) {
      const lastItem = results[limit! - 1];
      lastEvaluatedKey = {
        partitionKeyValue,
        sortKeyValue: lastItem.sort_key,
      };
    }

    return {
      items,
      lastEvaluatedKey,
      count: items.length,
      scannedCount: items.length,
    };
  }

  // Helper methods
  private applyUpdateExpression(
    item: DynamoDBItem,
    updateExpression: string,
    expressionAttributeNames?: Record<string, string>,
    expressionAttributeValues?: Record<string, any>
  ): DynamoDBItem {
    return applyUpdateExpressionToItem(
      item,
      updateExpression,
      expressionAttributeNames,
      expressionAttributeValues
    );
  }

  close() {
    this.db.close();
  }
}
