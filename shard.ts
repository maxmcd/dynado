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
import { evaluateConditionExpression } from "./condition-expression.ts";

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
        item_data TEXT NOT NULL,
        ongoing_transaction_id TEXT,
        last_update_timestamp INTEGER NOT NULL DEFAULT 0,
        lsn INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (table_name, partition_key)
      )
    `);

    // Index for table scans
    this.db.run(
      `CREATE INDEX IF NOT EXISTS idx_items_table ON items(table_name)`
    );
  }

  // Phase 1 of 2PC: Prepare
  async prepare(req: PrepareRequest): Promise<PrepareResponse> {
    // Serialize request to simulate DO boundary
    req = JSON.parse(JSON.stringify(req));

    const itemKey = this.getKeyString(req.key);

    // Read current item
    const result = this.db
      .query(
        `SELECT item_data, ongoing_transaction_id, last_update_timestamp, lsn
         FROM items
         WHERE table_name = ? AND partition_key = ?`
      )
      .get(req.tableName, itemKey) as any;

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
         WHERE table_name = ? AND partition_key = ?`,
        [req.transactionId, req.tableName, itemKey]
      );
    } else {
      // For new items (Put operation), create placeholder with lock
      const placeholderItem = { ...req.key };
      this.db.run(
        `INSERT INTO items
         (table_name, partition_key, item_data, ongoing_transaction_id, last_update_timestamp, lsn)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [
          req.tableName,
          itemKey,
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

    const itemKey = this.getKeyString(req.key);

    if (req.operation === "ConditionCheck") {
      // Just release the lock, no actual write
      this.db.run(
        `UPDATE items
         SET ongoing_transaction_id = NULL
         WHERE table_name = ? AND partition_key = ? AND ongoing_transaction_id = ?`,
        [req.tableName, itemKey, req.transactionId]
      );
      return;
    }

    if (req.operation === "Delete") {
      // Delete the item
      this.db.run(
        `DELETE FROM items
         WHERE table_name = ? AND partition_key = ? AND ongoing_transaction_id = ?`,
        [req.tableName, itemKey, req.transactionId]
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
           WHERE table_name = ? AND partition_key = ?`
        )
        .get(req.tableName, itemKey) as any;

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
        `SELECT lsn FROM items WHERE table_name = ? AND partition_key = ?`
      )
      .get(req.tableName, itemKey) as any;

    const newLsn = result ? result.lsn + 1 : 1;

    // Write item with updated metadata
    this.db.run(
      `INSERT OR REPLACE INTO items
       (table_name, partition_key, item_data, ongoing_transaction_id, last_update_timestamp, lsn)
       VALUES (?, ?, ?, NULL, ?, ?)`,
      [req.tableName, itemKey, JSON.stringify(finalItem), req.timestamp, newLsn]
    );
  }

  // Release: Clean up transaction lock on abort
  async release(req: ReleaseRequest): Promise<void> {
    // Serialize request to simulate DO boundary
    req = JSON.parse(JSON.stringify(req));

    for (const key of req.keys) {
      const itemKey = this.getKeyString(key);

      // Check if item was a placeholder (created during prepare)
      const result = this.db
        .query(
          `SELECT lsn FROM items WHERE table_name = ? AND partition_key = ?`
        )
        .get(req.tableName, itemKey) as any;

      if (result && result.lsn === 0) {
        // Placeholder item - delete it
        this.db.run(
          `DELETE FROM items
           WHERE table_name = ? AND partition_key = ? AND ongoing_transaction_id = ?`,
          [req.tableName, itemKey, req.transactionId]
        );
      } else {
        // Real item - just clear the lock
        this.db.run(
          `UPDATE items
           SET ongoing_transaction_id = NULL
           WHERE table_name = ? AND partition_key = ? AND ongoing_transaction_id = ?`,
          [req.tableName, itemKey, req.transactionId]
        );
      }
    }
  }

  // Regular operations (non-transactional)

  async putItem(tableName: string, key: string, item: DynamoDBItem) {
    const itemData = JSON.stringify(item);
    // For non-transactional operations, set timestamp to 0 to avoid conflicts
    // In production, we'd use a proper timestamp ordering scheme
    this.db.run(
      `INSERT OR REPLACE INTO items
       (table_name, partition_key, item_data, ongoing_transaction_id, last_update_timestamp, lsn)
       VALUES (?, ?, ?, NULL, 0, COALESCE((SELECT lsn + 1 FROM items WHERE table_name = ? AND partition_key = ?), 1))`,
      [tableName, key, itemData, tableName, key]
    );
  }

  async getItem(tableName: string, key: string): Promise<DynamoDBItem | null> {
    const result = this.db
      .query(
        `SELECT item_data FROM items WHERE table_name = ? AND partition_key = ?`
      )
      .get(tableName, key) as any;

    return result ? JSON.parse(result.item_data) : null;
  }

  async deleteItem(
    tableName: string,
    key: string
  ): Promise<DynamoDBItem | null> {
    const item = await this.getItem(tableName, key);
    if (!item) return null;

    this.db.run(
      "DELETE FROM items WHERE table_name = ? AND partition_key = ?",
      [tableName, key]
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

  // Helper methods

  private getKeyString(key: any): string {
    // Create deterministic string representation of key
    const keyAttrs = Object.keys(key).sort();
    return keyAttrs.map((attr) => JSON.stringify(key[attr])).join("#");
  }

  private applyUpdateExpression(
    item: DynamoDBItem,
    updateExpression: string,
    expressionAttributeNames?: Record<string, string>,
    expressionAttributeValues?: Record<string, any>
  ): DynamoDBItem {
    const updatedItem = { ...item };

    // Handle SET operations
    const setMatch = updateExpression.match(/SET\s+(.+?)(?=\s+REMOVE|\s+ADD|$)/i);
    if (setMatch && setMatch[1]) {
      const assignments = setMatch[1].split(",");
      for (const assignment of assignments) {
        const parts = assignment.split("=");
        if (parts.length === 2 && parts[0] && parts[1]) {
          const left = parts[0].trim();
          const right = parts[1].trim();
          let attrName = left;
          if (expressionAttributeNames && left.startsWith("#")) {
            const resolved = expressionAttributeNames[left];
            if (resolved) attrName = resolved;
          }
          let attrValue;
          if (expressionAttributeValues && right.startsWith(":")) {
            attrValue = expressionAttributeValues[right];
          } else {
            attrValue = right;
          }
          if (attrName) {
            updatedItem[attrName] = attrValue;
          }
        }
      }
    }

    // Handle REMOVE operations
    const removeMatch = updateExpression.match(
      /REMOVE\s+(.+?)(?=\s+SET|\s+ADD|$)/i
    );
    if (removeMatch && removeMatch[1]) {
      const attrs = removeMatch[1].split(",").map((s: string) => s.trim());
      for (const attr of attrs) {
        let attrName = attr;
        if (expressionAttributeNames && attr.startsWith("#")) {
          const resolved = expressionAttributeNames[attr];
          if (resolved) attrName = resolved;
        }
        if (attrName) {
          delete updatedItem[attrName];
        }
      }
    }

    // Handle ADD operations (for numbers)
    const addMatch = updateExpression.match(/ADD\s+(.+?)(?=\s+SET|\s+REMOVE|$)/i);
    if (addMatch && addMatch[1]) {
      const parts = addMatch[1].trim().split(/\s+/);
      if (parts.length >= 2 && parts[0] && parts[1]) {
        const left = parts[0];
        const right = parts[1];
        let attrName = left;
        if (expressionAttributeNames && left.startsWith("#")) {
          const resolved = expressionAttributeNames[left];
          if (resolved) attrName = resolved;
        }
        let addValue;
        if (expressionAttributeValues && right.startsWith(":")) {
          addValue = expressionAttributeValues[right];
        }
        if (attrName && addValue && addValue.N) {
          const currentVal = updatedItem[attrName]?.N
            ? parseInt(updatedItem[attrName].N)
            : 0;
          updatedItem[attrName] = { N: String(currentVal + parseInt(addValue.N)) };
        }
      }
    }

    return updatedItem;
  }

  close() {
    this.db.close();
  }
}
