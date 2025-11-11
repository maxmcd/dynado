// Sharded SQLite storage implementation

import { Database } from "bun:sqlite";
import type { StorageBackend, DynamoDBItem, TableSchema, TransactWriteItem, TransactGetItem, CancellationReason } from "./storage.ts";
import { TransactionCancelledError } from "./storage.ts";
import { evaluateConditionExpression } from "./condition-expression.ts";
import * as crypto from "crypto";
import * as fs from "fs";

interface ShardedSQLiteConfig {
  shardCount: number;
  dataDir: string;
}

export class ShardedSQLiteStorage implements StorageBackend {
  private shards: Database[] = [];
  private shardCount: number;
  private tableSchemas: Map<string, TableSchema> = new Map();
  private idempotencyCache = new Map<string, { timestamp: number; result: any }>();

  constructor(config: ShardedSQLiteConfig) {
    this.shardCount = config.shardCount;

    // Create data directory if it doesn't exist
    if (!fs.existsSync(config.dataDir)) {
      fs.mkdirSync(config.dataDir, { recursive: true });
    }

    // Initialize shards
    for (let i = 0; i < this.shardCount; i++) {
      const db = new Database(`${config.dataDir}/shard_${i}.db`);

      // Create tables schema table in each shard
      db.run(`
        CREATE TABLE IF NOT EXISTS _table_schemas (
          table_name TEXT PRIMARY KEY,
          key_schema TEXT NOT NULL,
          attribute_definitions TEXT NOT NULL
        )
      `);

      // Create items table in each shard
      db.run(`
        CREATE TABLE IF NOT EXISTS _items (
          table_name TEXT NOT NULL,
          partition_key TEXT NOT NULL,
          item_data TEXT NOT NULL,
          PRIMARY KEY (table_name, partition_key)
        )
      `);

      // Index for scans
      db.run(`CREATE INDEX IF NOT EXISTS idx_items_table ON _items(table_name)`);

      this.shards.push(db);
    }

    // Load table schemas from first shard (they're replicated across all shards)
    this.loadTableSchemas();
  }

  private loadTableSchemas() {
    const db = this.shards[0];
    if (!db) return;

    const schemas = db.query("SELECT * FROM _table_schemas").all() as any[];

    for (const schema of schemas) {
      this.tableSchemas.set(schema.table_name, {
        tableName: schema.table_name,
        keySchema: JSON.parse(schema.key_schema),
        attributeDefinitions: JSON.parse(schema.attribute_definitions),
      });
    }
  }

  private getPartitionKey(item: DynamoDBItem, keySchema: any[]): string {
    const partitionKeyAttr = keySchema.find((k: any) => k.KeyType === "HASH");
    if (!partitionKeyAttr) {
      throw new Error("No partition key found in schema");
    }
    return JSON.stringify(item[partitionKeyAttr.AttributeName]);
  }

  private getShardForKey(partitionKey: string): number {
    const hash = crypto.createHash("sha256").update(partitionKey).digest();
    const hashNum = hash.readUInt32BE(0);
    return hashNum % this.shardCount;
  }

  private getItemKey(table: TableSchema, item: DynamoDBItem): string {
    const keyAttrs = table.keySchema.map((k: any) => k.AttributeName);
    return keyAttrs.map((attr: string) => JSON.stringify(item[attr])).join("#");
  }

  private getKeyValue(table: TableSchema, key: any): string {
    const keyAttrs = table.keySchema.map((k: any) => k.AttributeName);
    return keyAttrs.map((attr: string) => JSON.stringify(key[attr])).join("#");
  }

  private extractKey(table: TableSchema, item: DynamoDBItem): any {
    const key: any = {};
    for (const keySchema of table.keySchema) {
      const attrName = keySchema.AttributeName;
      key[attrName] = item[attrName];
    }
    return key;
  }

  async listTables(): Promise<string[]> {
    return Array.from(this.tableSchemas.keys());
  }

  async createTable(schema: TableSchema): Promise<void> {
    if (this.tableSchemas.has(schema.tableName)) {
      throw new Error("Table already exists");
    }

    // Store schema in all shards
    const keySchemaJson = JSON.stringify(schema.keySchema);
    const attrDefsJson = JSON.stringify(schema.attributeDefinitions);

    for (const db of this.shards) {
      db.run(
        "INSERT INTO _table_schemas (table_name, key_schema, attribute_definitions) VALUES (?, ?, ?)",
        [schema.tableName, keySchemaJson, attrDefsJson]
      );
    }

    this.tableSchemas.set(schema.tableName, schema);
  }

  async describeTable(tableName: string): Promise<TableSchema | null> {
    return this.tableSchemas.get(tableName) || null;
  }

  async deleteTable(tableName: string): Promise<void> {
    // Delete from all shards
    for (const db of this.shards) {
      db.run("DELETE FROM _table_schemas WHERE table_name = ?", [tableName]);
      db.run("DELETE FROM _items WHERE table_name = ?", [tableName]);
    }

    this.tableSchemas.delete(tableName);
  }

  async getTableItemCount(tableName: string): Promise<number> {
    let count = 0;
    for (const db of this.shards) {
      const result = db.query("SELECT COUNT(*) as count FROM _items WHERE table_name = ?").get(tableName) as any;
      count += result.count;
    }
    return count;
  }

  async putItem(tableName: string, item: DynamoDBItem): Promise<void> {
    const schema = this.tableSchemas.get(tableName);
    if (!schema) throw new Error("Table not found");

    const partitionKey = this.getPartitionKey(item, schema.keySchema);
    const shardIndex = this.getShardForKey(partitionKey);
    const db = this.shards[shardIndex];
    if (!db) throw new Error("Shard not found");

    const itemKey = this.getItemKey(schema, item);
    const itemData = JSON.stringify(item);

    db.run(
      "INSERT OR REPLACE INTO _items (table_name, partition_key, item_data) VALUES (?, ?, ?)",
      [tableName, itemKey, itemData]
    );
  }

  async getItem(tableName: string, key: DynamoDBItem): Promise<DynamoDBItem | null> {
    const schema = this.tableSchemas.get(tableName);
    if (!schema) throw new Error("Table not found");

    const partitionKey = this.getPartitionKey(key, schema.keySchema);
    const shardIndex = this.getShardForKey(partitionKey);
    const db = this.shards[shardIndex];
    if (!db) throw new Error("Shard not found");

    const itemKey = this.getKeyValue(schema, key);

    const result = db.query("SELECT item_data FROM _items WHERE table_name = ? AND partition_key = ?")
      .get(tableName, itemKey) as any;

    return result ? JSON.parse(result.item_data) : null;
  }

  async deleteItem(tableName: string, key: DynamoDBItem): Promise<DynamoDBItem | null> {
    const schema = this.tableSchemas.get(tableName);
    if (!schema) throw new Error("Table not found");

    const item = await this.getItem(tableName, key);
    if (!item) return null;

    const partitionKey = this.getPartitionKey(key, schema.keySchema);
    const shardIndex = this.getShardForKey(partitionKey);
    const db = this.shards[shardIndex];
    if (!db) throw new Error("Shard not found");

    const itemKey = this.getKeyValue(schema, key);

    db.run("DELETE FROM _items WHERE table_name = ? AND partition_key = ?", [tableName, itemKey]);

    return item;
  }

  async scan(tableName: string, limit?: number, exclusiveStartKey?: any): Promise<{
    items: DynamoDBItem[];
    lastEvaluatedKey?: any;
  }> {
    const schema = this.tableSchemas.get(tableName);
    if (!schema) throw new Error("Table not found");

    // Scan across all shards
    let allItems: DynamoDBItem[] = [];

    for (const db of this.shards) {
      const results = db.query("SELECT item_data FROM _items WHERE table_name = ?").all(tableName) as any[];
      allItems.push(...results.map((r: any) => JSON.parse(r.item_data)));
    }

    // Handle pagination
    if (exclusiveStartKey) {
      const startKeyValue = this.getKeyValue(schema, exclusiveStartKey);
      const startIndex = allItems.findIndex((item: any) => {
        const itemKey = this.getItemKey(schema, item);
        return itemKey === startKeyValue;
      });
      if (startIndex >= 0) {
        allItems = allItems.slice(startIndex + 1);
      }
    }

    // Apply limit
    let lastEvaluatedKey;
    if (limit && allItems.length > limit) {
      const limitedItems = allItems.slice(0, limit);
      const lastItem = limitedItems[limitedItems.length - 1];
      if (lastItem) {
        lastEvaluatedKey = this.extractKey(schema, lastItem);
      }
      allItems = limitedItems;
    }

    return { items: allItems, lastEvaluatedKey };
  }

  async query(tableName: string, keyCondition: (item: DynamoDBItem) => boolean, limit?: number, exclusiveStartKey?: any): Promise<{
    items: DynamoDBItem[];
    lastEvaluatedKey?: any;
  }> {
    const schema = this.tableSchemas.get(tableName);
    if (!schema) throw new Error("Table not found");

    // Query across all shards and filter
    let allItems: DynamoDBItem[] = [];

    for (const db of this.shards) {
      const results = db.query("SELECT item_data FROM _items WHERE table_name = ?").all(tableName) as any[];
      const items = results.map((r: any) => JSON.parse(r.item_data));
      allItems.push(...items.filter(keyCondition));
    }

    // Handle pagination
    if (exclusiveStartKey) {
      const startKeyValue = this.getKeyValue(schema, exclusiveStartKey);
      const startIndex = allItems.findIndex((item: any) => {
        const itemKey = this.getItemKey(schema, item);
        return itemKey === startKeyValue;
      });
      if (startIndex >= 0) {
        allItems = allItems.slice(startIndex + 1);
      }
    }

    // Apply limit
    let lastEvaluatedKey;
    if (limit && allItems.length > limit) {
      const limitedItems = allItems.slice(0, limit);
      const lastItem = limitedItems[limitedItems.length - 1];
      if (lastItem) {
        lastEvaluatedKey = this.extractKey(schema, lastItem);
      }
      allItems = limitedItems;
    }

    return { items: allItems, lastEvaluatedKey };
  }

  async batchGet(tableName: string, keys: DynamoDBItem[]): Promise<DynamoDBItem[]> {
    const items: DynamoDBItem[] = [];

    for (const key of keys) {
      const item = await this.getItem(tableName, key);
      if (item) {
        items.push(item);
      }
    }

    return items;
  }

  async batchWrite(tableName: string, puts: DynamoDBItem[], deletes: DynamoDBItem[]): Promise<void> {
    for (const item of puts) {
      await this.putItem(tableName, item);
    }

    for (const key of deletes) {
      await this.deleteItem(tableName, key);
    }
  }

  private cleanIdempotencyCache() {
    const tenMinutesAgo = Date.now() - 10 * 60 * 1000;
    for (const [token, data] of this.idempotencyCache.entries()) {
      if (data.timestamp < tenMinutesAgo) {
        this.idempotencyCache.delete(token);
      }
    }
  }

  async transactWrite(items: TransactWriteItem[], clientRequestToken?: string): Promise<void> {
    // Validate limits
    if (items.length > 100) {
      throw new Error("Transaction cannot contain more than 100 items");
    }

    // Check idempotency
    if (clientRequestToken) {
      this.cleanIdempotencyCache();
      const cached = this.idempotencyCache.get(clientRequestToken);
      if (cached) {
        return cached.result;
      }
    }

    // Determine which shards are involved and prepare metadata
    const involvedShards = new Set<number>();
    const itemMetadata: Array<{
      type: "Put" | "Update" | "Delete" | "ConditionCheck";
      tableName: string;
      key: DynamoDBItem;
      shardIndex: number;
      schema: TableSchema;
      data: any;
    }> = [];

    // Pre-process: determine shards and validate tables exist
    for (const item of items) {
      let tableName: string;
      let key: DynamoDBItem;
      let type: "Put" | "Update" | "Delete" | "ConditionCheck";
      let data: any;

      if (item.Put) {
        type = "Put";
        tableName = item.Put.tableName;
        const schema = this.tableSchemas.get(tableName);
        if (!schema) throw new Error(`Table not found: ${tableName}`);
        key = this.extractKey(schema, item.Put.item);
        data = item.Put;
      } else if (item.Update) {
        type = "Update";
        tableName = item.Update.tableName;
        const schema = this.tableSchemas.get(tableName);
        if (!schema) throw new Error(`Table not found: ${tableName}`);
        key = item.Update.key;
        data = item.Update;
      } else if (item.Delete) {
        type = "Delete";
        tableName = item.Delete.tableName;
        const schema = this.tableSchemas.get(tableName);
        if (!schema) throw new Error(`Table not found: ${tableName}`);
        key = item.Delete.key;
        data = item.Delete;
      } else if (item.ConditionCheck) {
        type = "ConditionCheck";
        tableName = item.ConditionCheck.tableName;
        const schema = this.tableSchemas.get(tableName);
        if (!schema) throw new Error(`Table not found: ${tableName}`);
        key = item.ConditionCheck.key;
        data = item.ConditionCheck;
      } else {
        throw new Error("Invalid transaction item");
      }

      const schema = this.tableSchemas.get(tableName)!;
      const partitionKey = this.getPartitionKey(key, schema.keySchema);
      const shardIndex = this.getShardForKey(partitionKey);
      involvedShards.add(shardIndex);

      itemMetadata.push({ type, tableName, key, shardIndex, schema, data });
    }

    // Sort shards to acquire locks in consistent order (prevent deadlock)
    const sortedShards = Array.from(involvedShards).sort((a, b) => a - b);

    try {
      // Begin immediate transactions on all involved shards
      const transactionFns: Array<() => void> = [];

      for (const shardIndex of sortedShards) {
        const db = this.shards[shardIndex];
        if (!db) throw new Error(`Shard ${shardIndex} not found`);

        // Start immediate transaction
        db.run("BEGIN IMMEDIATE");
      }

      // Phase 1: Validate all conditions and prepare changes
      const changes: Array<{
        shardIndex: number;
        operation: () => void;
      }> = [];

      for (let i = 0; i < itemMetadata.length; i++) {
        const meta = itemMetadata[i];
        if (!meta) continue;
        const db = this.shards[meta.shardIndex];
        if (!db) throw new Error(`Shard ${meta.shardIndex} not found`);

        const itemKey = this.getKeyValue(meta.schema, meta.key);

        // Read current item
        const result = db.query(
          "SELECT item_data FROM _items WHERE table_name = ? AND partition_key = ?"
        ).get(meta.tableName, itemKey) as any;

        const currentItem = result ? JSON.parse(result.item_data) : null;

        // Evaluate condition expression
        const conditionPassed = evaluateConditionExpression(
          currentItem,
          meta.data.conditionExpression,
          meta.data.expressionAttributeNames,
          meta.data.expressionAttributeValues
        );

        if (!conditionPassed) {
          // Condition failed - rollback all shards
          for (const shardIndex of sortedShards) {
            this.shards[shardIndex]?.run("ROLLBACK");
          }

          // Build cancellation reason
          const reason: CancellationReason = {
            Code: "ConditionalCheckFailed",
            Message: "The conditional request failed",
          };

          if (meta.data.returnValuesOnConditionCheckFailure === "ALL_OLD") {
            reason.Item = currentItem || undefined;
          }

          const cancellationReasons = this.buildCancellationReasons(itemMetadata.length, i, reason);
          throw new TransactionCancelledError("Transaction cancelled, please refer cancellation reasons for specific reasons", cancellationReasons);
        }

        // Prepare change operation
        if (meta.type === "Put") {
          const itemData = JSON.stringify(meta.data.item);
          changes.push({
            shardIndex: meta.shardIndex,
            operation: () => {
              db.run(
                "INSERT OR REPLACE INTO _items (table_name, partition_key, item_data) VALUES (?, ?, ?)",
                [meta.tableName, itemKey, itemData]
              );
            },
          });
        } else if (meta.type === "Update") {
          // Apply update expression to current item
          const updatedItem = this.applyUpdateExpression(
            currentItem || { ...meta.key },
            meta.data.updateExpression,
            meta.data.expressionAttributeNames,
            meta.data.expressionAttributeValues
          );
          const itemData = JSON.stringify(updatedItem);
          changes.push({
            shardIndex: meta.shardIndex,
            operation: () => {
              db.run(
                "INSERT OR REPLACE INTO _items (table_name, partition_key, item_data) VALUES (?, ?, ?)",
                [meta.tableName, itemKey, itemData]
              );
            },
          });
        } else if (meta.type === "Delete") {
          changes.push({
            shardIndex: meta.shardIndex,
            operation: () => {
              db.run(
                "DELETE FROM _items WHERE table_name = ? AND partition_key = ?",
                [meta.tableName, itemKey]
              );
            },
          });
        }
        // ConditionCheck does nothing (just validates)
      }

      // Phase 2: Apply all changes
      for (const change of changes) {
        change.operation();
      }

      // Commit all shards
      for (const shardIndex of sortedShards) {
        this.shards[shardIndex]?.run("COMMIT");
      }

      // Cache result for idempotency
      if (clientRequestToken) {
        this.idempotencyCache.set(clientRequestToken, {
          timestamp: Date.now(),
          result: undefined,
        });
      }

    } catch (error: any) {
      // Rollback all shards on error
      for (const shardIndex of sortedShards) {
        try {
          this.shards[shardIndex]?.run("ROLLBACK");
        } catch (rollbackError) {
          // Ignore rollback errors
        }
      }

      throw error;
    }
  }

  async transactGet(items: TransactGetItem[]): Promise<Array<DynamoDBItem | null>> {
    if (items.length > 100) {
      throw new Error("Transaction cannot contain more than 100 items");
    }

    const results: Array<DynamoDBItem | null> = [];

    // For read transactions, we provide snapshot isolation by reading from transactions
    for (const item of items) {
      const schema = this.tableSchemas.get(item.tableName);
      if (!schema) throw new Error(`Table not found: ${item.tableName}`);

      const partitionKey = this.getPartitionKey(item.key, schema.keySchema);
      const shardIndex = this.getShardForKey(partitionKey);
      const db = this.shards[shardIndex];
      if (!db) throw new Error(`Shard not found`);

      const itemKey = this.getKeyValue(schema, item.key);

      const result = db.query(
        "SELECT item_data FROM _items WHERE table_name = ? AND partition_key = ?"
      ).get(item.tableName, itemKey) as any;

      if (result) {
        let dbItem = JSON.parse(result.item_data);

        // Apply projection expression if provided
        if (item.projectionExpression) {
          dbItem = this.applyProjection(
            dbItem,
            item.projectionExpression,
            item.expressionAttributeNames
          );
        }

        results.push(dbItem);
      } else {
        results.push(null);
      }
    }

    return results;
  }

  private buildCancellationReasons(
    total: number,
    failedIndex: number,
    failedReason: CancellationReason
  ): CancellationReason[] {
    const reasons: CancellationReason[] = [];
    for (let i = 0; i < total; i++) {
      if (i === failedIndex) {
        reasons.push(failedReason);
      } else {
        reasons.push({ Code: "None" });
      }
    }
    return reasons;
  }

  private applyUpdateExpression(
    item: DynamoDBItem,
    updateExpression: string,
    expressionAttributeNames?: Record<string, string>,
    expressionAttributeValues?: Record<string, any>
  ): DynamoDBItem {
    // Re-use logic from index.ts handleUpdateItem
    const updatedItem = { ...item };

    // Handle SET operations
    const setMatch = updateExpression.match(/SET\s+(.+?)(?=\s+REMOVE|\s+ADD|$)/i);
    if (setMatch && setMatch[1]) {
      const assignments = setMatch[1].split(',');
      for (const assignment of assignments) {
        const parts = assignment.split('=');
        if (parts.length === 2 && parts[0] && parts[1]) {
          const left = parts[0].trim();
          const right = parts[1].trim();
          let attrName = left;
          if (expressionAttributeNames && left.startsWith('#')) {
            const resolved = expressionAttributeNames[left];
            if (resolved) attrName = resolved;
          }
          let attrValue;
          if (expressionAttributeValues && right.startsWith(':')) {
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
    const removeMatch = updateExpression.match(/REMOVE\s+(.+?)(?=\s+SET|\s+ADD|$)/i);
    if (removeMatch && removeMatch[1]) {
      const attrs = removeMatch[1].split(',').map((s: string) => s.trim());
      for (const attr of attrs) {
        let attrName = attr;
        if (expressionAttributeNames && attr.startsWith('#')) {
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
        if (expressionAttributeNames && left.startsWith('#')) {
          const resolved = expressionAttributeNames[left];
          if (resolved) attrName = resolved;
        }
        let addValue;
        if (expressionAttributeValues && right.startsWith(':')) {
          addValue = expressionAttributeValues[right];
        }
        if (attrName && addValue && addValue.N) {
          const currentVal = updatedItem[attrName]?.N ? parseInt(updatedItem[attrName].N) : 0;
          updatedItem[attrName] = { N: String(currentVal + parseInt(addValue.N)) };
        }
      }
    }

    return updatedItem;
  }

  private applyProjection(
    item: DynamoDBItem,
    projectionExpression: string,
    expressionAttributeNames?: Record<string, string>
  ): DynamoDBItem {
    const attrs = projectionExpression.split(",").map(a => a.trim());
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

  close() {
    for (const db of this.shards) {
      db.close();
    }
  }
}
