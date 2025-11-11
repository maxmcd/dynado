// Router: Routes requests to appropriate shards/components
// Simulates DO-to-DO routing that would happen in Cloudflare Workers

import type { DynamoDBItem, TransactWriteItem, TransactGetItem } from "./types.ts";
import type { Shard } from "./shard.ts";
import type { MetadataStore } from "./metadata-store.ts";
import type { TransactionCoordinator } from "./coordinator.ts";

export class Router {
  private shards: Shard[];
  private metadataStore: MetadataStore;
  private coordinator: TransactionCoordinator;

  constructor(
    shards: Shard[],
    metadataStore: MetadataStore,
    coordinator: TransactionCoordinator
  ) {
    this.shards = shards;
    this.metadataStore = metadataStore;
    this.coordinator = coordinator;
  }

  // Table operations - route to metadata store

  async createTable(
    tableName: string,
    keySchema: any[],
    attributeDefinitions: any[]
  ): Promise<void> {
    await this.metadataStore.createTable({
      tableName,
      keySchema,
      attributeDefinitions,
    });
  }

  async describeTable(tableName: string) {
    return await this.metadataStore.describeTable(tableName);
  }

  async listTables(): Promise<string[]> {
    return await this.metadataStore.listTables();
  }

  async deleteTable(tableName: string): Promise<void> {
    // Delete from metadata
    await this.metadataStore.deleteTable(tableName);

    // Delete from all shards
    await Promise.all(
      this.shards.map((shard) => shard.deleteAllTableItems(tableName))
    );
  }

  async getTableItemCount(tableName: string): Promise<number> {
    // Fan out to all shards and sum
    const counts = await Promise.all(
      this.shards.map((shard) => shard.getItemCount(tableName))
    );
    return counts.reduce((sum, count) => sum + count, 0);
  }

  // Item operations - route to specific shard based on partition key

  async putItem(tableName: string, item: DynamoDBItem): Promise<void> {
    const { shard, keyString } = await this.routeToShard(tableName, item);
    await shard.putItem(tableName, keyString, item);
  }

  async getItem(
    tableName: string,
    key: DynamoDBItem
  ): Promise<DynamoDBItem | null> {
    const { shard, keyString } = await this.routeToShard(tableName, key);
    return await shard.getItem(tableName, keyString);
  }

  async deleteItem(
    tableName: string,
    key: DynamoDBItem
  ): Promise<DynamoDBItem | null> {
    const { shard, keyString } = await this.routeToShard(tableName, key);
    return await shard.deleteItem(tableName, keyString);
  }

  // Scan/Query operations - fan out to all shards

  async scan(
    tableName: string,
    limit?: number,
    exclusiveStartKey?: any
  ): Promise<{
    items: DynamoDBItem[];
    lastEvaluatedKey?: any;
  }> {
    // Fan out to all shards in parallel
    const shardResults = await Promise.all(
      this.shards.map((shard) => shard.scanTable(tableName))
    );

    // Flatten results
    let allItems = shardResults.flat();

    // Handle pagination (simplified)
    if (exclusiveStartKey) {
      const schema = await this.metadataStore.describeTable(tableName);
      if (schema) {
        const startKeyValue = this.getKeyString(exclusiveStartKey);
        const startIndex = allItems.findIndex((item: any) => {
          const itemKey = this.extractKey(schema.keySchema, item);
          const itemKeyString = this.getKeyString(itemKey);
          return itemKeyString === startKeyValue;
        });
        if (startIndex >= 0) {
          allItems = allItems.slice(startIndex + 1);
        }
      }
    }

    // Apply limit
    let lastEvaluatedKey;
    if (limit && allItems.length > limit) {
      const limitedItems = allItems.slice(0, limit);
      const lastItem = limitedItems[limitedItems.length - 1];
      if (lastItem) {
        const schema = await this.metadataStore.describeTable(tableName);
        if (schema) {
          lastEvaluatedKey = this.extractKey(schema.keySchema, lastItem);
        }
      }
      allItems = limitedItems;
    }

    return {
      items: allItems,
      lastEvaluatedKey,
    };
  }

  async query(
    tableName: string,
    keyCondition: (item: DynamoDBItem) => boolean,
    limit?: number,
    exclusiveStartKey?: any
  ): Promise<{
    items: DynamoDBItem[];
    lastEvaluatedKey?: any;
  }> {
    // For simplicity, scan all shards and filter
    // In production, we'd optimize to only query relevant shards based on partition key
    const scanResult = await this.scan(tableName, undefined, exclusiveStartKey);
    let items = scanResult.items.filter(keyCondition);

    // Apply limit
    let lastEvaluatedKey;
    if (limit && items.length > limit) {
      const limitedItems = items.slice(0, limit);
      const lastItem = limitedItems[limitedItems.length - 1];
      if (lastItem) {
        const schema = await this.metadataStore.describeTable(tableName);
        if (schema) {
          lastEvaluatedKey = this.extractKey(schema.keySchema, lastItem);
        }
      }
      items = limitedItems;
    }

    return {
      items,
      lastEvaluatedKey,
    };
  }

  // Batch operations

  async batchGet(
    tableName: string,
    keys: DynamoDBItem[]
  ): Promise<DynamoDBItem[]> {
    // Group keys by shard
    const keysByShard = new Map<number, Array<{ key: DynamoDBItem; keyString: string }>>();

    for (const key of keys) {
      const { shardIndex, keyString } = await this.routeToShard(tableName, key);
      if (!keysByShard.has(shardIndex)) {
        keysByShard.set(shardIndex, []);
      }
      keysByShard.get(shardIndex)!.push({ key, keyString });
    }

    // Fetch from each shard in parallel
    const results = await Promise.all(
      Array.from(keysByShard.entries()).map(async ([shardIndex, items]) => {
        const shard = this.shards[shardIndex];
        if (!shard) return [];

        const shardResults = await Promise.all(
          items.map((item) => shard.getItem(tableName, item.keyString))
        );

        return shardResults.filter((item) => item !== null) as DynamoDBItem[];
      })
    );

    return results.flat();
  }

  async batchWrite(
    tableName: string,
    puts: DynamoDBItem[],
    deletes: DynamoDBItem[]
  ): Promise<void> {
    // Group operations by shard
    const putsByShard = new Map<number, Array<{ item: DynamoDBItem; keyString: string }>>();
    const deletesByShard = new Map<number, Array<{ key: DynamoDBItem; keyString: string }>>();

    for (const item of puts) {
      const { shardIndex, keyString } = await this.routeToShard(tableName, item);
      if (!putsByShard.has(shardIndex)) {
        putsByShard.set(shardIndex, []);
      }
      putsByShard.get(shardIndex)!.push({ item, keyString });
    }

    for (const key of deletes) {
      const { shardIndex, keyString } = await this.routeToShard(tableName, key);
      if (!deletesByShard.has(shardIndex)) {
        deletesByShard.set(shardIndex, []);
      }
      deletesByShard.get(shardIndex)!.push({ key, keyString });
    }

    // Execute operations on each shard in parallel
    const allShardIndexes = new Set([
      ...putsByShard.keys(),
      ...deletesByShard.keys(),
    ]);

    await Promise.all(
      Array.from(allShardIndexes).map(async (shardIndex) => {
        const shard = this.shards[shardIndex];
        if (!shard) return;

        const puts = putsByShard.get(shardIndex) || [];
        const deletes = deletesByShard.get(shardIndex) || [];

        await Promise.all([
          ...puts.map((p) => shard.putItem(tableName, p.keyString, p.item)),
          ...deletes.map((d) => shard.deleteItem(tableName, d.keyString)),
        ]);
      })
    );
  }

  // Transaction operations - route to coordinator

  async transactWrite(
    items: TransactWriteItem[],
    clientRequestToken?: string
  ): Promise<void> {
    // Check if all items are on the same shard (single-partition optimization)
    const shardIndexes = new Set<number>();

    for (const item of items) {
      let tableName: string;
      let key: DynamoDBItem;

      if (item.Put) {
        tableName = item.Put.tableName;
        key = this.metadataStore.extractKey(tableName, item.Put.item);
      } else if (item.Update) {
        tableName = item.Update.tableName;
        key = item.Update.key;
      } else if (item.Delete) {
        tableName = item.Delete.tableName;
        key = item.Delete.key;
      } else if (item.ConditionCheck) {
        tableName = item.ConditionCheck.tableName;
        key = item.ConditionCheck.key;
      } else {
        throw new Error("Invalid transaction item");
      }

      const partitionKey = this.metadataStore.getPartitionKeyValue(tableName, key);
      const shardIndex = this.getShardIndex(partitionKey);
      shardIndexes.add(shardIndex);
    }

    // TODO: Implement single-partition optimization
    // For now, always use coordinator
    await this.coordinator.transactWrite(
      items,
      this.shards,
      this.metadataStore,
      clientRequestToken
    );
  }

  async transactGet(
    items: TransactGetItem[]
  ): Promise<Array<DynamoDBItem | null>> {
    return await this.coordinator.transactGet(
      items,
      this.shards,
      this.metadataStore
    );
  }

  // Helper methods

  private async routeToShard(
    tableName: string,
    item: DynamoDBItem
  ): Promise<{ shard: Shard; shardIndex: number; keyString: string }> {
    const partitionKey = this.metadataStore.getPartitionKeyValue(tableName, item);
    const shardIndex = this.getShardIndex(partitionKey);
    const shard = this.shards[shardIndex];

    if (!shard) {
      throw new Error(`Shard ${shardIndex} not found`);
    }

    const key = this.metadataStore.extractKey(tableName, item);
    const keyString = this.getKeyString(key);

    return { shard, shardIndex, keyString };
  }

  private getShardIndex(partitionKey: string): number {
    // Consistent hashing - same logic as coordinator
    let hash = 0;
    for (let i = 0; i < partitionKey.length; i++) {
      hash = (hash << 5) - hash + partitionKey.charCodeAt(i);
      hash = hash & hash; // Convert to 32-bit integer
    }
    return Math.abs(hash) % this.shards.length;
  }

  private getKeyString(key: any): string {
    const keyAttrs = Object.keys(key).sort();
    return keyAttrs.map((attr) => JSON.stringify(key[attr])).join("#");
  }

  private extractKey(keySchema: any[], item: DynamoDBItem): any {
    const key: any = {};
    for (const schema of keySchema) {
      const attrName = schema.AttributeName;
      key[attrName] = item[attrName];
    }
    return key;
  }
}
