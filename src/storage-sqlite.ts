// Sharded SQLite storage implementation - refactored to use DO-compatible architecture
// This is now a facade that delegates to Router, Shards, Coordinator, and MetadataStore

import type {
  StorageBackend,
  DynamoDBItem,
  TableSchema,
  TransactWriteItem,
  TransactGetItem,
} from './storage.ts'
import { Shard } from './shard.ts'
import { TransactionCoordinator } from './coordinator.ts'
import { MetadataStore } from './metadata-store.ts'
import { Router } from './router.ts'
import * as fs from 'fs'

interface ShardedSQLiteConfig {
  shardCount: number
  dataDir: string
}

export class ShardedSQLiteStorage implements StorageBackend {
  private router: Router
  private shards: Shard[] = []
  private coordinator: TransactionCoordinator
  private metadataStore: MetadataStore

  constructor(config: ShardedSQLiteConfig) {
    // Create data directory if it doesn't exist
    if (!fs.existsSync(config.dataDir)) {
      fs.mkdirSync(config.dataDir, { recursive: true })
    }

    // Initialize components

    // 1. Create shards
    for (let i = 0; i < config.shardCount; i++) {
      const shard = new Shard(`${config.dataDir}/shard_${i}.db`, i)
      this.shards.push(shard)
    }

    // 2. Create metadata store
    this.metadataStore = new MetadataStore(config.dataDir)

    // 3. Create transaction coordinator
    this.coordinator = new TransactionCoordinator(config.dataDir)

    // 4. Create router that ties everything together
    this.router = new Router(this.shards, this.metadataStore, this.coordinator)
  }

  // Table operations - delegate to router

  async listTables(): Promise<string[]> {
    return await this.router.listTables()
  }

  async createTable(schema: TableSchema): Promise<void> {
    await this.router.createTable(
      schema.tableName,
      schema.keySchema,
      schema.attributeDefinitions
    )
  }

  async describeTable(tableName: string): Promise<TableSchema | null> {
    return await this.router.describeTable(tableName)
  }

  async deleteTable(tableName: string): Promise<void> {
    await this.router.deleteTable(tableName)
  }

  async getTableItemCount(tableName: string): Promise<number> {
    return await this.router.getTableItemCount(tableName)
  }

  // Item operations - delegate to router

  async putItem(tableName: string, item: DynamoDBItem): Promise<void> {
    await this.router.putItem(tableName, item)
  }

  async getItem(
    tableName: string,
    key: DynamoDBItem
  ): Promise<DynamoDBItem | null> {
    return await this.router.getItem(tableName, key)
  }

  async deleteItem(
    tableName: string,
    key: DynamoDBItem
  ): Promise<DynamoDBItem | null> {
    return await this.router.deleteItem(tableName, key)
  }

  // Scan/Query operations - delegate to router

  async scan(
    tableName: string,
    limit?: number,
    exclusiveStartKey?: DynamoDBItem
  ): Promise<{
    items: DynamoDBItem[]
    lastEvaluatedKey?: DynamoDBItem
  }> {
    return await this.router.scan(tableName, limit, exclusiveStartKey)
  }

  async query(
    tableName: string,
    keyCondition: (item: DynamoDBItem) => boolean,
    limit?: number,
    exclusiveStartKey?: DynamoDBItem
  ): Promise<{
    items: DynamoDBItem[]
    lastEvaluatedKey?: DynamoDBItem
  }> {
    return await this.router.query(
      tableName,
      keyCondition,
      limit,
      exclusiveStartKey
    )
  }

  // Batch operations - delegate to router

  async batchGet(
    tableName: string,
    keys: DynamoDBItem[]
  ): Promise<DynamoDBItem[]> {
    return await this.router.batchGet(tableName, keys)
  }

  async batchWrite(
    tableName: string,
    puts: DynamoDBItem[],
    deletes: DynamoDBItem[]
  ): Promise<void> {
    await this.router.batchWrite(tableName, puts, deletes)
  }

  // Transaction operations - delegate to router (which delegates to coordinator)

  async transactWrite(
    items: TransactWriteItem[],
    clientRequestToken?: string
  ): Promise<void> {
    await this.router.transactWrite(items, clientRequestToken)
  }

  async transactGet(
    items: TransactGetItem[]
  ): Promise<Array<DynamoDBItem | null>> {
    return await this.router.transactGet(items)
  }

  // Cleanup

  close() {
    for (const shard of this.shards) {
      shard.close()
    }
    this.coordinator.close()
    this.metadataStore.close()
  }
}
