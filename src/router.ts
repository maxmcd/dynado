// Router: Routes requests to appropriate shards/components
// Simulates DO-to-DO routing that would happen in Cloudflare Workers

import type {
  DynamoDBItem,
  QueryRequest,
  QueryResponse,
  TableSchema,
} from './types.ts'
import type {
  AttributeDefinition,
  KeySchemaElement,
  TransactGetItem,
  TransactWriteItem,
} from '@aws-sdk/client-dynamodb'
import type { Shard } from './shard.ts'
import type { MetadataStore } from './metadata-store.ts'
import type { TransactionCoordinator } from './coordinator.ts'
import { getShardIndex } from './hash-utils.ts'

export class Router {
  #shards: Shard[]
  #coordinator: TransactionCoordinator
  #metadataStore: MetadataStore

  constructor(
    shards: Shard[],
    metadataStore: MetadataStore,
    coordinator: TransactionCoordinator
  ) {
    this.#shards = shards
    this.#coordinator = coordinator
    this.#metadataStore = metadataStore
  }

  // Table operations - route to metadata store

  async createTable({
    tableName,
    keySchema,
    attributeDefinitions,
  }: {
    tableName: string
    keySchema: KeySchemaElement[]
    attributeDefinitions: AttributeDefinition[]
  }): Promise<void> {}

  async deleteAllTableItems(tableName: string): Promise<void> {
    // Delete from all shards
    await Promise.all(
      this.#shards.map((shard) => shard.deleteAllTableItems(tableName))
    )
  }

  async getTableItemCount(tableName: string): Promise<number> {
    // Fan out to all shards and sum
    const counts = await Promise.all(
      this.#shards.map((shard) => shard.getItemCount(tableName))
    )
    return counts.reduce((sum, count) => sum + count, 0)
  }

  // Item operations - route to specific shard based on partition key

  async putItem(tableName: string, item: DynamoDBItem): Promise<void> {
    const { shard, partitionKeyValue, sortKeyValue } = await this.routeToShard(
      tableName,
      item
    )
    await shard.putItem(tableName, partitionKeyValue, sortKeyValue, item)
  }

  async getItem(
    tableName: string,
    key: DynamoDBItem
  ): Promise<DynamoDBItem | null> {
    const { shard, partitionKeyValue, sortKeyValue } = await this.routeToShard(
      tableName,
      key
    )
    return await shard.getItem(tableName, partitionKeyValue, sortKeyValue)
  }

  async deleteItem(
    tableName: string,
    key: DynamoDBItem
  ): Promise<DynamoDBItem | null> {
    const { shard, partitionKeyValue, sortKeyValue } = await this.routeToShard(
      tableName,
      key
    )
    return await shard.deleteItem(tableName, partitionKeyValue, sortKeyValue)
  }

  // Scan/Query operations - fan out to all shards

  async scan(
    schema: TableSchema,
    limit?: number,
    exclusiveStartKey?: DynamoDBItem
  ): Promise<{
    items: DynamoDBItem[]
    lastEvaluatedKey?: DynamoDBItem
  }> {
    // Fan out to all shards in parallel
    const shardResults = await Promise.all(
      this.#shards.map((shard) => shard.scanTable(schema.tableName))
    )
    // Flatten results
    let allItems = shardResults.flat()

    // Handle pagination (simplified)
    if (exclusiveStartKey) {
      const startKeyValue = this.getKeyString(exclusiveStartKey)
      const startIndex = allItems.findIndex((item: DynamoDBItem) => {
        const itemKey = this.extractKey(schema.keySchema, item)
        const itemKeyString = this.getKeyString(itemKey)
        return itemKeyString === startKeyValue
      })
      if (startIndex >= 0) {
        allItems = allItems.slice(startIndex + 1)
      }
    }

    // Apply limit
    let lastEvaluatedKey
    if (limit && allItems.length > limit) {
      const limitedItems = allItems.slice(0, limit)
      const lastItem = limitedItems[limitedItems.length - 1]
      if (lastItem) {
        lastEvaluatedKey = this.extractKey(schema.keySchema, lastItem)
      }
      allItems = limitedItems
    }

    return {
      items: allItems,
      lastEvaluatedKey,
    }
  }

  async query(
    schema: TableSchema,
    keyCondition: (item: DynamoDBItem) => boolean,
    limit?: number,
    exclusiveStartKey?: DynamoDBItem
  ): Promise<{
    items: DynamoDBItem[]
    lastEvaluatedKey?: DynamoDBItem
  }> {
    // For simplicity, scan all shards and filter
    // TODO: optimize to only query relevant shards based on partition key
    const scanResult = await this.scan(schema, undefined, exclusiveStartKey)
    let items = scanResult.items.filter(keyCondition)

    // Apply limit
    let lastEvaluatedKey
    if (limit && items.length > limit) {
      const limitedItems = items.slice(0, limit)
      const lastItem = limitedItems[limitedItems.length - 1]
      if (lastItem) {
        lastEvaluatedKey = this.extractKey(schema.keySchema, lastItem)
      }
      items = limitedItems
    }

    return {
      items,
      lastEvaluatedKey,
    }
  }

  // New query method with range key support
  async queryWithRangeKey(request: QueryRequest): Promise<QueryResponse> {
    // Route to the appropriate shard based on partition key
    const shardIndex = getShardIndex(
      request.partitionKeyValue,
      this.#shards.length
    )
    const shard = this.#shards[shardIndex]

    if (!shard) {
      throw new Error(`Shard ${shardIndex} not found`)
    }

    // Execute query on the shard
    return await shard.query(
      request.tableName,
      request.partitionKeyValue,
      request.sortKeyCondition,
      request.limit,
      request.scanIndexForward ?? true,
      request.exclusiveStartKey
    )
  }

  // Batch operations

  async batchGet(
    tableName: string,
    keys: DynamoDBItem[]
  ): Promise<DynamoDBItem[]> {
    // Group keys by shard
    const keysByShard = new Map<
      number,
      Array<{
        key: DynamoDBItem
        partitionKeyValue: string
        sortKeyValue: string
      }>
    >()

    for (const key of keys) {
      const { shardIndex, partitionKeyValue, sortKeyValue } =
        await this.routeToShard(tableName, key)
      if (!keysByShard.has(shardIndex)) {
        keysByShard.set(shardIndex, [])
      }
      keysByShard
        .get(shardIndex)!
        .push({ key, partitionKeyValue, sortKeyValue })
    }

    // Fetch from each shard in parallel
    const results = await Promise.all(
      Array.from(keysByShard.entries()).map(async ([shardIndex, items]) => {
        const shard = this.#shards[shardIndex]
        if (!shard) return []

        const shardResults = await Promise.all(
          items.map((item) =>
            shard.getItem(tableName, item.partitionKeyValue, item.sortKeyValue)
          )
        )

        return shardResults.filter((item) => item !== null) as DynamoDBItem[]
      })
    )

    return results.flat()
  }

  async batchWrite(
    tableName: string,
    puts: DynamoDBItem[],
    deletes: DynamoDBItem[]
  ): Promise<void> {
    // Group operations by shard
    const putsByShard = new Map<
      number,
      Array<{
        item: DynamoDBItem
        partitionKeyValue: string
        sortKeyValue: string
      }>
    >()
    const deletesByShard = new Map<
      number,
      Array<{
        key: DynamoDBItem
        partitionKeyValue: string
        sortKeyValue: string
      }>
    >()

    for (const item of puts) {
      const { shardIndex, partitionKeyValue, sortKeyValue } =
        await this.routeToShard(tableName, item)
      if (!putsByShard.has(shardIndex)) {
        putsByShard.set(shardIndex, [])
      }
      putsByShard
        .get(shardIndex)!
        .push({ item, partitionKeyValue, sortKeyValue })
    }

    for (const key of deletes) {
      const { shardIndex, partitionKeyValue, sortKeyValue } =
        await this.routeToShard(tableName, key)
      if (!deletesByShard.has(shardIndex)) {
        deletesByShard.set(shardIndex, [])
      }
      deletesByShard
        .get(shardIndex)!
        .push({ key, partitionKeyValue, sortKeyValue })
    }

    // Execute operations on each shard in parallel
    const allShardIndexes = new Set([
      ...putsByShard.keys(),
      ...deletesByShard.keys(),
    ])

    await Promise.all(
      Array.from(allShardIndexes).map(async (shardIndex) => {
        const shard = this.#shards[shardIndex]
        if (!shard) return

        const puts = putsByShard.get(shardIndex) || []
        const deletes = deletesByShard.get(shardIndex) || []

        await Promise.all([
          ...puts.map((p) =>
            shard.putItem(
              tableName,
              p.partitionKeyValue,
              p.sortKeyValue,
              p.item
            )
          ),
          ...deletes.map((d) =>
            shard.deleteItem(tableName, d.partitionKeyValue, d.sortKeyValue)
          ),
        ])
      })
    )
  }

  // Transaction operations - route to coordinator

  async transactWrite(
    items: TransactWriteItem[],
    clientRequestToken?: string
  ): Promise<void> {
    // Check if all items are on the same shard (single-partition optimization)
    const shardIndexes = new Set<number>()

    for (const item of items) {
      let tableName: string
      let key: DynamoDBItem

      if (item.Put) {
        if (!item.Put.TableName || !item.Put.Item) {
          throw {
            name: 'ValidationException',
            message: 'Put requests require TableName and Item',
          }
        }
        tableName = item.Put.TableName
        key = this.#metadataStore.extractKey(tableName, item.Put.Item)
      } else if (item.Update) {
        if (
          !item.Update.TableName ||
          !item.Update.Key ||
          !item.Update.UpdateExpression
        ) {
          throw {
            name: 'ValidationException',
            message:
              'Update requests require TableName, Key, and UpdateExpression',
          }
        }

        tableName = item.Update.TableName
        key = item.Update.Key
      } else if (item.Delete) {
        if (!item.Delete.TableName || !item.Delete.Key) {
          throw {
            name: 'ValidationException',
            message: 'Delete requests require TableName and Key',
          }
        }
        tableName = item.Delete.TableName
        key = item.Delete.Key
      } else if (item.ConditionCheck) {
        if (
          !item.ConditionCheck.TableName ||
          !item.ConditionCheck.Key ||
          !item.ConditionCheck.ConditionExpression
        ) {
          throw {
            name: 'ValidationException',
            message:
              'ConditionCheck requests require TableName, Key, and ConditionExpression',
          }
        }
        tableName = item.ConditionCheck.TableName
        key = item.ConditionCheck.Key
      } else {
        throw {
          name: 'ValidationException',
          message: 'Invalid transaction item',
        }
      }

      const partitionKey = this.#metadataStore.getPartitionKeyValue(
        tableName,
        key
      )
      const shardIndex = getShardIndex(partitionKey, this.#shards.length)
      shardIndexes.add(shardIndex)
    }

    // TODO: Implement single-partition optimization
    // For now, always use coordinator
    await this.#coordinator.transactWrite(
      items,
      this.#shards,
      this.#metadataStore,
      clientRequestToken
    )
  }

  async transactGet(
    items: TransactGetItem[]
  ): Promise<Array<DynamoDBItem | null>> {
    return await this.#coordinator.transactGet(
      items,
      this.#shards,
      this.#metadataStore
    )
  }

  // Helper methods

  private async routeToShard(
    tableName: string,
    item: DynamoDBItem
  ): Promise<{
    shard: Shard
    shardIndex: number
    keyString: string
    partitionKeyValue: string
    sortKeyValue: string
  }> {
    const keyValues = this.#metadataStore.extractKeyValues(tableName, item)
    const shardIndex = getShardIndex(
      keyValues.partitionKeyValue,
      this.#shards.length
    )
    const shard = this.#shards[shardIndex]

    if (!shard) {
      throw new Error(`Shard ${shardIndex} not found`)
    }

    const key = this.#metadataStore.extractKey(tableName, item)
    const keyString = this.getKeyString(key)

    return {
      shard,
      shardIndex,
      keyString,
      partitionKeyValue: keyValues.partitionKeyValue,
      sortKeyValue: keyValues.sortKeyValue,
    }
  }

  private getKeyString(key: DynamoDBItem): string {
    const keyAttrs = Object.keys(key).sort()
    return keyAttrs.map((attr) => JSON.stringify(key[attr])).join('#')
  }

  private extractKey(
    keySchema: TableSchema['keySchema'],
    item: DynamoDBItem
  ): DynamoDBItem {
    const key: DynamoDBItem = {}
    for (const schema of keySchema) {
      const attrName = schema.AttributeName
      if (!attrName) {
        throw new Error('Key schema missing AttributeName')
      }
      const value = item[attrName]
      if (value === undefined) {
        throw new Error(`Key attribute missing: ${attrName}`)
      }
      key[attrName] = value
    }
    return key
  }
}
