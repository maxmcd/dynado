// Storage abstraction interface for DynamoDB-compatible storage

import type {
  DynamoDBItem,
  TableSchema,
  TransactWriteItem,
  TransactGetItem,
} from './types.ts'

// Re-export types from types.ts for backward compatibility
export type {
  DynamoDBItem,
  TableSchema,
  TransactWriteItem,
  TransactGetItem,
} from './types.ts'

export { TransactionCancelledError } from './types.ts'

export interface StorageBackend {
  // Table operations
  listTables(): Promise<string[]>
  createTable(schema: TableSchema): Promise<void>
  describeTable(tableName: string): Promise<TableSchema | null>
  deleteTable(tableName: string): Promise<void>
  getTableItemCount(tableName: string): Promise<number>

  // Item operations
  putItem(tableName: string, item: DynamoDBItem): Promise<void>
  getItem(tableName: string, key: DynamoDBItem): Promise<DynamoDBItem | null>
  deleteItem(
    tableName: string,
    key: DynamoDBItem
  ): Promise<DynamoDBItem | null>

  // Scan/Query operations
  scan(
    tableName: string,
    limit?: number,
    exclusiveStartKey?: DynamoDBItem
  ): Promise<{
    items: DynamoDBItem[]
    lastEvaluatedKey?: DynamoDBItem
  }>

  query(
    tableName: string,
    keyCondition: (item: DynamoDBItem) => boolean,
    limit?: number,
    exclusiveStartKey?: DynamoDBItem
  ): Promise<{
    items: DynamoDBItem[]
    lastEvaluatedKey?: DynamoDBItem
  }>

  // Batch operations
  batchGet(tableName: string, keys: DynamoDBItem[]): Promise<DynamoDBItem[]>
  batchWrite(
    tableName: string,
    puts: DynamoDBItem[],
    deletes: DynamoDBItem[]
  ): Promise<void>

  // Transaction operations
  transactWrite(
    items: TransactWriteItem[],
    clientRequestToken?: string
  ): Promise<void>
  transactGet(
    items: TransactGetItem[]
  ): Promise<Array<DynamoDBItem | null>>
}
