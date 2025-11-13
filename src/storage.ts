// Storage abstraction interface for DynamoDB-compatible storage

// Re-export types from types.ts for backward compatibility
export type {
  DynamoDBItem,
  TableSchema,
  TransactWriteItem,
  TransactGetItem,
  CancellationReason,
} from './types.ts'

export { TransactionCancelledError } from './types.ts'

export interface StorageBackend {
  // Table operations
  listTables(): Promise<string[]>
  createTable(schema: import('./types.ts').TableSchema): Promise<void>
  describeTable(
    tableName: string
  ): Promise<import('./types.ts').TableSchema | null>
  deleteTable(tableName: string): Promise<void>
  getTableItemCount(tableName: string): Promise<number>

  // Item operations
  putItem(
    tableName: string,
    item: import('./types.ts').DynamoDBItem
  ): Promise<void>
  getItem(
    tableName: string,
    key: import('./types.ts').DynamoDBItem
  ): Promise<import('./types.ts').DynamoDBItem | null>
  deleteItem(
    tableName: string,
    key: import('./types.ts').DynamoDBItem
  ): Promise<import('./types.ts').DynamoDBItem | null>

  // Scan/Query operations
  scan(
    tableName: string,
    limit?: number,
    exclusiveStartKey?: any
  ): Promise<{
    items: import('./types.ts').DynamoDBItem[]
    lastEvaluatedKey?: any
  }>

  query(
    tableName: string,
    keyCondition: (item: import('./types.ts').DynamoDBItem) => boolean,
    limit?: number,
    exclusiveStartKey?: any
  ): Promise<{
    items: import('./types.ts').DynamoDBItem[]
    lastEvaluatedKey?: any
  }>

  // Batch operations
  batchGet(
    tableName: string,
    keys: import('./types.ts').DynamoDBItem[]
  ): Promise<import('./types.ts').DynamoDBItem[]>
  batchWrite(
    tableName: string,
    puts: import('./types.ts').DynamoDBItem[],
    deletes: import('./types.ts').DynamoDBItem[]
  ): Promise<void>

  // Transaction operations
  transactWrite(
    items: import('./types.ts').TransactWriteItem[],
    clientRequestToken?: string
  ): Promise<void>
  transactGet(
    items: import('./types.ts').TransactGetItem[]
  ): Promise<Array<import('./types.ts').DynamoDBItem | null>>
}
