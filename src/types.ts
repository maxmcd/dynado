// Shared types for DO-compatible architecture

import type {
  AttributeDefinition,
  AttributeValue,
  CancellationReason,
  KeySchemaElement,
} from '@aws-sdk/client-dynamodb'

export type DynamoDBItem = Record<string, AttributeValue>

export interface TableSchema {
  tableName: string
  keySchema: KeySchemaElement[]
  attributeDefinitions: AttributeDefinition[]
}

export interface TransactWriteItem {
  Put?: {
    tableName: string
    item: DynamoDBItem
    conditionExpression?: string
    expressionAttributeNames?: Record<string, string>
    expressionAttributeValues?: Record<string, AttributeValue>
    returnValuesOnConditionCheckFailure?: 'ALL_OLD' | 'NONE'
  }
  Update?: {
    tableName: string
    key: DynamoDBItem
    updateExpression: string
    conditionExpression?: string
    expressionAttributeNames?: Record<string, string>
    expressionAttributeValues?: Record<string, AttributeValue>
    returnValuesOnConditionCheckFailure?: 'ALL_OLD' | 'NONE'
  }
  Delete?: {
    tableName: string
    key: DynamoDBItem
    conditionExpression?: string
    expressionAttributeNames?: Record<string, string>
    expressionAttributeValues?: Record<string, AttributeValue>
    returnValuesOnConditionCheckFailure?: 'ALL_OLD' | 'NONE'
  }
  ConditionCheck?: {
    tableName: string
    key: DynamoDBItem
    conditionExpression: string
    expressionAttributeNames?: Record<string, string>
    expressionAttributeValues?: Record<string, AttributeValue>
    returnValuesOnConditionCheckFailure?: 'ALL_OLD' | 'NONE'
  }
}

export interface TransactGetItem {
  tableName: string
  key: DynamoDBItem
  projectionExpression?: string
  expressionAttributeNames?: Record<string, string>
}

export class TransactionCancelledError extends Error {
  override name = 'TransactionCanceledException' as const
  cancellationReasons: CancellationReason[]

  constructor(message: string, cancellationReasons: CancellationReason[]) {
    super(message)
    this.cancellationReasons = cancellationReasons
  }
}

// Transaction states following DynamoDB's 2PC protocol
export type TransactionState =
  | 'PREPARING'
  | 'COMMITTING'
  | 'COMMITTED'
  | 'CANCELLED'

// Transaction record stored in coordinator's ledger
export interface TransactionRecord {
  transactionId: string
  state: TransactionState
  timestamp: number
  clientRequestToken?: string
  items: TransactWriteItem[]
  createdAt: number
  completedAt?: number
  cancellationReasons?: CancellationReason[]
}

// Messages for 2PC protocol between Coordinator and Shards

export interface PrepareRequest {
  transactionId: string
  timestamp: number
  tableName: string
  operation: 'Put' | 'Update' | 'Delete' | 'ConditionCheck'
  key: DynamoDBItem
  partitionKeyValue: string // Extracted partition key value for storage
  sortKeyValue: string // Extracted sort key value (empty string if no sort key)
  item?: DynamoDBItem // For Put
  updateExpression?: string // For Update
  conditionExpression?: string
  expressionAttributeNames?: Record<string, string>
  expressionAttributeValues?: Record<string, AttributeValue>
  returnValuesOnConditionCheckFailure?: 'ALL_OLD' | 'NONE'
}

export interface PrepareResponse {
  accepted: boolean
  reason?:
    | 'ConditionalCheckFailed'
    | 'TransactionConflict'
    | 'TimestampConflict'
  message?: string
  lsn?: number // For read validation in Phase 2
  item?: DynamoDBItem // Returned when returnValuesOnConditionCheckFailure is set
}

export interface CommitRequest {
  transactionId: string
  timestamp: number
  tableName: string
  operation: 'Put' | 'Update' | 'Delete' | 'ConditionCheck'
  key: DynamoDBItem
  partitionKeyValue: string // Extracted partition key value for storage
  sortKeyValue: string // Extracted sort key value (empty string if no sort key)
  item?: DynamoDBItem // For Put/Update
  updateExpression?: string // For Update
  expressionAttributeNames?: Record<string, string>
  expressionAttributeValues?: Record<string, AttributeValue>
}

export interface ReleaseRequest {
  transactionId: string
  tableName: string
  keys: DynamoDBItem[]
  keyValues: Array<{ partitionKeyValue: string; sortKeyValue: string }> // Extracted key values for storage
}

// Result of routing a key to a shard
export interface ShardTarget {
  shardIndex: number
  shardId: string
}

// Range query support

export type SortKeyConditionOperator =
  | '='
  | '<'
  | '>'
  | '<='
  | '>='
  | 'BETWEEN'
  | 'begins_with'

export interface SortKeyCondition {
  operator: SortKeyConditionOperator
  value: AttributeValue // Single value for =, <, >, <=, >=, begins_with
  value2?: AttributeValue // Second value for BETWEEN
}

export interface QueryRequest {
  tableName: string
  partitionKeyValue: string // Required: must specify partition key
  sortKeyCondition?: SortKeyCondition // Optional: filter on sort key
  limit?: number
  scanIndexForward?: boolean // true = ascending (default), false = descending
  exclusiveStartKey?: {
    partitionKeyValue: string
    sortKeyValue: string
  }
}

export interface QueryResponse {
  items: DynamoDBItem[]
  lastEvaluatedKey?: {
    partitionKeyValue: string
    sortKeyValue: string
  }
  count: number
  scannedCount: number
}
