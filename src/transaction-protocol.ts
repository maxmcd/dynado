// Two-Phase Commit (2PC) protocol helpers following DynamoDB's implementation

import type {
  TransactWriteItem,
  PrepareRequest,
  CancellationReason,
} from './types.ts'

// Generate unique transaction ID
export function generateTransactionId(): string {
  return `tx_${Date.now()}_${Math.random().toString(36).substring(2, 15)}`
}

// Monotonic timestamp generator for transaction ordering
let lastTimestamp = 0

export function generateTransactionTimestamp(): number {
  const now = Date.now()
  // Ensure timestamp is always increasing
  if (now <= lastTimestamp) {
    lastTimestamp++
    return lastTimestamp
  }
  lastTimestamp = now
  return now
}

// Convert TransactWriteItem to PrepareRequest format
export function transactItemToPrepareRequest(
  item: TransactWriteItem,
  transactionId: string,
  timestamp: number
): PrepareRequest {
  if (item.Put) {
    return {
      transactionId,
      timestamp,
      tableName: item.Put.tableName,
      operation: 'Put',
      key: extractKeyFromItem(item.Put.item), // Will be determined by table schema
      partitionKeyValue: '', // Will be extracted from key by coordinator
      sortKeyValue: '', // Will be extracted from key by coordinator (empty if no sort key)
      item: item.Put.item,
      conditionExpression: item.Put.conditionExpression,
      expressionAttributeNames: item.Put.expressionAttributeNames,
      expressionAttributeValues: item.Put.expressionAttributeValues,
      returnValuesOnConditionCheckFailure:
        item.Put.returnValuesOnConditionCheckFailure,
    }
  } else if (item.Update) {
    return {
      transactionId,
      timestamp,
      tableName: item.Update.tableName,
      operation: 'Update',
      key: item.Update.key,
      partitionKeyValue: '', // Will be extracted from key by coordinator
      sortKeyValue: '', // Will be extracted from key by coordinator (empty if no sort key)
      updateExpression: item.Update.updateExpression,
      conditionExpression: item.Update.conditionExpression,
      expressionAttributeNames: item.Update.expressionAttributeNames,
      expressionAttributeValues: item.Update.expressionAttributeValues,
      returnValuesOnConditionCheckFailure:
        item.Update.returnValuesOnConditionCheckFailure,
    }
  } else if (item.Delete) {
    return {
      transactionId,
      timestamp,
      tableName: item.Delete.tableName,
      operation: 'Delete',
      key: item.Delete.key,
      partitionKeyValue: '', // Will be extracted from key by coordinator
      sortKeyValue: '', // Will be extracted from key by coordinator (empty if no sort key)
      conditionExpression: item.Delete.conditionExpression,
      expressionAttributeNames: item.Delete.expressionAttributeNames,
      expressionAttributeValues: item.Delete.expressionAttributeValues,
      returnValuesOnConditionCheckFailure:
        item.Delete.returnValuesOnConditionCheckFailure,
    }
  } else if (item.ConditionCheck) {
    return {
      transactionId,
      timestamp,
      tableName: item.ConditionCheck.tableName,
      operation: 'ConditionCheck',
      key: item.ConditionCheck.key,
      partitionKeyValue: '', // Will be extracted from key by coordinator
      sortKeyValue: '', // Will be extracted from key by coordinator (empty if no sort key)
      conditionExpression: item.ConditionCheck.conditionExpression,
      expressionAttributeNames: item.ConditionCheck.expressionAttributeNames,
      expressionAttributeValues: item.ConditionCheck.expressionAttributeValues,
      returnValuesOnConditionCheckFailure:
        item.ConditionCheck.returnValuesOnConditionCheckFailure,
    }
  }

  throw new Error('Invalid TransactWriteItem: no operation specified')
}

// Helper to extract key from item (simplified - actual implementation needs schema)
function extractKeyFromItem(item: any): any {
  // This is a placeholder - actual implementation will need table schema
  // to know which attributes are part of the key
  return item
}

// Build cancellation reasons array for transaction failure
export function buildCancellationReasons(
  total: number,
  failedIndex: number,
  failedReason: CancellationReason
): CancellationReason[] {
  const reasons: CancellationReason[] = []
  for (let i = 0; i < total; i++) {
    if (i === failedIndex) {
      reasons.push(failedReason)
    } else {
      reasons.push({ Code: 'None' })
    }
  }
  return reasons
}

// Check if all prepare responses were accepted
export function allPreparesAccepted(
  responses: Array<{ accepted: boolean }>
): boolean {
  return responses.every((r) => r.accepted)
}

// Find the first failed prepare response
export function findFirstFailure(
  responses: Array<{ accepted: boolean; reason?: string; message?: string }>
): { index: number; reason: string; message?: string } | null {
  for (let i = 0; i < responses.length; i++) {
    const response = responses[i]
    if (response && !response.accepted) {
      return {
        index: i,
        reason: response.reason || 'Unknown',
        message: response.message,
      }
    }
  }
  return null
}

// Serialize message for transmission (simulates DO-to-DO communication)
export function serializeMessage<T>(message: T): T {
  // In actual DO implementation, this would be actual serialization
  // For now, we deep clone to simulate the serialization boundary
  return JSON.parse(JSON.stringify(message))
}
