// TransactionCoordinator: Implements DynamoDB's Two-Phase Commit protocol
// In DO architecture, this would be a pool of Durable Objects

import { Database } from 'bun:sqlite'
import {
  TransactionCanceledException,
  type AttributeValue,
  type CancellationReason,
  type TransactGetItem,
  type TransactWriteItem,
} from '@aws-sdk/client-dynamodb'
import type {
  DynamoDBItem,
  TransactionRecord,
  PrepareRequest,
  CommitRequest,
  ReleaseRequest,
} from './types.ts'
import type { Shard } from './shard.ts'
import type { MetadataStore } from './metadata-store.ts'
import { getShardIndex } from './hash-utils.ts'
import * as fs from 'fs'
import { MAX_ITEMS_PER_TRANSACTION } from './index.ts'

interface IdempotencyCacheEntry {
  timestamp: number
  result: void
}

// Generate unique transaction ID
export function generateTransactionId(): string {
  return `tx_${Date.now()}_${Math.random().toString(36).substring(2, 15)}`
}

// TODO: Monotonic timestamp generator for transaction ordering
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

export class TransactionCoordinator {
  private db: Database
  private idempotencyCache = new Map<string, IdempotencyCacheEntry>()
  private readonly CACHE_TTL_MS = 10 * 60 * 1000 // 10 minutes

  constructor(dataDir: string) {
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true })
    }

    this.db = new Database(`${dataDir}/coordinator.db`)

    // Create transaction ledger table
    this.db.run(`
      CREATE TABLE IF NOT EXISTS transaction_ledger (
        transaction_id TEXT PRIMARY KEY,
        state TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        client_request_token TEXT,
        items TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        completed_at INTEGER,
        cancellation_reasons TEXT
      )
    `)

    // Index for cleanup queries
    this.db.run(`
      CREATE INDEX IF NOT EXISTS idx_ledger_completed
      ON transaction_ledger(completed_at)
    `)

    // TODO: move to alarm
    // Periodically clean up old transactions and cache
    setInterval(() => this.cleanup(), 60 * 1000) // Every minute
  }

  // Execute TransactWriteItems using 2PC protocol
  async transactWrite(
    items: TransactWriteItem[],
    shards: Shard[],
    metadataStore: MetadataStore,
    clientRequestToken?: string
  ): Promise<void> {
    // Validate
    if (!items || items.length === 0) {
      throw new Error('TransactItems cannot be empty')
    }
    if (items.length > MAX_ITEMS_PER_TRANSACTION) {
      throw new Error('Transaction cannot contain more than 100 items')
    }

    // Check idempotency cache
    if (clientRequestToken) {
      this.cleanIdempotencyCache()
      const cached = this.idempotencyCache.get(clientRequestToken)
      if (cached) {
        return cached.result
      }
    }

    const transactionId = generateTransactionId()
    const timestamp = generateTransactionTimestamp()

    // Record transaction as PREPARING
    this.recordTransaction({
      transactionId,
      state: 'PREPARING',
      timestamp,
      clientRequestToken,
      items,
      createdAt: Date.now(),
    })

    try {
      // Group items by shard and prepare requests
      const shardOperations = await this.groupItemsByShards(
        items,
        shards,
        metadataStore,
        transactionId,
        timestamp
      )

      // PHASE 1: PREPARE
      // Send prepare requests to all involved shards in parallel
      const preparePromises = shardOperations.map(async (op) => {
        const shard = shards[op.shardIndex]
        if (!shard) {
          throw new Error(`Shard ${op.shardIndex} not found`)
        }
        return await shard.prepare(op.prepareRequest)
      })

      const prepareResponses = await Promise.all(preparePromises)

      // Check if all shards accepted
      const allAccepted = prepareResponses.every((r) => r.accepted)

      if (!allAccepted) {
        // Find first failure
        const failureIndex = prepareResponses.findIndex((r) => !r.accepted)
        const failedResponse = prepareResponses[failureIndex]

        if (!failedResponse) {
          throw new Error('Failed to find failure response')
        }

        // Build cancellation reason
        const reason: CancellationReason = {
          Code: failedResponse.reason || 'Unknown',
          Message: failedResponse.message || 'The conditional request failed',
        }

        if (failedResponse.item) {
          reason.Item = failedResponse.item
        }

        const cancellationReasons = buildCancellationReasons(
          items.length,
          failureIndex,
          reason
        )

        // Update ledger
        this.updateTransactionState(
          transactionId,
          'CANCELLED',
          cancellationReasons
        )

        // RELEASE: Clean up locks on all shards
        await this.releaseAllShards(shardOperations, shards, transactionId)

        throw new TransactionCanceledException({
          $metadata: {},
          message:
            'Transaction cancelled, please refer cancellation reasons for specific reasons',
          CancellationReasons: cancellationReasons,
        })
      }

      // All shards accepted - proceed to Phase 2
      this.updateTransactionState(transactionId, 'COMMITTING')

      // PHASE 2: COMMIT
      // This phase MUST complete - retry indefinitely on failure
      await this.commitAllShards(
        shardOperations,
        shards,
        transactionId,
        timestamp
      )

      // Mark as committed
      this.updateTransactionState(transactionId, 'COMMITTED')

      // Cache result for idempotency
      if (clientRequestToken) {
        this.idempotencyCache.set(clientRequestToken, {
          timestamp: Date.now(),
          result: undefined,
        })
      }
    } catch (error: unknown) {
      // If error is TransactionCanceledException, it's already handled
      if (error instanceof TransactionCanceledException) {
        throw error
      }

      // For other errors during Phase 1, try to release locks
      try {
        const shardOperations = await this.groupItemsByShards(
          items,
          shards,
          metadataStore,
          transactionId,
          timestamp
        )
        await this.releaseAllShards(shardOperations, shards, transactionId)
      } catch {
        // Ignore release errors
      }

      throw error
    }
  }

  // Execute TransactGetItems
  async transactGet(
    items: TransactGetItem[],
    shards: Shard[],
    metadataStore: MetadataStore
  ): Promise<Array<DynamoDBItem | null>> {
    if (!items || items.length === 0) {
      throw new Error('TransactItems cannot be empty')
    }
    if (items.length > MAX_ITEMS_PER_TRANSACTION) {
      throw new Error(
        `Transaction cannot contain more than ${MAX_ITEMS_PER_TRANSACTION} items`
      )
    }

    // For read transactions, we just read from each shard
    // In a real implementation, we'd use snapshot isolation
    const results: Array<DynamoDBItem | null> = []

    for (const item of items) {
      if (!item.Get) {
        throw { name: 'ValidationException', message: 'Get is required' }
      }
      if (item.Get?.Key === undefined || item.Get.TableName === undefined) {
        throw {
          name: 'ValidationException',
          message: 'Get requests require TableName and Key',
        }
      }

      const keyValues = metadataStore.extractKeyValuesFromKey(
        item.Get.TableName,
        item.Get.Key
      )
      const shardIndex = getShardIndex(
        keyValues.partitionKeyValue,
        shards.length
      )
      const shard = shards[shardIndex]

      if (!shard) {
        throw new Error(`Shard ${shardIndex} not found`)
      }

      const dbItem = await shard.getItem(
        item.Get.TableName,
        keyValues.partitionKeyValue,
        keyValues.sortKeyValue
      )

      if (dbItem) {
        // Apply projection expression if provided
        let resultItem = dbItem
        if (item.Get.ProjectionExpression !== undefined) {
          resultItem = this.applyProjection(
            dbItem,
            item.Get.ProjectionExpression,
            item.Get.ExpressionAttributeNames
          )
        }
        results.push(resultItem)
      } else {
        results.push(null)
      }
    }

    return results
  }

  // Helper: Group items by which shard they belong to
  private async groupItemsByShards(
    items: TransactWriteItem[],
    shards: Shard[],
    metadataStore: MetadataStore,
    transactionId: string,
    timestamp: number
  ): Promise<
    Array<{
      shardIndex: number
      prepareRequest: PrepareRequest
      commitRequest: CommitRequest
      releaseKey: DynamoDBItem
    }>
  > {
    const operations: Array<{
      shardIndex: number
      prepareRequest: PrepareRequest
      commitRequest: CommitRequest
      releaseKey: DynamoDBItem
    }> = []

    for (const item of items) {
      let tableName: string
      let key: DynamoDBItem
      let operation: 'Put' | 'Update' | 'Delete' | 'ConditionCheck'
      let itemData: DynamoDBItem | undefined
      let updateExpression: string | undefined
      let conditionExpression: string | undefined
      let expressionAttributeNames: Record<string, string> | undefined
      let expressionAttributeValues: Record<string, AttributeValue> | undefined
      let returnValuesOnConditionCheckFailure: 'ALL_OLD' | 'NONE' | undefined

      if (item.Put) {
        operation = 'Put'
        tableName = item.Put.TableName!
        key = metadataStore.extractKey(tableName, item.Put.Item!)
        itemData = item.Put.Item
        conditionExpression = item.Put.ConditionExpression
        expressionAttributeNames = item.Put.ExpressionAttributeNames
        expressionAttributeValues = item.Put.ExpressionAttributeValues
        returnValuesOnConditionCheckFailure =
          item.Put.ReturnValuesOnConditionCheckFailure
      } else if (item.Update) {
        operation = 'Update'
        tableName = item.Update.TableName!
        key = item.Update.Key!
        updateExpression = item.Update.UpdateExpression
        conditionExpression = item.Update.ConditionExpression
        expressionAttributeNames = item.Update.ExpressionAttributeNames
        expressionAttributeValues = item.Update.ExpressionAttributeValues
        returnValuesOnConditionCheckFailure =
          item.Update.ReturnValuesOnConditionCheckFailure
      } else if (item.Delete) {
        operation = 'Delete'
        tableName = item.Delete.TableName!
        key = item.Delete.Key!
        conditionExpression = item.Delete.ConditionExpression
        expressionAttributeNames = item.Delete.ExpressionAttributeNames
        expressionAttributeValues = item.Delete.ExpressionAttributeValues
        returnValuesOnConditionCheckFailure =
          item.Delete.ReturnValuesOnConditionCheckFailure
      } else if (item.ConditionCheck) {
        operation = 'ConditionCheck'
        tableName = item.ConditionCheck.TableName!
        key = item.ConditionCheck.Key!
        conditionExpression = item.ConditionCheck.ConditionExpression
        expressionAttributeNames = item.ConditionCheck.ExpressionAttributeNames
        expressionAttributeValues =
          item.ConditionCheck.ExpressionAttributeValues
        returnValuesOnConditionCheckFailure =
          item.ConditionCheck.ReturnValuesOnConditionCheckFailure
      } else {
        throw new Error('Invalid transaction item')
      }

      // Determine which shard this item belongs to
      const partitionKey = metadataStore.getPartitionKeyValue(tableName, key)
      const shardIndex = getShardIndex(partitionKey, shards.length)

      // Extract key values for storage
      const keyValues = metadataStore.extractKeyValuesFromKey(tableName, key)

      const prepareRequest: PrepareRequest = {
        transactionId,
        timestamp,
        tableName,
        operation,
        key,
        partitionKeyValue: keyValues.partitionKeyValue,
        sortKeyValue: keyValues.sortKeyValue,
        item: itemData,
        updateExpression,
        conditionExpression,
        expressionAttributeNames,
        expressionAttributeValues,
        returnValuesOnConditionCheckFailure,
      }

      const commitRequest: CommitRequest = {
        transactionId,
        timestamp,
        tableName,
        operation,
        key,
        partitionKeyValue: keyValues.partitionKeyValue,
        sortKeyValue: keyValues.sortKeyValue,
        item: itemData,
        updateExpression,
        expressionAttributeNames,
        expressionAttributeValues,
      }

      operations.push({
        shardIndex,
        prepareRequest,
        commitRequest,
        releaseKey: key,
      })
    }

    return operations
  }

  // Phase 2: Commit on all shards with retry
  private async commitAllShards(
    operations: Array<{
      shardIndex: number
      commitRequest: CommitRequest
    }>,
    shards: Shard[],
    transactionId: string,
    timestamp: number
  ): Promise<void> {
    // Commit operations must succeed - retry with exponential backoff
    const MAX_RETRIES = 10
    const INITIAL_DELAY = 100

    for (const op of operations) {
      const shard = shards[op.shardIndex]
      if (!shard) {
        throw new Error(`Shard ${op.shardIndex} not found`)
      }

      let retries = 0
      let delay = INITIAL_DELAY
      let lastErrorMessage = 'Unknown error'

      while (retries < MAX_RETRIES) {
        try {
          await shard.commit(op.commitRequest)
          break // Success
        } catch (error: unknown) {
          lastErrorMessage =
            error instanceof Error ? error.message : String(error)
          retries++

          if (retries >= MAX_RETRIES) {
            // Transaction is now in an inconsistent state
            // In production, this would trigger:
            // 1. Alert to operations team
            // 2. Record to dead letter queue for manual recovery
            // 3. Coordinator recovery process would pick this up
            const err = new Error(
              `Failed to commit transaction ${transactionId} on shard ${op.shardIndex} after ${MAX_RETRIES} retries. ` +
                `Transaction is in COMMITTING state and requires manual recovery. ` +
                `Original error: ${lastErrorMessage}`
            )
            // Record this failed commit for recovery
            this.recordFailedCommit(transactionId, op.shardIndex, err)
            throw err
          }

          // Exponential backoff
          await new Promise((resolve) => setTimeout(resolve, delay))
          delay = Math.min(delay * 2, 5000)
        }
      }
    }
  }

  // Record failed commits for recovery (in production, would use separate recovery table)
  private recordFailedCommit(
    transactionId: string,
    shardIndex: number,
    error: Error
  ): void {
    // TODO: revisit with DOs, make sure progres is halted before we continue.
    console.error(
      `CRITICAL: Transaction ${transactionId} failed to commit on shard ${shardIndex}. ` +
        `This requires manual intervention or automated recovery. Error: ${error.message}`
    )

    // Update transaction state to indicate partial commit
    try {
      this.db.run(
        `UPDATE transaction_ledger
         SET state = 'COMMITTING_FAILED', completed_at = ?
         WHERE transaction_id = ?`,
        [Date.now(), transactionId]
      )
    } catch (dbError) {
      console.error(
        `Failed to update transaction ledger for ${transactionId}:`,
        dbError
      )
    }
  }

  // Release locks on all shards
  private async releaseAllShards(
    operations: Array<{
      shardIndex: number
      prepareRequest: PrepareRequest
      releaseKey: DynamoDBItem
    }>,
    shards: Shard[],
    transactionId: string
  ): Promise<void> {
    // Group release requests by shard and table
    const releaseByShardMap = new Map<
      number,
      Map<
        string,
        {
          keys: DynamoDBItem[]
          keyValues: Array<{
            partitionKeyValue: string
            sortKeyValue: string
          }>
        }
      >
    >()

    for (const op of operations) {
      let tableMap = releaseByShardMap.get(op.shardIndex)
      if (!tableMap) {
        tableMap = new Map()
        releaseByShardMap.set(op.shardIndex, tableMap)
      }

      if (!tableMap.has(op.prepareRequest.tableName)) {
        tableMap.set(op.prepareRequest.tableName, {
          keys: [],
          keyValues: [],
        })
      }

      const entry = tableMap.get(op.prepareRequest.tableName)!
      entry.keys.push(op.releaseKey)
      entry.keyValues.push({
        partitionKeyValue: op.prepareRequest.partitionKeyValue,
        sortKeyValue: op.prepareRequest.sortKeyValue,
      })
    }

    // Send release requests in parallel per shard/table pair
    const releasePromises = Array.from(releaseByShardMap.entries()).flatMap(
      ([shardIndex, tableMap]) =>
        Array.from(tableMap.entries()).map(async ([tableName, data]) => {
          const shard = shards[shardIndex]
          if (!shard) {
            return
          }

          const releaseRequest: ReleaseRequest = {
            transactionId,
            tableName,
            keys: data.keys,
            keyValues: data.keyValues,
          }

          try {
            await shard.release(releaseRequest)
          } catch (error) {
            // Ignore release errors (best effort)
            console.error(`Failed to release on shard ${shardIndex}:`, error)
          }
        })
    )

    await Promise.all(releasePromises)
  }

  // Transaction ledger operations

  private recordTransaction(record: TransactionRecord): void {
    this.db.run(
      `INSERT INTO transaction_ledger
       (transaction_id, state, timestamp, client_request_token, items, created_at, completed_at, cancellation_reasons)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        record.transactionId,
        record.state,
        record.timestamp,
        record.clientRequestToken || null,
        JSON.stringify(record.items),
        record.createdAt,
        record.completedAt || null,
        record.cancellationReasons
          ? JSON.stringify(record.cancellationReasons)
          : null,
      ]
    )
  }

  private updateTransactionState(
    transactionId: string,
    state: 'PREPARING' | 'COMMITTING' | 'COMMITTED' | 'CANCELLED',
    cancellationReasons?: CancellationReason[]
  ): void {
    const completedAt = ['COMMITTED', 'CANCELLED'].includes(state)
      ? Date.now()
      : null

    this.db.run(
      `UPDATE transaction_ledger
       SET state = ?, completed_at = ?, cancellation_reasons = ?
       WHERE transaction_id = ?`,
      [
        state,
        completedAt,
        cancellationReasons ? JSON.stringify(cancellationReasons) : null,
        transactionId,
      ]
    )
  }

  // Helper methods

  private applyProjection(
    item: DynamoDBItem,
    projectionExpression: string,
    expressionAttributeNames?: Record<string, string>
  ): DynamoDBItem {
    const attrs = projectionExpression.split(',').map((a) => a.trim())
    const projected: DynamoDBItem = {} as DynamoDBItem

    for (const attr of attrs) {
      let attrName = attr
      if (expressionAttributeNames && attr.startsWith('#')) {
        const resolved = expressionAttributeNames[attr]
        if (resolved) attrName = resolved
      }
      if (attrName) {
        const value = item[attrName]
        if (value !== undefined) {
          projected[attrName] = value
        }
      }
    }

    return projected
  }

  private cleanIdempotencyCache(): void {
    const cutoff = Date.now() - this.CACHE_TTL_MS
    for (const [token, entry] of this.idempotencyCache.entries()) {
      if (entry.timestamp < cutoff) {
        this.idempotencyCache.delete(token)
      }
    }
  }

  private cleanup(): void {
    // Clean up old completed transactions (keep for 10 minutes)
    const cutoff = Date.now() - this.CACHE_TTL_MS
    this.db.run(
      'DELETE FROM transaction_ledger WHERE completed_at IS NOT NULL AND completed_at < ?',
      [cutoff]
    )

    // Clean idempotency cache
    this.cleanIdempotencyCache()
  }

  close() {
    this.db.close()
  }
}
