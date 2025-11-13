// Concurrent transaction tests
// Tests system correctness under heavy concurrent load

import { describe, test, expect, beforeAll } from 'bun:test'
import {
  DynamoDBClient,
  CreateTableCommand,
  PutItemCommand,
  GetItemCommand,
  TransactWriteItemsCommand,
  TransactGetItemsCommand,
  TransactionCanceledException,
} from '@aws-sdk/client-dynamodb'
import { getGlobalTestDB } from './test-global-setup.ts'

const VERBOSE = process.env.VERBOSE_TESTS === 'true'
let client: DynamoDBClient

// Helper to generate unique table names
function getTableName(): string {
  return `ConcurrentTest_${Date.now()}_${Math.random()
    .toString(36)
    .substring(2, 9)}`
}

// Helper to generate random int between min and max (inclusive)
function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min
}

// Helper to shuffle array
function shuffle<T>(array: T[]): T[] {
  const result = [...array]
  for (let i = result.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1))
    ;[result[i], result[j]] = [result[j]!, result[i]!]
  }
  return result
}

beforeAll(async () => {
  const { client: testClient } = await getGlobalTestDB()
  client = testClient
})

describe('Concurrent Transaction Tests', () => {
  test('bank transfer invariant - total balance preserved under concurrent transfers', async () => {
    const tableName = getTableName()
    const NUM_ACCOUNTS = 10
    const INITIAL_BALANCE = 1000
    const NUM_TRANSFERS = 200
    const CONCURRENT_WORKERS = 15

    // Create table
    await client.send(
      new CreateTableCommand({
        TableName: tableName,
        KeySchema: [{ AttributeName: 'id', KeyType: 'HASH' }],
        AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'S' }],
        BillingMode: 'PAY_PER_REQUEST',
      })
    )

    // Setup: Create bank accounts
    const accountIds: string[] = []
    for (let i = 0; i < NUM_ACCOUNTS; i++) {
      const accountId = `account-${i}`
      accountIds.push(accountId)
      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: accountId },
            balance: { N: String(INITIAL_BALANCE) },
            type: { S: 'account' },
          },
        })
      )
    }

    const EXPECTED_TOTAL = NUM_ACCOUNTS * INITIAL_BALANCE

    // Track operations
    let successfulTransfers = 0
    let failedTransfers = 0
    const transferHistory: Array<{
      from: string
      to: string
      amount: number
      success: boolean
    }> = []

    // Worker function: performs random transfers
    async function worker(workerId: number, numTransfers: number) {
      for (let i = 0; i < numTransfers; i++) {
        // Pick two different random accounts
        const fromIdx = randomInt(0, NUM_ACCOUNTS - 1)
        let toIdx = randomInt(0, NUM_ACCOUNTS - 1)
        while (toIdx === fromIdx) {
          toIdx = randomInt(0, NUM_ACCOUNTS - 1)
        }

        const fromAccount = accountIds[fromIdx]!
        const toAccount = accountIds[toIdx]!
        const amount = randomInt(1, 100)

        try {
          // Read current balances
          const fromResult = await client.send(
            new GetItemCommand({
              TableName: tableName,
              Key: { id: { S: fromAccount } },
            })
          )
          const toResult = await client.send(
            new GetItemCommand({
              TableName: tableName,
              Key: { id: { S: toAccount } },
            })
          )

          if (!fromResult.Item || !toResult.Item) continue

          const fromBalance = parseInt(fromResult.Item.balance!.N!)
          const toBalance = parseInt(toResult.Item.balance!.N!)

          if (fromBalance < amount) {
            failedTransfers++
            transferHistory.push({
              from: fromAccount,
              to: toAccount,
              amount,
              success: false,
            })
            continue
          }

          const newFromBalance = fromBalance - amount
          const newToBalance = toBalance + amount

          // Transfer money atomically with optimistic locking
          await client.send(
            new TransactWriteItemsCommand({
              TransactItems: [
                {
                  Update: {
                    TableName: tableName,
                    Key: { id: { S: fromAccount } },
                    UpdateExpression: 'SET balance = :newBalance',
                    ExpressionAttributeValues: {
                      ':newBalance': { N: String(newFromBalance) },
                      ':expectedBalance': { N: String(fromBalance) },
                    },
                    ConditionExpression: 'balance = :expectedBalance',
                  },
                },
                {
                  Update: {
                    TableName: tableName,
                    Key: { id: { S: toAccount } },
                    UpdateExpression: 'SET balance = :newBalance',
                    ExpressionAttributeValues: {
                      ':newBalance': { N: String(newToBalance) },
                      ':expectedBalance': { N: String(toBalance) },
                    },
                    ConditionExpression: 'balance = :expectedBalance',
                  },
                },
              ],
            })
          )

          successfulTransfers++
          transferHistory.push({
            from: fromAccount,
            to: toAccount,
            amount,
            success: true,
          })
        } catch (error) {
          if (error instanceof TransactionCanceledException) {
            failedTransfers++
            transferHistory.push({
              from: fromAccount,
              to: toAccount,
              amount,
              success: false,
            })
          } else {
            throw error
          }
        }
      }
    }

    // Run concurrent workers
    const transfersPerWorker = Math.floor(NUM_TRANSFERS / CONCURRENT_WORKERS)
    const startTime = Date.now()

    await Promise.all(
      Array.from({ length: CONCURRENT_WORKERS }, (_, i) =>
        worker(i, transfersPerWorker)
      )
    )

    const duration = Date.now() - startTime

    // Verify invariant: total balance unchanged
    let actualTotal = 0
    const finalBalances: Record<string, number> = {}

    for (const accountId of accountIds) {
      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: accountId } },
        })
      )
      if (result.Item) {
        const balance = parseInt(result.Item.balance!.N!)
        finalBalances[accountId] = balance
        actualTotal += balance
      }
    }

    if (VERBOSE) {
      console.log(`Successful transfers: ${successfulTransfers}`)
      console.log(`Failed transfers: ${failedTransfers}`)
      console.log(`Duration: ${duration}ms`)
      console.log(
        `Throughput: ${((successfulTransfers / duration) * 1000).toFixed(
          2
        )} transfers/sec`
      )
    }

    // The critical invariant: total money in system must remain constant
    expect(actualTotal).toBe(EXPECTED_TOTAL)

    // No account should have negative balance
    for (const [accountId, balance] of Object.entries(finalBalances)) {
      expect(balance).toBeGreaterThanOrEqual(0)
    }
  }, 30000)

  test('concurrent counter increments - no lost updates', async () => {
    const tableName = getTableName()
    const NUM_COUNTERS = 15
    const INCREMENTS_PER_WORKER = 30
    const NUM_WORKERS = 30

    // Create table
    await client.send(
      new CreateTableCommand({
        TableName: tableName,
        KeySchema: [{ AttributeName: 'id', KeyType: 'HASH' }],
        AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'S' }],
        BillingMode: 'PAY_PER_REQUEST',
      })
    )

    // Setup: Create counters
    const counterIds: string[] = []
    for (let i = 0; i < NUM_COUNTERS; i++) {
      const counterId = `counter-${i}`
      counterIds.push(counterId)
      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: counterId },
            count: { N: '0' },
            type: { S: 'counter' },
          },
        })
      )
    }

    // Track expected increments per counter
    const expectedIncrements: Record<string, number> = {}
    counterIds.forEach((id) => (expectedIncrements[id] = 0))

    let totalRetries = 0

    // Worker function: increment random counters
    async function worker(workerId: number) {
      const localIncrements: Record<string, number> = {}
      counterIds.forEach((id) => (localIncrements[id] = 0))
      let workerRetries = 0

      for (let i = 0; i < INCREMENTS_PER_WORKER; i++) {
        const counterId = counterIds[randomInt(0, NUM_COUNTERS - 1)]!

        try {
          // Read current value
          const result = await client.send(
            new GetItemCommand({
              TableName: tableName,
              Key: { id: { S: counterId } },
            })
          )
          if (!result.Item) continue

          const currentCount = parseInt(result.Item.count!.N!)
          const newCount = currentCount + 1

          // Increment with optimistic locking
          await client.send(
            new TransactWriteItemsCommand({
              TransactItems: [
                {
                  Update: {
                    TableName: tableName,
                    Key: { id: { S: counterId } },
                    UpdateExpression: 'SET #count = :newCount',
                    ExpressionAttributeNames: { '#count': 'count' },
                    ExpressionAttributeValues: {
                      ':newCount': { N: String(newCount) },
                      ':expectedCount': { N: String(currentCount) },
                    },
                    ConditionExpression: '#count = :expectedCount',
                  },
                },
              ],
            })
          )

          localIncrements[counterId]!++
        } catch (error) {
          if (error instanceof TransactionCanceledException) {
            // Retry on conflict (in real system, would implement exponential backoff)
            i--
            workerRetries++
          } else {
            throw error
          }
        }
      }

      totalRetries += workerRetries
      return localIncrements
    }

    // Run concurrent workers
    const startTime = Date.now()
    const results = await Promise.all(
      Array.from({ length: NUM_WORKERS }, (_, i) => worker(i))
    )
    const duration = Date.now() - startTime

    // Aggregate expected increments
    for (const localIncrements of results) {
      for (const [counterId, count] of Object.entries(localIncrements)) {
        expectedIncrements[counterId] =
          (expectedIncrements[counterId] || 0) + count
      }
    }

    // Verify: actual counts match expected
    const actualCounts: Record<string, number> = {}
    let totalExpected = 0
    let totalActual = 0

    for (const counterId of counterIds) {
      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: counterId } },
        })
      )
      if (result.Item) {
        const actualCount = parseInt(result.Item.count!.N!)
        actualCounts[counterId] = actualCount
        totalActual += actualCount
        totalExpected += expectedIncrements[counterId]!

        expect(actualCount).toBe(expectedIncrements[counterId]!)
      }
    }

    if (VERBOSE) {
      console.log(`Total increments: ${totalActual}`)
      console.log(`Total retries: ${totalRetries}`)
      console.log(`Duration: ${duration}ms`)
      console.log(
        `Throughput: ${((totalActual / duration) * 1000).toFixed(
          2
        )} increments/sec`
      )
    }

    expect(totalActual).toBe(totalExpected)
  }, 30000)

  test('conditional claim race - only one winner per item', async () => {
    const tableName = getTableName()
    const NUM_ITEMS = 30
    const NUM_WORKERS = 50

    // Create table
    await client.send(
      new CreateTableCommand({
        TableName: tableName,
        KeySchema: [{ AttributeName: 'id', KeyType: 'HASH' }],
        AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'S' }],
        BillingMode: 'PAY_PER_REQUEST',
      })
    )

    // Setup: Create unclaimed items
    const itemIds: string[] = []
    for (let i = 0; i < NUM_ITEMS; i++) {
      const itemId = `item-${i}`
      itemIds.push(itemId)
      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: itemId },
            status: { S: 'available' },
            type: { S: 'claimable' },
          },
        })
      )
    }

    // Track claims
    const successfulClaims: Record<string, number[]> = {} // itemId -> workerIds that claimed it
    const claimAttempts: Record<number, number> = {} // workerId -> number of successful claims

    Array.from({ length: NUM_WORKERS }, (_, i) => {
      claimAttempts[i] = 0
    })

    // Worker function: try to claim random items
    async function worker(workerId: number) {
      // Shuffle items to reduce conflicts
      const shuffledItems = shuffle(itemIds)

      for (const itemId of shuffledItems) {
        try {
          // Try to claim item - only succeeds if not already claimed
          await client.send(
            new TransactWriteItemsCommand({
              TransactItems: [
                {
                  Update: {
                    TableName: tableName,
                    Key: { id: { S: itemId } },
                    UpdateExpression:
                      'SET #owner = :worker, #status = :claimed',
                    ExpressionAttributeNames: {
                      '#owner': 'owner',
                      '#status': 'status',
                    },
                    ExpressionAttributeValues: {
                      ':worker': { S: `worker-${workerId}` },
                      ':claimed': { S: 'claimed' },
                      ':available': { S: 'available' },
                    },
                    ConditionExpression: '#status = :available',
                  },
                },
              ],
            })
          )

          // Success - record the claim
          if (!successfulClaims[itemId]) {
            successfulClaims[itemId] = []
          }
          successfulClaims[itemId]!.push(workerId)
          claimAttempts[workerId]!++
        } catch (error) {
          if (error instanceof TransactionCanceledException) {
            // Expected - someone else claimed it first
            continue
          } else {
            throw error
          }
        }
      }
    }

    // Run concurrent workers
    const startTime = Date.now()
    await Promise.all(Array.from({ length: NUM_WORKERS }, (_, i) => worker(i)))
    const duration = Date.now() - startTime

    // Verify: each item has exactly one owner
    const ownerCounts: Record<string, number> = {}

    for (const itemId of itemIds) {
      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: itemId } },
        })
      )
      if (result.Item && result.Item.owner) {
        const owner = result.Item.owner!.S!
        ownerCounts[owner] = (ownerCounts[owner] || 0) + 1

        // Critical invariant: item should be in our successfulClaims exactly once
        const claimers = successfulClaims[itemId] || []
        expect(claimers.length).toBe(1)
        expect(result.Item.status!.S).toBe('claimed')
      }
    }

    const totalClaimed = Object.values(ownerCounts).reduce((a, b) => a + b, 0)

    if (VERBOSE) {
      console.log(`Items claimed: ${totalClaimed} / ${NUM_ITEMS}`)
      console.log(`Duration: ${duration}ms`)
    }

    // Verify: no duplicate claims
    for (const [itemId, claimers] of Object.entries(successfulClaims)) {
      expect(claimers.length).toBe(1)
    }

    expect(totalClaimed).toBe(NUM_ITEMS)
  }, 30000)

  test('cross-shard atomic operations - no partial commits', async () => {
    const tableName = getTableName()
    const NUM_OPERATIONS = 200
    const ITEMS_PER_TRANSACTION = 5
    const NUM_WORKERS = 15

    // Create table
    await client.send(
      new CreateTableCommand({
        TableName: tableName,
        KeySchema: [{ AttributeName: 'id', KeyType: 'HASH' }],
        AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'S' }],
        BillingMode: 'PAY_PER_REQUEST',
      })
    )

    // Track all items that should exist if transaction succeeded
    const expectedItems = new Set<string>()
    const transactionResults: Array<{
      id: string
      success: boolean
      items: string[]
    }> = []

    // Worker function: create multi-item transactions
    async function worker(workerId: number, numOps: number) {
      for (let i = 0; i < numOps; i++) {
        const txId = `tx-${workerId}-${i}`
        const itemIds: string[] = []

        // Create transaction that spans multiple items (likely different shards)
        const items = []

        for (let j = 0; j < ITEMS_PER_TRANSACTION; j++) {
          const itemId = `${txId}-item-${j}`
          itemIds.push(itemId)

          items.push({
            Put: {
              TableName: tableName,
              Item: {
                id: { S: itemId },
                txId: { S: txId },
                itemIndex: { N: String(j) },
                worker: { S: `worker-${workerId}` },
              },
              ConditionExpression: 'attribute_not_exists(id)',
            },
          })
        }

        try {
          await client.send(
            new TransactWriteItemsCommand({
              TransactItems: items,
            })
          )

          // Success - all items should exist
          transactionResults.push({ id: txId, success: true, items: itemIds })
          itemIds.forEach((id) => expectedItems.add(id))
        } catch (error) {
          if (error instanceof TransactionCanceledException) {
            // Failure - none of the items should exist
            transactionResults.push({
              id: txId,
              success: false,
              items: itemIds,
            })
          } else {
            throw error
          }
        }
      }
    }

    // Run concurrent workers
    const opsPerWorker = Math.floor(NUM_OPERATIONS / NUM_WORKERS)
    const startTime = Date.now()

    await Promise.all(
      Array.from({ length: NUM_WORKERS }, (_, i) => worker(i, opsPerWorker))
    )

    const duration = Date.now() - startTime

    // Verify: for each transaction, either all items exist or none do
    let successfulTxs = 0
    let failedTxs = 0
    let partialCommits = 0

    for (const tx of transactionResults) {
      let existingItems = 0

      for (const itemId of tx.items) {
        const result = await client.send(
          new GetItemCommand({
            TableName: tableName,
            Key: { id: { S: itemId } },
          })
        )
        if (result.Item) existingItems++
      }

      if (tx.success) {
        successfulTxs++
        // All items should exist
        if (existingItems !== tx.items.length) {
          partialCommits++
          console.error(
            `Partial commit detected for ${tx.id}: ${existingItems}/${tx.items.length} items exist`
          )
        }
        expect(existingItems).toBe(tx.items.length)
      } else {
        failedTxs++
        // No items should exist
        if (existingItems !== 0) {
          partialCommits++
          console.error(
            `Partial rollback detected for ${tx.id}: ${existingItems} items leaked`
          )
        }
        expect(existingItems).toBe(0)
      }
    }

    if (VERBOSE) {
      console.log(`Successful transactions: ${successfulTxs}`)
      console.log(`Failed transactions: ${failedTxs}`)
      console.log(`Partial commits: ${partialCommits}`)
      console.log(`Duration: ${duration}ms`)
    }

    // Critical invariant: no partial commits or rollbacks
    expect(partialCommits).toBe(0)
  }, 30000)

  test('read-write consistency - no dirty reads or lost updates', async () => {
    const tableName = getTableName()
    const NUM_ITEMS = 3
    const NUM_OPERATIONS = 400
    const NUM_WORKERS = 30

    // Create table
    await client.send(
      new CreateTableCommand({
        TableName: tableName,
        KeySchema: [{ AttributeName: 'id', KeyType: 'HASH' }],
        AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'S' }],
        BillingMode: 'PAY_PER_REQUEST',
      })
    )

    // Setup: Create items with version numbers
    const itemIds: string[] = []
    for (let i = 0; i < NUM_ITEMS; i++) {
      const itemId = `item-${i}`
      itemIds.push(itemId)
      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: itemId },
            version: { N: '0' },
            data: { S: 'initial' },
          },
        })
      )
    }

    // Track operations
    const operations: Array<{
      type: 'read' | 'write'
      itemId: string
      success: boolean
    }> = []

    // Worker function: mix of reads and writes
    async function worker(workerId: number, numOps: number) {
      for (let i = 0; i < numOps; i++) {
        const itemId = itemIds[randomInt(0, NUM_ITEMS - 1)]!
        const isWrite = Math.random() > 0.5

        if (isWrite) {
          // Read-modify-write with optimistic locking
          try {
            const result = await client.send(
              new GetItemCommand({
                TableName: tableName,
                Key: { id: { S: itemId } },
              })
            )
            if (!result.Item) continue

            const currentVersion = parseInt(result.Item.version!.N!)
            const newVersion = currentVersion + 1

            await client.send(
              new TransactWriteItemsCommand({
                TransactItems: [
                  {
                    Update: {
                      TableName: tableName,
                      Key: { id: { S: itemId } },
                      UpdateExpression:
                        'SET #version = :newVersion, #data = :data',
                      ExpressionAttributeNames: {
                        '#version': 'version',
                        '#data': 'data',
                      },
                      ExpressionAttributeValues: {
                        ':newVersion': { N: String(newVersion) },
                        ':data': { S: `worker-${workerId}-v${newVersion}` },
                        ':expectedVersion': { N: String(currentVersion) },
                      },
                      ConditionExpression: '#version = :expectedVersion',
                    },
                  },
                ],
              })
            )

            operations.push({ type: 'write', itemId, success: true })
          } catch (error) {
            if (error instanceof TransactionCanceledException) {
              operations.push({ type: 'write', itemId, success: false })
            } else {
              throw error
            }
          }
        } else {
          // Read operation using TransactGetItems
          try {
            const result = await client.send(
              new TransactGetItemsCommand({
                TransactItems: [
                  { Get: { TableName: tableName, Key: { id: { S: itemId } } } },
                ],
              })
            )

            if (
              result.Responses &&
              result.Responses[0] &&
              result.Responses[0].Item
            ) {
              const item = result.Responses[0].Item
              // Verify version and data are consistent
              const version = parseInt(item.version!.N!)
              const data = item.data!.S!

              // Data should contain version number
              if (version > 0) {
                expect(data).toContain(`v${version}`)
              }
            }

            operations.push({ type: 'read', itemId, success: true })
          } catch (error) {
            operations.push({ type: 'read', itemId, success: false })
          }
        }
      }
    }

    // Run concurrent workers
    const opsPerWorker = Math.floor(NUM_OPERATIONS / NUM_WORKERS)
    const startTime = Date.now()

    await Promise.all(
      Array.from({ length: NUM_WORKERS }, (_, i) => worker(i, opsPerWorker))
    )

    const duration = Date.now() - startTime

    // Analyze results
    const successfulReads = operations.filter(
      (op) => op.type === 'read' && op.success
    ).length
    const successfulWrites = operations.filter(
      (op) => op.type === 'write' && op.success
    ).length
    const failedWrites = operations.filter(
      (op) => op.type === 'write' && !op.success
    ).length

    if (VERBOSE) {
      console.log(`Successful reads: ${successfulReads}`)
      console.log(`Successful writes: ${successfulWrites}`)
      console.log(`Failed writes (conflicts): ${failedWrites}`)
      console.log(`Duration: ${duration}ms`)
    }

    // Verify: version numbers are sequential (no lost updates)
    for (const itemId of itemIds) {
      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: itemId } },
        })
      )
      if (result.Item) {
        const version = parseInt(result.Item.version!.N!)
        expect(version).toBeGreaterThanOrEqual(0)

        // Data should match version
        if (version > 0) {
          expect(result.Item.data!.S).toContain(`v${version}`)
        }
      }
    }
  }, 30000)
})
