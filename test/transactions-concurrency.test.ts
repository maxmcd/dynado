// Tests for concurrent transaction conflicts and locking behavior
// Migrated to use HTTP API via AWS SDK

import { test, expect, beforeAll, describe } from 'bun:test'
import {
  DynamoDBClient,
  PutItemCommand,
  GetItemCommand,
  TransactWriteItemsCommand,
  TransactGetItemsCommand,
  TransactionCanceledException,
} from '@aws-sdk/client-dynamodb'
import { getGlobalTestDB, createTable, uniqueTableName } from './helpers.ts'

describe('Concurrent Transaction Conflicts', () => {
  let client: DynamoDBClient

  // Helper to generate unique table names
  function getTableName(): string {
    return uniqueTableName('ConcurrencyTest')
  }

  beforeAll(async () => {
    const { client: testClient } = await getGlobalTestDB()
    client = testClient
  })

  test('should serialize concurrent transactions on same item', async () => {
    const tableName = getTableName()
    await createTable(client, tableName)

    // Put initial item
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: 'counter' }, value: { N: '0' } },
      })
    )

    // Run 10 concurrent transactions incrementing the same counter
    const promises = []
    for (let i = 0; i < 10; i++) {
      promises.push(
        (async () => {
          // Retry on conflicts
          let attempts = 0
          while (attempts < 5) {
            try {
              await client.send(
                new TransactWriteItemsCommand({
                  TransactItems: [
                    {
                      Update: {
                        TableName: tableName,
                        Key: { id: { S: 'counter' } },
                        UpdateExpression: 'ADD #v :inc',
                        ExpressionAttributeNames: { '#v': 'value' },
                        ExpressionAttributeValues: { ':inc': { N: '1' } },
                      },
                    },
                  ],
                })
              )
              break
            } catch (error) {
              attempts++
              if (attempts >= 5) throw error
              // Small random delay before retry
              await new Promise((resolve) =>
                setTimeout(resolve, Math.random() * 10)
              )
            }
          }
        })()
      )
    }

    await Promise.all(promises)

    // Counter should be exactly 10
    const result = await client.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: 'counter' } },
      })
    )
    expect(result.Item?.value!.N).toBe('10')
  })

  test('should handle concurrent transactions on different items', async () => {
    const tableName = getTableName()
    await createTable(client, tableName)

    // Multiple transactions on different items should all succeed
    const promises = []
    for (let i = 0; i < 20; i++) {
      promises.push(
        client.send(
          new TransactWriteItemsCommand({
            TransactItems: [
              {
                Put: {
                  TableName: tableName,
                  Item: { id: { S: `item-${i}` }, value: { N: String(i) } },
                },
              },
            ],
          })
        )
      )
    }

    await Promise.all(promises)

    // All items should exist
    for (let i = 0; i < 20; i++) {
      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: `item-${i}` } },
        })
      )
      expect(result.Item).toBeDefined()
      expect(result.Item?.value!.N).toBe(String(i))
    }
  })

  test('should handle race condition with Put operations', async () => {
    const tableName = getTableName()
    await createTable(client, tableName)

    // Two transactions trying to create the same item with attribute_not_exists
    // Only one should succeed
    const promises = [
      client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: tableName,
                Item: { id: { S: 'race-item' }, creator: { S: 'txn1' } },
                ConditionExpression: 'attribute_not_exists(id)',
              },
            },
          ],
        })
      ),
      client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: tableName,
                Item: { id: { S: 'race-item' }, creator: { S: 'txn2' } },
                ConditionExpression: 'attribute_not_exists(id)',
              },
            },
          ],
        })
      ),
    ]

    const results = await Promise.allSettled(promises)

    // Exactly one should succeed, one should fail
    const succeeded = results.filter((r) => r.status === 'fulfilled').length
    const failed = results.filter((r) => r.status === 'rejected').length

    expect(succeeded).toBe(1)
    expect(failed).toBe(1)

    // Item should exist with one of the creators
    const result = await client.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: 'race-item' } },
      })
    )
    expect(result.Item).toBeDefined()
    expect(['txn1', 'txn2']).toContain(result.Item?.creator?.S!)
  })

  test('should handle bank transfer scenario correctly', async () => {
    const tableName = getTableName()
    await createTable(client, tableName)

    // Classic bank transfer: deduct from A, add to B
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: 'account-A' }, balance: { N: '1000' } },
      })
    )
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: 'account-B' }, balance: { N: '500' } },
      })
    )

    // Transfer 100 from A to B
    await client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Update: {
              TableName: tableName,
              Key: { id: { S: 'account-A' } },
              UpdateExpression: 'SET balance = balance - :amount',
              ConditionExpression: 'balance >= :amount',
              ExpressionAttributeValues: { ':amount': { N: '100' } },
            },
          },
          {
            Update: {
              TableName: tableName,
              Key: { id: { S: 'account-B' } },
              UpdateExpression: 'SET balance = balance + :amount',
              ExpressionAttributeValues: { ':amount': { N: '100' } },
            },
          },
        ],
      })
    )

    const accountA = await client.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: 'account-A' } },
      })
    )
    const accountB = await client.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: 'account-B' } },
      })
    )

    expect(accountA.Item?.balance?.N).toBe('900')
    expect(accountB.Item?.balance?.N).toBe('600')
  })

  test('should rollback bank transfer if insufficient funds', async () => {
    const tableName = getTableName()
    await createTable(client, tableName)

    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: 'account-C' }, balance: { N: '50' } },
      })
    )
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: 'account-D' }, balance: { N: '100' } },
      })
    )

    // Try to transfer 100 from C to D (should fail - insufficient funds)
    try {
      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'account-C' } },
                UpdateExpression: 'SET balance = balance - :amount',
                ConditionExpression: 'balance >= :amount',
                ExpressionAttributeValues: { ':amount': { N: '100' } },
              },
            },
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'account-D' } },
                UpdateExpression: 'SET balance = balance + :amount',
                ExpressionAttributeValues: { ':amount': { N: '100' } },
              },
            },
          ],
        })
      )
      expect.unreachable('Transaction should have failed')
    } catch (error: any) {
      expect(error).toBeInstanceOf(TransactionCanceledException)
    }

    // Both accounts should be unchanged
    const accountC = await client.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: 'account-C' } },
      })
    )
    const accountD = await client.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: 'account-D' } },
      })
    )

    expect(accountC.Item?.balance?.N).toBe('50')
    expect(accountD.Item?.balance?.N).toBe('100')
  })

  test('should handle multiple concurrent bank transfers', async () => {
    const tableName = getTableName()
    await createTable(client, tableName)

    // Setup accounts
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: 'bank-1' }, balance: { N: '1000' } },
      })
    )
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: 'bank-2' }, balance: { N: '1000' } },
      })
    )

    // Perform 5 concurrent transfers of 100 each
    const promises = []
    for (let i = 0; i < 5; i++) {
      promises.push(
        (async () => {
          let attempts = 0
          while (attempts < 5) {
            try {
              await client.send(
                new TransactWriteItemsCommand({
                  TransactItems: [
                    {
                      Update: {
                        TableName: tableName,
                        Key: { id: { S: 'bank-1' } },
                        UpdateExpression: 'SET balance = balance - :amount',
                        ConditionExpression: 'balance >= :amount',
                        ExpressionAttributeValues: { ':amount': { N: '100' } },
                      },
                    },
                    {
                      Update: {
                        TableName: tableName,
                        Key: { id: { S: 'bank-2' } },
                        UpdateExpression: 'SET balance = balance + :amount',
                        ExpressionAttributeValues: { ':amount': { N: '100' } },
                      },
                    },
                  ],
                })
              )
              break
            } catch (error) {
              attempts++
              if (attempts >= 5) throw error
              await new Promise((resolve) =>
                setTimeout(resolve, Math.random() * 10)
              )
            }
          }
        })()
      )
    }

    await Promise.all(promises)

    // Total balance should be preserved
    const account1 = await client.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: 'bank-1' } },
      })
    )
    const account2 = await client.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: 'bank-2' } },
      })
    )

    const total =
      parseInt(account1.Item?.balance?.N || '0') +
      parseInt(account2.Item?.balance?.N || '0')
    expect(total).toBe(2000) // Total preserved

    expect(account1.Item?.balance?.N).toBe('500')
    expect(account2.Item?.balance?.N).toBe('1500')
  })

  test('should handle concurrent reads (TransactGetItems)', async () => {
    const tableName = getTableName()
    await createTable(client, tableName)

    // Setup items
    for (let i = 0; i < 5; i++) {
      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: `read-${i}` },
            value: { N: String(i) },
          },
        })
      )
    }

    // Perform many concurrent reads
    const promises = []
    for (let i = 0; i < 20; i++) {
      promises.push(
        client.send(
          new TransactGetItemsCommand({
            TransactItems: [
              { Get: { TableName: tableName, Key: { id: { S: 'read-0' } } } },
              { Get: { TableName: tableName, Key: { id: { S: 'read-1' } } } },
              { Get: { TableName: tableName, Key: { id: { S: 'read-2' } } } },
            ],
          })
        )
      )
    }

    const results = await Promise.all(promises)

    // All reads should succeed
    for (const result of results) {
      expect(result.Responses?.length).toBe(3)
      expect(result.Responses?.[0]?.Item?.value?.N).toBe('0')
      expect(result.Responses?.[1]?.Item?.value?.N).toBe('1')
      expect(result.Responses?.[2]?.Item?.value?.N).toBe('2')
    }
  })

  test('should handle mixed concurrent reads and writes', async () => {
    const tableName = getTableName()
    await createTable(client, tableName)

    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: 'mixed' }, counter: { N: '0' } },
      })
    )

    const promises = []

    // 10 writers
    for (let i = 0; i < 10; i++) {
      promises.push(
        (async () => {
          let attempts = 0
          while (attempts < 5) {
            try {
              await client.send(
                new TransactWriteItemsCommand({
                  TransactItems: [
                    {
                      Update: {
                        TableName: tableName,
                        Key: { id: { S: 'mixed' } },
                        UpdateExpression: 'ADD #counter :inc',
                        ExpressionAttributeNames: { '#counter': 'counter' },
                        ExpressionAttributeValues: { ':inc': { N: '1' } },
                      },
                    },
                  ],
                })
              )
              break
            } catch (error) {
              attempts++
              if (attempts >= 5) throw error
              await new Promise((resolve) =>
                setTimeout(resolve, Math.random() * 5)
              )
            }
          }
        })()
      )
    }

    // 10 readers
    for (let i = 0; i < 10; i++) {
      promises.push(
        client.send(
          new TransactGetItemsCommand({
            TransactItems: [
              { Get: { TableName: tableName, Key: { id: { S: 'mixed' } } } },
            ],
          })
        )
      )
    }

    await Promise.all(promises)

    // Final counter should be 10
    const result = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: 'mixed' } } })
    )
    expect(result.Item?.counter?.N).toBe('10')
  })

  test('should prevent lost updates with optimistic locking pattern', async () => {
    const tableName = getTableName()
    await createTable(client, tableName)

    // Setup item with version number
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: {
          id: { S: 'versioned' },
          value: { N: '100' },
          version: { N: '1' },
        },
      })
    )

    // Two transactions trying to update with version check
    // Both read version 1, but only first should succeed
    const txn1 = client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Update: {
              TableName: tableName,
              Key: { id: { S: 'versioned' } },
              UpdateExpression: 'SET #v = :newval, version = :newver',
              ConditionExpression: 'version = :oldver',
              ExpressionAttributeNames: { '#v': 'value' },
              ExpressionAttributeValues: {
                ':newval': { N: '200' },
                ':oldver': { N: '1' },
                ':newver': { N: '2' },
              },
            },
          },
        ],
      })
    )

    const txn2 = client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Update: {
              TableName: tableName,
              Key: { id: { S: 'versioned' } },
              UpdateExpression: 'SET #v = :newval, version = :newver',
              ConditionExpression: 'version = :oldver',
              ExpressionAttributeNames: { '#v': 'value' },
              ExpressionAttributeValues: {
                ':newval': { N: '300' },
                ':oldver': { N: '1' },
                ':newver': { N: '2' },
              },
            },
          },
        ],
      })
    )

    const results = await Promise.allSettled([txn1, txn2])

    // One should succeed, one should fail
    const succeeded = results.filter((r) => r.status === 'fulfilled').length
    const failed = results.filter((r) => r.status === 'rejected').length

    expect(succeeded).toBe(1)
    expect(failed).toBe(1)

    const result = await client.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: 'versioned' } },
      })
    )
    expect(result.Item?.version?.N).toBe('2')
    expect(['200', '300']).toContain(result.Item?.value?.N!)
  })
})
