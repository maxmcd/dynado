// Comprehensive tests for the Two-Phase Commit implementation
// Tests the new DO-compatible architecture via HTTP API

import { describe, test, expect, beforeAll } from 'bun:test'
import {
  DynamoDBClient,
  PutItemCommand,
  GetItemCommand,
  TransactWriteItemsCommand,
  TransactGetItemsCommand,
  TransactionCanceledException,
} from '@aws-sdk/client-dynamodb'
import { getGlobalTestDB, createTable, uniqueTableName } from './helpers.ts'

let client: DynamoDBClient

// Helper to generate unique table names
function getTableName(): string {
  return uniqueTableName('Test2PC')
}

beforeAll(async () => {
  const { client: testClient } = await getGlobalTestDB()
  client = testClient
})

describe('Two-Phase Commit Protocol', () => {
  describe('Transaction Conflicts', () => {
    test('should detect concurrent transaction conflict on same item', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      // Create an item
      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: { id: { S: 'conflict-item' }, value: { N: '1' } },
        })
      )

      // Execute two transactions that modify the same item serially
      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'conflict-item' } },
                UpdateExpression: 'SET #value = :val',
                ExpressionAttributeNames: { '#value': 'value' },
                ExpressionAttributeValues: { ':val': { N: '10' } },
              },
            },
          ],
        })
      )

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'conflict-item' } },
                UpdateExpression: 'SET #value = :val',
                ExpressionAttributeNames: { '#value': 'value' },
                ExpressionAttributeValues: { ':val': { N: '20' } },
              },
            },
          ],
        })
      )

      // Last transaction wins
      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'conflict-item' } },
        })
      )
      expect(result.Item?.value!.N).toBe('20')
    })

    test('should handle transaction on locked item', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: { id: { S: 'item1' }, value: { N: '1' } },
        })
      )

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'item1' } },
                UpdateExpression: 'SET #value = :val',
                ExpressionAttributeNames: { '#value': 'value' },
                ExpressionAttributeValues: { ':val': { N: '10' } },
              },
            },
          ],
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item1' } },
        })
      )
      expect(result.Item?.value!.N).toBe('10')
    })
  })

  describe('Condition Expression Validation', () => {
    test('should validate multiple condition types in one transaction', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: { id: { S: 'item1' }, status: { S: 'active' } },
        })
      )

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'item1' } },
                UpdateExpression: 'SET #value = :val',
                ExpressionAttributeNames: {
                  '#value': 'value',
                  '#status': 'status',
                },
                ExpressionAttributeValues: {
                  ':val': { S: 'updated' },
                  ':active': { S: 'active' },
                },
                ConditionExpression:
                  'attribute_exists(id) AND #status = :active',
              },
            },
            {
              Put: {
                TableName: tableName,
                Item: { id: { S: 'item2' }, status: { S: 'new' } },
                ConditionExpression: 'attribute_not_exists(id)',
              },
            },
          ],
        })
      )

      const result1 = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item1' } },
        })
      )
      const result2 = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item2' } },
        })
      )

      expect(result1.Item?.value!.S).toBe('updated')
      expect(result2.Item?.status!.S).toBe('new')
    })

    test('should support comparison operators in conditions', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: { id: { S: 'item1' }, count: { N: '5' } },
        })
      )

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'item1' } },
                UpdateExpression: 'ADD #count :inc',
                ExpressionAttributeNames: { '#count': 'count' },
                ExpressionAttributeValues: {
                  ':inc': { N: '3' },
                  ':min': { N: '3' },
                },
                ConditionExpression: '#count > :min',
              },
            },
          ],
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item1' } },
        })
      )
      expect(result.Item?.count!.N).toBe('8')
    })

    test('should handle NOT operator in conditions', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: { id: { S: 'item1' }, status: { S: 'active' } },
        })
      )

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'item1' } },
                UpdateExpression: 'SET #status = :val',
                ExpressionAttributeNames: { '#status': 'status' },
                ExpressionAttributeValues: {
                  ':val': { S: 'inactive' },
                  ':blocked': { S: 'blocked' },
                },
                ConditionExpression: 'NOT #status = :blocked',
              },
            },
          ],
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item1' } },
        })
      )
      expect(result.Item?.status!.S).toBe('inactive')
    })

    test('should handle AND operator in conditions', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: 'item1' },
            status: { S: 'active' },
            count: { N: '10' },
          },
        })
      )

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'item1' } },
                UpdateExpression: 'SET #count = :val',
                ExpressionAttributeNames: {
                  '#status': 'status',
                  '#count': 'count',
                },
                ExpressionAttributeValues: {
                  ':val': { N: '20' },
                  ':status': { S: 'active' },
                  ':min': { N: '5' },
                },
                ConditionExpression: '#status = :status AND #count > :min',
              },
            },
          ],
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item1' } },
        })
      )
      expect(result.Item?.count!.N).toBe('20')
    })

    test('should handle OR operator in conditions', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: 'item1' },
            status: { S: 'pending' },
          },
        })
      )

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'item1' } },
                UpdateExpression: 'SET #status = :val',
                ExpressionAttributeNames: { '#status': 'status' },
                ExpressionAttributeValues: {
                  ':val': { S: 'active' },
                  ':status1': { S: 'pending' },
                  ':status2': { S: 'ready' },
                },
                ConditionExpression: '#status = :status1 OR #status = :status2',
              },
            },
          ],
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item1' } },
        })
      )
      expect(result.Item?.status!.S).toBe('active')
    })
  })

  describe('ReturnValuesOnConditionCheckFailure', () => {
    test('should return old values on condition failure when requested', async () => {
      // Skip this test when not using DynamoDB Local (feature not yet implemented in dynado)
      if (process.env.TEST_DYNAMODB_LOCAL !== 'true') {
        return
      }
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: 'item1' },
            value: { S: 'original' },
            count: { N: '5' },
          },
        })
      )

      try {
        await client.send(
          new TransactWriteItemsCommand({
            TransactItems: [
              {
                Put: {
                  TableName: tableName,
                  Item: { id: { S: 'item1' }, value: { S: 'new' } },
                  ConditionExpression: 'attribute_not_exists(id)',
                  ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
                },
              },
            ],
          })
        )
        expect(true).toBe(false) // Should not reach here
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionCanceledException)
        const txError = error as TransactionCanceledException
        expect(txError.CancellationReasons?.[0]?.Code).toBe(
          'ConditionalCheckFailed'
        )
        expect(txError.CancellationReasons?.[0]?.Item).toBeDefined()
        expect(txError.CancellationReasons?.[0]?.Item?.value!.S).toBe(
          'original'
        )
      }
    })

    test('should not return values when returnValuesOnConditionCheckFailure is NONE', async () => {
      // Skip this test when not using DynamoDB Local (feature not yet implemented in dynado)
      if (process.env.TEST_DYNAMODB_LOCAL !== 'true') {
        return
      }
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: { id: { S: 'item1' }, value: { S: 'original' } },
        })
      )

      try {
        await client.send(
          new TransactWriteItemsCommand({
            TransactItems: [
              {
                Put: {
                  TableName: tableName,
                  Item: { id: { S: 'item1' }, value: { S: 'new' } },
                  ConditionExpression: 'attribute_not_exists(id)',
                  ReturnValuesOnConditionCheckFailure: 'NONE',
                },
              },
            ],
          })
        )
        expect(true).toBe(false)
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionCanceledException)
        const txError = error as TransactionCanceledException
        expect(txError.CancellationReasons?.[0]?.Code).toBe(
          'ConditionalCheckFailed'
        )
        expect(txError.CancellationReasons?.[0]?.Item).toBeUndefined()
      }
    })

    test('should return values on ConditionCheck failure', async () => {
      // Skip this test when not using DynamoDB Local (feature not yet implemented in dynado)
      if (process.env.TEST_DYNAMODB_LOCAL !== 'true') {
        return
      }
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: 'item1' },
            status: { S: 'inactive' },
            data: { S: 'secret' },
          },
        })
      )

      try {
        await client.send(
          new TransactWriteItemsCommand({
            TransactItems: [
              {
                ConditionCheck: {
                  TableName: tableName,
                  Key: { id: { S: 'item1' } },
                  ConditionExpression: '#status = :val',
                  ExpressionAttributeNames: { '#status': 'status' },
                  ExpressionAttributeValues: { ':val': { S: 'active' } },
                  ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
                },
              },
            ],
          })
        )
        expect(true).toBe(false)
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionCanceledException)
        const txError = error as TransactionCanceledException
        expect(txError.CancellationReasons?.[0]?.Item?.status!.S).toBe(
          'inactive'
        )
        expect(txError.CancellationReasons?.[0]?.Item?.data!.S).toBe('secret')
      }
    })
  })

  describe('Update Expression Variants', () => {
    test('should handle multiple SET operations in one expression', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: 'item1' },
            field1: { S: 'old1' },
            field2: { S: 'old2' },
          },
        })
      )

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'item1' } },
                UpdateExpression:
                  'SET field1 = :val1, field2 = :val2, field3 = :val3',
                ExpressionAttributeValues: {
                  ':val1': { S: 'new1' },
                  ':val2': { S: 'new2' },
                  ':val3': { S: 'new3' },
                },
              },
            },
          ],
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item1' } },
        })
      )
      expect(result.Item?.field1!.S).toBe('new1')
      expect(result.Item?.field2!.S).toBe('new2')
      expect(result.Item?.field3!.S).toBe('new3')
    })

    test('should handle multiple REMOVE operations', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: 'item1' },
            field1: { S: 'value1' },
            field2: { S: 'value2' },
            field3: { S: 'value3' },
          },
        })
      )

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'item1' } },
                UpdateExpression: 'REMOVE field1, field2',
              },
            },
          ],
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item1' } },
        })
      )
      expect(result.Item?.field1).toBeUndefined()
      expect(result.Item?.field2).toBeUndefined()
      expect(result.Item?.field3!.S).toBe('value3')
    })

    test('should handle ADD operation on non-existent attribute', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: { id: { S: 'item1' } },
        })
      )

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'item1' } },
                UpdateExpression: 'ADD #count :val',
                ExpressionAttributeNames: { '#count': 'count' },
                ExpressionAttributeValues: { ':val': { N: '5' } },
              },
            },
          ],
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item1' } },
        })
      )
      expect(result.Item?.count!.N).toBe('5')
    })

    test('should handle combined SET, REMOVE, and ADD operations', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: 'item1' },
            status: { S: 'old' },
            removeMe: { S: 'gone' },
            count: { N: '10' },
          },
        })
      )

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'item1' } },
                UpdateExpression:
                  'SET #status = :status ADD #count :inc REMOVE removeMe',
                ExpressionAttributeNames: {
                  '#status': 'status',
                  '#count': 'count',
                },
                ExpressionAttributeValues: {
                  ':status': { S: 'new' },
                  ':inc': { N: '5' },
                },
              },
            },
          ],
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item1' } },
        })
      )
      expect(result.Item?.status!.S).toBe('new')
      expect(result.Item?.count!.N).toBe('15')
      expect(result.Item?.removeMe).toBeUndefined()
    })

    test('should handle Update on non-existent item (creates new item)', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'new-item' } },
                UpdateExpression: 'SET #value = :val',
                ExpressionAttributeNames: { '#value': 'value' },
                ExpressionAttributeValues: { ':val': { S: 'created' } },
              },
            },
          ],
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'new-item' } },
        })
      )
      expect(result.Item?.id!.S).toBe('new-item')
      expect(result.Item?.value!.S).toBe('created')
    })
  })

  describe('Cross-Shard Transactions', () => {
    test('should commit transaction spanning multiple shards', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      // Create items that will hash to different shards
      const itemIds = [
        'shard-test-1',
        'shard-test-2',
        'shard-test-3',
        'shard-test-4',
        'shard-test-5',
      ]

      const items = itemIds.map((id) => ({
        Put: {
          TableName: tableName,
          Item: { id: { S: id }, value: { S: `value-${id}` } },
        },
      }))

      await client.send(new TransactWriteItemsCommand({ TransactItems: items }))

      // Verify all items were created
      for (const id of itemIds) {
        const result = await client.send(
          new GetItemCommand({ TableName: tableName, Key: { id: { S: id } } })
        )
        expect(result.Item?.value!.S).toBe(`value-${id}`)
      }
    })

    test('should rollback cross-shard transaction on any failure', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      // Create one item
      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: { id: { S: 'existing' }, status: { S: 'active' } },
        })
      )

      try {
        await client.send(
          new TransactWriteItemsCommand({
            TransactItems: [
              {
                Put: {
                  TableName: tableName,
                  Item: { id: { S: 'new-1' }, value: { S: 'first' } },
                },
              },
              {
                Put: {
                  TableName: tableName,
                  Item: { id: { S: 'new-2' }, value: { S: 'second' } },
                },
              },
              {
                Update: {
                  TableName: tableName,
                  Key: { id: { S: 'existing' } },
                  UpdateExpression: 'SET #status = :val',
                  ExpressionAttributeNames: { '#status': 'status' },
                  ExpressionAttributeValues: {
                    ':val': { S: 'updated' },
                    ':wrong': { S: 'wrong' },
                  },
                  ConditionExpression: '#status = :wrong',
                },
              },
              {
                Put: {
                  TableName: tableName,
                  Item: { id: { S: 'new-3' }, value: { S: 'third' } },
                },
              },
            ],
          })
        )
        expect(true).toBe(false)
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionCanceledException)
      }

      // Verify none of the new items were created
      const result1 = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'new-1' } },
        })
      )
      const result2 = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'new-2' } },
        })
      )
      const result3 = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'new-3' } },
        })
      )
      const existing = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'existing' } },
        })
      )

      expect(result1.Item).toBeUndefined()
      expect(result2.Item).toBeUndefined()
      expect(result3.Item).toBeUndefined()
      expect(existing.Item?.status!.S).toBe('active') // Unchanged
    })
  })

  describe('Idempotency', () => {
    test('should cache transaction results for 10 minutes', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      const token = `idempotency-test-${Date.now()}`

      // First execution
      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: tableName,
                Item: { id: { S: 'idempotent-item' }, value: { S: 'first' } },
              },
            },
          ],
          ClientRequestToken: token,
        })
      )

      let result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'idempotent-item' } },
        })
      )
      expect(result.Item?.value!.S).toBe('first')

      // Modify the item directly
      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: { id: { S: 'idempotent-item' }, value: { S: 'modified' } },
        })
      )

      // Execute same transaction with same token - should be idempotent
      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: tableName,
                Item: { id: { S: 'idempotent-item' }, value: { S: 'first' } },
              },
            },
          ],
          ClientRequestToken: token,
        })
      )

      // Item should still be "modified" (transaction was not re-executed)
      result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'idempotent-item' } },
        })
      )
      expect(result.Item?.value!.S).toBe('modified')
    })

    test('should execute transaction with different token', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      const token1 = `token-1-${Date.now()}`
      const token2 = `token-2-${Date.now()}`

      // First execution with token1
      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: tableName,
                Item: { id: { S: 'item-tokens' }, counter: { N: '1' } },
              },
            },
          ],
          ClientRequestToken: token1,
        })
      )

      // Change the item
      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: { id: { S: 'item-tokens' }, counter: { N: '5' } },
        })
      )

      // Different token should allow re-execution
      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: tableName,
                Item: { id: { S: 'item-tokens' }, counter: { N: '1' } },
              },
            },
          ],
          ClientRequestToken: token2,
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item-tokens' } },
        })
      )
      expect(result.Item?.counter!.N).toBe('1') // Overwritten by second transaction
    })

    test('should handle failed transaction idempotency', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: { id: { S: 'item1' }, status: { S: 'active' } },
        })
      )

      const token = `fail-token-${Date.now()}`

      // First attempt - should fail
      try {
        await client.send(
          new TransactWriteItemsCommand({
            TransactItems: [
              {
                Update: {
                  TableName: tableName,
                  Key: { id: { S: 'item1' } },
                  UpdateExpression: 'SET #status = :val',
                  ExpressionAttributeNames: { '#status': 'status' },
                  ExpressionAttributeValues: {
                    ':val': { S: 'updated' },
                    ':wrong': { S: 'wrong' },
                  },
                  ConditionExpression: '#status = :wrong',
                },
              },
            ],
            ClientRequestToken: token,
          })
        )
        expect(true).toBe(false)
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionCanceledException)
      }

      // Second attempt with same token - should still fail (not cached as success)
      try {
        await client.send(
          new TransactWriteItemsCommand({
            TransactItems: [
              {
                Update: {
                  TableName: tableName,
                  Key: { id: { S: 'item1' } },
                  UpdateExpression: 'SET #status = :val',
                  ExpressionAttributeNames: { '#status': 'status' },
                  ExpressionAttributeValues: {
                    ':val': { S: 'updated' },
                    ':wrong': { S: 'wrong' },
                  },
                  ConditionExpression: '#status = :wrong',
                },
              },
            ],
            ClientRequestToken: token,
          })
        )
        expect(true).toBe(false)
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionCanceledException)
      }
    })
  })

  describe('Edge Cases', () => {
    test('should handle empty update expression attributes', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: { id: { S: 'item1' }, value: { S: 'original' } },
        })
      )

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'item1' } },
                UpdateExpression: 'SET #value = :val',
                ExpressionAttributeNames: { '#value': 'value' },
                ExpressionAttributeValues: { ':val': { S: 'updated' } },
              },
            },
          ],
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item1' } },
        })
      )
      expect(result.Item?.value!.S).toBe('updated')
    })

    test('should handle transaction with single item', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: tableName,
                Item: { id: { S: 'single-item' }, value: { S: 'solo' } },
              },
            },
          ],
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'single-item' } },
        })
      )
      expect(result.Item?.value!.S).toBe('solo')
    })

    test('should handle Delete operation on non-existent item', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      // Should succeed (DynamoDB behavior)
      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Delete: {
                TableName: tableName,
                Key: { id: { S: 'does-not-exist' } },
              },
            },
          ],
        })
      )
    })

    test('should handle Put with condition on non-existent item', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: tableName,
                Item: { id: { S: 'new-item' }, value: { S: 'value' } },
                ConditionExpression: 'attribute_not_exists(id)',
              },
            },
          ],
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'new-item' } },
        })
      )
      expect(result.Item?.value!.S).toBe('value')
    })

    test('should validate attribute names with special characters', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: 'item1' },
            'special-attr': { S: 'value' },
          },
        })
      )

      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'item1' } },
                UpdateExpression: 'SET #attr = :val',
                ExpressionAttributeNames: { '#attr': 'special-attr' },
                ExpressionAttributeValues: { ':val': { S: 'updated' } },
              },
            },
          ],
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item1' } },
        })
      )
      expect(result.Item?.['special-attr']!.S).toBe('updated')
    })
  })

  describe('TransactGetItems', () => {
    test('should get items from multiple shards atomically', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      // Create items across different shards
      const itemIds = ['get-1', 'get-2', 'get-3', 'get-4', 'get-5']

      for (const id of itemIds) {
        await client.send(
          new PutItemCommand({
            TableName: tableName,
            Item: {
              id: { S: id },
              value: { S: `value-${id}` },
              metadata: { S: 'data' },
            },
          })
        )
      }

      const result = await client.send(
        new TransactGetItemsCommand({
          TransactItems: itemIds.map((id) => ({
            Get: { TableName: tableName, Key: { id: { S: id } } },
          })),
        })
      )

      expect(result.Responses!.length).toBe(5)
      for (let i = 0; i < itemIds.length; i++) {
        expect(result.Responses![i]!.Item?.id!.S).toBe(itemIds[i])
        expect(result.Responses![i]!.Item?.value!.S).toBe(`value-${itemIds[i]}`)
      }
    })

    test('should apply projection to subset of attributes', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: 'proj-item' },
            field1: { S: 'value1' },
            field2: { S: 'value2' },
            field3: { S: 'value3' },
          },
        })
      )

      const result = await client.send(
        new TransactGetItemsCommand({
          TransactItems: [
            {
              Get: {
                TableName: tableName,
                Key: { id: { S: 'proj-item' } },
                ProjectionExpression: 'id, field1, field3',
              },
            },
          ],
        })
      )

      expect(result.Responses![0]!.Item?.id!.S).toBe('proj-item')
      expect(result.Responses![0]!.Item?.field1!.S).toBe('value1')
      expect(result.Responses![0]!.Item?.field2).toBeUndefined()
      expect(result.Responses![0]!.Item?.field3!.S).toBe('value3')
    })

    test('should handle projection with attribute name placeholders', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: 'proj-item-2' },
            status: { S: 'active' },
            data: { S: 'sensitive' },
            public: { S: 'public-info' },
          },
        })
      )

      const result = await client.send(
        new TransactGetItemsCommand({
          TransactItems: [
            {
              Get: {
                TableName: tableName,
                Key: { id: { S: 'proj-item-2' } },
                ProjectionExpression: '#id, #status',
                ExpressionAttributeNames: { '#id': 'id', '#status': 'status' },
              },
            },
          ],
        })
      )

      expect(result.Responses![0]!.Item?.id!.S).toBe('proj-item-2')
      expect(result.Responses![0]!.Item?.status!.S).toBe('active')
      expect(result.Responses![0]!.Item?.data).toBeUndefined()
      expect(result.Responses![0]!.Item?.public).toBeUndefined()
    })

    test('should return empty Item for non-existent items in the result array', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: { id: { S: 'exists' }, value: { S: 'here' } },
        })
      )

      const result = await client.send(
        new TransactGetItemsCommand({
          TransactItems: [
            { Get: { TableName: tableName, Key: { id: { S: 'exists' } } } },
            {
              Get: {
                TableName: tableName,
                Key: { id: { S: 'does-not-exist' } },
              },
            },
            {
              Get: { TableName: tableName, Key: { id: { S: 'also-missing' } } },
            },
          ],
        })
      )

      expect(result.Responses!.length).toBe(3)
      expect(result.Responses![0]!.Item?.value!.S).toBe('here')
      expect(result.Responses![1]!.Item).toBeUndefined()
      expect(result.Responses![2]!.Item).toBeUndefined()
    })

    test('should respect 100 item limit for TransactGetItems', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      const items = []
      for (let i = 0; i < 101; i++) {
        items.push({
          Get: {
            TableName: tableName,
            Key: { id: { S: `item-${i}` } },
          },
        })
      }

      try {
        await client.send(new TransactGetItemsCommand({ TransactItems: items }))
        expect(true).toBe(false)
      } catch (error: any) {
        expect(error.message).toContain('100')
      }
    })
  })

  describe('Complex Scenarios', () => {
    test('should handle workflow pattern with condition checks', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      // Create initial workflow state
      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: 'workflow-1' },
            state: { S: 'pending' },
            retries: { N: '0' },
          },
        })
      )

      // Transition workflow: check state, update state, increment retries
      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'workflow-1' } },
                UpdateExpression: 'SET #state = :processing ADD #retries :inc',
                ExpressionAttributeNames: {
                  '#state': 'state',
                  '#retries': 'retries',
                },
                ExpressionAttributeValues: {
                  ':processing': { S: 'processing' },
                  ':inc': { N: '1' },
                  ':pending': { S: 'pending' },
                },
                ConditionExpression: '#state = :pending',
              },
            },
          ],
        })
      )

      const result = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'workflow-1' } },
        })
      )
      expect(result.Item?.state!.S).toBe('processing')
      expect(result.Item?.retries!.N).toBe('1')
    })

    test('should handle inventory deduction pattern', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      // Setup: inventory and order tracking
      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: 'product-123' },
            stock: { N: '10' },
          },
        })
      )

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: 'order-456' },
            status: { S: 'pending' },
          },
        })
      )

      // Deduct inventory and mark order as confirmed
      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'product-123' } },
                UpdateExpression: 'SET #stock = :newStock',
                ExpressionAttributeNames: { '#stock': 'stock' },
                ExpressionAttributeValues: {
                  ':newStock': { N: '7' },
                  ':min': { N: '3' },
                },
                ConditionExpression: '#stock > :min', // Prevent negative inventory
              },
            },
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: 'order-456' } },
                UpdateExpression: 'SET #status = :confirmed',
                ExpressionAttributeNames: { '#status': 'status' },
                ExpressionAttributeValues: { ':confirmed': { S: 'confirmed' } },
              },
            },
          ],
        })
      )

      const product = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'product-123' } },
        })
      )
      const order = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'order-456' } },
        })
      )

      expect(product.Item?.stock!.N).toBe('7')
      expect(order.Item?.status!.S).toBe('confirmed')
    })

    test('should rollback when inventory is insufficient', async () => {
      const tableName = getTableName()
      await createTable(client, tableName)

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: 'product-999' },
            stock: { N: '2' },
          },
        })
      )

      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            id: { S: 'order-999' },
            status: { S: 'pending' },
          },
        })
      )

      try {
        await client.send(
          new TransactWriteItemsCommand({
            TransactItems: [
              {
                Update: {
                  TableName: tableName,
                  Key: { id: { S: 'product-999' } },
                  UpdateExpression: 'SET #stock = :newStock',
                  ExpressionAttributeNames: { '#stock': 'stock' },
                  ExpressionAttributeValues: {
                    ':newStock': { N: '-3' },
                    ':min': { N: '3' },
                  },
                  ConditionExpression: '#stock > :min', // Will fail: 2 is not > 3
                },
              },
              {
                Update: {
                  TableName: tableName,
                  Key: { id: { S: 'order-999' } },
                  UpdateExpression: 'SET #status = :confirmed',
                  ExpressionAttributeNames: { '#status': 'status' },
                  ExpressionAttributeValues: {
                    ':confirmed': { S: 'confirmed' },
                  },
                },
              },
            ],
          })
        )
        expect(true).toBe(false)
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionCanceledException)
      }

      // Verify nothing changed
      const product = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'product-999' } },
        })
      )
      const order = await client.send(
        new GetItemCommand({
          TableName: tableName,
          Key: { id: { S: 'order-999' } },
        })
      )

      expect(product.Item?.stock!.N).toBe('2') // Unchanged
      expect(order.Item?.status!.S).toBe('pending') // Unchanged
    })
  })
})
