import {
  describe,
  test,
  expect,
  beforeAll,
  afterEach,
  afterAll,
} from 'bun:test'
import {
  DynamoDBClient,
  CreateTableCommand,
  DeleteTableCommand,
  PutItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  QueryCommand,
  GetItemCommand,
  TransactWriteItemsCommand,
  TransactionCanceledException,
} from '@aws-sdk/client-dynamodb'
import {
  getGlobalTestDB,
  cleanupGlobalTestDB,
  createTable,
  cleanupTables,
  uniqueTableName,
  trackTable,
} from './helpers.ts'

describe('HTTP regression coverage', () => {
  let client: DynamoDBClient
  const createdTables: string[] = []

  beforeAll(async () => {
    const testDB = await getGlobalTestDB()
    client = testDB.client
  })

  afterEach(async () => {
    await cleanupTables(client, createdTables)
  })

  afterAll(async () => {
    await cleanupGlobalTestDB()
  })

  async function createSimpleTable(): Promise<string> {
    const tableName = trackTable(
      createdTables,
      uniqueTableName('RegressionTable')
    )
    await createTable(client, tableName)
    return tableName
  }

  test('conditional updates should reject when the condition fails', async () => {
    const tableName = await createSimpleTable()

    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: {
          id: { S: 'item-1' },
          version: { N: '1' },
          payload: { S: 'original' },
        },
      })
    )

    await expect(
      client.send(
        new UpdateItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item-1' } },
          UpdateExpression: 'SET #p = :next',
          ConditionExpression: '#v = :expected',
          ExpressionAttributeNames: {
            '#p': 'payload',
            '#v': 'version',
          },
          ExpressionAttributeValues: {
            ':next': { S: 'should reject' },
            ':expected': { N: '0' },
          },
          ReturnValues: 'ALL_NEW',
        })
      )
    ).rejects.toHaveProperty('name', 'ConditionalCheckFailedException')
  })

  test('delete without ReturnValues should not return attributes', async () => {
    const tableName = await createSimpleTable()
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: {
          id: { S: 'item-1' },
          note: { S: 'to be deleted' },
        },
      })
    )

    const response = await client.send(
      new DeleteItemCommand({
        TableName: tableName,
        Key: { id: { S: 'item-1' } },
        // Rely on the default ReturnValues=NONE
      })
    )

    expect(response.Attributes).toBeUndefined()
  })

  test('conditional put should reject when the item already exists', async () => {
    const tableName = await createSimpleTable()
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: 'item-1' }, payload: { S: 'original' } },
      })
    )

    await expect(
      client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: { id: { S: 'item-1' }, payload: { S: 'should fail' } },
          ConditionExpression: 'attribute_not_exists(id)',
        })
      )
    ).rejects.toHaveProperty('name', 'ConditionalCheckFailedException')

    const current = await client.send(
      new QueryCommand({
        TableName: tableName,
        KeyConditionExpression: '#id = :id',
        ExpressionAttributeNames: { '#id': 'id' },
        ExpressionAttributeValues: { ':id': { S: 'item-1' } },
      })
    )
    expect(current.Items?.[0]?.payload?.S).toBe('original')
  })

  test('conditional delete should reject when the predicate is false', async () => {
    const tableName = await createSimpleTable()
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: {
          id: { S: 'item-1' },
          state: { S: 'open' },
        },
      })
    )

    await expect(
      client.send(
        new DeleteItemCommand({
          TableName: tableName,
          Key: { id: { S: 'item-1' } },
          ConditionExpression: '#state = :expected',
          ExpressionAttributeNames: { '#state': 'state' },
          ExpressionAttributeValues: { ':expected': { S: 'closed' } },
        })
      )
    ).rejects.toHaveProperty('name', 'ConditionalCheckFailedException')

    const remaining = await client.send(
      new QueryCommand({
        TableName: tableName,
        KeyConditionExpression: '#id = :id',
        ExpressionAttributeNames: { '#id': 'id' },
        ExpressionAttributeValues: { ':id': { S: 'item-1' } },
      })
    )
    expect(remaining.Count).toBe(1)
  })

  test('update defaults to ReturnValues=NONE', async () => {
    const tableName = await createSimpleTable()
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: 'item-1' }, payload: { S: 'v1' } },
      })
    )

    const updateResponse = await client.send(
      new UpdateItemCommand({
        TableName: tableName,
        Key: { id: { S: 'item-1' } },
        UpdateExpression: 'SET payload = :next',
        ExpressionAttributeValues: { ':next': { S: 'v2' } },
        // rely on default ReturnValues
      })
    )

    expect(updateResponse.Attributes).toBeUndefined()

    const verify = await client.send(
      new QueryCommand({
        TableName: tableName,
        KeyConditionExpression: '#id = :id',
        ExpressionAttributeNames: { '#id': 'id' },
        ExpressionAttributeValues: { ':id': { S: 'item-1' } },
      })
    )
    expect(verify.Items?.[0]?.payload?.S).toBe('v2')
  })

  test('multi-table transaction failure should release locks for every table', async () => {
    const tableA = await createSimpleTable()
    const tableB = await createSimpleTable()
    const sharedKey = { id: { S: 'shared-key' } }

    await expect(
      client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: tableA,
                Item: { ...sharedKey, payload: { S: 'A' } },
              },
            },
            {
              Put: {
                TableName: tableB,
                Item: { ...sharedKey, payload: { S: 'B' } },
              },
            },
            {
              ConditionCheck: {
                TableName: tableA,
                Key: { id: { S: 'missing' } },
                ConditionExpression: 'attribute_exists(id)',
              },
            },
          ],
        })
      )
    ).rejects.toBeInstanceOf(TransactionCanceledException)

    // Follow-up transaction touching tableB should succeed if locks were released
    await client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Put: {
              TableName: tableB,
              Item: { ...sharedKey, payload: { S: 'B2' } },
            },
          },
        ],
      })
    )
  })

  test('query pagination in descending order should return next items', async () => {
    const tableName = uniqueTableName('RegressionRange')

    await client.send(
      new CreateTableCommand({
        TableName: tableName,
        KeySchema: [
          { AttributeName: 'id', KeyType: 'HASH' },
          { AttributeName: 'sk', KeyType: 'RANGE' },
        ],
        AttributeDefinitions: [
          { AttributeName: 'id', AttributeType: 'S' },
          { AttributeName: 'sk', AttributeType: 'S' },
        ],
        BillingMode: 'PAY_PER_REQUEST',
      })
    )

    const baseItem = { id: { S: 'partition-1' } }
    for (const sk of ['001', '002', '003']) {
      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: { ...baseItem, sk: { S: sk }, value: { N: sk } },
        })
      )
    }

    const firstPage = await client.send(
      new QueryCommand({
        TableName: tableName,
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'id' },
        ExpressionAttributeValues: { ':pk': baseItem.id },
        ScanIndexForward: false,
        Limit: 1,
      })
    )

    expect(firstPage.Items?.[0]?.sk?.S).toBe('003')
    expect(firstPage.LastEvaluatedKey).toBeDefined()

    const secondPage = await client.send(
      new QueryCommand({
        TableName: tableName,
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'id' },
        ExpressionAttributeValues: { ':pk': baseItem.id },
        ScanIndexForward: false,
        Limit: 1,
        ExclusiveStartKey: firstPage.LastEvaluatedKey,
      })
    )

    expect(secondPage.Items?.[0]?.sk?.S).toBe('002')
  })

  test('reads should not observe placeholders from in-flight transactions', async () => {
    const tableName = await createSimpleTable()
    const targetKeys = Array.from({ length: 25 }, (_, idx) => `txn-${idx}`)
    const stopTime = Date.now() + 1500
    const anomalies: string[] = []

    const writers = targetKeys.map(async (key) => {
      try {
        await client.send(
          new TransactWriteItemsCommand({
            TransactItems: [
              {
                Put: {
                  TableName: tableName,
                  Item: {
                    id: { S: key },
                    payload: { S: `value-${key}` },
                  },
                },
              },
              {
                ConditionCheck: {
                  TableName: tableName,
                  Key: { id: { S: 'missing-sentinel' } },
                  ConditionExpression: 'attribute_exists(id)', // always fails
                },
              },
            ],
          })
        )
      } catch {
        // expected to fail
      }
    })

    while (Date.now() < stopTime && anomalies.length === 0) {
      for (const key of targetKeys) {
        const resp = await client.send(
          new GetItemCommand({
            TableName: tableName,
            Key: { id: { S: key } },
          })
        )

        if (resp.Item && !('payload' in resp.Item)) {
          anomalies.push(key)
          break
        }
      }
    }

    await Promise.allSettled(writers)

    expect(anomalies).toHaveLength(0)
  })
})
