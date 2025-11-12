// Tests for range query functionality with sort keys
// Uses HTTP API via AWS SDK

import {
  test,
  expect,
  beforeAll,
  beforeEach,
  afterEach,
  afterAll,
  describe,
} from "bun:test";
import {
  DynamoDBClient,
  CreateTableCommand,
  DeleteTableCommand,
  PutItemCommand,
  QueryCommand,
} from "@aws-sdk/client-dynamodb";
import { getGlobalTestDB } from "./test-global-setup.ts";

describe("Range Queries", () => {
  let client: DynamoDBClient;
  const createdTables: string[] = [];

  beforeAll(async () => {
    const testDB = await getGlobalTestDB();
    client = testDB.client;
  });

  beforeEach(async () => {
    // Create table with composite key (partition + sort)
    const tableName = `RangeTest_${Date.now()}`;
    createdTables.push(tableName);

    await client.send(
      new CreateTableCommand({
        TableName: tableName,
        KeySchema: [
          { AttributeName: "userId", KeyType: "HASH" },
          { AttributeName: "timestamp", KeyType: "RANGE" },
        ],
        AttributeDefinitions: [
          { AttributeName: "userId", AttributeType: "S" },
          { AttributeName: "timestamp", AttributeType: "N" },
        ],
        BillingMode: "PAY_PER_REQUEST",
      })
    );

    // Insert test data with various timestamps
    const testData = [
      { userId: { S: "user1" }, timestamp: { N: "100" }, data: { S: "a" } },
      { userId: { S: "user1" }, timestamp: { N: "200" }, data: { S: "b" } },
      { userId: { S: "user1" }, timestamp: { N: "300" }, data: { S: "c" } },
      { userId: { S: "user1" }, timestamp: { N: "400" }, data: { S: "d" } },
      { userId: { S: "user1" }, timestamp: { N: "500" }, data: { S: "e" } },
      { userId: { S: "user2" }, timestamp: { N: "100" }, data: { S: "f" } },
      { userId: { S: "user2" }, timestamp: { N: "200" }, data: { S: "g" } },
    ];

    for (const item of testData) {
      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: item,
        })
      );
    }
  });

  afterEach(async () => {
    // Clean up tables
    for (const tableName of createdTables) {
      try {
        await client.send(new DeleteTableCommand({ TableName: tableName }));
      } catch (error) {
        // Ignore errors
      }
    }
    createdTables.length = 0;
  });

  function getTableName(): string {
    return createdTables[createdTables.length - 1]!;
  }

  test("should query all items for a partition key without sort key condition", async () => {
    const result = await client.send(
      new QueryCommand({
        TableName: getTableName(),
        KeyConditionExpression: "userId = :userId",
        ExpressionAttributeValues: {
          ":userId": { S: "user1" },
        },
      })
    );

    expect(result.Items!.length).toBe(5);
    expect(result.Count).toBe(5);
    // Items should be in ascending order by default
    expect(result.Items![0]!.timestamp!.N).toBe("100");
    expect(result.Items![4]!.timestamp!.N).toBe("500");
  });

  test("should query with = operator", async () => {
    const result = await client.send(
      new QueryCommand({
        TableName: getTableName(),
        KeyConditionExpression: "userId = :userId AND #ts = :timestamp",
        ExpressionAttributeNames: {
          "#ts": "timestamp",
        },
        ExpressionAttributeValues: {
          ":userId": { S: "user1" },
          ":timestamp": { N: "300" },
        },
      })
    );

    expect(result.Items!.length).toBe(1);
    expect(result.Items![0]!.timestamp!.N).toBe("300");
    expect(result.Items![0]!.data!.S).toBe("c");
  });

  test("should query with < operator", async () => {
    const result = await client.send(
      new QueryCommand({
        TableName: getTableName(),
        KeyConditionExpression: "userId = :userId AND #ts < :timestamp",
        ExpressionAttributeNames: {
          "#ts": "timestamp",
        },
        ExpressionAttributeValues: {
          ":userId": { S: "user1" },
          ":timestamp": { N: "300" },
        },
      })
    );

    expect(result.Items!.length).toBe(2);
    expect(result.Items![0]!.timestamp!.N).toBe("100");
    expect(result.Items![1]!.timestamp!.N).toBe("200");
  });

  test("should query with > operator", async () => {
    const result = await client.send(
      new QueryCommand({
        TableName: getTableName(),
        KeyConditionExpression: "userId = :userId AND #ts > :timestamp",
        ExpressionAttributeNames: {
          "#ts": "timestamp",
        },
        ExpressionAttributeValues: {
          ":userId": { S: "user1" },
          ":timestamp": { N: "300" },
        },
      })
    );

    expect(result.Items!.length).toBe(2);
    expect(result.Items![0]!.timestamp!.N).toBe("400");
    expect(result.Items![1]!.timestamp!.N).toBe("500");
  });

  test("should query with <= operator", async () => {
    const result = await client.send(
      new QueryCommand({
        TableName: getTableName(),
        KeyConditionExpression: "userId = :userId AND #ts <= :timestamp",
        ExpressionAttributeNames: {
          "#ts": "timestamp",
        },
        ExpressionAttributeValues: {
          ":userId": { S: "user1" },
          ":timestamp": { N: "300" },
        },
      })
    );

    expect(result.Items!.length).toBe(3);
    expect(result.Items![0]!.timestamp!.N).toBe("100");
    expect(result.Items![1]!.timestamp!.N).toBe("200");
    expect(result.Items![2]!.timestamp!.N).toBe("300");
  });

  test("should query with >= operator", async () => {
    const result = await client.send(
      new QueryCommand({
        TableName: getTableName(),
        KeyConditionExpression: "userId = :userId AND #ts >= :timestamp",
        ExpressionAttributeNames: {
          "#ts": "timestamp",
        },
        ExpressionAttributeValues: {
          ":userId": { S: "user1" },
          ":timestamp": { N: "300" },
        },
      })
    );

    expect(result.Items!.length).toBe(3);
    expect(result.Items![0]!.timestamp!.N).toBe("300");
    expect(result.Items![1]!.timestamp!.N).toBe("400");
    expect(result.Items![2]!.timestamp!.N).toBe("500");
  });

  test("should query with BETWEEN operator", async () => {
    const result = await client.send(
      new QueryCommand({
        TableName: getTableName(),
        KeyConditionExpression:
          "userId = :userId AND #ts BETWEEN :start AND :end",
        ExpressionAttributeNames: {
          "#ts": "timestamp",
        },
        ExpressionAttributeValues: {
          ":userId": { S: "user1" },
          ":start": { N: "200" },
          ":end": { N: "400" },
        },
      })
    );

    expect(result.Items!.length).toBe(3);
    expect(result.Items![0]!.timestamp!.N).toBe("200");
    expect(result.Items![1]!.timestamp!.N).toBe("300");
    expect(result.Items![2]!.timestamp!.N).toBe("400");
  });

  test("should query with begins_with operator", async () => {
    // Create a table with string sort keys for begins_with testing
    const tableName = `BeginsWith_${Date.now()}`;
    createdTables.push(tableName);

    await client.send(
      new CreateTableCommand({
        TableName: tableName,
        KeySchema: [
          { AttributeName: "category", KeyType: "HASH" },
          { AttributeName: "itemId", KeyType: "RANGE" },
        ],
        AttributeDefinitions: [
          { AttributeName: "category", AttributeType: "S" },
          { AttributeName: "itemId", AttributeType: "S" },
        ],
        BillingMode: "PAY_PER_REQUEST",
      })
    );

    // Insert test data
    const items = [
      {
        category: { S: "products" },
        itemId: { S: "PROD-001" },
        name: { S: "Item 1" },
      },
      {
        category: { S: "products" },
        itemId: { S: "PROD-002" },
        name: { S: "Item 2" },
      },
      {
        category: { S: "products" },
        itemId: { S: "PROD-003" },
        name: { S: "Item 3" },
      },
      {
        category: { S: "products" },
        itemId: { S: "ORDER-001" },
        name: { S: "Order 1" },
      },
      {
        category: { S: "products" },
        itemId: { S: "ORDER-002" },
        name: { S: "Order 2" },
      },
    ];

    for (const item of items) {
      await client.send(
        new PutItemCommand({
          TableName: tableName,
          Item: item,
        })
      );
    }

    const result = await client.send(
      new QueryCommand({
        TableName: tableName,
        KeyConditionExpression:
          "category = :category AND begins_with(itemId, :prefix)",
        ExpressionAttributeValues: {
          ":category": { S: "products" },
          ":prefix": { S: "PROD" },
        },
      })
    );

    expect(result.Items!.length).toBe(3);
    expect(result.Items![0]!.itemId!.S).toBe("PROD-001");
    expect(result.Items![1]!.itemId!.S).toBe("PROD-002");
    expect(result.Items![2]!.itemId!.S).toBe("PROD-003");
  });

  test("should support descending order with ScanIndexForward=false", async () => {
    const result = await client.send(
      new QueryCommand({
        TableName: getTableName(),
        KeyConditionExpression: "userId = :userId",
        ExpressionAttributeValues: {
          ":userId": { S: "user1" },
        },
        ScanIndexForward: false,
      })
    );

    expect(result.Items!.length).toBe(5);
    // Items should be in descending order
    expect(result.Items![0]!.timestamp!.N).toBe("500");
    expect(result.Items![4]!.timestamp!.N).toBe("100");
  });

  test("should support pagination with Limit", async () => {
    const result = await client.send(
      new QueryCommand({
        TableName: getTableName(),
        KeyConditionExpression: "userId = :userId",
        ExpressionAttributeValues: {
          ":userId": { S: "user1" },
        },
        Limit: 2,
      })
    );

    expect(result.Items!.length).toBe(2);
    expect(result.Items![0]!.timestamp!.N).toBe("100");
    expect(result.Items![1]!.timestamp!.N).toBe("200");
    expect(result.LastEvaluatedKey).toBeDefined();
  });

  test("should support pagination with ExclusiveStartKey", async () => {
    // First page
    const result1 = await client.send(
      new QueryCommand({
        TableName: getTableName(),
        KeyConditionExpression: "userId = :userId",
        ExpressionAttributeValues: {
          ":userId": { S: "user1" },
        },
        Limit: 2,
      })
    );

    expect(result1.Items!.length).toBe(2);

    // Second page using LastEvaluatedKey
    const result2 = await client.send(
      new QueryCommand({
        TableName: getTableName(),
        KeyConditionExpression: "userId = :userId",
        ExpressionAttributeValues: {
          ":userId": { S: "user1" },
        },
        Limit: 2,
        ExclusiveStartKey: result1.LastEvaluatedKey,
      })
    );

    expect(result2.Items!.length).toBe(2);
    expect(result2.Items![0]!.timestamp!.N).toBe("300");
    expect(result2.Items![1]!.timestamp!.N).toBe("400");
  });

  test("should isolate queries by partition key", async () => {
    const result1 = await client.send(
      new QueryCommand({
        TableName: getTableName(),
        KeyConditionExpression: "userId = :userId",
        ExpressionAttributeValues: {
          ":userId": { S: "user1" },
        },
      })
    );

    const result2 = await client.send(
      new QueryCommand({
        TableName: getTableName(),
        KeyConditionExpression: "userId = :userId",
        ExpressionAttributeValues: {
          ":userId": { S: "user2" },
        },
      })
    );

    expect(result1.Items!.length).toBe(5);
    expect(result2.Items!.length).toBe(2);

    // Verify all items have correct userId
    for (const item of result1.Items!) {
      expect(item!.userId!.S).toBe("user1");
    }
    for (const item of result2.Items!) {
      expect(item!.userId!.S).toBe("user2");
    }
  });

  test("should return empty results when no items match", async () => {
    const result = await client.send(
      new QueryCommand({
        TableName: getTableName(),
        KeyConditionExpression: "userId = :userId AND #ts > :timestamp",
        ExpressionAttributeNames: {
          "#ts": "timestamp",
        },
        ExpressionAttributeValues: {
          ":userId": { S: "user1" },
          ":timestamp": { N: "999" },
        },
      })
    );

    expect(result.Items!.length).toBe(0);
    expect(result.Count).toBe(0);
    expect(result.LastEvaluatedKey).toBeUndefined();
  });

  test("should handle complex pagination with conditions", async () => {
    const result = await client.send(
      new QueryCommand({
        TableName: getTableName(),
        KeyConditionExpression: "userId = :userId AND #ts >= :timestamp",
        ExpressionAttributeNames: {
          "#ts": "timestamp",
        },
        ExpressionAttributeValues: {
          ":userId": { S: "user1" },
          ":timestamp": { N: "200" },
        },
        Limit: 2,
      })
    );

    expect(result.Items!.length).toBe(2);
    expect(result.Items![0]!.timestamp!.N).toBe("200");
    expect(result.Items![1]!.timestamp!.N).toBe("300");
    expect(result.LastEvaluatedKey).toBeDefined();
  });
});
