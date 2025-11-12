import {
  describe,
  test,
  expect,
  beforeAll,
  afterEach,
  afterAll,
} from "bun:test";
import {
  DynamoDBClient,
  ListTablesCommand,
  CreateTableCommand,
  DescribeTableCommand,
  PutItemCommand,
  GetItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  ScanCommand,
  QueryCommand,
  DeleteTableCommand,
  BatchGetItemCommand,
  BatchWriteItemCommand,
} from "@aws-sdk/client-dynamodb";
import { DB } from "./index.ts";
import CRC32 from "crc-32";
import { startTestDB } from "./test-db-setup.ts";
import type { TestDBSetup } from "./test-db-setup.ts";

// Test helpers
let tableCounter = 0;
const createdTables: string[] = [];

function getUniqueTableName(prefix: string = "Test"): string {
  const tableName = `${prefix}Table_${Date.now()}_${tableCounter++}`;
  createdTables.push(tableName);
  return tableName;
}

async function createTable(client: DynamoDBClient, tableName: string) {
  await client.send(
    new CreateTableCommand({
      TableName: tableName,
      KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
      AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
      BillingMode: "PAY_PER_REQUEST",
    })
  );
  return tableName;
}

async function createTableWithItems(
  client: DynamoDBClient,
  tableName: string,
  items: Array<{ id: string; [key: string]: any }>
) {
  await createTable(client, tableName);

  for (const item of items) {
    const dynamoItem: any = {};
    for (const [key, value] of Object.entries(item)) {
      if (typeof value === "string") {
        dynamoItem[key] = { S: value };
      } else if (typeof value === "number") {
        dynamoItem[key] = { N: String(value) };
      } else {
        dynamoItem[key] = value;
      }
    }

    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: dynamoItem,
      })
    );
  }

  return tableName;
}

async function deleteTable(client: DynamoDBClient, tableName: string) {
  try {
    await client.send(new DeleteTableCommand({ TableName: tableName }));
  } catch (error) {
    // Ignore errors if table doesn't exist
  }
}

async function cleanupTables(client: DynamoDBClient) {
  for (const tableName of createdTables) {
    await deleteTable(client, tableName);
  }
  createdTables.length = 0;
}

describe("DynamoDB Implementation", () => {
  let client: DynamoDBClient;
  let testDB: TestDBSetup;

  beforeAll(async () => {
    // Conditionally start either dynado or DynamoDB Local
    testDB = await startTestDB();

    client = new DynamoDBClient({
      endpoint: testDB.endpoint,
      region: "local",
      credentials: {
        accessKeyId: "test",
        secretAccessKey: "test",
      },
    });
  });

  afterAll(async () => {
    await testDB.cleanup();
  });

  afterEach(async () => {
    // Clean up all tables created during tests
    await cleanupTables(client);
  });

  test("should list tables", async () => {
    const tableName = getUniqueTableName();
    await createTable(client, tableName);

    const response = await client.send(new ListTablesCommand({}));

    expect(response.TableNames).toBeDefined();
    expect(Array.isArray(response.TableNames)).toBe(true);
    expect(response.TableNames).toContain(tableName);
  });

  test("should create and describe a table", async () => {
    const tableName = getUniqueTableName();

    const createResponse = await client.send(
      new CreateTableCommand({
        TableName: tableName,
        KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
        AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
        BillingMode: "PAY_PER_REQUEST",
      })
    );
    createdTables.push(tableName);

    expect(createResponse.TableDescription).toBeDefined();
    expect(createResponse.TableDescription?.TableName).toBe(tableName);
    expect(createResponse.TableDescription?.TableStatus).toBe("ACTIVE");

    const describeResponse = await client.send(
      new DescribeTableCommand({ TableName: tableName })
    );
    expect(describeResponse.Table?.TableName).toBe(tableName);
    expect(describeResponse.Table?.TableStatus).toBe("ACTIVE");
  });

  test("should put and get an item", async () => {
    const tableName = await createTableWithItems(client, getUniqueTableName(), [
      { id: "item-1", name: "Test Item", count: 42 },
    ]);

    const getResponse = await client.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: "item-1" } },
      })
    );

    expect(getResponse.Item).toBeDefined();
    expect(getResponse.Item!.id!.S).toBe("item-1");
    expect(getResponse.Item!.name!.S).toBe("Test Item");
    expect(getResponse.Item!.count!.N).toBe("42");
  });

  test("should return undefined for non-existent item", async () => {
    const tableName = await createTable(client, getUniqueTableName());

    const response = await client.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: "non-existent" } },
      })
    );

    expect(response.Item).toBeUndefined();
  });

  test("should update an item with SET", async () => {
    const tableName = await createTableWithItems(client, getUniqueTableName(), [
      { id: "item-1", name: "Original", count: 10 },
    ]);

    const updateResponse = await client.send(
      new UpdateItemCommand({
        TableName: tableName,
        Key: { id: { S: "item-1" } },
        UpdateExpression: "SET #n = :name, #c = :count",
        ExpressionAttributeNames: { "#n": "name", "#c": "count" },
        ExpressionAttributeValues: {
          ":name": { S: "Updated" },
          ":count": { N: "100" },
        },
        ReturnValues: "ALL_NEW",
      })
    );

    expect(updateResponse.Attributes!.name!.S).toBe("Updated");
    expect(updateResponse.Attributes!.count!.N).toBe("100");
  });

  test("should update an item with REMOVE", async () => {
    const tableName = await createTableWithItems(client, getUniqueTableName(), [
      { id: "item-1", name: "Test", extra: "Remove Me" },
    ]);

    const updateResponse = await client.send(
      new UpdateItemCommand({
        TableName: tableName,
        Key: { id: { S: "item-1" } },
        UpdateExpression: "REMOVE extra",
        ReturnValues: "ALL_NEW",
      })
    );

    expect(updateResponse.Attributes!.id!.S).toBe("item-1");
    expect(updateResponse.Attributes!.extra).toBeUndefined();
  });

  test("should update an item with ADD", async () => {
    const tableName = await createTableWithItems(client, getUniqueTableName(), [
      { id: "item-1", counter: 5 },
    ]);

    const updateResponse = await client.send(
      new UpdateItemCommand({
        TableName: tableName,
        Key: { id: { S: "item-1" } },
        UpdateExpression: "ADD #counter :inc",
        ExpressionAttributeNames: { "#counter": "counter" },
        ExpressionAttributeValues: { ":inc": { N: "3" } },
        ReturnValues: "ALL_NEW",
      })
    );

    expect(updateResponse.Attributes!.counter!.N).toBe("8");
  });

  test("should delete an item", async () => {
    const tableName = await createTableWithItems(client, getUniqueTableName(), [
      { id: "item-1", name: "To Delete" },
    ]);

    const deleteResponse = await client.send(
      new DeleteItemCommand({
        TableName: tableName,
        Key: { id: { S: "item-1" } },
        ReturnValues: "ALL_OLD",
      })
    );

    expect(deleteResponse.Attributes).toBeDefined();
    expect(deleteResponse.Attributes!.id!.S).toBe("item-1");

    const getResponse = await client.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: "item-1" } },
      })
    );

    expect(getResponse.Item).toBeUndefined();
  });

  test("should scan all items", async () => {
    const tableName = await createTableWithItems(client, getUniqueTableName(), [
      { id: "item-1", name: "First", count: 10 },
      { id: "item-2", name: "Second", count: 20 },
      { id: "item-3", name: "Third", count: 30 },
    ]);

    const scanResponse = await client.send(
      new ScanCommand({ TableName: tableName })
    );

    expect(scanResponse.Items).toBeDefined();
    expect(scanResponse.Count).toBe(3);
    expect(scanResponse.Items!.length).toBe(3);
  });

  test("should scan with limit", async () => {
    const tableName = await createTableWithItems(client, getUniqueTableName(), [
      { id: "item-1", name: "First" },
      { id: "item-2", name: "Second" },
      { id: "item-3", name: "Third" },
    ]);

    const scanResponse = await client.send(
      new ScanCommand({ TableName: tableName, Limit: 2 })
    );

    expect(scanResponse.Count).toBe(2);
    expect(scanResponse.LastEvaluatedKey).toBeDefined();
  });

  test("should scan with pagination", async () => {
    const tableName = await createTableWithItems(client, getUniqueTableName(), [
      { id: "item-1", name: "First" },
      { id: "item-2", name: "Second" },
      { id: "item-3", name: "Third" },
      { id: "item-4", name: "Fourth" },
    ]);

    const firstScan = await client.send(
      new ScanCommand({ TableName: tableName, Limit: 2 })
    );

    expect(firstScan.Items!.length).toBe(2);
    expect(firstScan.LastEvaluatedKey).toBeDefined();

    const secondScan = await client.send(
      new ScanCommand({
        TableName: tableName,
        Limit: 2,
        ExclusiveStartKey: firstScan.LastEvaluatedKey,
      })
    );

    expect(secondScan.Items!.length).toBeGreaterThan(0);
  });

  test("should filter scan results", async () => {
    const tableName = await createTableWithItems(client, getUniqueTableName(), [
      { id: "item-1", name: "Match", count: 10 },
      { id: "item-2", name: "NoMatch", count: 20 },
      { id: "item-3", name: "Match", count: 30 },
    ]);

    const scanResponse = await client.send(
      new ScanCommand({
        TableName: tableName,
        FilterExpression: "#n = :nameVal",
        ExpressionAttributeNames: { "#n": "name" },
        ExpressionAttributeValues: { ":nameVal": { S: "Match" } },
      })
    );

    expect(scanResponse.Count).toBe(2);
    expect(scanResponse.Items!.every((item) => item.name!.S === "Match")).toBe(
      true
    );
  });

  test("should query items by key", async () => {
    const tableName = await createTableWithItems(client, getUniqueTableName(), [
      { id: "key-1", name: "First" },
      { id: "key-2", name: "Second" },
    ]);

    const queryResponse = await client.send(
      new QueryCommand({
        TableName: tableName,
        KeyConditionExpression: "#id = :idVal",
        ExpressionAttributeNames: { "#id": "id" },
        ExpressionAttributeValues: { ":idVal": { S: "key-2" } },
      })
    );

    expect(queryResponse.Count).toBe(1);
    expect(queryResponse.Items![0]!.id!.S).toBe("key-2");
  });

  test("should batch get items", async () => {
    const tableName = await createTableWithItems(client, getUniqueTableName(), [
      { id: "batch-1", name: "First" },
      { id: "batch-2", name: "Second" },
      { id: "batch-3", name: "Third" },
    ]);

    const batchGetResponse = await client.send(
      new BatchGetItemCommand({
        RequestItems: {
          [tableName]: {
            Keys: [{ id: { S: "batch-1" } }, { id: { S: "batch-3" } }],
          },
        },
      })
    );

    expect(batchGetResponse.Responses![tableName]).toBeDefined();
    expect(batchGetResponse.Responses![tableName]!.length).toBe(2);
  });

  test("should batch write items", async () => {
    const tableName = await createTable(client, getUniqueTableName());

    await client.send(
      new BatchWriteItemCommand({
        RequestItems: {
          [tableName]: [
            {
              PutRequest: {
                Item: { id: { S: "batch-1" }, name: { S: "First" } },
              },
            },
            {
              PutRequest: {
                Item: { id: { S: "batch-2" }, name: { S: "Second" } },
              },
            },
          ],
        },
      })
    );

    const getResponse = await client.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: "batch-1" } },
      })
    );

    expect(getResponse.Item!.name!.S).toBe("First");
  });

  test("should delete a table", async () => {
    const tableName = await createTable(client, getUniqueTableName());

    const deleteResponse = await client.send(
      new DeleteTableCommand({ TableName: tableName })
    );

    expect(deleteResponse.TableDescription?.TableName).toBe(tableName);
  });

  test("should include valid X-Amz-Crc32 header in responses", async () => {
    // Skip this test when testing against DynamoDB Local (it doesn't include this header)
    if (process.env.TEST_DYNAMODB_LOCAL === "true") {
      return;
    }

    // Make a raw HTTP request to check headers
    const response = await fetch(testDB.endpoint + "/", {
      method: "POST",
      headers: {
        "x-amz-target": "DynamoDB_20120810.ListTables",
        "Content-Type": "application/x-amz-json-1.0",
      },
      body: JSON.stringify({}),
    });

    const checksumHeader = response.headers.get("X-Amz-Crc32");
    expect(checksumHeader).toBeDefined();
    expect(checksumHeader).not.toBe("");

    // Get the response body
    const responseBody = await response.text();

    // Compute the CRC32 checksum of the response body
    const computedChecksum = CRC32.str(responseBody) >>> 0; // Convert to unsigned 32-bit

    // Verify the checksum in the header matches the computed checksum
    expect(checksumHeader).toBe(String(computedChecksum));
  });
});
