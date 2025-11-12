// Tests for transaction operations
// Uses HTTP API via AWS SDK

import {
  describe,
  test,
  expect,
  beforeAll,
  beforeEach,
  afterEach,
  afterAll,
} from "bun:test";
import {
  DynamoDBClient,
  CreateTableCommand,
  DeleteTableCommand,
  PutItemCommand,
  GetItemCommand,
  TransactWriteItemsCommand,
  TransactGetItemsCommand,
} from "@aws-sdk/client-dynamodb";
import { getGlobalTestDB } from "./test-global-setup.ts";

describe("Transaction Operations", () => {
  let client: DynamoDBClient;
  const createdTables: string[] = [];

  beforeAll(async () => {
    const testDB = await getGlobalTestDB();
    client = testDB.client;
  });

  beforeEach(async () => {
    // Create test table
    const tableName = `TransactTest_${Date.now()}`;
    createdTables.push(tableName);

    await client.send(
      new CreateTableCommand({
        TableName: tableName,
        KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
        AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
        BillingMode: "PAY_PER_REQUEST",
      })
    );
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

  test("should put multiple items atomically", async () => {
    const tableName = getTableName();

    await client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Put: {
              TableName: tableName,
              Item: { id: { S: "item1" }, value: { S: "First" } },
            },
          },
          {
            Put: {
              TableName: tableName,
              Item: { id: { S: "item2" }, value: { S: "Second" } },
            },
          },
          {
            Put: {
              TableName: tableName,
              Item: { id: { S: "item3" }, value: { S: "Third" } },
            },
          },
        ],
      })
    );

    // Verify all items were created
    const item1 = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item1" } } })
    );
    const item2 = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item2" } } })
    );
    const item3 = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item3" } } })
    );

    expect(item1.Item?.value.S).toBe("First");
    expect(item2.Item?.value.S).toBe("Second");
    expect(item3.Item?.value.S).toBe("Third");
  });

  test("should update multiple items atomically", async () => {
    const tableName = getTableName();

    // Create initial items
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: "item1" }, count: { N: "1" } },
      })
    );
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: "item2" }, count: { N: "2" } },
      })
    );

    await client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Update: {
              TableName: tableName,
              Key: { id: { S: "item1" } },
              UpdateExpression: "SET #count = :val",
              ExpressionAttributeNames: { "#count": "count" },
              ExpressionAttributeValues: { ":val": { N: "10" } },
            },
          },
          {
            Update: {
              TableName: tableName,
              Key: { id: { S: "item2" } },
              UpdateExpression: "SET #count = :val",
              ExpressionAttributeNames: { "#count": "count" },
              ExpressionAttributeValues: { ":val": { N: "20" } },
            },
          },
        ],
      })
    );

    const item1 = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item1" } } })
    );
    const item2 = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item2" } } })
    );

    expect(item1.Item?.count.N).toBe("10");
    expect(item2.Item?.count.N).toBe("20");
  });

  test("should delete multiple items atomically", async () => {
    const tableName = getTableName();

    // Create initial items
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: "item1" }, value: { S: "First" } },
      })
    );
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: "item2" }, value: { S: "Second" } },
      })
    );

    await client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Delete: {
              TableName: tableName,
              Key: { id: { S: "item1" } },
            },
          },
          {
            Delete: {
              TableName: tableName,
              Key: { id: { S: "item2" } },
            },
          },
        ],
      })
    );

    const item1 = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item1" } } })
    );
    const item2 = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item2" } } })
    );

    expect(item1.Item).toBeUndefined();
    expect(item2.Item).toBeUndefined();
  });

  test("should handle mixed operations (Put, Update, Delete)", async () => {
    const tableName = getTableName();

    // Create initial item
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: "item2" }, value: { S: "Old" } },
      })
    );

    await client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Put: {
              TableName: tableName,
              Item: { id: { S: "item1" }, value: { S: "New" } },
            },
          },
          {
            Update: {
              TableName: tableName,
              Key: { id: { S: "item2" } },
              UpdateExpression: "SET #value = :val",
              ExpressionAttributeNames: { "#value": "value" },
              ExpressionAttributeValues: { ":val": { S: "Updated" } },
            },
          },
          {
            Put: {
              TableName: tableName,
              Item: { id: { S: "item3" }, value: { S: "Another" } },
            },
          },
        ],
      })
    );

    const item1 = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item1" } } })
    );
    const item2 = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item2" } } })
    );
    const item3 = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item3" } } })
    );

    expect(item1.Item?.value.S).toBe("New");
    expect(item2.Item?.value.S).toBe("Updated");
    expect(item3.Item?.value.S).toBe("Another");
  });

  test("should rollback on condition failure", async () => {
    const tableName = getTableName();

    // Create initial item
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: "item1" }, value: { S: "Original" } },
      })
    );

    // Should throw TransactionCanceledException
    try {
      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: tableName,
                Item: { id: { S: "item2" }, value: { S: "New" } },
              },
            },
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: "item1" } },
                UpdateExpression: "SET #value = :val",
                ExpressionAttributeNames: { "#value": "value" },
                ExpressionAttributeValues: {
                  ":val": { S: "Updated" },
                  ":expected": { S: "WrongValue" },
                },
                ConditionExpression: "#value = :expected",
              },
            },
          ],
        })
      );
      expect(true).toBe(false); // Should not reach here
    } catch (error: any) {
      expect(error.name).toBe("TransactionCanceledException");
    }

    // Verify rollback - item2 should not exist and item1 should be unchanged
    const item1 = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item1" } } })
    );
    const item2 = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item2" } } })
    );

    expect(item1.Item).toBeDefined();
    expect(item1.Item?.value?.S).toBe("Original");
    expect(item2.Item).toBeUndefined();
  });

  test("should validate attribute_not_exists condition", async () => {
    const tableName = getTableName();

    // First write should succeed
    await client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Put: {
              TableName: tableName,
              Item: { id: { S: "item1" }, value: { S: "New" } },
              ConditionExpression: "attribute_not_exists(id)",
            },
          },
        ],
      })
    );

    // Second write should fail (item exists now)
    try {
      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: tableName,
                Item: { id: { S: "item1" }, value: { S: "New" } },
                ConditionExpression: "attribute_not_exists(id)",
              },
            },
          ],
        })
      );
      expect(true).toBe(false); // Should not reach here
    } catch (error: any) {
      expect(error.name).toBe("TransactionCanceledException");
    }
  });

  test("should validate attribute_exists condition", async () => {
    const tableName = getTableName();

    // Should fail because item doesn't exist
    try {
      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: tableName,
                Key: { id: { S: "item1" } },
                UpdateExpression: "SET #value = :val",
                ExpressionAttributeNames: { "#value": "value" },
                ExpressionAttributeValues: { ":val": { S: "Updated" } },
                ConditionExpression: "attribute_exists(id)",
              },
            },
          ],
        })
      );
      expect(true).toBe(false); // Should not reach here
    } catch (error: any) {
      expect(error.name).toBe("TransactionCanceledException");
    }

    // Create the item
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: "item1" }, value: { S: "Original" } },
      })
    );

    // Now should succeed
    await client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Update: {
              TableName: tableName,
              Key: { id: { S: "item1" } },
              UpdateExpression: "SET #value = :val",
              ExpressionAttributeNames: { "#value": "value" },
              ExpressionAttributeValues: { ":val": { S: "Updated" } },
              ConditionExpression: "attribute_exists(id)",
            },
          },
        ],
      })
    );

    const item = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item1" } } })
    );
    expect(item.Item?.value.S).toBe("Updated");
  });

  test("should handle ConditionCheck operation", async () => {
    const tableName = getTableName();

    // Create item
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: "item1" }, status: { S: "active" } },
      })
    );

    // Should succeed because condition passes
    await client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            ConditionCheck: {
              TableName: tableName,
              Key: { id: { S: "item1" } },
              ConditionExpression: "#status = :val",
              ExpressionAttributeNames: { "#status": "status" },
              ExpressionAttributeValues: { ":val": { S: "active" } },
            },
          },
          {
            Put: {
              TableName: tableName,
              Item: { id: { S: "item2" }, value: { S: "New" } },
            },
          },
        ],
      })
    );

    const item2 = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item2" } } })
    );
    expect(item2.Item?.value.S).toBe("New");

    // Now try with failing condition
    try {
      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              ConditionCheck: {
                TableName: tableName,
                Key: { id: { S: "item1" } },
                ConditionExpression: "#status = :val",
                ExpressionAttributeNames: { "#status": "status" },
                ExpressionAttributeValues: { ":val": { S: "inactive" } },
              },
            },
            {
              Put: {
                TableName: tableName,
                Item: { id: { S: "item3" }, value: { S: "Should not exist" } },
              },
            },
          ],
        })
      );
      expect(true).toBe(false); // Should not reach here
    } catch (error: any) {
      expect(error.name).toBe("TransactionCanceledException");
    }

    const item3 = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item3" } } })
    );
    expect(item3.Item).toBeUndefined();
  });

  test("should get multiple items atomically", async () => {
    const tableName = getTableName();

    // Create items
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: "item1" }, value: { S: "First" } },
      })
    );
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: "item2" }, value: { S: "Second" } },
      })
    );
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: "item3" }, value: { S: "Third" } },
      })
    );

    const result = await client.send(
      new TransactGetItemsCommand({
        TransactItems: [
          {
            Get: {
              TableName: tableName,
              Key: { id: { S: "item1" } },
            },
          },
          {
            Get: {
              TableName: tableName,
              Key: { id: { S: "item2" } },
            },
          },
          {
            Get: {
              TableName: tableName,
              Key: { id: { S: "item3" } },
            },
          },
        ],
      })
    );

    expect(result.Responses!.length).toBe(3);
    expect(result.Responses![0]!.Item!.value.S).toBe("First");
    expect(result.Responses![1]!.Item!.value.S).toBe("Second");
    expect(result.Responses![2]!.Item!.value.S).toBe("Third");
  });

  test("should handle missing items in TransactGetItems", async () => {
    const tableName = getTableName();

    // Create only one item
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: "item1" }, value: { S: "First" } },
      })
    );

    const result = await client.send(
      new TransactGetItemsCommand({
        TransactItems: [
          {
            Get: {
              TableName: tableName,
              Key: { id: { S: "item1" } },
            },
          },
          {
            Get: {
              TableName: tableName,
              Key: { id: { S: "item2" } }, // Doesn't exist
            },
          },
        ],
      })
    );

    expect(result.Responses!.length).toBe(2);
    expect(result.Responses![0]!.Item!.value.S).toBe("First");
    expect(result.Responses![1]!.Item).toBeUndefined();
  });

  test("should apply projection expression in TransactGetItems", async () => {
    const tableName = getTableName();

    // Create item with multiple attributes
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: {
          id: { S: "item1" },
          name: { S: "Test" },
          value: { S: "Value" },
          extra: { S: "Extra" },
        },
      })
    );

    const result = await client.send(
      new TransactGetItemsCommand({
        TransactItems: [
          {
            Get: {
              TableName: tableName,
              Key: { id: { S: "item1" } },
              ProjectionExpression: "id, #name",
              ExpressionAttributeNames: { "#name": "name" },
            },
          },
        ],
      })
    );

    expect(result.Responses!.length).toBe(1);
    expect(result.Responses![0]!.Item!.id.S).toBe("item1");
    expect(result.Responses![0]!.Item!.name.S).toBe("Test");
    expect(result.Responses![0]!.Item!.value).toBeUndefined();
    expect(result.Responses![0]!.Item!.extra).toBeUndefined();
  });

  test("should support idempotency with client request token", async () => {
    const tableName = getTableName();

    const token = "unique-token-123";

    // First execution
    await client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Put: {
              TableName: tableName,
              Item: { id: { S: "item1" }, value: { S: "First" } },
            },
          },
        ],
        ClientRequestToken: token,
      })
    );

    // Verify item was created
    let item = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item1" } } })
    );
    expect(item.Item?.value.S).toBe("First");

    // Update the item directly
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: "item1" }, value: { S: "Changed" } },
      })
    );

    // Second execution with same token should be idempotent (return cached result)
    await client.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Put: {
              TableName: tableName,
              Item: { id: { S: "item1" }, value: { S: "First" } },
            },
          },
        ],
        ClientRequestToken: token,
      })
    );

    // Item should still be "Changed" (transaction was not re-executed)
    item = await client.send(
      new GetItemCommand({ TableName: tableName, Key: { id: { S: "item1" } } })
    );
    expect(item.Item?.value.S).toBe("Changed");
  });

  test("should handle transactions across multiple items", async () => {
    const tableName = getTableName();

    const items = [];
    for (let i = 0; i < 20; i++) {
      items.push({
        Put: {
          TableName: tableName,
          Item: { id: { S: `item-${i}` }, value: { N: String(i) } },
        },
      });
    }

    await client.send(
      new TransactWriteItemsCommand({
        TransactItems: items,
      })
    );

    // Verify some items were created
    const item0 = await client.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: "item-0" } },
      })
    );
    const item19 = await client.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { id: { S: "item-19" } },
      })
    );

    expect(item0.Item?.value.N).toBe("0");
    expect(item19.Item?.value.N).toBe("19");
  });

  test("should reject transaction with more than 100 items", async () => {
    const tableName = getTableName();

    const items = [];
    for (let i = 0; i < 101; i++) {
      items.push({
        Put: {
          TableName: tableName,
          Item: { id: { S: `item-${i}` }, value: { N: String(i) } },
        },
      });
    }

    try {
      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: items,
        })
      );
      expect(true).toBe(false); // Should not reach here
    } catch (error: any) {
      expect(error.name).toBe("ValidationException");
    }
  });
});
