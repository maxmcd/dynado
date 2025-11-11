import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { ShardedSQLiteStorage } from "./storage-sqlite.ts";
import type { TransactWriteItem, TransactGetItem } from "./storage.ts";
import { TransactionCancelledError } from "./storage.ts";
import * as fs from "fs";

const TEST_DATA_DIR = "./test-data-transactions";

function cleanupTestData() {
  if (fs.existsSync(TEST_DATA_DIR)) {
    fs.rmSync(TEST_DATA_DIR, { recursive: true });
  }
}

describe("Transaction Operations", () => {
  let storage: ShardedSQLiteStorage;

  beforeEach(() => {
    cleanupTestData();
    fs.mkdirSync(TEST_DATA_DIR, { recursive: true });
    storage = new ShardedSQLiteStorage({
      shardCount: 4,
      dataDir: TEST_DATA_DIR,
    });

    // Create test table
    storage.createTable({
      tableName: "TransactTest",
      keySchema: [{ AttributeName: "id", KeyType: "HASH" }],
      attributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
    });
  });

  afterEach(() => {
    storage.close();
    cleanupTestData();
  });

  test("should put multiple items atomically", async () => {
    const items: TransactWriteItem[] = [
      {
        Put: {
          tableName: "TransactTest",
          item: { id: { S: "item1" }, value: { S: "First" } },
        },
      },
      {
        Put: {
          tableName: "TransactTest",
          item: { id: { S: "item2" }, value: { S: "Second" } },
        },
      },
      {
        Put: {
          tableName: "TransactTest",
          item: { id: { S: "item3" }, value: { S: "Third" } },
        },
      },
    ];

    await storage.transactWrite(items);

    // Verify all items were created
    const item1 = await storage.getItem("TransactTest", { id: { S: "item1" } });
    const item2 = await storage.getItem("TransactTest", { id: { S: "item2" } });
    const item3 = await storage.getItem("TransactTest", { id: { S: "item3" } });

    expect(item1?.value.S).toBe("First");
    expect(item2?.value.S).toBe("Second");
    expect(item3?.value.S).toBe("Third");
  });

  test("should update multiple items atomically", async () => {
    // Create initial items
    await storage.putItem("TransactTest", { id: { S: "item1" }, count: { N: "1" } });
    await storage.putItem("TransactTest", { id: { S: "item2" }, count: { N: "2" } });

    const items: TransactWriteItem[] = [
      {
        Update: {
          tableName: "TransactTest",
          key: { id: { S: "item1" } },
          updateExpression: "SET #count = :val",
          expressionAttributeNames: { "#count": "count" },
          expressionAttributeValues: { ":val": { N: "10" } },
        },
      },
      {
        Update: {
          tableName: "TransactTest",
          key: { id: { S: "item2" } },
          updateExpression: "SET #count = :val",
          expressionAttributeNames: { "#count": "count" },
          expressionAttributeValues: { ":val": { N: "20" } },
        },
      },
    ];

    await storage.transactWrite(items);

    const item1 = await storage.getItem("TransactTest", { id: { S: "item1" } });
    const item2 = await storage.getItem("TransactTest", { id: { S: "item2" } });

    expect(item1?.count.N).toBe("10");
    expect(item2?.count.N).toBe("20");
  });

  test("should delete multiple items atomically", async () => {
    // Create initial items
    await storage.putItem("TransactTest", { id: { S: "item1" }, value: { S: "First" } });
    await storage.putItem("TransactTest", { id: { S: "item2" }, value: { S: "Second" } });

    const items: TransactWriteItem[] = [
      {
        Delete: {
          tableName: "TransactTest",
          key: { id: { S: "item1" } },
        },
      },
      {
        Delete: {
          tableName: "TransactTest",
          key: { id: { S: "item2" } },
        },
      },
    ];

    await storage.transactWrite(items);

    const item1 = await storage.getItem("TransactTest", { id: { S: "item1" } });
    const item2 = await storage.getItem("TransactTest", { id: { S: "item2" } });

    expect(item1).toBeNull();
    expect(item2).toBeNull();
  });

  test("should handle mixed operations (Put, Update, Delete)", async () => {
    // Create initial item
    await storage.putItem("TransactTest", { id: { S: "item2" }, value: { S: "Old" } });

    const items: TransactWriteItem[] = [
      {
        Put: {
          tableName: "TransactTest",
          item: { id: { S: "item1" }, value: { S: "New" } },
        },
      },
      {
        Update: {
          tableName: "TransactTest",
          key: { id: { S: "item2" } },
          updateExpression: "SET #value = :val",
          expressionAttributeNames: { "#value": "value" },
          expressionAttributeValues: { ":val": { S: "Updated" } },
        },
      },
      {
        Put: {
          tableName: "TransactTest",
          item: { id: { S: "item3" }, value: { S: "Another" } },
        },
      },
    ];

    await storage.transactWrite(items);

    const item1 = await storage.getItem("TransactTest", { id: { S: "item1" } });
    const item2 = await storage.getItem("TransactTest", { id: { S: "item2" } });
    const item3 = await storage.getItem("TransactTest", { id: { S: "item3" } });

    expect(item1?.value.S).toBe("New");
    expect(item2?.value.S).toBe("Updated");
    expect(item3?.value.S).toBe("Another");
  });

  test("should rollback on condition failure", async () => {
    // Create initial item
    await storage.putItem("TransactTest", { id: { S: "item1" }, value: { S: "Original" } });

    const items: TransactWriteItem[] = [
      {
        Put: {
          tableName: "TransactTest",
          item: { id: { S: "item2" }, value: { S: "New" } },
        },
      },
      {
        Update: {
          tableName: "TransactTest",
          key: { id: { S: "item1" } },
          updateExpression: "SET #value = :val",
          expressionAttributeNames: { "#value": "value" },
          expressionAttributeValues: { ":val": { S: "Updated" }, ":expected": { S: "WrongValue" } },
          conditionExpression: "#value = :expected",
        },
      },
    ];

    // Should throw TransactionCancelledError
    try {
      await storage.transactWrite(items);
      expect(true).toBe(false); // Should not reach here
    } catch (error) {
      expect(error).toBeInstanceOf(TransactionCancelledError);
      const txError = error as TransactionCancelledError;
      expect(txError.cancellationReasons[0]?.Code).toBe("None");
      expect(txError.cancellationReasons[1]?.Code).toBe("ConditionalCheckFailed");
    }

    // Verify rollback - item2 should not exist and item1 should be unchanged
    const item1 = await storage.getItem("TransactTest", { id: { S: "item1" } });
    const item2 = await storage.getItem("TransactTest", { id: { S: "item2" } });

    expect(item1).not.toBeNull();
    expect(item1?.value?.S).toBe("Original");
    expect(item2).toBeNull();
  });

  test("should validate attribute_not_exists condition", async () => {
    const items: TransactWriteItem[] = [
      {
        Put: {
          tableName: "TransactTest",
          item: { id: { S: "item1" }, value: { S: "New" } },
          conditionExpression: "attribute_not_exists(id)",
        },
      },
    ];

    // First write should succeed
    await storage.transactWrite(items);

    // Second write should fail (item exists now)
    try {
      await storage.transactWrite(items);
      expect(true).toBe(false); // Should not reach here
    } catch (error) {
      expect(error).toBeInstanceOf(TransactionCancelledError);
    }
  });

  test("should validate attribute_exists condition", async () => {
    const items: TransactWriteItem[] = [
      {
        Update: {
          tableName: "TransactTest",
          key: { id: { S: "item1" } },
          updateExpression: "SET #value = :val",
          expressionAttributeNames: { "#value": "value" },
          expressionAttributeValues: { ":val": { S: "Updated" } },
          conditionExpression: "attribute_exists(id)",
        },
      },
    ];

    // Should fail because item doesn't exist
    try {
      await storage.transactWrite(items);
      expect(true).toBe(false); // Should not reach here
    } catch (error) {
      expect(error).toBeInstanceOf(TransactionCancelledError);
    }

    // Create the item
    await storage.putItem("TransactTest", { id: { S: "item1" }, value: { S: "Original" } });

    // Now should succeed
    await storage.transactWrite(items);

    const item = await storage.getItem("TransactTest", { id: { S: "item1" } });
    expect(item?.value.S).toBe("Updated");
  });

  test("should handle ConditionCheck operation", async () => {
    // Create item
    await storage.putItem("TransactTest", { id: { S: "item1" }, status: { S: "active" } });

    const items: TransactWriteItem[] = [
      {
        ConditionCheck: {
          tableName: "TransactTest",
          key: { id: { S: "item1" } },
          conditionExpression: "#status = :val",
          expressionAttributeNames: { "#status": "status" },
          expressionAttributeValues: { ":val": { S: "active" } },
        },
      },
      {
        Put: {
          tableName: "TransactTest",
          item: { id: { S: "item2" }, value: { S: "New" } },
        },
      },
    ];

    // Should succeed because condition passes
    await storage.transactWrite(items);

    const item2 = await storage.getItem("TransactTest", { id: { S: "item2" } });
    expect(item2?.value.S).toBe("New");

    // Now try with failing condition
    const failingItems: TransactWriteItem[] = [
      {
        ConditionCheck: {
          tableName: "TransactTest",
          key: { id: { S: "item1" } },
          conditionExpression: "#status = :val",
          expressionAttributeNames: { "#status": "status" },
          expressionAttributeValues: { ":val": { S: "inactive" } },
        },
      },
      {
        Put: {
          tableName: "TransactTest",
          item: { id: { S: "item3" }, value: { S: "Should not exist" } },
        },
      },
    ];

    try {
      await storage.transactWrite(failingItems);
      expect(true).toBe(false); // Should not reach here
    } catch (error) {
      expect(error).toBeInstanceOf(TransactionCancelledError);
    }

    const item3 = await storage.getItem("TransactTest", { id: { S: "item3" } });
    expect(item3).toBeNull();
  });

  test("should get multiple items atomically", async () => {
    // Create items
    await storage.putItem("TransactTest", { id: { S: "item1" }, value: { S: "First" } });
    await storage.putItem("TransactTest", { id: { S: "item2" }, value: { S: "Second" } });
    await storage.putItem("TransactTest", { id: { S: "item3" }, value: { S: "Third" } });

    const items: TransactGetItem[] = [
      {
        tableName: "TransactTest",
        key: { id: { S: "item1" } },
      },
      {
        tableName: "TransactTest",
        key: { id: { S: "item2" } },
      },
      {
        tableName: "TransactTest",
        key: { id: { S: "item3" } },
      },
    ];

    const results = await storage.transactGet(items);

    expect(results.length).toBe(3);
    expect(results[0]?.value.S).toBe("First");
    expect(results[1]?.value.S).toBe("Second");
    expect(results[2]?.value.S).toBe("Third");
  });

  test("should handle missing items in transactGet", async () => {
    // Create only one item
    await storage.putItem("TransactTest", { id: { S: "item1" }, value: { S: "First" } });

    const items: TransactGetItem[] = [
      {
        tableName: "TransactTest",
        key: { id: { S: "item1" } },
      },
      {
        tableName: "TransactTest",
        key: { id: { S: "item2" } }, // Doesn't exist
      },
    ];

    const results = await storage.transactGet(items);

    expect(results.length).toBe(2);
    expect(results[0]?.value.S).toBe("First");
    expect(results[1]).toBeNull();
  });

  test("should apply projection expression in transactGet", async () => {
    // Create item with multiple attributes
    await storage.putItem("TransactTest", {
      id: { S: "item1" },
      name: { S: "Test" },
      value: { S: "Value" },
      extra: { S: "Extra" },
    });

    const items: TransactGetItem[] = [
      {
        tableName: "TransactTest",
        key: { id: { S: "item1" } },
        projectionExpression: "id, #name",
        expressionAttributeNames: { "#name": "name" },
      },
    ];

    const results = await storage.transactGet(items);

    expect(results.length).toBe(1);
    expect(results[0]?.id.S).toBe("item1");
    expect(results[0]?.name.S).toBe("Test");
    expect(results[0]?.value).toBeUndefined();
    expect(results[0]?.extra).toBeUndefined();
  });

  test("should support idempotency with client request token", async () => {
    const items: TransactWriteItem[] = [
      {
        Put: {
          tableName: "TransactTest",
          item: { id: { S: "item1" }, value: { S: "First" } },
        },
      },
    ];

    const token = "unique-token-123";

    // First execution
    await storage.transactWrite(items, token);

    // Verify item was created
    let item = await storage.getItem("TransactTest", { id: { S: "item1" } });
    expect(item?.value.S).toBe("First");

    // Update the item directly
    await storage.putItem("TransactTest", { id: { S: "item1" }, value: { S: "Changed" } });

    // Second execution with same token should be idempotent (return cached result)
    await storage.transactWrite(items, token);

    // Item should still be "Changed" (transaction was not re-executed)
    item = await storage.getItem("TransactTest", { id: { S: "item1" } });
    expect(item?.value.S).toBe("Changed");
  });

  test("should handle transactions across multiple shards", async () => {
    // Create items that will definitely go to different shards
    const items: TransactWriteItem[] = [];

    for (let i = 0; i < 20; i++) {
      items.push({
        Put: {
          tableName: "TransactTest",
          item: { id: { S: `item-${i}` }, value: { N: String(i) } },
        },
      });
    }

    await storage.transactWrite(items);

    // Verify all items were created
    const count = await storage.getTableItemCount("TransactTest");
    expect(count).toBe(20);
  });

  test("should reject transaction with more than 100 items", async () => {
    const items: TransactWriteItem[] = [];

    for (let i = 0; i < 101; i++) {
      items.push({
        Put: {
          tableName: "TransactTest",
          item: { id: { S: `item-${i}` }, value: { N: String(i) } },
        },
      });
    }

    try {
      await storage.transactWrite(items);
      expect(true).toBe(false); // Should not reach here
    } catch (error: any) {
      expect(error.message).toContain("100 items");
    }
  });
});
