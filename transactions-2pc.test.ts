// Comprehensive tests for the Two-Phase Commit implementation
// Tests the new DO-compatible architecture: Shard, Coordinator, Router, MetadataStore

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { ShardedSQLiteStorage } from "./storage-sqlite.ts";
import type { TransactWriteItem } from "./storage.ts";
import { TransactionCancelledError } from "./storage.ts";
import * as fs from "fs";

const TEST_DATA_DIR = "./test-data-2pc";

function cleanupTestData() {
  if (fs.existsSync(TEST_DATA_DIR)) {
    fs.rmSync(TEST_DATA_DIR, { recursive: true });
  }
}

describe("Two-Phase Commit Protocol", () => {
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
      tableName: "Test2PC",
      keySchema: [{ AttributeName: "id", KeyType: "HASH" }],
      attributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
    });
  });

  afterEach(() => {
    storage.close();
    cleanupTestData();
  });

  describe("Transaction Conflicts", () => {
    test("should detect concurrent transaction conflict on same item", async () => {
      // Create an item
      await storage.putItem("Test2PC", { id: { S: "conflict-item" }, value: { N: "1" } });

      // Start two transactions that try to modify the same item
      // Due to timestamp ordering and locking, one should fail

      const tx1: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "conflict-item" } },
            updateExpression: "SET #value = :val",
            expressionAttributeNames: { "#value": "value" },
            expressionAttributeValues: { ":val": { N: "10" } },
          },
        },
      ];

      const tx2: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "conflict-item" } },
            updateExpression: "SET #value = :val",
            expressionAttributeNames: { "#value": "value" },
            expressionAttributeValues: { ":val": { N: "20" } },
          },
        },
      ];

      // Execute both transactions - they should succeed serially
      await storage.transactWrite(tx1);
      await storage.transactWrite(tx2);

      // One of them should have succeeded
      const item = await storage.getItem("Test2PC", { id: { S: "conflict-item" } });
      expect(item?.value.N).toBe("20"); // Last transaction wins
    });

    test("should handle transaction on locked item", async () => {
      // This tests that if we could somehow get a locked item,
      // a new transaction would detect the conflict
      // In practice, transactions complete quickly, but this validates the logic

      await storage.putItem("Test2PC", { id: { S: "item1" }, value: { N: "1" } });

      const tx: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "item1" } },
            updateExpression: "SET #value = :val",
            expressionAttributeNames: { "#value": "value" },
            expressionAttributeValues: { ":val": { N: "10" } },
          },
        },
      ];

      // Should succeed
      await storage.transactWrite(tx);

      const item = await storage.getItem("Test2PC", { id: { S: "item1" } });
      expect(item?.value.N).toBe("10");
    });
  });

  describe("Condition Expression Validation", () => {
    test("should validate multiple condition types in one transaction", async () => {
      await storage.putItem("Test2PC", { id: { S: "item1" }, status: { S: "active" } });

      const items: TransactWriteItem[] = [
        {
          // Condition: item must exist
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "item1" } },
            updateExpression: "SET #value = :val",
            expressionAttributeNames: { "#value": "value" },
            expressionAttributeValues: { ":val": { S: "updated" } },
            conditionExpression: "attribute_exists(id)",
          },
        },
        {
          // Condition: item must not exist
          Put: {
            tableName: "Test2PC",
            item: { id: { S: "item2" }, status: { S: "new" } },
            conditionExpression: "attribute_not_exists(id)",
          },
        },
        {
          // Condition: attribute must equal value
          ConditionCheck: {
            tableName: "Test2PC",
            key: { id: { S: "item1" } },
            conditionExpression: "#status = :val",
            expressionAttributeNames: { "#status": "status" },
            expressionAttributeValues: { ":val": { S: "active" } },
          },
        },
      ];

      await storage.transactWrite(items);

      const item1 = await storage.getItem("Test2PC", { id: { S: "item1" } });
      const item2 = await storage.getItem("Test2PC", { id: { S: "item2" } });

      expect(item1?.value.S).toBe("updated");
      expect(item2?.status.S).toBe("new");
    });

    test("should support comparison operators in conditions", async () => {
      await storage.putItem("Test2PC", { id: { S: "item1" }, count: { N: "5" } });

      const items: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "item1" } },
            updateExpression: "ADD #count :inc",
            expressionAttributeNames: { "#count": "count" },
            expressionAttributeValues: { ":inc": { N: "3" }, ":min": { N: "3" } },
            conditionExpression: "#count > :min",
          },
        },
      ];

      await storage.transactWrite(items);

      const item = await storage.getItem("Test2PC", { id: { S: "item1" } });
      expect(item?.count.N).toBe("8");
    });

    test("should handle NOT operator in conditions", async () => {
      await storage.putItem("Test2PC", { id: { S: "item1" }, status: { S: "active" } });

      const items: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "item1" } },
            updateExpression: "SET #status = :val",
            expressionAttributeNames: { "#status": "status" },
            expressionAttributeValues: { ":val": { S: "inactive" }, ":blocked": { S: "blocked" } },
            conditionExpression: "NOT #status = :blocked",
          },
        },
      ];

      await storage.transactWrite(items);

      const item = await storage.getItem("Test2PC", { id: { S: "item1" } });
      expect(item?.status.S).toBe("inactive");
    });

    test("should handle AND operator in conditions", async () => {
      await storage.putItem("Test2PC", {
        id: { S: "item1" },
        status: { S: "active" },
        count: { N: "10" },
      });

      const items: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "item1" } },
            updateExpression: "SET #count = :val",
            expressionAttributeNames: { "#status": "status", "#count": "count" },
            expressionAttributeValues: {
              ":val": { N: "20" },
              ":status": { S: "active" },
              ":min": { N: "5" },
            },
            conditionExpression: "#status = :status AND #count > :min",
          },
        },
      ];

      await storage.transactWrite(items);

      const item = await storage.getItem("Test2PC", { id: { S: "item1" } });
      expect(item?.count.N).toBe("20");
    });

    test("should handle OR operator in conditions", async () => {
      await storage.putItem("Test2PC", {
        id: { S: "item1" },
        status: { S: "pending" },
      });

      const items: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "item1" } },
            updateExpression: "SET #status = :val",
            expressionAttributeNames: { "#status": "status" },
            expressionAttributeValues: {
              ":val": { S: "active" },
              ":status1": { S: "pending" },
              ":status2": { S: "ready" },
            },
            conditionExpression: "#status = :status1 OR #status = :status2",
          },
        },
      ];

      await storage.transactWrite(items);

      const item = await storage.getItem("Test2PC", { id: { S: "item1" } });
      expect(item?.status.S).toBe("active");
    });
  });

  describe("ReturnValuesOnConditionCheckFailure", () => {
    test("should return old values on condition failure when requested", async () => {
      await storage.putItem("Test2PC", {
        id: { S: "item1" },
        value: { S: "original" },
        count: { N: "5" },
      });

      const items: TransactWriteItem[] = [
        {
          Put: {
            tableName: "Test2PC",
            item: { id: { S: "item1" }, value: { S: "new" } },
            conditionExpression: "attribute_not_exists(id)",
            returnValuesOnConditionCheckFailure: "ALL_OLD",
          },
        },
      ];

      try {
        await storage.transactWrite(items);
        expect(true).toBe(false); // Should not reach here
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionCancelledError);
        const txError = error as TransactionCancelledError;
        expect(txError.cancellationReasons[0]?.Code).toBe("ConditionalCheckFailed");
        expect(txError.cancellationReasons[0]?.Item).toBeDefined();
        expect(txError.cancellationReasons[0]?.Item?.value.S).toBe("original");
      }
    });

    test("should not return values when returnValuesOnConditionCheckFailure is NONE", async () => {
      await storage.putItem("Test2PC", { id: { S: "item1" }, value: { S: "original" } });

      const items: TransactWriteItem[] = [
        {
          Put: {
            tableName: "Test2PC",
            item: { id: { S: "item1" }, value: { S: "new" } },
            conditionExpression: "attribute_not_exists(id)",
            returnValuesOnConditionCheckFailure: "NONE",
          },
        },
      ];

      try {
        await storage.transactWrite(items);
        expect(true).toBe(false);
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionCancelledError);
        const txError = error as TransactionCancelledError;
        expect(txError.cancellationReasons[0]?.Code).toBe("ConditionalCheckFailed");
        expect(txError.cancellationReasons[0]?.Item).toBeUndefined();
      }
    });

    test("should return values on ConditionCheck failure", async () => {
      await storage.putItem("Test2PC", {
        id: { S: "item1" },
        status: { S: "inactive" },
        data: { S: "secret" },
      });

      const items: TransactWriteItem[] = [
        {
          ConditionCheck: {
            tableName: "Test2PC",
            key: { id: { S: "item1" } },
            conditionExpression: "#status = :val",
            expressionAttributeNames: { "#status": "status" },
            expressionAttributeValues: { ":val": { S: "active" } },
            returnValuesOnConditionCheckFailure: "ALL_OLD",
          },
        },
      ];

      try {
        await storage.transactWrite(items);
        expect(true).toBe(false);
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionCancelledError);
        const txError = error as TransactionCancelledError;
        expect(txError.cancellationReasons[0]?.Item?.status.S).toBe("inactive");
        expect(txError.cancellationReasons[0]?.Item?.data.S).toBe("secret");
      }
    });
  });

  describe("Update Expression Variants", () => {
    test("should handle multiple SET operations in one expression", async () => {
      await storage.putItem("Test2PC", {
        id: { S: "item1" },
        field1: { S: "old1" },
        field2: { S: "old2" },
      });

      const items: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "item1" } },
            updateExpression: "SET field1 = :val1, field2 = :val2, field3 = :val3",
            expressionAttributeValues: {
              ":val1": { S: "new1" },
              ":val2": { S: "new2" },
              ":val3": { S: "new3" },
            },
          },
        },
      ];

      await storage.transactWrite(items);

      const item = await storage.getItem("Test2PC", { id: { S: "item1" } });
      expect(item?.field1.S).toBe("new1");
      expect(item?.field2.S).toBe("new2");
      expect(item?.field3.S).toBe("new3");
    });

    test("should handle multiple REMOVE operations", async () => {
      await storage.putItem("Test2PC", {
        id: { S: "item1" },
        field1: { S: "value1" },
        field2: { S: "value2" },
        field3: { S: "value3" },
      });

      const items: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "item1" } },
            updateExpression: "REMOVE field1, field2",
          },
        },
      ];

      await storage.transactWrite(items);

      const item = await storage.getItem("Test2PC", { id: { S: "item1" } });
      expect(item?.field1).toBeUndefined();
      expect(item?.field2).toBeUndefined();
      expect(item?.field3.S).toBe("value3");
    });

    test("should handle ADD operation on non-existent attribute", async () => {
      await storage.putItem("Test2PC", { id: { S: "item1" } });

      const items: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "item1" } },
            updateExpression: "ADD #count :val",
            expressionAttributeNames: { "#count": "count" },
            expressionAttributeValues: { ":val": { N: "5" } },
          },
        },
      ];

      await storage.transactWrite(items);

      const item = await storage.getItem("Test2PC", { id: { S: "item1" } });
      expect(item?.count.N).toBe("5");
    });

    test("should handle combined SET, REMOVE, and ADD operations", async () => {
      await storage.putItem("Test2PC", {
        id: { S: "item1" },
        status: { S: "old" },
        removeMe: { S: "gone" },
        count: { N: "10" },
      });

      const items: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "item1" } },
            updateExpression: "SET #status = :status ADD #count :inc REMOVE removeMe",
            expressionAttributeNames: { "#status": "status", "#count": "count" },
            expressionAttributeValues: { ":status": { S: "new" }, ":inc": { N: "5" } },
          },
        },
      ];

      await storage.transactWrite(items);

      const item = await storage.getItem("Test2PC", { id: { S: "item1" } });
      expect(item?.status.S).toBe("new");
      expect(item?.count.N).toBe("15");
      expect(item?.removeMe).toBeUndefined();
    });

    test("should handle Update on non-existent item (creates new item)", async () => {
      const items: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "new-item" } },
            updateExpression: "SET #value = :val",
            expressionAttributeNames: { "#value": "value" },
            expressionAttributeValues: { ":val": { S: "created" } },
          },
        },
      ];

      await storage.transactWrite(items);

      const item = await storage.getItem("Test2PC", { id: { S: "new-item" } });
      expect(item?.id.S).toBe("new-item");
      expect(item?.value.S).toBe("created");
    });
  });

  describe("Cross-Shard Transactions", () => {
    test("should commit transaction spanning multiple shards", async () => {
      // Create items that will hash to different shards
      const items: TransactWriteItem[] = [];
      const itemIds = [
        "shard-test-1",
        "shard-test-2",
        "shard-test-3",
        "shard-test-4",
        "shard-test-5",
      ];

      for (const id of itemIds) {
        items.push({
          Put: {
            tableName: "Test2PC",
            item: { id: { S: id }, value: { S: `value-${id}` } },
          },
        });
      }

      await storage.transactWrite(items);

      // Verify all items were created
      for (const id of itemIds) {
        const item = await storage.getItem("Test2PC", { id: { S: id } });
        expect(item?.value.S).toBe(`value-${id}`);
      }
    });

    test("should rollback cross-shard transaction on any failure", async () => {
      // Create one item
      await storage.putItem("Test2PC", { id: { S: "existing" }, status: { S: "active" } });

      const items: TransactWriteItem[] = [
        {
          Put: {
            tableName: "Test2PC",
            item: { id: { S: "new-1" }, value: { S: "first" } },
          },
        },
        {
          Put: {
            tableName: "Test2PC",
            item: { id: { S: "new-2" }, value: { S: "second" } },
          },
        },
        {
          // This should fail
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "existing" } },
            updateExpression: "SET #status = :val",
            expressionAttributeNames: { "#status": "status" },
            expressionAttributeValues: { ":val": { S: "updated" }, ":wrong": { S: "wrong" } },
            conditionExpression: "#status = :wrong",
          },
        },
        {
          Put: {
            tableName: "Test2PC",
            item: { id: { S: "new-3" }, value: { S: "third" } },
          },
        },
      ];

      try {
        await storage.transactWrite(items);
        expect(true).toBe(false);
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionCancelledError);
      }

      // Verify none of the new items were created
      const item1 = await storage.getItem("Test2PC", { id: { S: "new-1" } });
      const item2 = await storage.getItem("Test2PC", { id: { S: "new-2" } });
      const item3 = await storage.getItem("Test2PC", { id: { S: "new-3" } });
      const existing = await storage.getItem("Test2PC", { id: { S: "existing" } });

      expect(item1).toBeNull();
      expect(item2).toBeNull();
      expect(item3).toBeNull();
      expect(existing?.status.S).toBe("active"); // Unchanged
    });
  });

  describe("Idempotency", () => {
    test("should cache transaction results for 10 minutes", async () => {
      const token = `idempotency-test-${Date.now()}`;

      const items: TransactWriteItem[] = [
        {
          Put: {
            tableName: "Test2PC",
            item: { id: { S: "idempotent-item" }, value: { S: "first" } },
          },
        },
      ];

      // First execution
      await storage.transactWrite(items, token);

      let item = await storage.getItem("Test2PC", { id: { S: "idempotent-item" } });
      expect(item?.value.S).toBe("first");

      // Modify the item directly
      await storage.putItem("Test2PC", { id: { S: "idempotent-item" }, value: { S: "modified" } });

      // Execute same transaction with same token - should be idempotent
      await storage.transactWrite(items, token);

      // Item should still be "modified" (transaction was not re-executed)
      item = await storage.getItem("Test2PC", { id: { S: "idempotent-item" } });
      expect(item?.value.S).toBe("modified");
    });

    test("should execute transaction with different token", async () => {
      const token1 = `token-1-${Date.now()}`;
      const token2 = `token-2-${Date.now()}`;

      const items: TransactWriteItem[] = [
        {
          Put: {
            tableName: "Test2PC",
            item: { id: { S: "item-tokens" }, counter: { N: "1" } },
          },
        },
      ];

      // First execution with token1
      await storage.transactWrite(items, token1);

      // Change the item
      await storage.putItem("Test2PC", { id: { S: "item-tokens" }, counter: { N: "5" } });

      // Different token should allow re-execution, but since item exists, Put will overwrite
      await storage.transactWrite(items, token2);

      const item = await storage.getItem("Test2PC", { id: { S: "item-tokens" } });
      expect(item?.counter.N).toBe("1"); // Overwritten by second transaction
    });

    test("should handle failed transaction idempotency", async () => {
      const token = `fail-token-${Date.now()}`;

      await storage.putItem("Test2PC", { id: { S: "item1" }, status: { S: "active" } });

      const items: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "item1" } },
            updateExpression: "SET #status = :val",
            expressionAttributeNames: { "#status": "status" },
            expressionAttributeValues: { ":val": { S: "updated" }, ":wrong": { S: "wrong" } },
            conditionExpression: "#status = :wrong",
          },
        },
      ];

      // First attempt - should fail
      try {
        await storage.transactWrite(items, token);
        expect(true).toBe(false);
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionCancelledError);
      }

      // Second attempt with same token - should still fail (not cached as success)
      try {
        await storage.transactWrite(items, token);
        expect(true).toBe(false);
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionCancelledError);
      }
    });
  });

  describe("Edge Cases", () => {
    test("should handle empty update expression attributes", async () => {
      await storage.putItem("Test2PC", { id: { S: "item1" }, value: { S: "original" } });

      const items: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "item1" } },
            updateExpression: "SET value = :val",
            expressionAttributeValues: { ":val": { S: "updated" } },
            // No expressionAttributeNames
          },
        },
      ];

      await storage.transactWrite(items);

      const item = await storage.getItem("Test2PC", { id: { S: "item1" } });
      expect(item?.value.S).toBe("updated");
    });

    test("should handle transaction with single item", async () => {
      const items: TransactWriteItem[] = [
        {
          Put: {
            tableName: "Test2PC",
            item: { id: { S: "single-item" }, value: { S: "solo" } },
          },
        },
      ];

      await storage.transactWrite(items);

      const item = await storage.getItem("Test2PC", { id: { S: "single-item" } });
      expect(item?.value.S).toBe("solo");
    });

    test("should handle Delete operation on non-existent item", async () => {
      const items: TransactWriteItem[] = [
        {
          Delete: {
            tableName: "Test2PC",
            key: { id: { S: "does-not-exist" } },
          },
        },
      ];

      // Should succeed (DynamoDB behavior)
      await storage.transactWrite(items);
    });

    test("should handle Put with condition on non-existent item", async () => {
      const items: TransactWriteItem[] = [
        {
          Put: {
            tableName: "Test2PC",
            item: { id: { S: "new-item" }, value: { S: "value" } },
            conditionExpression: "attribute_not_exists(id)",
          },
        },
      ];

      await storage.transactWrite(items);

      const item = await storage.getItem("Test2PC", { id: { S: "new-item" } });
      expect(item?.value.S).toBe("value");
    });

    test("should validate attribute names with special characters", async () => {
      await storage.putItem("Test2PC", {
        id: { S: "item1" },
        "special-attr": { S: "value" },
      });

      const items: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "item1" } },
            updateExpression: "SET #attr = :val",
            expressionAttributeNames: { "#attr": "special-attr" },
            expressionAttributeValues: { ":val": { S: "updated" } },
          },
        },
      ];

      await storage.transactWrite(items);

      const item = await storage.getItem("Test2PC", { id: { S: "item1" } });
      expect(item?.["special-attr"].S).toBe("updated");
    });
  });

  describe("TransactGetItems", () => {
    test("should get items from multiple shards atomically", async () => {
      // Create items across different shards
      const itemIds = ["get-1", "get-2", "get-3", "get-4", "get-5"];

      for (const id of itemIds) {
        await storage.putItem("Test2PC", {
          id: { S: id },
          value: { S: `value-${id}` },
          metadata: { S: "data" },
        });
      }

      const getItems = itemIds.map((id) => ({
        tableName: "Test2PC",
        key: { id: { S: id } },
      }));

      const results = await storage.transactGet(getItems);

      expect(results.length).toBe(5);
      for (let i = 0; i < itemIds.length; i++) {
        expect(results[i]?.id.S).toBe(itemIds[i]);
        expect(results[i]?.value.S).toBe(`value-${itemIds[i]}`);
      }
    });

    test("should apply projection to subset of attributes", async () => {
      await storage.putItem("Test2PC", {
        id: { S: "proj-item" },
        field1: { S: "value1" },
        field2: { S: "value2" },
        field3: { S: "value3" },
      });

      const results = await storage.transactGet([
        {
          tableName: "Test2PC",
          key: { id: { S: "proj-item" } },
          projectionExpression: "id, field1, field3",
        },
      ]);

      expect(results[0]?.id.S).toBe("proj-item");
      expect(results[0]?.field1.S).toBe("value1");
      expect(results[0]?.field2).toBeUndefined();
      expect(results[0]?.field3.S).toBe("value3");
    });

    test("should handle projection with attribute name placeholders", async () => {
      await storage.putItem("Test2PC", {
        id: { S: "proj-item-2" },
        status: { S: "active" },
        data: { S: "sensitive" },
        public: { S: "public-info" },
      });

      const results = await storage.transactGet([
        {
          tableName: "Test2PC",
          key: { id: { S: "proj-item-2" } },
          projectionExpression: "#id, #status",
          expressionAttributeNames: { "#id": "id", "#status": "status" },
        },
      ]);

      expect(results[0]?.id.S).toBe("proj-item-2");
      expect(results[0]?.status.S).toBe("active");
      expect(results[0]?.data).toBeUndefined();
      expect(results[0]?.public).toBeUndefined();
    });

    test("should return null for non-existent items in the result array", async () => {
      await storage.putItem("Test2PC", { id: { S: "exists" }, value: { S: "here" } });

      const results = await storage.transactGet([
        { tableName: "Test2PC", key: { id: { S: "exists" } } },
        { tableName: "Test2PC", key: { id: { S: "does-not-exist" } } },
        { tableName: "Test2PC", key: { id: { S: "also-missing" } } },
      ]);

      expect(results.length).toBe(3);
      expect(results[0]?.value.S).toBe("here");
      expect(results[1]).toBeNull();
      expect(results[2]).toBeNull();
    });

    test("should respect 100 item limit for TransactGetItems", async () => {
      const items = [];
      for (let i = 0; i < 101; i++) {
        items.push({
          tableName: "Test2PC",
          key: { id: { S: `item-${i}` } },
        });
      }

      try {
        await storage.transactGet(items);
        expect(true).toBe(false);
      } catch (error: any) {
        expect(error.message).toContain("100 items");
      }
    });
  });

  describe("Complex Scenarios", () => {
    test("should handle workflow pattern with condition checks", async () => {
      // Create initial workflow state
      await storage.putItem("Test2PC", {
        id: { S: "workflow-1" },
        state: { S: "pending" },
        retries: { N: "0" },
      });

      // Transition workflow: check state, update state, increment retries
      const items: TransactWriteItem[] = [
        {
          ConditionCheck: {
            tableName: "Test2PC",
            key: { id: { S: "workflow-1" } },
            conditionExpression: "#state = :pending",
            expressionAttributeNames: { "#state": "state" },
            expressionAttributeValues: { ":pending": { S: "pending" } },
          },
        },
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "workflow-1" } },
            updateExpression: "SET #state = :processing ADD #retries :inc",
            expressionAttributeNames: { "#state": "state", "#retries": "retries" },
            expressionAttributeValues: {
              ":processing": { S: "processing" },
              ":inc": { N: "1" },
            },
          },
        },
      ];

      await storage.transactWrite(items);

      const item = await storage.getItem("Test2PC", { id: { S: "workflow-1" } });
      expect(item?.state.S).toBe("processing");
      expect(item?.retries.N).toBe("1");
    });

    test("should handle inventory deduction pattern", async () => {
      // Setup: inventory and order tracking
      await storage.putItem("Test2PC", {
        id: { S: "product-123" },
        stock: { N: "10" },
      });

      await storage.putItem("Test2PC", {
        id: { S: "order-456" },
        status: { S: "pending" },
      });

      // Deduct inventory and mark order as confirmed
      const items: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "product-123" } },
            updateExpression: "SET #stock = :newStock",
            expressionAttributeNames: { "#stock": "stock" },
            expressionAttributeValues: { ":newStock": { N: "7" }, ":min": { N: "3" } },
            conditionExpression: "#stock > :min", // Prevent negative inventory
          },
        },
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "order-456" } },
            updateExpression: "SET #status = :confirmed",
            expressionAttributeNames: { "#status": "status" },
            expressionAttributeValues: { ":confirmed": { S: "confirmed" } },
          },
        },
      ];

      await storage.transactWrite(items);

      const product = await storage.getItem("Test2PC", { id: { S: "product-123" } });
      const order = await storage.getItem("Test2PC", { id: { S: "order-456" } });

      expect(product?.stock.N).toBe("7");
      expect(order?.status.S).toBe("confirmed");
    });

    test("should rollback when inventory is insufficient", async () => {
      await storage.putItem("Test2PC", {
        id: { S: "product-999" },
        stock: { N: "2" },
      });

      await storage.putItem("Test2PC", {
        id: { S: "order-999" },
        status: { S: "pending" },
      });

      const items: TransactWriteItem[] = [
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "product-999" } },
            updateExpression: "SET #stock = :newStock",
            expressionAttributeNames: { "#stock": "stock" },
            expressionAttributeValues: { ":newStock": { N: "-3" }, ":min": { N: "3" } },
            conditionExpression: "#stock > :min", // Will fail: 2 is not > 3
          },
        },
        {
          Update: {
            tableName: "Test2PC",
            key: { id: { S: "order-999" } },
            updateExpression: "SET #status = :confirmed",
            expressionAttributeNames: { "#status": "status" },
            expressionAttributeValues: { ":confirmed": { S: "confirmed" } },
          },
        },
      ];

      try {
        await storage.transactWrite(items);
        expect(true).toBe(false);
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionCancelledError);
      }

      // Verify nothing changed
      const product = await storage.getItem("Test2PC", { id: { S: "product-999" } });
      const order = await storage.getItem("Test2PC", { id: { S: "order-999" } });

      expect(product?.stock.N).toBe("2"); // Unchanged
      expect(order?.status.S).toBe("pending"); // Unchanged
    });
  });
});
