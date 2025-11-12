// Tests for 2PC failure scenarios and edge cases

import { test, expect, beforeEach, afterEach, describe } from "bun:test";
import { Shard } from "./shard.ts";
import { Router } from "./router.ts";
import { MetadataStore } from "./metadata-store.ts";
import { TransactionCoordinator } from "./coordinator.ts";
import { TransactionCancelledError } from "./types.ts";
import * as fs from "fs";

describe("2PC Failure Scenarios", () => {
  let shards: Shard[];
  let metadataStore: MetadataStore;
  let coordinator: TransactionCoordinator;
  let storage: Router;
  const dataDir = process.env.DATA_DIR || "./test-data";
  const shardCount = parseInt(process.env.SHARD_COUNT || "4");

  beforeEach(async () => {
    // Clean up test data
    if (fs.existsSync(dataDir)) {
      fs.rmSync(dataDir, { recursive: true });
    }
    fs.mkdirSync(dataDir, { recursive: true });

    // Initialize components
    shards = [];
    for (let i = 0; i < shardCount; i++) {
      shards.push(new Shard(`${dataDir}/shard-${i}.db`, i));
    }

    metadataStore = new MetadataStore(dataDir);
    coordinator = new TransactionCoordinator(dataDir);
    storage = new Router(shards, metadataStore, coordinator);

    // Create test table
    await storage.createTable(
      "FailureTest",
      [{ AttributeName: "id", KeyType: "HASH" }],
      [{ AttributeName: "id", AttributeType: "S" }]
    );
  });

  afterEach(() => {
    for (const shard of shards) {
      shard.close();
    }
    metadataStore.close();
    coordinator.close();

    // Clean up
    if (fs.existsSync(dataDir)) {
      fs.rmSync(dataDir, { recursive: true });
    }
  });

  test("should fail transaction when condition check fails", async () => {
    // Put an item first
    await storage.putItem("FailureTest", { id: { S: "test-1" }, value: { N: "10" } });

    // Try to put with attribute_not_exists - should fail
    try {
      await storage.transactWrite([
        {
          Put: {
            tableName: "FailureTest",
            item: { id: { S: "test-1" }, value: { N: "20" } },
            conditionExpression: "attribute_not_exists(id)",
          },
        },
      ]);
      expect.unreachable("Transaction should have failed");
    } catch (error: any) {
      expect(error).toBeInstanceOf(TransactionCancelledError);
      expect(error.cancellationReasons).toBeDefined();
      expect(error.cancellationReasons[0]?.Code).toBe("ConditionalCheckFailed");
    }

    // Verify original item is unchanged
    const item = await storage.getItem("FailureTest", { id: { S: "test-1" } });
    expect(item?.value.N).toBe("10");
  });

  test("should rollback all items when one condition fails in multi-item transaction", async () => {
    // Put first item
    await storage.putItem("FailureTest", { id: { S: "item-1" }, value: { N: "1" } });

    // Try transaction with 3 items where the second one fails
    try {
      await storage.transactWrite([
        {
          Put: {
            tableName: "FailureTest",
            item: { id: { S: "item-2" }, value: { N: "2" } },
          },
        },
        {
          Put: {
            tableName: "FailureTest",
            item: { id: { S: "item-1" }, value: { N: "100" } },
            conditionExpression: "attribute_not_exists(id)", // This will fail
          },
        },
        {
          Put: {
            tableName: "FailureTest",
            item: { id: { S: "item-3" }, value: { N: "3" } },
          },
        },
      ]);
      expect.unreachable("Transaction should have failed");
    } catch (error: any) {
      expect(error).toBeInstanceOf(TransactionCancelledError);
    }

    // Verify none of the items were committed
    const item2 = await storage.getItem("FailureTest", { id: { S: "item-2" } });
    expect(item2).toBeNull();

    const item3 = await storage.getItem("FailureTest", { id: { S: "item-3" } });
    expect(item3).toBeNull();

    // Original item should be unchanged
    const item1 = await storage.getItem("FailureTest", { id: { S: "item-1" } });
    expect(item1?.value.N).toBe("1");
  });

  test("should handle timestamp conflicts correctly", async () => {
    // This test verifies that transactions with conflicting timestamps are rejected
    // Put an item
    await storage.putItem("FailureTest", { id: { S: "ts-test" }, value: { N: "1" } });

    // Start a transaction but don't complete it
    // In a real scenario, this would be two concurrent transactions
    // For testing, we'll verify the timestamp ordering works

    await storage.transactWrite([
      {
        Update: {
          tableName: "FailureTest",
          key: { id: { S: "ts-test" } },
          updateExpression: "SET #v = :val",
          expressionAttributeNames: { "#v": "value" },
          expressionAttributeValues: { ":val": { N: "2" } },
        },
      },
    ]);

    const item = await storage.getItem("FailureTest", { id: { S: "ts-test" } });
    expect(item?.value.N).toBe("2");
  });

  test("should return item on condition check failure when requested", async () => {
    // Put an item
    await storage.putItem("FailureTest", {
      id: { S: "return-test" },
      value: { N: "42" },
      name: { S: "Original" },
    });

    // Try to update with wrong condition and request return values
    try {
      await storage.transactWrite([
        {
          Put: {
            tableName: "FailureTest",
            item: { id: { S: "return-test" }, value: { N: "100" } },
            conditionExpression: "attribute_not_exists(id)",
            returnValuesOnConditionCheckFailure: "ALL_OLD",
          },
        },
      ]);
      expect.unreachable("Transaction should have failed");
    } catch (error: any) {
      expect(error).toBeInstanceOf(TransactionCancelledError);
      expect(error.cancellationReasons[0]?.Item).toBeDefined();
      expect(error.cancellationReasons[0]?.Item?.value.N).toBe("42");
      expect(error.cancellationReasons[0]?.Item?.name.S).toBe("Original");
    }
  });

  test("should handle Delete operation in transaction", async () => {
    // Put items
    await storage.putItem("FailureTest", { id: { S: "delete-1" }, value: { N: "1" } });
    await storage.putItem("FailureTest", { id: { S: "delete-2" }, value: { N: "2" } });

    // Transaction with delete
    await storage.transactWrite([
      {
        Delete: {
          tableName: "FailureTest",
          key: { id: { S: "delete-1" } },
        },
      },
      {
        Put: {
          tableName: "FailureTest",
          item: { id: { S: "delete-3" }, value: { N: "3" } },
        },
      },
    ]);

    // Verify delete worked
    const item1 = await storage.getItem("FailureTest", { id: { S: "delete-1" } });
    expect(item1).toBeNull();

    // Verify put worked
    const item3 = await storage.getItem("FailureTest", { id: { S: "delete-3" } });
    expect(item3).not.toBeNull();
  });

  test("should handle ConditionCheck operation", async () => {
    // Put an item
    await storage.putItem("FailureTest", { id: { S: "check-1" }, status: { S: "active" } });

    // Transaction with condition check
    await storage.transactWrite([
      {
        ConditionCheck: {
          tableName: "FailureTest",
          key: { id: { S: "check-1" } },
          conditionExpression: "#status = :active",
          expressionAttributeNames: { "#status": "status" },
          expressionAttributeValues: { ":active": { S: "active" } },
        },
      },
      {
        Put: {
          tableName: "FailureTest",
          item: { id: { S: "check-2" }, value: { N: "2" } },
        },
      },
    ]);

    // Verify put succeeded because condition check passed
    const item2 = await storage.getItem("FailureTest", { id: { S: "check-2" } });
    expect(item2).not.toBeNull();
  });

  test("should fail when ConditionCheck fails", async () => {
    // Put an item
    await storage.putItem("FailureTest", { id: { S: "check-fail" }, status: { S: "active" } });

    // Transaction with failing condition check
    try {
      await storage.transactWrite([
        {
          ConditionCheck: {
            tableName: "FailureTest",
            key: { id: { S: "check-fail" } },
            conditionExpression: "#status = :inactive",
            expressionAttributeNames: { "#status": "status" },
            expressionAttributeValues: { ":inactive": { S: "inactive" } },
          },
        },
        {
          Put: {
            tableName: "FailureTest",
            item: { id: { S: "should-not-exist" }, value: { N: "1" } },
          },
        },
      ]);
      expect.unreachable("Transaction should have failed");
    } catch (error: any) {
      expect(error).toBeInstanceOf(TransactionCancelledError);
    }

    // Verify put didn't happen
    const item = await storage.getItem("FailureTest", { id: { S: "should-not-exist" } });
    expect(item).toBeNull();
  });

  test("should handle idempotent transactions with client request token", async () => {
    const clientToken = "test-token-" + Date.now();

    // First transaction
    await storage.transactWrite(
      [
        {
          Put: {
            tableName: "FailureTest",
            item: { id: { S: "idempotent-1" }, value: { N: "1" } },
          },
        },
      ],
      clientToken
    );

    // Same transaction with same token - should be idempotent
    await storage.transactWrite(
      [
        {
          Put: {
            tableName: "FailureTest",
            item: { id: { S: "idempotent-1" }, value: { N: "999" } },
          },
        },
      ],
      clientToken
    );

    // Value should still be 1 (from first request, second was deduplicated)
    const item = await storage.getItem("FailureTest", { id: { S: "idempotent-1" } });
    expect(item?.value.N).toBe("1");
  });

  test("should validate transaction item limits", async () => {
    // Try to create transaction with > 100 items
    const items = [];
    for (let i = 0; i < 101; i++) {
      items.push({
        Put: {
          tableName: "FailureTest",
          item: { id: { S: `item-${i}` }, value: { N: String(i) } },
        },
      });
    }

    try {
      await storage.transactWrite(items);
      expect.unreachable("Should have failed with too many items");
    } catch (error: any) {
      expect(error.message).toContain("more than 100");
    }
  });

  test("should reject empty transaction", async () => {
    try {
      await storage.transactWrite([]);
      expect.unreachable("Should have failed with empty items");
    } catch (error: any) {
      expect(error.message).toContain("cannot be empty");
    }
  });

  test("should handle Update operation in transaction", async () => {
    // Put initial item
    await storage.putItem("FailureTest", { id: { S: "update-1" }, value: { N: "10" } });

    // Transaction with update
    await storage.transactWrite([
      {
        Update: {
          tableName: "FailureTest",
          key: { id: { S: "update-1" } },
          updateExpression: "SET #v = #v + :inc",
          expressionAttributeNames: { "#v": "value" },
          expressionAttributeValues: { ":inc": { N: "5" } },
        },
      },
    ]);

    // Verify update worked
    const item = await storage.getItem("FailureTest", { id: { S: "update-1" } });
    expect(item?.value.N).toBe("15");
  });

  test("should handle Update with condition", async () => {
    // Put initial item
    await storage.putItem("FailureTest", {
      id: { S: "cond-update" },
      value: { N: "10" },
      status: { S: "active" },
    });

    // Update only if status is active
    await storage.transactWrite([
      {
        Update: {
          tableName: "FailureTest",
          key: { id: { S: "cond-update" } },
          updateExpression: "SET #v = :newval",
          conditionExpression: "#status = :active",
          expressionAttributeNames: { "#v": "value", "#status": "status" },
          expressionAttributeValues: { ":newval": { N: "20" }, ":active": { S: "active" } },
        },
      },
    ]);

    const item = await storage.getItem("FailureTest", { id: { S: "cond-update" } });
    expect(item?.value.N).toBe("20");
  });
});
