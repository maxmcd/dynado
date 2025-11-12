import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { ShardedSQLiteStorage } from "./storage-sqlite.ts";
import * as fs from "fs";

const TEST_DATA_DIR = "./test-data";

function cleanupTestData() {
  if (fs.existsSync(TEST_DATA_DIR)) {
    fs.rmSync(TEST_DATA_DIR, { recursive: true });
  }
}

describe("Sharded SQLite Storage", () => {
  let storage: ShardedSQLiteStorage;

  beforeEach(() => {
    cleanupTestData();
    fs.mkdirSync(TEST_DATA_DIR, { recursive: true });
    storage = new ShardedSQLiteStorage({
      shardCount: 4,
      dataDir: TEST_DATA_DIR,
    });
  });

  afterEach(() => {
    storage.close();
    cleanupTestData();
  });

  test("should create and list tables", async () => {
    await storage.createTable({
      tableName: "TestTable",
      keySchema: [{ AttributeName: "id", KeyType: "HASH" }],
      attributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
    });

    const tables = await storage.listTables();
    expect(tables).toContain("TestTable");
  });

  test("should store and retrieve items across shards", async () => {
    await storage.createTable({
      tableName: "Users",
      keySchema: [{ AttributeName: "userId", KeyType: "HASH" }],
      attributeDefinitions: [{ AttributeName: "userId", AttributeType: "S" }],
    });

    // Put items with different keys (they should go to different shards)
    const items = [
      { userId: { S: "user-1" }, name: { S: "Alice" } },
      { userId: { S: "user-2" }, name: { S: "Bob" } },
      { userId: { S: "user-3" }, name: { S: "Charlie" } },
      { userId: { S: "user-4" }, name: { S: "Diana" } },
      { userId: { S: "user-5" }, name: { S: "Eve" } },
    ];

    for (const item of items) {
      await storage.putItem("Users", item);
    }

    // Retrieve items
    for (const item of items) {
      const retrieved = await storage.getItem("Users", { userId: item.userId });
      expect(retrieved).toEqual(item);
    }
  });

  test("should distribute items across shards based on hash", async () => {
    await storage.createTable({
      tableName: "Products",
      keySchema: [{ AttributeName: "productId", KeyType: "HASH" }],
      attributeDefinitions: [
        { AttributeName: "productId", AttributeType: "S" },
      ],
    });

    // Add many items to ensure distribution across shards
    const itemCount = 100;
    for (let i = 0; i < itemCount; i++) {
      await storage.putItem("Products", {
        productId: { S: `product-${i}` },
        name: { S: `Product ${i}` },
      });
    }

    const count = await storage.getTableItemCount("Products");
    expect(count).toBe(itemCount);

    // Verify we can scan all items
    const scanResult = await storage.scan("Products");
    expect(scanResult.items.length).toBe(itemCount);
  });

  test("should handle updates and deletes", async () => {
    await storage.createTable({
      tableName: "Items",
      keySchema: [{ AttributeName: "id", KeyType: "HASH" }],
      attributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
    });

    // Put item
    await storage.putItem("Items", {
      id: { S: "item-1" },
      value: { N: "10" },
    });

    // Update item
    await storage.putItem("Items", {
      id: { S: "item-1" },
      value: { N: "20" },
    });

    let item = await storage.getItem("Items", { id: { S: "item-1" } });
    expect(item?.value.N).toBe("20");

    // Delete item
    const deleted = await storage.deleteItem("Items", { id: { S: "item-1" } });
    expect(deleted?.value.N).toBe("20");

    // Verify deletion
    item = await storage.getItem("Items", { id: { S: "item-1" } });
    expect(item).toBeNull();
  });

  test("should handle batch operations", async () => {
    await storage.createTable({
      tableName: "BatchTest",
      keySchema: [{ AttributeName: "id", KeyType: "HASH" }],
      attributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
    });

    // Batch write
    const puts = [
      { id: { S: "batch-1" }, data: { S: "First" } },
      { id: { S: "batch-2" }, data: { S: "Second" } },
      { id: { S: "batch-3" }, data: { S: "Third" } },
    ];

    await storage.batchWrite("BatchTest", puts, []);

    // Batch get
    const keys = [
      { id: { S: "batch-1" } },
      { id: { S: "batch-2" } },
      { id: { S: "batch-3" } },
    ];

    const items = await storage.batchGet("BatchTest", keys);
    expect(items.length).toBe(3);
    expect(items.some((item) => item.id.S === "batch-1")).toBe(true);
  });

  test("should query items with condition", async () => {
    await storage.createTable({
      tableName: "QueryTest",
      keySchema: [{ AttributeName: "id", KeyType: "HASH" }],
      attributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
    });

    await storage.putItem("QueryTest", {
      id: { S: "item-1" },
      category: { S: "electronics" },
      name: { S: "Phone" },
    });

    await storage.putItem("QueryTest", {
      id: { S: "item-2" },
      category: { S: "books" },
      name: { S: "Novel" },
    });

    await storage.putItem("QueryTest", {
      id: { S: "item-3" },
      category: { S: "electronics" },
      name: { S: "Laptop" },
    });

    const result = await storage.query(
      "QueryTest",
      (item) => item.category?.S === "electronics"
    );

    expect(result.items.length).toBe(2);
    expect(
      result.items.every((item) => item.category.S === "electronics")
    ).toBe(true);
  });

  test("should persist data across storage instances", async () => {
    await storage.createTable({
      tableName: "PersistTest",
      keySchema: [{ AttributeName: "id", KeyType: "HASH" }],
      attributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
    });

    await storage.putItem("PersistTest", {
      id: { S: "persist-1" },
      data: { S: "Persisted" },
    });

    storage.close();

    // Create new storage instance
    const newStorage = new ShardedSQLiteStorage({
      shardCount: 4,
      dataDir: TEST_DATA_DIR,
    });

    const tables = await newStorage.listTables();
    expect(tables).toContain("PersistTest");

    const item = await newStorage.getItem("PersistTest", {
      id: { S: "persist-1" },
    });
    expect(item?.data.S).toBe("Persisted");

    newStorage.close();
  });

  test("should handle table deletion", async () => {
    await storage.createTable({
      tableName: "DeleteTest",
      keySchema: [{ AttributeName: "id", KeyType: "HASH" }],
      attributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
    });

    await storage.putItem("DeleteTest", {
      id: { S: "test-1" },
      value: { S: "Test" },
    });

    await storage.deleteTable("DeleteTest");

    const tables = await storage.listTables();
    expect(tables).not.toContain("DeleteTest");

    // Verify items are also deleted
    const count = await storage.getTableItemCount("DeleteTest");
    expect(count).toBe(0);
  });
});
