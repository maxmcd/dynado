// Tests for range query functionality with sort keys

import { test, expect, beforeEach, afterEach, describe } from "bun:test";
import { Shard } from "./shard.ts";
import { Router } from "./router.ts";
import { MetadataStore } from "./metadata-store.ts";
import { TransactionCoordinator } from "./coordinator.ts";
import * as fs from "fs";
import type { QueryRequest } from "./types.ts";

describe("Range Queries", () => {
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
    coordinator = new TransactionCoordinator(`${dataDir}/coordinator.db`);
    storage = new Router(shards, metadataStore, coordinator);

    // Create table with composite key (partition + sort)
    await storage.createTable(
      "RangeTest",
      [
        { AttributeName: "userId", KeyType: "HASH" },
        { AttributeName: "timestamp", KeyType: "RANGE" },
      ],
      [
        { AttributeName: "userId", AttributeType: "S" },
        { AttributeName: "timestamp", AttributeType: "N" },
      ]
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
      await storage.putItem("RangeTest", item);
    }
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

  test("should query all items for a partition key without sort key condition", async () => {
    const keyValues = metadataStore.extractKeyValues("RangeTest", {
      userId: { S: "user1" },
      timestamp: { N: "0" },
    });

    const request: QueryRequest = {
      tableName: "RangeTest",
      partitionKeyValue: keyValues.partitionKeyValue,
    };

    const result = await storage.queryWithRangeKey(request);

    expect(result.items.length).toBe(5);
    expect(result.count).toBe(5);
    // Items should be in ascending order by default
    expect(result.items[0]?.timestamp.N).toBe("100");
    expect(result.items[4]?.timestamp.N).toBe("500");
  });

  test("should query with = operator", async () => {
    const keyValues = metadataStore.extractKeyValues("RangeTest", {
      userId: { S: "user1" },
      timestamp: { N: "0" },
    });

    const request: QueryRequest = {
      tableName: "RangeTest",
      partitionKeyValue: keyValues.partitionKeyValue,
      sortKeyCondition: {
        operator: "=",
        value: { N: "300" },
      },
    };

    const result = await storage.queryWithRangeKey(request);

    expect(result.items.length).toBe(1);
    expect(result.items[0]?.timestamp.N).toBe("300");
    expect(result.items[0]?.data.S).toBe("c");
  });

  test("should query with < operator", async () => {
    const keyValues = metadataStore.extractKeyValues("RangeTest", {
      userId: { S: "user1" },
      timestamp: { N: "0" },
    });

    const request: QueryRequest = {
      tableName: "RangeTest",
      partitionKeyValue: keyValues.partitionKeyValue,
      sortKeyCondition: {
        operator: "<",
        value: { N: "300" },
      },
    };

    const result = await storage.queryWithRangeKey(request);

    expect(result.items.length).toBe(2);
    expect(result.items[0]?.timestamp.N).toBe("100");
    expect(result.items[1]?.timestamp.N).toBe("200");
  });

  test("should query with > operator", async () => {
    const keyValues = metadataStore.extractKeyValues("RangeTest", {
      userId: { S: "user1" },
      timestamp: { N: "0" },
    });

    const request: QueryRequest = {
      tableName: "RangeTest",
      partitionKeyValue: keyValues.partitionKeyValue,
      sortKeyCondition: {
        operator: ">",
        value: { N: "300" },
      },
    };

    const result = await storage.queryWithRangeKey(request);

    expect(result.items.length).toBe(2);
    expect(result.items[0]?.timestamp.N).toBe("400");
    expect(result.items[1]?.timestamp.N).toBe("500");
  });

  test("should query with <= operator", async () => {
    const keyValues = metadataStore.extractKeyValues("RangeTest", {
      userId: { S: "user1" },
      timestamp: { N: "0" },
    });

    const request: QueryRequest = {
      tableName: "RangeTest",
      partitionKeyValue: keyValues.partitionKeyValue,
      sortKeyCondition: {
        operator: "<=",
        value: { N: "300" },
      },
    };

    const result = await storage.queryWithRangeKey(request);

    expect(result.items.length).toBe(3);
    expect(result.items[0]?.timestamp.N).toBe("100");
    expect(result.items[1]?.timestamp.N).toBe("200");
    expect(result.items[2]?.timestamp.N).toBe("300");
  });

  test("should query with >= operator", async () => {
    const keyValues = metadataStore.extractKeyValues("RangeTest", {
      userId: { S: "user1" },
      timestamp: { N: "0" },
    });

    const request: QueryRequest = {
      tableName: "RangeTest",
      partitionKeyValue: keyValues.partitionKeyValue,
      sortKeyCondition: {
        operator: ">=",
        value: { N: "300" },
      },
    };

    const result = await storage.queryWithRangeKey(request);

    expect(result.items.length).toBe(3);
    expect(result.items[0]?.timestamp.N).toBe("300");
    expect(result.items[1]?.timestamp.N).toBe("400");
    expect(result.items[2]?.timestamp.N).toBe("500");
  });

  test("should query with BETWEEN operator", async () => {
    const keyValues = metadataStore.extractKeyValues("RangeTest", {
      userId: { S: "user1" },
      timestamp: { N: "0" },
    });

    const request: QueryRequest = {
      tableName: "RangeTest",
      partitionKeyValue: keyValues.partitionKeyValue,
      sortKeyCondition: {
        operator: "BETWEEN",
        value: { N: "200" },
        value2: { N: "400" },
      },
    };

    const result = await storage.queryWithRangeKey(request);

    expect(result.items.length).toBe(3);
    expect(result.items[0]?.timestamp.N).toBe("200");
    expect(result.items[1]?.timestamp.N).toBe("300");
    expect(result.items[2]?.timestamp.N).toBe("400");
  });

  test("should query with begins_with operator", async () => {
    // Create a table with string sort keys for begins_with testing
    await storage.createTable(
      "BeginsWith",
      [
        { AttributeName: "category", KeyType: "HASH" },
        { AttributeName: "itemId", KeyType: "RANGE" },
      ],
      [
        { AttributeName: "category", AttributeType: "S" },
        { AttributeName: "itemId", AttributeType: "S" },
      ]
    );

    // Insert test data
    const items = [
      { category: { S: "products" }, itemId: { S: "PROD-001" }, name: { S: "Item 1" } },
      { category: { S: "products" }, itemId: { S: "PROD-002" }, name: { S: "Item 2" } },
      { category: { S: "products" }, itemId: { S: "PROD-003" }, name: { S: "Item 3" } },
      { category: { S: "products" }, itemId: { S: "ORDER-001" }, name: { S: "Order 1" } },
      { category: { S: "products" }, itemId: { S: "ORDER-002" }, name: { S: "Order 2" } },
    ];

    for (const item of items) {
      await storage.putItem("BeginsWith", item);
    }

    const keyValues = metadataStore.extractKeyValues("BeginsWith", {
      category: { S: "products" },
      itemId: { S: "" },
    });

    const request: QueryRequest = {
      tableName: "BeginsWith",
      partitionKeyValue: keyValues.partitionKeyValue,
      sortKeyCondition: {
        operator: "begins_with",
        value: { S: "PROD" },
      },
    };

    const result = await storage.queryWithRangeKey(request);

    expect(result.items.length).toBe(3);
    expect(result.items[0]?.itemId.S).toBe("PROD-001");
    expect(result.items[1]?.itemId.S).toBe("PROD-002");
    expect(result.items[2]?.itemId.S).toBe("PROD-003");
  });

  test("should support descending order with scanIndexForward=false", async () => {
    const keyValues = metadataStore.extractKeyValues("RangeTest", {
      userId: { S: "user1" },
      timestamp: { N: "0" },
    });

    const request: QueryRequest = {
      tableName: "RangeTest",
      partitionKeyValue: keyValues.partitionKeyValue,
      scanIndexForward: false,
    };

    const result = await storage.queryWithRangeKey(request);

    expect(result.items.length).toBe(5);
    // Items should be in descending order
    expect(result.items[0]?.timestamp.N).toBe("500");
    expect(result.items[4]?.timestamp.N).toBe("100");
  });

  test("should support pagination with limit", async () => {
    const keyValues = metadataStore.extractKeyValues("RangeTest", {
      userId: { S: "user1" },
      timestamp: { N: "0" },
    });

    const request: QueryRequest = {
      tableName: "RangeTest",
      partitionKeyValue: keyValues.partitionKeyValue,
      limit: 2,
    };

    const result = await storage.queryWithRangeKey(request);

    expect(result.items.length).toBe(2);
    expect(result.items[0]?.timestamp.N).toBe("100");
    expect(result.items[1]?.timestamp.N).toBe("200");
    expect(result.lastEvaluatedKey).toBeDefined();
    expect(result.lastEvaluatedKey?.sortKeyValue).toBe(JSON.stringify({ N: "200" }));
  });

  test("should support pagination with exclusiveStartKey", async () => {
    const keyValues = metadataStore.extractKeyValues("RangeTest", {
      userId: { S: "user1" },
      timestamp: { N: "0" },
    });

    // First page
    const request1: QueryRequest = {
      tableName: "RangeTest",
      partitionKeyValue: keyValues.partitionKeyValue,
      limit: 2,
    };

    const result1 = await storage.queryWithRangeKey(request1);
    expect(result1.items.length).toBe(2);

    // Second page using lastEvaluatedKey
    const request2: QueryRequest = {
      tableName: "RangeTest",
      partitionKeyValue: keyValues.partitionKeyValue,
      limit: 2,
      exclusiveStartKey: result1.lastEvaluatedKey,
    };

    const result2 = await storage.queryWithRangeKey(request2);
    expect(result2.items.length).toBe(2);
    expect(result2.items[0]?.timestamp.N).toBe("300");
    expect(result2.items[1]?.timestamp.N).toBe("400");
  });

  test("should isolate queries by partition key", async () => {
    const keyValuesUser1 = metadataStore.extractKeyValues("RangeTest", {
      userId: { S: "user1" },
      timestamp: { N: "0" },
    });

    const keyValuesUser2 = metadataStore.extractKeyValues("RangeTest", {
      userId: { S: "user2" },
      timestamp: { N: "0" },
    });

    const request1: QueryRequest = {
      tableName: "RangeTest",
      partitionKeyValue: keyValuesUser1.partitionKeyValue,
    };

    const request2: QueryRequest = {
      tableName: "RangeTest",
      partitionKeyValue: keyValuesUser2.partitionKeyValue,
    };

    const result1 = await storage.queryWithRangeKey(request1);
    const result2 = await storage.queryWithRangeKey(request2);

    expect(result1.items.length).toBe(5);
    expect(result2.items.length).toBe(2);

    // Verify all items have correct userId
    for (const item of result1.items) {
      expect(item.userId.S).toBe("user1");
    }
    for (const item of result2.items) {
      expect(item.userId.S).toBe("user2");
    }
  });

  test("should return empty results when no items match", async () => {
    const keyValues = metadataStore.extractKeyValues("RangeTest", {
      userId: { S: "user1" },
      timestamp: { N: "0" },
    });

    const request: QueryRequest = {
      tableName: "RangeTest",
      partitionKeyValue: keyValues.partitionKeyValue,
      sortKeyCondition: {
        operator: ">",
        value: { N: "999" },
      },
    };

    const result = await storage.queryWithRangeKey(request);

    expect(result.items.length).toBe(0);
    expect(result.count).toBe(0);
    expect(result.lastEvaluatedKey).toBeUndefined();
  });

  test("should handle complex pagination with conditions", async () => {
    const keyValues = metadataStore.extractKeyValues("RangeTest", {
      userId: { S: "user1" },
      timestamp: { N: "0" },
    });

    const request: QueryRequest = {
      tableName: "RangeTest",
      partitionKeyValue: keyValues.partitionKeyValue,
      sortKeyCondition: {
        operator: ">=",
        value: { N: "200" },
      },
      limit: 2,
    };

    const result = await storage.queryWithRangeKey(request);

    expect(result.items.length).toBe(2);
    expect(result.items[0]?.timestamp.N).toBe("200");
    expect(result.items[1]?.timestamp.N).toBe("300");
    expect(result.lastEvaluatedKey).toBeDefined();
  });
});
