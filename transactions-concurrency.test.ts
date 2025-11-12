// Tests for concurrent transaction conflicts and locking behavior

import { test, expect, beforeEach, afterEach, describe } from "bun:test";
import { Shard } from "./shard.ts";
import { Router } from "./router.ts";
import { MetadataStore } from "./metadata-store.ts";
import { TransactionCoordinator } from "./coordinator.ts";
import { TransactionCancelledError } from "./types.ts";
import * as fs from "fs";

describe("Concurrent Transaction Conflicts", () => {
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
      "ConcurrencyTest",
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

  test("should serialize concurrent transactions on same item", async () => {
    // Put initial item
    await storage.putItem("ConcurrencyTest", { id: { S: "counter" }, value: { N: "0" } });

    // Run 10 concurrent transactions incrementing the same counter
    const promises = [];
    for (let i = 0; i < 10; i++) {
      promises.push(
        (async () => {
          // Retry on conflicts
          let attempts = 0;
          while (attempts < 5) {
            try {
              await storage.transactWrite([
                {
                  Update: {
                    tableName: "ConcurrencyTest",
                    key: { id: { S: "counter" } },
                    updateExpression: "ADD #v :inc",
                    expressionAttributeNames: { "#v": "value" },
                    expressionAttributeValues: { ":inc": { N: "1" } },
                  },
                },
              ]);
              break;
            } catch (error) {
              attempts++;
              if (attempts >= 5) throw error;
              // Small random delay before retry
              await new Promise((resolve) => setTimeout(resolve, Math.random() * 10));
            }
          }
        })()
      );
    }

    await Promise.all(promises);

    // Counter should be exactly 10
    const item = await storage.getItem("ConcurrencyTest", { id: { S: "counter" } });
    expect(item?.value.N).toBe("10");
  });

  test("should handle concurrent transactions on different items", async () => {
    // Multiple transactions on different items should all succeed
    const promises = [];
    for (let i = 0; i < 20; i++) {
      promises.push(
        storage.transactWrite([
          {
            Put: {
              tableName: "ConcurrencyTest",
              item: { id: { S: `item-${i}` }, value: { N: String(i) } },
            },
          },
        ])
      );
    }

    await Promise.all(promises);

    // All items should exist
    for (let i = 0; i < 20; i++) {
      const item = await storage.getItem("ConcurrencyTest", { id: { S: `item-${i}` } });
      expect(item).not.toBeNull();
      expect(item?.value.N).toBe(String(i));
    }
  });

  test("should handle race condition with Put operations", async () => {
    // Two transactions trying to create the same item with attribute_not_exists
    // Only one should succeed
    const promises = [
      storage.transactWrite([
        {
          Put: {
            tableName: "ConcurrencyTest",
            item: { id: { S: "race-item" }, creator: { S: "txn1" } },
            conditionExpression: "attribute_not_exists(id)",
          },
        },
      ]),
      storage.transactWrite([
        {
          Put: {
            tableName: "ConcurrencyTest",
            item: { id: { S: "race-item" }, creator: { S: "txn2" } },
            conditionExpression: "attribute_not_exists(id)",
          },
        },
      ]),
    ];

    const results = await Promise.allSettled(promises);

    // Exactly one should succeed, one should fail
    const succeeded = results.filter((r) => r.status === "fulfilled").length;
    const failed = results.filter((r) => r.status === "rejected").length;

    expect(succeeded).toBe(1);
    expect(failed).toBe(1);

    // Item should exist with one of the creators
    const item = await storage.getItem("ConcurrencyTest", { id: { S: "race-item" } });
    expect(item).not.toBeNull();
    expect(["txn1", "txn2"]).toContain(item?.creator.S);
  });

  test("should handle bank transfer scenario correctly", async () => {
    // Classic bank transfer: deduct from A, add to B
    await storage.putItem("ConcurrencyTest", { id: { S: "account-A" }, balance: { N: "1000" } });
    await storage.putItem("ConcurrencyTest", { id: { S: "account-B" }, balance: { N: "500" } });

    // Transfer 100 from A to B
    await storage.transactWrite([
      {
        Update: {
          tableName: "ConcurrencyTest",
          key: { id: { S: "account-A" } },
          updateExpression: "SET balance = balance - :amount",
          conditionExpression: "balance >= :amount",
          expressionAttributeValues: { ":amount": { N: "100" } },
        },
      },
      {
        Update: {
          tableName: "ConcurrencyTest",
          key: { id: { S: "account-B" } },
          updateExpression: "SET balance = balance + :amount",
          expressionAttributeValues: { ":amount": { N: "100" } },
        },
      },
    ]);

    const accountA = await storage.getItem("ConcurrencyTest", { id: { S: "account-A" } });
    const accountB = await storage.getItem("ConcurrencyTest", { id: { S: "account-B" } });

    expect(accountA?.balance.N).toBe("900");
    expect(accountB?.balance.N).toBe("600");
  });

  test("should rollback bank transfer if insufficient funds", async () => {
    await storage.putItem("ConcurrencyTest", { id: { S: "account-C" }, balance: { N: "50" } });
    await storage.putItem("ConcurrencyTest", { id: { S: "account-D" }, balance: { N: "100" } });

    // Try to transfer 100 from C to D (should fail - insufficient funds)
    try {
      await storage.transactWrite([
        {
          Update: {
            tableName: "ConcurrencyTest",
            key: { id: { S: "account-C" } },
            updateExpression: "SET balance = balance - :amount",
            conditionExpression: "balance >= :amount",
            expressionAttributeValues: { ":amount": { N: "100" } },
          },
        },
        {
          Update: {
            tableName: "ConcurrencyTest",
            key: { id: { S: "account-D" } },
            updateExpression: "SET balance = balance + :amount",
            expressionAttributeValues: { ":amount": { N: "100" } },
          },
        },
      ]);
      expect.unreachable("Transaction should have failed");
    } catch (error: any) {
      expect(error).toBeInstanceOf(TransactionCancelledError);
    }

    // Both accounts should be unchanged
    const accountC = await storage.getItem("ConcurrencyTest", { id: { S: "account-C" } });
    const accountD = await storage.getItem("ConcurrencyTest", { id: { S: "account-D" } });

    expect(accountC?.balance.N).toBe("50");
    expect(accountD?.balance.N).toBe("100");
  });

  test("should handle multiple concurrent bank transfers", async () => {
    // Setup accounts
    await storage.putItem("ConcurrencyTest", { id: { S: "bank-1" }, balance: { N: "1000" } });
    await storage.putItem("ConcurrencyTest", { id: { S: "bank-2" }, balance: { N: "1000" } });

    // Perform 5 concurrent transfers of 100 each
    const promises = [];
    for (let i = 0; i < 5; i++) {
      promises.push(
        (async () => {
          let attempts = 0;
          while (attempts < 5) {
            try {
              await storage.transactWrite([
                {
                  Update: {
                    tableName: "ConcurrencyTest",
                    key: { id: { S: "bank-1" } },
                    updateExpression: "SET balance = balance - :amount",
                    conditionExpression: "balance >= :amount",
                    expressionAttributeValues: { ":amount": { N: "100" } },
                  },
                },
                {
                  Update: {
                    tableName: "ConcurrencyTest",
                    key: { id: { S: "bank-2" } },
                    updateExpression: "SET balance = balance + :amount",
                    expressionAttributeValues: { ":amount": { N: "100" } },
                  },
                },
              ]);
              break;
            } catch (error) {
              attempts++;
              if (attempts >= 5) throw error;
              await new Promise((resolve) => setTimeout(resolve, Math.random() * 10));
            }
          }
        })()
      );
    }

    await Promise.all(promises);

    // Total balance should be preserved
    const account1 = await storage.getItem("ConcurrencyTest", { id: { S: "bank-1" } });
    const account2 = await storage.getItem("ConcurrencyTest", { id: { S: "bank-2" } });

    const total =
      parseInt(account1?.balance.N || "0") + parseInt(account2?.balance.N || "0");
    expect(total).toBe(2000); // Total preserved

    expect(account1?.balance.N).toBe("500");
    expect(account2?.balance.N).toBe("1500");
  });

  test("should handle concurrent reads (TransactGetItems)", async () => {
    // Setup items
    for (let i = 0; i < 5; i++) {
      await storage.putItem("ConcurrencyTest", {
        id: { S: `read-${i}` },
        value: { N: String(i) },
      });
    }

    // Perform many concurrent reads
    const promises = [];
    for (let i = 0; i < 20; i++) {
      promises.push(
        storage.transactGet([
          { tableName: "ConcurrencyTest", key: { id: { S: "read-0" } } },
          { tableName: "ConcurrencyTest", key: { id: { S: "read-1" } } },
          { tableName: "ConcurrencyTest", key: { id: { S: "read-2" } } },
        ])
      );
    }

    const results = await Promise.all(promises);

    // All reads should succeed
    for (const result of results) {
      expect(result.length).toBe(3);
      expect(result[0]?.value.N).toBe("0");
      expect(result[1]?.value.N).toBe("1");
      expect(result[2]?.value.N).toBe("2");
    }
  });

  test("should handle mixed concurrent reads and writes", async () => {
    await storage.putItem("ConcurrencyTest", { id: { S: "mixed" }, counter: { N: "0" } });

    const promises = [];

    // 10 writers
    for (let i = 0; i < 10; i++) {
      promises.push(
        (async () => {
          let attempts = 0;
          while (attempts < 5) {
            try {
              await storage.transactWrite([
                {
                  Update: {
                    tableName: "ConcurrencyTest",
                    key: { id: { S: "mixed" } },
                    updateExpression: "ADD counter :inc",
                    expressionAttributeValues: { ":inc": { N: "1" } },
                  },
                },
              ]);
              break;
            } catch (error) {
              attempts++;
              if (attempts >= 5) throw error;
              await new Promise((resolve) => setTimeout(resolve, Math.random() * 5));
            }
          }
        })()
      );
    }

    // 10 readers
    for (let i = 0; i < 10; i++) {
      promises.push(
        storage.transactGet([{ tableName: "ConcurrencyTest", key: { id: { S: "mixed" } } }])
      );
    }

    const results = await Promise.all(promises);

    // Final counter should be 10
    const item = await storage.getItem("ConcurrencyTest", { id: { S: "mixed" } });
    expect(item?.counter.N).toBe("10");
  });

  test("should prevent lost updates with optimistic locking pattern", async () => {
    // Setup item with version number
    await storage.putItem("ConcurrencyTest", {
      id: { S: "versioned" },
      value: { N: "100" },
      version: { N: "1" },
    });

    // Two transactions trying to update with version check
    // Both read version 1, but only first should succeed
    const txn1 = storage.transactWrite([
      {
        Update: {
          tableName: "ConcurrencyTest",
          key: { id: { S: "versioned" } },
          updateExpression: "SET #v = :newval, version = :newver",
          conditionExpression: "version = :oldver",
          expressionAttributeNames: { "#v": "value" },
          expressionAttributeValues: {
            ":newval": { N: "200" },
            ":oldver": { N: "1" },
            ":newver": { N: "2" },
          },
        },
      },
    ]);

    const txn2 = storage.transactWrite([
      {
        Update: {
          tableName: "ConcurrencyTest",
          key: { id: { S: "versioned" } },
          updateExpression: "SET #v = :newval, version = :newver",
          conditionExpression: "version = :oldver",
          expressionAttributeNames: { "#v": "value" },
          expressionAttributeValues: {
            ":newval": { N: "300" },
            ":oldver": { N: "1" },
            ":newver": { N: "2" },
          },
        },
      },
    ]);

    const results = await Promise.allSettled([txn1, txn2]);

    // One should succeed, one should fail
    const succeeded = results.filter((r) => r.status === "fulfilled").length;
    const failed = results.filter((r) => r.status === "rejected").length;

    expect(succeeded).toBe(1);
    expect(failed).toBe(1);

    const item = await storage.getItem("ConcurrencyTest", { id: { S: "versioned" } });
    expect(item?.version.N).toBe("2");
    expect(["200", "300"]).toContain(item?.value.N);
  });
});
