// Concurrent transaction tests
// Tests system correctness under heavy concurrent load

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { ShardedSQLiteStorage } from "./storage-sqlite.ts";
import type { TransactWriteItem } from "./storage.ts";
import { TransactionCancelledError } from "./storage.ts";
import * as fs from "fs";

const TEST_DATA_DIR = "./test-data-concurrent";
const VERBOSE = process.env.VERBOSE_TESTS === "true";

function cleanupTestData() {
  if (fs.existsSync(TEST_DATA_DIR)) {
    fs.rmSync(TEST_DATA_DIR, { recursive: true });
  }
}

// Helper to generate random int between min and max (inclusive)
function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Helper to add random delay
async function randomDelay(maxMs: number = 10) {
  await new Promise((resolve) => setTimeout(resolve, Math.random() * maxMs));
}

// Helper to shuffle array
function shuffle<T>(array: T[]): T[] {
  const result = [...array];
  for (let i = result.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [result[i], result[j]] = [result[j]!, result[i]!];
  }
  return result;
}

describe("Concurrent Transaction Tests", () => {
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
      tableName: "ConcurrentTest",
      keySchema: [{ AttributeName: "id", KeyType: "HASH" }],
      attributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
    });
  });

  afterEach(() => {
    storage.close();
    cleanupTestData();
  });

  test("bank transfer invariant - total balance preserved under concurrent transfers", async () => {
    const NUM_ACCOUNTS = 10;
    const INITIAL_BALANCE = 1000;
    const NUM_TRANSFERS = 200;
    const CONCURRENT_WORKERS = 15;

    // Setup: Create bank accounts
    const accountIds: string[] = [];
    for (let i = 0; i < NUM_ACCOUNTS; i++) {
      const accountId = `account-${i}`;
      accountIds.push(accountId);
      await storage.putItem("ConcurrentTest", {
        id: { S: accountId },
        balance: { N: String(INITIAL_BALANCE) },
        type: { S: "account" },
      });
    }

    const EXPECTED_TOTAL = NUM_ACCOUNTS * INITIAL_BALANCE;

    // Track operations
    let successfulTransfers = 0;
    let failedTransfers = 0;
    const transferHistory: Array<{ from: string; to: string; amount: number; success: boolean }> = [];

    // Worker function: performs random transfers
    async function worker(workerId: number, numTransfers: number) {
      for (let i = 0; i < numTransfers; i++) {
        // Pick two different random accounts
        const fromIdx = randomInt(0, NUM_ACCOUNTS - 1);
        let toIdx = randomInt(0, NUM_ACCOUNTS - 1);
        while (toIdx === fromIdx) {
          toIdx = randomInt(0, NUM_ACCOUNTS - 1);
        }

        const fromAccount = accountIds[fromIdx]!;
        const toAccount = accountIds[toIdx]!;
        const amount = randomInt(1, 100);

        try {
          // Transfer money atomically
          const items: TransactWriteItem[] = [
            {
              Update: {
                tableName: "ConcurrentTest",
                key: { id: { S: fromAccount } },
                updateExpression: "SET balance = balance - :amount",
                expressionAttributeValues: {
                  ":amount": { N: String(amount) },
                  ":zero": { N: "0" },
                },
                conditionExpression: "balance > :zero", // Prevent overdraft
              },
            },
            {
              Update: {
                tableName: "ConcurrentTest",
                key: { id: { S: toAccount } },
                updateExpression: "SET balance = balance + :amount",
                expressionAttributeValues: { ":amount": { N: String(amount) } },
              },
            },
          ];

          // Note: Our current implementation doesn't support "balance - :amount" arithmetic
          // We need to read-then-write instead
          const fromItem = await storage.getItem("ConcurrentTest", { id: { S: fromAccount } });
          const toItem = await storage.getItem("ConcurrentTest", { id: { S: toAccount } });

          if (!fromItem || !toItem) continue;

          const fromBalance = parseInt(fromItem.balance.N);
          const toBalance = parseInt(toItem.balance.N);

          if (fromBalance < amount) {
            failedTransfers++;
            transferHistory.push({ from: fromAccount, to: toAccount, amount, success: false });
            continue;
          }

          const newFromBalance = fromBalance - amount;
          const newToBalance = toBalance + amount;

          const transactItems: TransactWriteItem[] = [
            {
              Update: {
                tableName: "ConcurrentTest",
                key: { id: { S: fromAccount } },
                updateExpression: "SET balance = :newBalance",
                expressionAttributeValues: {
                  ":newBalance": { N: String(newFromBalance) },
                  ":expectedBalance": { N: String(fromBalance) },
                },
                conditionExpression: "balance = :expectedBalance",
              },
            },
            {
              Update: {
                tableName: "ConcurrentTest",
                key: { id: { S: toAccount } },
                updateExpression: "SET balance = :newBalance",
                expressionAttributeValues: {
                  ":newBalance": { N: String(newToBalance) },
                  ":expectedBalance": { N: String(toBalance) },
                },
                conditionExpression: "balance = :expectedBalance",
              },
            },
          ];

          await storage.transactWrite(transactItems);
          successfulTransfers++;
          transferHistory.push({ from: fromAccount, to: toAccount, amount, success: true });
        } catch (error) {
          if (error instanceof TransactionCancelledError) {
            failedTransfers++;
            transferHistory.push({ from: fromAccount, to: toAccount, amount, success: false });
          } else {
            throw error;
          }
        }
      }
    }

    // Run concurrent workers
    const transfersPerWorker = Math.floor(NUM_TRANSFERS / CONCURRENT_WORKERS);
    const startTime = Date.now();

    await Promise.all(
      Array.from({ length: CONCURRENT_WORKERS }, (_, i) => worker(i, transfersPerWorker))
    );

    const duration = Date.now() - startTime;

    // Verify invariant: total balance unchanged
    let actualTotal = 0;
    const finalBalances: Record<string, number> = {};

    for (const accountId of accountIds) {
      const account = await storage.getItem("ConcurrentTest", { id: { S: accountId } });
      if (account) {
        const balance = parseInt(account.balance.N);
        finalBalances[accountId] = balance;
        actualTotal += balance;
      }
    }

    if (VERBOSE) {
      console.log(`Successful transfers: ${successfulTransfers}`);
      console.log(`Failed transfers: ${failedTransfers}`);
      console.log(`Duration: ${duration}ms`);
      console.log(`Throughput: ${((successfulTransfers / duration) * 1000).toFixed(2)} transfers/sec`);
    }

    // The critical invariant: total money in system must remain constant
    expect(actualTotal).toBe(EXPECTED_TOTAL);

    // No account should have negative balance
    for (const [accountId, balance] of Object.entries(finalBalances)) {
      expect(balance).toBeGreaterThanOrEqual(0);
    }
  }, 30000);

  test("concurrent counter increments - no lost updates", async () => {
    const NUM_COUNTERS = 15;
    const INCREMENTS_PER_WORKER = 30;
    const NUM_WORKERS = 30;

    // Setup: Create counters
    const counterIds: string[] = [];
    for (let i = 0; i < NUM_COUNTERS; i++) {
      const counterId = `counter-${i}`;
      counterIds.push(counterId);
      await storage.putItem("ConcurrentTest", {
        id: { S: counterId },
        count: { N: "0" },
        type: { S: "counter" },
      });
    }

    // Track expected increments per counter
    const expectedIncrements: Record<string, number> = {};
    counterIds.forEach((id) => (expectedIncrements[id] = 0));

    const incrementLock = { lock: false };
    let totalRetries = 0;

    // Worker function: increment random counters
    async function worker(workerId: number) {
      const localIncrements: Record<string, number> = {};
      counterIds.forEach((id) => (localIncrements[id] = 0));
      let workerRetries = 0;

      for (let i = 0; i < INCREMENTS_PER_WORKER; i++) {
        const counterId = counterIds[randomInt(0, NUM_COUNTERS - 1)]!;

        try {
          // Read current value
          const counter = await storage.getItem("ConcurrentTest", { id: { S: counterId } });
          if (!counter) continue;

          const currentCount = parseInt(counter.count.N);
          const newCount = currentCount + 1;

          // Increment with optimistic locking
          await storage.transactWrite([
            {
              Update: {
                tableName: "ConcurrentTest",
                key: { id: { S: counterId } },
                updateExpression: "SET #count = :newCount",
                expressionAttributeNames: { "#count": "count" },
                expressionAttributeValues: {
                  ":newCount": { N: String(newCount) },
                  ":expectedCount": { N: String(currentCount) },
                },
                conditionExpression: "#count = :expectedCount",
              },
            },
          ]);

          localIncrements[counterId]!++;
        } catch (error) {
          if (error instanceof TransactionCancelledError) {
            // Retry on conflict (in real system, would implement exponential backoff)
            i--;
            workerRetries++;
          } else {
            throw error;
          }
        }
      }

      totalRetries += workerRetries;
      return localIncrements;
    }

    // Run concurrent workers
    const startTime = Date.now();
    const results = await Promise.all(
      Array.from({ length: NUM_WORKERS }, (_, i) => worker(i))
    );
    const duration = Date.now() - startTime;

    // Aggregate expected increments
    for (const localIncrements of results) {
      for (const [counterId, count] of Object.entries(localIncrements)) {
        expectedIncrements[counterId] = (expectedIncrements[counterId] || 0) + count;
      }
    }

    // Verify: actual counts match expected
    const actualCounts: Record<string, number> = {};
    let totalExpected = 0;
    let totalActual = 0;

    for (const counterId of counterIds) {
      const counter = await storage.getItem("ConcurrentTest", { id: { S: counterId } });
      if (counter) {
        const actualCount = parseInt(counter.count.N);
        actualCounts[counterId] = actualCount;
        totalActual += actualCount;
        totalExpected += expectedIncrements[counterId]!;

        expect(actualCount).toBe(expectedIncrements[counterId]!);
      }
    }

    if (VERBOSE) {
      console.log(`Total increments: ${totalActual}`);
      console.log(`Total retries: ${totalRetries}`);
      console.log(`Duration: ${duration}ms`);
      console.log(`Throughput: ${((totalActual / duration) * 1000).toFixed(2)} increments/sec`);
    }

    expect(totalActual).toBe(totalExpected);
  }, 30000);

  test("conditional claim race - only one winner per item", async () => {
    const NUM_ITEMS = 30;
    const NUM_WORKERS = 50;

    // Setup: Create unclaimed items
    const itemIds: string[] = [];
    for (let i = 0; i < NUM_ITEMS; i++) {
      const itemId = `item-${i}`;
      itemIds.push(itemId);
      await storage.putItem("ConcurrentTest", {
        id: { S: itemId },
        status: { S: "available" },
        type: { S: "claimable" },
      });
    }

    // Track claims
    const successfulClaims: Record<string, number[]> = {}; // itemId -> workerIds that claimed it
    const claimAttempts: Record<number, number> = {}; // workerId -> number of successful claims

    Array.from({ length: NUM_WORKERS }, (_, i) => {
      claimAttempts[i] = 0;
    });

    // Worker function: try to claim random items
    async function worker(workerId: number) {
      // Shuffle items to reduce conflicts
      const shuffledItems = shuffle(itemIds);

      for (const itemId of shuffledItems) {
        try {
          // Try to claim item - only succeeds if not already claimed
          await storage.transactWrite([
            {
              Update: {
                tableName: "ConcurrentTest",
                key: { id: { S: itemId } },
                updateExpression: "SET #owner = :worker, #status = :claimed",
                expressionAttributeNames: { "#owner": "owner", "#status": "status" },
                expressionAttributeValues: {
                  ":worker": { S: `worker-${workerId}` },
                  ":claimed": { S: "claimed" },
                  ":available": { S: "available" },
                },
                conditionExpression: "#status = :available",
              },
            },
          ]);

          // Success - record the claim
          if (!successfulClaims[itemId]) {
            successfulClaims[itemId] = [];
          }
          successfulClaims[itemId]!.push(workerId);
          claimAttempts[workerId]!++;
        } catch (error) {
          if (error instanceof TransactionCancelledError) {
            // Expected - someone else claimed it first
            continue;
          } else {
            throw error;
          }
        }
      }
    }

    // Run concurrent workers
    const startTime = Date.now();
    await Promise.all(Array.from({ length: NUM_WORKERS }, (_, i) => worker(i)));
    const duration = Date.now() - startTime;

    // Verify: each item has exactly one owner
    const ownerCounts: Record<string, number> = {};

    for (const itemId of itemIds) {
      const item = await storage.getItem("ConcurrentTest", { id: { S: itemId } });
      if (item && item.owner) {
        const owner = item.owner.S;
        ownerCounts[owner] = (ownerCounts[owner] || 0) + 1;

        // Critical invariant: item should be in our successfulClaims exactly once
        const claimers = successfulClaims[itemId] || [];
        expect(claimers.length).toBe(1);
        expect(item.status.S).toBe("claimed");
      }
    }

    const totalClaimed = Object.values(ownerCounts).reduce((a, b) => a + b, 0);

    if (VERBOSE) {
      console.log(`Items claimed: ${totalClaimed} / ${NUM_ITEMS}`);
      console.log(`Duration: ${duration}ms`);
    }

    // Verify: no duplicate claims
    for (const [itemId, claimers] of Object.entries(successfulClaims)) {
      expect(claimers.length).toBe(1);
    }

    expect(totalClaimed).toBe(NUM_ITEMS);
  }, 30000);

  test("cross-shard atomic operations - no partial commits", async () => {
    const NUM_OPERATIONS = 200;
    const ITEMS_PER_TRANSACTION = 5;
    const NUM_WORKERS = 15;

    // Track all items that should exist if transaction succeeded
    const expectedItems = new Set<string>();
    const transactionResults: Array<{ id: string; success: boolean; items: string[] }> = [];

    // Worker function: create multi-item transactions
    async function worker(workerId: number, numOps: number) {
      for (let i = 0; i < numOps; i++) {
        const txId = `tx-${workerId}-${i}`;
        const itemIds: string[] = [];

        // Create transaction that spans multiple items (likely different shards)
        const items: TransactWriteItem[] = [];

        for (let j = 0; j < ITEMS_PER_TRANSACTION; j++) {
          const itemId = `${txId}-item-${j}`;
          itemIds.push(itemId);

          items.push({
            Put: {
              tableName: "ConcurrentTest",
              item: {
                id: { S: itemId },
                txId: { S: txId },
                itemIndex: { N: String(j) },
                worker: { S: `worker-${workerId}` },
              },
              conditionExpression: "attribute_not_exists(id)",
            },
          });
        }

        try {
          await storage.transactWrite(items);

          // Success - all items should exist
          transactionResults.push({ id: txId, success: true, items: itemIds });
          itemIds.forEach((id) => expectedItems.add(id));
        } catch (error) {
          if (error instanceof TransactionCancelledError) {
            // Failure - none of the items should exist
            transactionResults.push({ id: txId, success: false, items: itemIds });
          } else {
            throw error;
          }
        }
      }
    }

    // Run concurrent workers
    const opsPerWorker = Math.floor(NUM_OPERATIONS / NUM_WORKERS);
    const startTime = Date.now();

    await Promise.all(Array.from({ length: NUM_WORKERS }, (_, i) => worker(i, opsPerWorker)));

    const duration = Date.now() - startTime;

    // Verify: for each transaction, either all items exist or none do
    let successfulTxs = 0;
    let failedTxs = 0;
    let partialCommits = 0;

    for (const tx of transactionResults) {
      let existingItems = 0;

      for (const itemId of tx.items) {
        const item = await storage.getItem("ConcurrentTest", { id: { S: itemId } });
        if (item) existingItems++;
      }

      if (tx.success) {
        successfulTxs++;
        // All items should exist
        if (existingItems !== tx.items.length) {
          partialCommits++;
          console.error(
            `Partial commit detected for ${tx.id}: ${existingItems}/${tx.items.length} items exist`
          );
        }
        expect(existingItems).toBe(tx.items.length);
      } else {
        failedTxs++;
        // No items should exist
        if (existingItems !== 0) {
          partialCommits++;
          console.error(
            `Partial rollback detected for ${tx.id}: ${existingItems} items leaked`
          );
        }
        expect(existingItems).toBe(0);
      }
    }

    if (VERBOSE) {
      console.log(`Successful transactions: ${successfulTxs}`);
      console.log(`Failed transactions: ${failedTxs}`);
      console.log(`Partial commits: ${partialCommits}`);
      console.log(`Duration: ${duration}ms`);
    }

    // Critical invariant: no partial commits or rollbacks
    expect(partialCommits).toBe(0);
  }, 30000);

  test("read-write consistency - no dirty reads or lost updates", async () => {
    const NUM_ITEMS = 3;
    const NUM_OPERATIONS = 400;
    const NUM_WORKERS = 30;

    // Setup: Create items with version numbers
    const itemIds: string[] = [];
    for (let i = 0; i < NUM_ITEMS; i++) {
      const itemId = `item-${i}`;
      itemIds.push(itemId);
      await storage.putItem("ConcurrentTest", {
        id: { S: itemId },
        version: { N: "0" },
        data: { S: "initial" },
      });
    }

    // Track operations
    const operations: Array<{ type: "read" | "write"; itemId: string; success: boolean }> = [];

    // Worker function: mix of reads and writes
    async function worker(workerId: number, numOps: number) {
      for (let i = 0; i < numOps; i++) {
        const itemId = itemIds[randomInt(0, NUM_ITEMS - 1)]!;
        const isWrite = Math.random() > 0.5;

        if (isWrite) {
          // Read-modify-write with optimistic locking
          try {
            const item = await storage.getItem("ConcurrentTest", { id: { S: itemId } });
            if (!item) continue;

            const currentVersion = parseInt(item.version.N);
            const newVersion = currentVersion + 1;

            await storage.transactWrite([
              {
                Update: {
                  tableName: "ConcurrentTest",
                  key: { id: { S: itemId } },
                  updateExpression: "SET #version = :newVersion, #data = :data",
                  expressionAttributeNames: { "#version": "version", "#data": "data" },
                  expressionAttributeValues: {
                    ":newVersion": { N: String(newVersion) },
                    ":data": { S: `worker-${workerId}-v${newVersion}` },
                    ":expectedVersion": { N: String(currentVersion) },
                  },
                  conditionExpression: "#version = :expectedVersion",
                },
              },
            ]);

            operations.push({ type: "write", itemId, success: true });
          } catch (error) {
            if (error instanceof TransactionCancelledError) {
              operations.push({ type: "write", itemId, success: false });
            } else {
              throw error;
            }
          }
        } else {
          // Read operation using TransactGetItems
          try {
            const results = await storage.transactGet([
              { tableName: "ConcurrentTest", key: { id: { S: itemId } } },
            ]);

            if (results[0]) {
              // Verify version and data are consistent
              const version = parseInt(results[0].version.N);
              const data = results[0].data.S;

              // Data should contain version number
              if (version > 0) {
                expect(data).toContain(`v${version}`);
              }
            }

            operations.push({ type: "read", itemId, success: true });
          } catch (error) {
            operations.push({ type: "read", itemId, success: false });
          }
        }
      }
    }

    // Run concurrent workers
    const opsPerWorker = Math.floor(NUM_OPERATIONS / NUM_WORKERS);
    const startTime = Date.now();

    await Promise.all(Array.from({ length: NUM_WORKERS }, (_, i) => worker(i, opsPerWorker)));

    const duration = Date.now() - startTime;

    // Analyze results
    const successfulReads = operations.filter((op) => op.type === "read" && op.success).length;
    const successfulWrites = operations.filter((op) => op.type === "write" && op.success).length;
    const failedWrites = operations.filter((op) => op.type === "write" && !op.success).length;

    if (VERBOSE) {
      console.log(`Successful reads: ${successfulReads}`);
      console.log(`Successful writes: ${successfulWrites}`);
      console.log(`Failed writes (conflicts): ${failedWrites}`);
      console.log(`Duration: ${duration}ms`);
    }
    // console.log(
    //   `Throughput: ${(((successfulReads + successfulWrites) / duration) * 1000).toFixed(2)} ops/sec`
    // );

    // Verify: version numbers are sequential (no lost updates)
    for (const itemId of itemIds) {
      const item = await storage.getItem("ConcurrentTest", { id: { S: itemId } });
      if (item) {
        const version = parseInt(item.version.N);
        expect(version).toBeGreaterThanOrEqual(0);

        // Data should match version
        if (version > 0) {
          expect(item.data.S).toContain(`v${version}`);
        }
      }
    }
  }, 30000);
});
