// Global test setup that starts a single DB instance shared across all test files
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { startTestDB } from "./test-db-setup.ts";
import type { TestDBSetup } from "./test-db-setup.ts";

let globalTestDB: TestDBSetup | null = null;
let globalClient: DynamoDBClient | null = null;
let initPromise: Promise<void> | null = null;
let refCount = 0;

/**
 * Gets or creates the global test DB and client.
 * Call this in beforeAll() of each test file.
 * Uses a lock to ensure only one initialization happens even when called concurrently.
 */
export async function getGlobalTestDB(): Promise<{
  client: DynamoDBClient;
  endpoint: string;
}> {
  // If already initialized, return immediately
  if (globalTestDB && globalClient) {
    return {
      client: globalClient,
      endpoint: globalTestDB.endpoint,
    };
  }

  // If initialization is in progress, wait for it
  if (initPromise) {
    await initPromise;
    return {
      client: globalClient!,
      endpoint: globalTestDB!.endpoint,
    };
  }

  // Start initialization
  initPromise = (async () => {
    console.log("Starting global test DB...");
    globalTestDB = await startTestDB();
    globalClient = new DynamoDBClient({
      endpoint: globalTestDB.endpoint,
      region: "local",
      credentials: {
        accessKeyId: "test",
        secretAccessKey: "test",
      },
    });
  })();

  await initPromise;
  refCount++;

  return {
    client: globalClient!,
    endpoint: globalTestDB!.endpoint,
  };
}

/**
 * Cleans up the global test DB.
 * Uses reference counting - only cleans up when all test files are done.
 * Call this in afterAll() of each test file.
 */
export async function cleanupGlobalTestDB(): Promise<void> {
  refCount--;
  if (refCount === 0 && globalTestDB) {
    console.log("Cleaning up global test DB...");
    await globalTestDB.cleanup();
    globalTestDB = null;
    globalClient = null;
    initPromise = null;
  }
}

// Cleanup on process exit
process.on("exit", () => {
  if (globalTestDB) {
    console.log("Emergency cleanup of test DB on exit");
  }
});
