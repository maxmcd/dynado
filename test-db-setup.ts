// Helper to conditionally start either dynado or DynamoDB Local for testing

import { GenericContainer, Wait } from "testcontainers";
import type { StartedTestContainer } from "testcontainers";
import { DB } from "./index.ts";
import { HttpWaitStrategy } from "testcontainers/build/wait-strategies/http-wait-strategy";

export interface TestDBSetup {
  endpoint: string;
  cleanup: () => Promise<void>;
}

/**
 * Starts a DynamoDB-compatible server for testing.
 * - If TEST_DYNAMODB_LOCAL=true: starts DynamoDB Local in Docker
 * - Otherwise: starts the dynado server
 */
export async function startTestDB(): Promise<TestDBSetup> {
  const useDynamoDBLocal = process.env.TEST_DYNAMODB_LOCAL === "true";

  if (useDynamoDBLocal) {
    console.log("Starting DynamoDB Local container for testing...");

    const container = await new GenericContainer("amazon/dynamodb-local:3.1.0")
      .withExposedPorts(8000)
      .withEntrypoint(["java", "-Djava.library.path=./DynamoDBLocal_lib"])
      .withCommand(["-jar", "DynamoDBLocal.jar"])
      .withWaitStrategy(Wait.forHttp("/", 8000).forStatusCode(400))
      .start();

    const host = container.getHost();
    const port = container.getMappedPort(8000);
    const endpoint = `http://${host}:${port}`;

    console.log(`DynamoDB Local started at ${endpoint}`);

    // Give it a moment to fully initialize
    await new Promise((resolve) => setTimeout(resolve, 2000));

    return {
      endpoint,
      cleanup: async () => {
        console.log("Stopping DynamoDB Local container...");
        await container.stop();
      },
    };
  } else {
    // Start dynado server
    console.log("Starting dynado server for testing...");
    const db = new DB();
    await import("./index.ts");

    // Give the server a moment to start
    await new Promise((resolve) => setTimeout(resolve, 100));

    return {
      endpoint: "http://localhost:8000",
      cleanup: async () => {
        await db.deleteAllData();
      },
    };
  }
}
