// Global test setup that starts a single DB instance shared across all test files
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { GenericContainer, Wait } from 'testcontainers'
import { DB } from '../src/index.ts'

let globalTestDB: TestDBSetup | null = null
let globalClient: DynamoDBClient | null = null
let initPromise: Promise<void> | null = null
let refCount = 0

/**
 * Gets or creates the global test DB and client.
 * Call this in beforeAll() of each test file.
 * Uses a lock to ensure only one initialization happens even when called concurrently.
 */
export async function getGlobalTestDB(): Promise<{
  client: DynamoDBClient
  endpoint: string
}> {
  // If already initialized, return immediately
  if (globalTestDB && globalClient) {
    return {
      client: globalClient,
      endpoint: globalTestDB.endpoint,
    }
  }

  // If initialization is in progress, wait for it
  if (initPromise) {
    await initPromise
    return {
      client: globalClient!,
      endpoint: globalTestDB!.endpoint,
    }
  }

  // Start initialization
  initPromise = (async () => {
    console.log('Starting global test DB...')
    globalTestDB = await startTestDB()
    globalClient = new DynamoDBClient({
      endpoint: globalTestDB.endpoint,
      region: 'local',
      credentials: {
        accessKeyId: 'test',
        secretAccessKey: 'test',
      },
    })
  })()

  await initPromise
  refCount++

  return {
    client: globalClient!,
    endpoint: globalTestDB!.endpoint,
  }
}

/**
 * Cleans up the global test DB.
 * Uses reference counting - only cleans up when all test files are done.
 * Call this in afterAll() of each test file.
 */
export async function cleanupGlobalTestDB(): Promise<void> {
  refCount--
  if (refCount === 0 && globalTestDB) {
    console.log('Cleaning up global test DB...')
    await globalTestDB.cleanup()
    globalTestDB = null
    globalClient = null
    initPromise = null
  }
}

// Cleanup on process exit
process.on('exit', () => {
  if (globalTestDB) {
    console.log('Emergency cleanup of test DB on exit')
  }
})

export interface TestDBSetup {
  endpoint: string
  cleanup: () => Promise<void>
}

/**
 * Starts a DynamoDB-compatible server for testing.
 * - If TEST_DYNAMODB_LOCAL=true: starts DynamoDB Local in Docker
 * - Otherwise: starts the dynado server
 */
export async function startTestDB(): Promise<TestDBSetup> {
  const useDynamoDBLocal = process.env.TEST_DYNAMODB_LOCAL === 'true'

  if (useDynamoDBLocal) {
    console.log('Starting DynamoDB Local container for testing...')

    const container = await new GenericContainer('amazon/dynamodb-local:3.1.0')
      .withExposedPorts(8000)
      .withEntrypoint(['java', '-Djava.library.path=./DynamoDBLocal_lib'])
      .withCommand(['-jar', 'DynamoDBLocal.jar'])
      .withWaitStrategy(Wait.forHttp('/', 8000).forStatusCode(400))
      .start()

    const host = container.getHost()
    const port = container.getMappedPort(8000)
    const endpoint = `http://${host}:${port}`

    console.log(`DynamoDB Local started at ${endpoint}`)

    // Give it a moment to fully initialize
    await new Promise((resolve) => setTimeout(resolve, 2000))

    return {
      endpoint,
      cleanup: async () => {
        console.log('Stopping DynamoDB Local container...')
        await container.stop()
      },
    }
  } else {
    // Start dynado server
    console.log('Starting dynado server for testing...')
    const db = new DB()

    return {
      endpoint: 'http://localhost:8000',
      cleanup: async () => {
        await db.deleteAllData()
      },
    }
  }
}
