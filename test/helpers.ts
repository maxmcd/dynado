// Global test setup that starts a single DB instance shared across all test files
import {
  DynamoDBClient,
  CreateTableCommand,
  DeleteTableCommand,
  PutItemCommand,
} from '@aws-sdk/client-dynamodb'
import type {
  AttributeValue,
  CreateTableCommandInput,
} from '@aws-sdk/client-dynamodb'
import { GenericContainer, Wait } from 'testcontainers'
import { DB } from '../src/index.ts'
import { createConfig } from '../src/config.ts'
import * as fs from 'fs/promises'
import * as os from 'os'
import * as path from 'path'

let tableCounter = 0
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
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'dynado-test-'))

    const db = new DB(createConfig({ port: 0, dataDir: tmpDir }))
    const port = db.server.port

    return {
      endpoint: `http://localhost:${port}`,
      cleanup: async () => {
        await db.server.stop()
        await fs.rm(tmpDir, { recursive: true })
      },
    }
  }
}

/**
 * Generates a globally unique table name using a shared counter.
 */
export function uniqueTableName(prefix: string = 'Test'): string {
  return `${prefix}_${Date.now()}_${tableCounter++}`
}

/**
 * Tracks a table name inside the provided array and returns it.
 */
export function trackTable(tables: string[], tableName: string): string {
  tables.push(tableName)
  return tableName
}

type CreateTableOptions = Omit<
  CreateTableCommandInput,
  'TableName' | 'KeySchema' | 'AttributeDefinitions' | 'BillingMode'
> & {
  tableName?: string
  keySchema?: NonNullable<CreateTableCommandInput['KeySchema']>
  attributeDefinitions?: NonNullable<
    CreateTableCommandInput['AttributeDefinitions']
  >
  billingMode?: CreateTableCommandInput['BillingMode']
}

/**
 * Creates a DynamoDB table with sensible defaults for tests.
 */
export type { CreateTableOptions }

/**
 * Creates a DynamoDB table with sensible defaults for tests.
 */
export async function createTable(
  client: DynamoDBClient,
  tableNameOrOptions?: string | CreateTableOptions,
  maybeOptions: CreateTableOptions = {}
): Promise<string> {
  const options =
    typeof tableNameOrOptions === 'string'
      ? { ...maybeOptions, tableName: tableNameOrOptions }
      : (tableNameOrOptions ?? {})

  const {
    tableName = uniqueTableName(),
    keySchema = [{ AttributeName: 'id', KeyType: 'HASH' }],
    attributeDefinitions = [{ AttributeName: 'id', AttributeType: 'S' }],
    billingMode = 'PAY_PER_REQUEST',
    ...rest
  } = options

  await client.send(
    new CreateTableCommand({
      TableName: tableName,
      KeySchema: keySchema,
      AttributeDefinitions: attributeDefinitions,
      BillingMode: billingMode,
      ...rest,
    })
  )

  return tableName
}

/**
 * Converts friendly JS objects into DynamoDB AttributeValues.
 * Allows tests to pass either raw AttributeValues or plain primitives.
 */
function normalizeItem(
  item: Record<string, any>
): Record<string, AttributeValue> {
  const result: Record<string, AttributeValue> = {}
  for (const [key, value] of Object.entries(item)) {
    result[key] = toAttributeValue(value)
  }
  return result
}

function isAttributeValue(value: any): value is AttributeValue {
  if (!value || typeof value !== 'object') return false
  const attributeKeys = [
    'S',
    'N',
    'B',
    'BOOL',
    'NULL',
    'SS',
    'NS',
    'BS',
    'M',
    'L',
  ]
  return attributeKeys.some((key) => key in value)
}

function toAttributeValue(value: any): AttributeValue {
  if (isAttributeValue(value)) {
    return value
  }
  if (typeof value === 'string') {
    return { S: value }
  }
  if (typeof value === 'number') {
    return { N: String(value) }
  }
  if (typeof value === 'boolean') {
    return { BOOL: value }
  }
  if (value === null) {
    return { NULL: true }
  }
  if (Array.isArray(value)) {
    return { L: value.map((entry) => toAttributeValue(entry)) }
  }
  if (value instanceof Uint8Array) {
    return { B: value }
  }
  if (typeof value === 'object') {
    const map: Record<string, AttributeValue> = {}
    for (const [key, nestedValue] of Object.entries(value)) {
      map[key] = toAttributeValue(nestedValue)
    }
    return { M: map }
  }
  throw new Error(`Unsupported attribute value type: ${typeof value}`)
}

type CreateTableWithItemsOptions = CreateTableOptions & {
  tableName?: string
}

/**
 * Creates a table (with custom options if needed) and seeds it with items.
 */
export async function createTableWithItems(
  client: DynamoDBClient,
  tableNameOrItems: string | Array<Record<string, any>>,
  itemsOrOptions?: Array<Record<string, any>> | CreateTableWithItemsOptions,
  maybeOptions: CreateTableWithItemsOptions = {}
): Promise<string> {
  let items: Array<Record<string, any>>
  let options: CreateTableWithItemsOptions

  if (typeof tableNameOrItems === 'string') {
    items = Array.isArray(itemsOrOptions) ? itemsOrOptions : []
    options = { ...maybeOptions, tableName: tableNameOrItems }
  } else {
    items = tableNameOrItems
    options = (itemsOrOptions as CreateTableWithItemsOptions | undefined) ?? {}
  }

  const resolvedTableName = await createTable(client, options)

  for (const item of items) {
    await client.send(
      new PutItemCommand({
        TableName: resolvedTableName,
        Item: normalizeItem(item),
      })
    )
  }

  return resolvedTableName
}

/**
 * Best-effort table deletion helper for tests.
 */
export async function deleteTestTable(
  client: DynamoDBClient,
  tableName: string
): Promise<void> {
  try {
    await client.send(new DeleteTableCommand({ TableName: tableName }))
  } catch {
    // Ignore failures (table may already be gone)
  }
}

/**
 * Deletes every table listed in-place and clears the tracking array.
 */
export async function cleanupTables(
  client: DynamoDBClient,
  tableNames: string[]
): Promise<void> {
  for (const tableName of tableNames) {
    await deleteTestTable(client, tableName)
  }
  tableNames.length = 0
}
