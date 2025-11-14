// DynamoDB-compatible server with sharded SQLite storage

import { getConfigFromEnv, type Config } from './config.ts'
import {
  TransactionCanceledException,
  type AttributeValue,
  type BatchGetItemCommandInput,
  type BatchWriteItemCommandInput,
  type CreateTableCommandInput,
  type DeleteItemCommandInput,
  type DeleteTableCommandInput,
  type DescribeTableCommandInput,
  type GetItemCommandInput,
  type ListTablesCommandInput,
  type PutItemCommandInput,
  type QueryCommandInput,
  type ScanCommandInput,
  type TransactGetItem,
  type TransactGetItemsCommandInput,
  type TransactWriteItemsCommandInput,
  type UpdateItemCommandInput,
  type WriteRequest,
} from '@aws-sdk/client-dynamodb'
import * as fs from 'fs/promises'
import * as nodeFs from 'fs'
import CRC32 from 'crc-32'
import { evaluateKeyCondition } from './expression-parser/key-condition-evaluator.ts'
import {
  applyUpdateExpressionToItem,
  evaluateConditionExpression,
} from './expression-parser/index.ts'
import { Router } from './router.ts'
import { Shard } from './shard.ts'
import { MetadataStore } from './metadata-store.ts'
import { TransactionCoordinator } from './coordinator.ts'
import { type DynamoDBItem, type TableSchema } from './types.ts'

export const MAX_ITEMS_PER_TRANSACTION = 100

export class DB {
  server: Bun.Server<undefined>
  router: Router
  metadataStore: MetadataStore
  config: Config

  constructor(config?: Config) {
    this.config = config ?? getConfigFromEnv()
    // Create data directory if it doesn't exist
    if (!nodeFs.existsSync(this.config.dataDir)) {
      nodeFs.mkdirSync(this.config.dataDir, { recursive: true })
    }

    const shards: Shard[] = []

    // 1. Create shards
    for (let i = 0; i < this.config.shardCount; i++) {
      const shard = new Shard(`${this.config.dataDir}/shard_${i}.db`, i)
      shards.push(shard)
    }

    // 2. Create metadata store
    this.metadataStore = new MetadataStore(this.config.dataDir)
    // 3. Create transaction coordinator
    const coordinator = new TransactionCoordinator(this.config.dataDir)

    // 4. Create router that ties everything together
    this.router = new Router(shards, this.metadataStore, coordinator)
    this.server = Bun.serve({
      port: this.config.port,
      fetch: (req) => this.handleDynamoDBRequest(req),
    })
  }

  async deleteAllData() {
    await fs.rm(this.config.dataDir, { recursive: true })
  }

  async handleDynamoDBRequest(req: Request): Promise<Response> {
    const target = req.headers.get('x-amz-target')

    if (!target) {
      const body = JSON.stringify({
        __type: 'MissingAuthenticationTokenException',
      })
      const checksum = CRC32.str(body) >>> 0 // Convert to unsigned 32-bit
      return new Response(body, {
        status: 400,
        headers: {
          'Content-Type': 'application/x-amz-json-1.0',
          'X-Amz-Crc32': String(checksum),
        },
      })
    }

    const operation = target.split('.')[1]
    const body = (await req.json()) as unknown

    try {
      let response

      switch (operation) {
        case 'ListTables':
          response = await this.handleListTables(body as ListTablesCommandInput)
          break
        case 'CreateTable':
          response = await this.handleCreateTable(
            body as CreateTableCommandInput
          )
          break
        case 'DescribeTable':
          response = await this.handleDescribeTable(
            body as DescribeTableCommandInput
          )
          break
        case 'PutItem':
          response = await this.handlePutItem(body as PutItemCommandInput)
          break
        case 'GetItem':
          response = await this.handleGetItem(body as GetItemCommandInput)
          break
        case 'UpdateItem':
          response = await this.handleUpdateItem(body as UpdateItemCommandInput)
          break
        case 'DeleteItem':
          response = await this.handleDeleteItem(body as DeleteItemCommandInput)
          break
        case 'BatchGetItem':
          response = await this.handleBatchGetItem(
            body as BatchGetItemCommandInput
          )
          break
        case 'BatchWriteItem':
          response = await this.handleBatchWriteItem(
            body as BatchWriteItemCommandInput
          )
          break
        case 'Scan':
          response = await this.handleScan(body as ScanCommandInput)
          break
        case 'Query':
          response = await this.handleQuery(body as QueryCommandInput)
          break
        case 'DeleteTable':
          response = await this.handleDeleteTable(
            body as DeleteTableCommandInput
          )
          break
        case 'TransactWriteItems':
          response = await this.handleTransactWriteItems(
            body as TransactWriteItemsCommandInput
          )
          break
        case 'TransactGetItems':
          response = await this.handleTransactGetItems(
            body as TransactGetItemsCommandInput
          )
          break
        default:
          const errorBody = JSON.stringify({
            __type: 'UnknownOperationException',
          })
          const errorChecksum = CRC32.str(errorBody) >>> 0 // Convert to unsigned 32-bit
          return new Response(errorBody, {
            status: 400,
            headers: {
              'Content-Type': 'application/x-amz-json-1.0',
              'X-Amz-Crc32': String(errorChecksum),
            },
          })
      }

      const responseBody = JSON.stringify(response)
      const responseChecksum = CRC32.str(responseBody) >>> 0 // Convert to unsigned 32-bit
      return new Response(responseBody, {
        status: 200,
        headers: {
          'Content-Type': 'application/x-amz-json-1.0',
          'X-Amz-Crc32': String(responseChecksum),
        },
      })
    } catch (error: unknown) {
      const errorPayload = serializeError(error)
      const catchBody = JSON.stringify(errorPayload)
      const catchChecksum = CRC32.str(catchBody) >>> 0 // Convert to unsigned 32-bit
      return new Response(catchBody, {
        status: 400,
        headers: {
          'Content-Type': 'application/x-amz-json-1.0',
          'X-Amz-Crc32': String(catchChecksum),
        },
      })
    }
  }

  async handleListTables(_body: ListTablesCommandInput) {
    const tableNames = await this.metadataStore.listTables()
    return { TableNames: tableNames }
  }

  async handleCreateTable(body: CreateTableCommandInput) {
    const { TableName, KeySchema, AttributeDefinitions } = body

    if (!TableName || !KeySchema || !AttributeDefinitions) {
      throw {
        name: 'ValidationException',
        message: 'TableName, KeySchema, and AttributeDefinitions are required',
      }
    }

    await this.metadataStore.createTable({
      tableName: TableName,
      keySchema: KeySchema,
      attributeDefinitions: AttributeDefinitions,
    })

    return {
      TableDescription: {
        TableName,
        KeySchema,
        AttributeDefinitions,
        TableStatus: 'ACTIVE',
        CreationDateTime: Math.floor(Date.now() / 1000),
      },
    }
  }

  async handlePutItem(body: PutItemCommandInput) {
    const {
      TableName,
      Item,
      ConditionExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      ReturnValues,
    } = body

    if (!TableName || !Item) {
      throw {
        name: 'ValidationException',
        message: 'TableName and Item are required',
      }
    }

    const existingItem = await this.router.getItem(TableName, Item)
    assertConditionExpression(
      existingItem,
      ConditionExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues
    )
    await this.router.putItem(TableName, Item)

    if (ReturnValues === 'ALL_OLD') {
      return { Attributes: existingItem || {} }
    }

    return {}
  }

  async handleGetItem(body: GetItemCommandInput) {
    const { TableName, Key } = body

    if (!TableName || !Key) {
      throw {
        name: 'ValidationException',
        message: 'TableName and Key are required',
      }
    }

    const item = await this.router.getItem(TableName, Key)

    if (item) {
      return { Item: item }
    }
    return {}
  }

  async handleDescribeTable(body: DescribeTableCommandInput) {
    const { TableName } = body

    if (!TableName) {
      throw { name: 'ValidationException', message: 'TableName is required' }
    }

    const table = await this.metadataStore.describeTable(TableName)
    if (!table) {
      throw { name: 'ResourceNotFoundException', message: 'Table not found' }
    }

    const itemCount = await this.router.getTableItemCount(TableName)

    return {
      Table: {
        TableName: table.tableName,
        KeySchema: table.keySchema,
        AttributeDefinitions: table.attributeDefinitions,
        TableStatus: 'ACTIVE',
        CreationDateTime: Math.floor(Date.now() / 1000),
        ItemCount: itemCount,
      },
    }
  }

  async handleUpdateItem(body: UpdateItemCommandInput) {
    const {
      TableName,
      Key,
      UpdateExpression,
      ExpressionAttributeValues,
      ExpressionAttributeNames,
      ConditionExpression,
      ReturnValues,
    } = body

    if (!TableName || !Key) {
      throw {
        name: 'ValidationException',
        message: 'TableName and Key are required',
      }
    }

    // TODO: cache this?
    const table = await this.metadataStore.describeTable(TableName)
    if (!table) {
      throw { name: 'ResourceNotFoundException', message: 'Table not found' }
    }

    const oldItem = await this.router.getItem(TableName, Key)
    assertConditionExpression(
      oldItem,
      ConditionExpression,
      ExpressionAttributeNames ?? undefined,
      ExpressionAttributeValues ?? undefined
    )
    let item: DynamoDBItem = oldItem ? { ...oldItem } : { ...Key }

    if (UpdateExpression) {
      item = applyUpdateExpressionToItem(
        item,
        UpdateExpression,
        ExpressionAttributeNames,
        ExpressionAttributeValues ?? undefined
      )
    }

    await this.router.putItem(TableName, item)

    switch (ReturnValues) {
      case 'ALL_OLD':
      case 'UPDATED_OLD':
        return { Attributes: oldItem || {} }
      case 'ALL_NEW':
      case 'UPDATED_NEW':
        return { Attributes: item }
      case 'NONE':
      case undefined:
        return {}
      default:
        return {}
    }
  }

  async handleDeleteItem(body: DeleteItemCommandInput) {
    const {
      TableName,
      Key,
      ReturnValues,
      ConditionExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    } = body

    if (!TableName || !Key) {
      throw {
        name: 'ValidationException',
        message: 'TableName and Key are required',
      }
    }

    const existingItem = await this.router.getItem(TableName, Key)
    assertConditionExpression(
      existingItem,
      ConditionExpression,
      ExpressionAttributeNames ?? undefined,
      ExpressionAttributeValues ?? undefined
    )
    await this.router.deleteItem(TableName, Key)

    if (ReturnValues === 'ALL_OLD') {
      return { Attributes: existingItem || {} }
    }

    return {}
  }

  async handleScan(body: ScanCommandInput) {
    const {
      TableName,
      Limit,
      FilterExpression,
      ExpressionAttributeValues,
      ExpressionAttributeNames,
      ExclusiveStartKey,
    } = body

    if (!TableName) {
      throw { name: 'ValidationException', message: 'TableName is required' }
    }

    const schema = await this.metadataStore.describeTable(TableName)
    if (!schema) {
      throw { name: 'ResourceNotFoundException', message: 'Table not found' }
    }

    const scanResult = await this.router.scan(
      schema,
      undefined,
      ExclusiveStartKey
    )
    let items = scanResult.items
    const scannedCount = items.length

    // Apply FilterExpression
    if (FilterExpression) {
      items = applyFilterExpression(
        items,
        FilterExpression,
        ExpressionAttributeValues,
        ExpressionAttributeNames
      )
    }

    // Apply limit
    let lastEvaluatedKey
    if (Limit && items.length > Limit) {
      const limitedItems = items.slice(0, Limit)
      const lastItem = limitedItems[limitedItems.length - 1]
      if (lastItem) {
        lastEvaluatedKey = extractKey(schema, lastItem)
      }
      items = limitedItems
    }

    const result: {
      Items: DynamoDBItem[]
      Count: number
      ScannedCount: number
      LastEvaluatedKey?: DynamoDBItem
    } = {
      Items: items,
      Count: items.length,
      ScannedCount: scannedCount,
    }

    if (lastEvaluatedKey) {
      result.LastEvaluatedKey = lastEvaluatedKey
    }

    return result
  }

  async handleQuery(body: QueryCommandInput) {
    const {
      TableName,
      KeyConditionExpression,
      FilterExpression,
      ExpressionAttributeValues,
      ExpressionAttributeNames,
      Limit,
      ExclusiveStartKey,
      ScanIndexForward = true,
    } = body

    if (!TableName) {
      throw { name: 'ValidationException', message: 'TableName is required' }
    }

    const schema = await this.metadataStore.describeTable(TableName)
    if (!schema) {
      throw { name: 'ResourceNotFoundException', message: 'Table not found' }
    }

    // Build filter function from KeyConditionExpression using proper parser
    let keyCondition = (_item: DynamoDBItem) => true

    if (KeyConditionExpression) {
      keyCondition = (item: DynamoDBItem) => {
        return evaluateKeyCondition(
          item,
          KeyConditionExpression,
          ExpressionAttributeNames,
          ExpressionAttributeValues
        )
      }
    }

    const queryResult = await this.router.query(schema, keyCondition)
    let items = queryResult.items

    // Sort items by sort key based on ScanIndexForward
    if (schema.keySchema.length > 1 && items.length > 0) {
      const sortKeyElement = schema.keySchema[1]
      const sortKeyName = sortKeyElement?.AttributeName
      if (sortKeyName) {
        items.sort((a, b) => {
          const aVal = a[sortKeyName]
          const bVal = b[sortKeyName]

          // Handle string sort keys
          if (aVal?.S !== undefined && bVal?.S !== undefined) {
            const comparison = aVal.S.localeCompare(bVal.S)
            return ScanIndexForward ? comparison : -comparison
          }

          // Handle number sort keys
          if (aVal?.N !== undefined && bVal?.N !== undefined) {
            const comparison = parseFloat(aVal.N) - parseFloat(bVal.N)
            return ScanIndexForward ? comparison : -comparison
          }

          return 0
        })
      }
    }

    if (ExclusiveStartKey) {
      const exclusiveKeyString = getKeyString(ExclusiveStartKey)
      const startIndex = items.findIndex((item) => {
        const key = extractKey(schema, item)
        return getKeyString(key) === exclusiveKeyString
      })
      if (startIndex >= 0) {
        items = items.slice(startIndex + 1)
      }
    }

    const scannedCount = items.length

    // Apply FilterExpression
    if (FilterExpression) {
      items = applyFilterExpression(
        items,
        FilterExpression,
        ExpressionAttributeValues,
        ExpressionAttributeNames
      )
    }

    // Apply limit
    let lastEvaluatedKey: DynamoDBItem | undefined
    if (Limit && items.length > Limit) {
      const limitedItems = items.slice(0, Limit)
      const lastItem = limitedItems[limitedItems.length - 1]
      if (lastItem) {
        lastEvaluatedKey = extractKey(schema, lastItem)
      }
      items = limitedItems
    }

    return {
      Items: items,
      Count: items.length,
      ScannedCount: scannedCount,
      LastEvaluatedKey: lastEvaluatedKey,
    }
  }

  async handleDeleteTable(body: DeleteTableCommandInput) {
    const { TableName } = body

    if (!TableName) {
      throw { name: 'ValidationException', message: 'TableName is required' }
    }

    await this.metadataStore.deleteTable(TableName)
    // TODO: defer?
    await this.router.deleteAllTableItems(TableName)
    return { TableDescription: { TableName, TableStatus: 'DELETING' } }
  }

  async handleBatchGetItem(body: BatchGetItemCommandInput) {
    const { RequestItems } = body

    if (!RequestItems || Object.keys(RequestItems).length === 0) {
      throw {
        name: 'ValidationException',
        message: 'RequestItems must contain at least one entry',
      }
    }

    const responses: Record<string, DynamoDBItem[]> = {}

    for (const [tableName, request] of Object.entries(RequestItems)) {
      const keys = request.Keys ?? []
      const items = await this.router.batchGet(tableName, keys)
      responses[tableName] = items
    }

    return { Responses: responses }
  }

  async handleBatchWriteItem(body: BatchWriteItemCommandInput) {
    const { RequestItems } = body

    if (!RequestItems || Object.keys(RequestItems).length === 0) {
      throw {
        name: 'ValidationException',
        message: 'RequestItems must contain at least one entry',
      }
    }

    for (const [tableName, requests] of Object.entries(RequestItems)) {
      const puts: DynamoDBItem[] = []
      const deletes: DynamoDBItem[] = []

      for (const request of requests as WriteRequest[]) {
        if (request.PutRequest?.Item) {
          puts.push(request.PutRequest.Item)
        } else if (request.DeleteRequest?.Key) {
          deletes.push(request.DeleteRequest.Key)
        }
      }

      await this.router.batchWrite(tableName, puts, deletes)
    }

    return {}
  }

  async handleTransactWriteItems(body: TransactWriteItemsCommandInput) {
    const { TransactItems, ClientRequestToken } = body

    if (!TransactItems || TransactItems.length === 0) {
      throw {
        name: 'ValidationException',
        message: 'TransactItems is required',
      }
    }

    if (TransactItems.length > MAX_ITEMS_PER_TRANSACTION) {
      throw {
        name: 'ValidationException',
        message: 'Transaction cannot contain more than 100 items',
      }
    }

    try {
      await this.router.transactWrite(TransactItems, ClientRequestToken)
      return {}
    } catch (error: unknown) {
      if (error instanceof TransactionCanceledException) {
        throw {
          name: 'TransactionCanceledException',
          message: error.message,
          CancellationReasons: error.CancellationReasons,
        }
      }
      throw error
    }
  }

  async handleTransactGetItems(body: TransactGetItemsCommandInput) {
    const { TransactItems } = body

    if (!TransactItems || TransactItems.length === 0) {
      throw {
        name: 'ValidationException',
        message: 'TransactItems is required',
      }
    }

    if (TransactItems.length > MAX_ITEMS_PER_TRANSACTION) {
      throw {
        name: 'ValidationException',
        message: `Transaction cannot contain more than ${MAX_ITEMS_PER_TRANSACTION} items`,
      }
    }

    const results = await this.router.transactGet(TransactItems)

    return {
      Responses: results.map((item) => ({ Item: item })),
    }
  }
}

// Helper to apply FilterExpression
function applyFilterExpression(
  items: DynamoDBItem[],
  filterExpression: string,
  expressionAttributeValues?: Record<string, AttributeValue>,
  expressionAttributeNames?: Record<string, string>
): DynamoDBItem[] {
  return items.filter((item) => {
    // Simple filter for equality: "#attr = :value"
    const eqMatch = filterExpression.match(/([#\w]+)\s*=\s*(:\w+)/)
    if (eqMatch && eqMatch[1] && eqMatch[2]) {
      const resolvedName =
        eqMatch[1].startsWith('#') && expressionAttributeNames
          ? (expressionAttributeNames[eqMatch[1]] ?? eqMatch[1])
          : eqMatch[1]
      const valueRef = eqMatch[2]

      const filterValue = expressionAttributeValues?.[valueRef]
      const itemValue = resolvedName ? item[resolvedName] : undefined
      if (filterValue && itemValue !== undefined) {
        return JSON.stringify(itemValue) === JSON.stringify(filterValue)
      }
    }

    // Simple filter for greater than: "#attr > :value"
    const gtMatch = filterExpression.match(/([#\w]+)\s*>\s*(:\w+)/)
    if (gtMatch && gtMatch[1] && gtMatch[2]) {
      const resolvedName =
        gtMatch[1].startsWith('#') && expressionAttributeNames
          ? (expressionAttributeNames[gtMatch[1]] ?? gtMatch[1])
          : gtMatch[1]
      const valueRef = gtMatch[2]

      const filterValue = expressionAttributeValues?.[valueRef]
      const candidate = resolvedName ? item[resolvedName] : undefined
      if (filterValue?.N && candidate?.N) {
        return parseInt(candidate.N) > parseInt(filterValue.N)
      }
    }

    return true
  })
}

// Helper to extract key from item
function extractKey(schema: TableSchema, item: DynamoDBItem): DynamoDBItem {
  const key: DynamoDBItem = {} as DynamoDBItem
  for (const keySchema of schema.keySchema) {
    const attrName = keySchema.AttributeName
    if (!attrName) {
      throw new Error('Key schema entry missing AttributeName')
    }
    const value = item[attrName]
    if (value === undefined) {
      throw new Error(`Key attribute missing from item: ${attrName}`)
    }
    key[attrName] = value
  }
  return key
}

function getKeyString(key: DynamoDBItem): string {
  const keyAttrs = Object.keys(key).sort()
  return keyAttrs.map((attr) => JSON.stringify(key[attr])).join('#')
}

function assertConditionExpression(
  currentItem: DynamoDBItem | null,
  conditionExpression?: string,
  expressionAttributeNames?: Record<string, string>,
  expressionAttributeValues?: Record<string, AttributeValue>
): void {
  const passed = evaluateConditionExpression(
    currentItem,
    conditionExpression,
    expressionAttributeNames,
    expressionAttributeValues
  )
  if (!passed) {
    throw {
      name: 'ConditionalCheckFailedException',
      message: 'The conditional request failed',
    }
  }
}

function serializeError(error: unknown): Record<string, any> {
  if (error instanceof Error) {
    const payload: Record<string, any> = {
      __type: error.name || 'InternalFailure',
      message: error.message || 'Unknown error',
    }
    for (const key of Object.keys(error)) {
      if (key === 'name' || key === 'message') continue
      payload[key] = (error as any)[key]
    }
    return payload
  }

  if (typeof error === 'object' && error !== null) {
    const record = error as Record<string, any>
    const payload: Record<string, any> = {
      __type: typeof record.name === 'string' ? record.name : 'InternalFailure',
      message:
        typeof record.message === 'string' ? record.message : 'Unknown error',
    }
    for (const [key, value] of Object.entries(record)) {
      if (key === 'name' || key === 'message') continue
      payload[key] = value
    }
    return payload
  }

  return {
    __type: 'InternalFailure',
    message: 'Unknown error',
  }
}
