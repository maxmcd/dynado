// DynamoDB-compatible server with sharded SQLite storage

import { getConfigFromEnv } from './config.ts'
import { ShardedSQLiteStorage } from './storage-sqlite.ts'
import type {
  StorageBackend,
  DynamoDBItem,
  TableSchema,
  TransactWriteItem,
  TransactGetItem,
} from './storage.ts'
import type {
  AttributeValue,
  BatchGetItemCommandInput,
  BatchWriteItemCommandInput,
  CreateTableCommandInput,
  DeleteItemCommandInput,
  DeleteTableCommandInput,
  DescribeTableCommandInput,
  GetItemCommandInput,
  KeysAndAttributes,
  ListTablesCommandInput,
  PutItemCommandInput,
  QueryCommandInput,
  ScanCommandInput,
  TransactGetItemsCommandInput,
  TransactWriteItemsCommandInput,
  UpdateItemCommandInput,
  WriteRequest,
} from '@aws-sdk/client-dynamodb'
import { TransactionCancelledError } from './storage.ts'
import * as fs from 'fs/promises'
import CRC32 from 'crc-32'
import { evaluateKeyCondition } from './expression-parser/key-condition-evaluator.ts'
import { applyUpdateExpressionToItem } from './expression-parser/index.ts'

export class DB {
  storage: StorageBackend
  server: Bun.Server<undefined>
  config: ReturnType<typeof getConfigFromEnv>

  constructor() {
    this.config = getConfigFromEnv()
    this.storage = new ShardedSQLiteStorage({
      shardCount: this.config.shardCount,
      dataDir: this.config.dataDir,
    })
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
          response = await this.handleCreateTable(body as CreateTableCommandInput)
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
      const catchBody = JSON.stringify({
        __type: error instanceof Error ? error.name : 'InternalFailure',
        message: error instanceof Error ? error.message : 'Unknown error',
      })
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
    const tableNames = await this.storage.listTables()
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

    await this.storage.createTable({
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
    const { TableName, Item } = body

    if (!TableName || !Item) {
      throw {
        name: 'ValidationException',
        message: 'TableName and Item are required',
      }
    }

    await this.storage.putItem(TableName, Item)

    return {}
  }

  async handleGetItem(body: GetItemCommandInput) {
    const { TableName, Key } = body

    if (!TableName || !Key) {
      throw { name: 'ValidationException', message: 'TableName and Key are required' }
    }

    const item = await this.storage.getItem(TableName, Key)

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

    const table = await this.storage.describeTable(TableName)
    if (!table) {
      throw { name: 'ResourceNotFoundException', message: 'Table not found' }
    }

    const itemCount = await this.storage.getTableItemCount(TableName)

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
      ReturnValues,
    } = body

    if (!TableName || !Key) {
      throw {
        name: 'ValidationException',
        message: 'TableName and Key are required',
      }
    }

    const table = await this.storage.describeTable(TableName)
    if (!table) {
      throw { name: 'ResourceNotFoundException', message: 'Table not found' }
    }

    const oldItem = await this.storage.getItem(TableName, Key)
    let item: DynamoDBItem = oldItem ? { ...oldItem } : { ...Key }

    if (UpdateExpression) {
      item = applyUpdateExpressionToItem(
        item,
        UpdateExpression,
        ExpressionAttributeNames,
        ExpressionAttributeValues ?? undefined
      )
    }

    await this.storage.putItem(TableName, item)

    switch (ReturnValues) {
      case 'ALL_OLD':
      case 'UPDATED_OLD':
        return { Attributes: oldItem || {} }
      case 'ALL_NEW':
      case 'UPDATED_NEW':
        return { Attributes: item }
      default:
        return { Attributes: item }
    }
  }

  async handleDeleteItem(body: DeleteItemCommandInput) {
    const { TableName, Key, ReturnValues } = body

    if (!TableName || !Key) {
      throw {
        name: 'ValidationException',
        message: 'TableName and Key are required',
      }
    }

    const item = await this.storage.deleteItem(TableName, Key)

    if (item) {
      // Return attributes by default (ALL_OLD behavior)
      return { Attributes: item }
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

    const table = await this.storage.describeTable(TableName)
    if (!table) {
      throw { name: 'ResourceNotFoundException', message: 'Table not found' }
    }

    const scanResult = await this.storage.scan(
      TableName,
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
        lastEvaluatedKey = extractKey(table, lastItem)
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

    const table = await this.storage.describeTable(TableName)
    if (!table) {
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

    const queryResult = await this.storage.query(
      TableName,
      keyCondition,
      undefined,
      ExclusiveStartKey
    )
    let items = queryResult.items

    // Sort items by sort key based on ScanIndexForward
    if (table.keySchema.length > 1 && items.length > 0) {
      const sortKeyElement = table.keySchema[1]
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
        lastEvaluatedKey = extractKey(table, lastItem)
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

    await this.storage.deleteTable(TableName)
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
      const items = await this.storage.batchGet(tableName, keys)
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

      await this.storage.batchWrite(tableName, puts, deletes)
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

    if (TransactItems.length > 25) {
      throw {
        name: 'ValidationException',
        message: 'Transaction cannot contain more than 25 items',
      }
    }

    const items: TransactWriteItem[] = TransactItems.map((item) => {
      if (item.Put) {
        const { TableName, Item, ConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues, ReturnValuesOnConditionCheckFailure } =
          item.Put
        if (!TableName || !Item) {
          throw {
            name: 'ValidationException',
            message: 'Put requests require TableName and Item',
          }
        }
        return {
          Put: {
            tableName: TableName,
            item: Item,
            conditionExpression: ConditionExpression,
            expressionAttributeNames: ExpressionAttributeNames,
            expressionAttributeValues: ExpressionAttributeValues,
            returnValuesOnConditionCheckFailure:
              ReturnValuesOnConditionCheckFailure,
          },
        }
      }
      if (item.Update) {
        const {
          TableName,
          Key,
          UpdateExpression,
          ConditionExpression,
          ExpressionAttributeNames,
          ExpressionAttributeValues,
          ReturnValuesOnConditionCheckFailure,
        } = item.Update
        if (!TableName || !Key || !UpdateExpression) {
          throw {
            name: 'ValidationException',
            message: 'Update requests require TableName, Key, and UpdateExpression',
          }
        }
        return {
          Update: {
            tableName: TableName,
            key: Key,
            updateExpression: UpdateExpression,
            conditionExpression: ConditionExpression,
            expressionAttributeNames: ExpressionAttributeNames,
            expressionAttributeValues: ExpressionAttributeValues,
            returnValuesOnConditionCheckFailure:
              ReturnValuesOnConditionCheckFailure,
          },
        }
      }
      if (item.Delete) {
        const {
          TableName,
          Key,
          ConditionExpression,
          ExpressionAttributeNames,
          ExpressionAttributeValues,
          ReturnValuesOnConditionCheckFailure,
        } = item.Delete
        if (!TableName || !Key) {
          throw {
            name: 'ValidationException',
            message: 'Delete requests require TableName and Key',
          }
        }
        return {
          Delete: {
            tableName: TableName,
            key: Key,
            conditionExpression: ConditionExpression,
            expressionAttributeNames: ExpressionAttributeNames,
            expressionAttributeValues: ExpressionAttributeValues,
            returnValuesOnConditionCheckFailure:
              ReturnValuesOnConditionCheckFailure,
          },
        }
      }
      if (item.ConditionCheck) {
        const {
          TableName,
          Key,
          ConditionExpression,
          ExpressionAttributeNames,
          ExpressionAttributeValues,
          ReturnValuesOnConditionCheckFailure,
        } = item.ConditionCheck
        if (!TableName || !Key || !ConditionExpression) {
          throw {
            name: 'ValidationException',
            message:
              'ConditionCheck requests require TableName, Key, and ConditionExpression',
          }
        }
        return {
          ConditionCheck: {
            tableName: TableName,
            key: Key,
            conditionExpression: ConditionExpression,
            expressionAttributeNames: ExpressionAttributeNames,
            expressionAttributeValues: ExpressionAttributeValues,
            returnValuesOnConditionCheckFailure:
              ReturnValuesOnConditionCheckFailure,
          },
        }
      }
      throw {
        name: 'ValidationException',
        message: 'Invalid transaction item',
      }
    })

    try {
      await this.storage.transactWrite(items, ClientRequestToken)
      return {}
    } catch (error: unknown) {
      if (error instanceof TransactionCancelledError) {
        throw {
          name: 'TransactionCanceledException',
          message: error.message,
          CancellationReasons: error.cancellationReasons,
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

    if (TransactItems.length > 25) {
      throw {
        name: 'ValidationException',
        message: 'Transaction cannot contain more than 25 items',
      }
    }

    const items: TransactGetItem[] = TransactItems.map((item) => {
      if (!item.Get) {
        throw { name: 'ValidationException', message: 'Get is required' }
      }
      const {
        TableName,
        Key,
        ProjectionExpression,
        ExpressionAttributeNames,
      } = item.Get
      if (!TableName || !Key) {
        throw {
          name: 'ValidationException',
          message: 'Get requests require TableName and Key',
        }
      }

      return {
        tableName: TableName,
        key: Key,
        projectionExpression: ProjectionExpression,
        expressionAttributeNames: ExpressionAttributeNames,
      }
    })

    const results = await this.storage.transactGet(items)

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
          ? expressionAttributeNames[eqMatch[1]] ?? eqMatch[1]
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
          ? expressionAttributeNames[gtMatch[1]] ?? gtMatch[1]
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
function extractKey(table: TableSchema, item: DynamoDBItem): DynamoDBItem {
  const key: DynamoDBItem = {} as DynamoDBItem
  for (const keySchema of table.keySchema) {
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
