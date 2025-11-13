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
import { TransactionCancelledError } from './storage.ts'
import * as fs from 'fs/promises'
import CRC32 from 'crc-32'
import { evaluateKeyCondition } from './expression-parser/key-condition-evaluator.ts'

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
    const body = await req.json()

    try {
      let response

      switch (operation) {
        case 'ListTables':
          response = await this.handleListTables(body)
          break
        case 'CreateTable':
          response = await this.handleCreateTable(body)
          break
        case 'DescribeTable':
          response = await this.handleDescribeTable(body)
          break
        case 'PutItem':
          response = await this.handlePutItem(body)
          break
        case 'GetItem':
          response = await this.handleGetItem(body)
          break
        case 'UpdateItem':
          response = await this.handleUpdateItem(body)
          break
        case 'DeleteItem':
          response = await this.handleDeleteItem(body)
          break
        case 'BatchGetItem':
          response = await this.handleBatchGetItem(body)
          break
        case 'BatchWriteItem':
          response = await this.handleBatchWriteItem(body)
          break
        case 'Scan':
          response = await this.handleScan(body)
          break
        case 'Query':
          response = await this.handleQuery(body)
          break
        case 'DeleteTable':
          response = await this.handleDeleteTable(body)
          break
        case 'TransactWriteItems':
          response = await this.handleTransactWriteItems(body)
          break
        case 'TransactGetItems':
          response = await this.handleTransactGetItems(body)
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
    } catch (error: any) {
      const catchBody = JSON.stringify({
        __type: error.name,
        message: error.message,
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

  async handleListTables(body: any) {
    const tableNames = await this.storage.listTables()
    return { TableNames: tableNames }
  }

  async handleCreateTable(body: any) {
    const { TableName, KeySchema, AttributeDefinitions } = body

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

  async handlePutItem(body: any) {
    const { TableName, Item } = body

    await this.storage.putItem(TableName, Item)

    return {}
  }

  async handleGetItem(body: any) {
    const { TableName, Key } = body

    const item = await this.storage.getItem(TableName, Key)

    if (item) {
      return { Item: item }
    }
    return {}
  }

  async handleDescribeTable(body: any) {
    const { TableName } = body

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

  async handleUpdateItem(body: any) {
    const {
      TableName,
      Key,
      UpdateExpression,
      ExpressionAttributeValues,
      ExpressionAttributeNames,
      ReturnValues,
    } = body

    const table = await this.storage.describeTable(TableName)
    if (!table) {
      throw { name: 'ResourceNotFoundException', message: 'Table not found' }
    }

    const oldItem = await this.storage.getItem(TableName, Key)
    let item: DynamoDBItem = oldItem ? { ...oldItem } : { ...Key }

    // Enhanced UpdateExpression parser (supports SET, REMOVE, ADD)
    if (UpdateExpression) {
      // Handle SET operations
      const setMatch = UpdateExpression.match(
        /SET\s+(.+?)(?=\s+REMOVE|\s+ADD|$)/i
      )
      if (setMatch) {
        const assignments = setMatch[1].split(',')
        for (const assignment of assignments) {
          const parts = assignment.split('=')
          if (parts.length === 2) {
            const left = parts[0].trim()
            const right = parts[1].trim()
            let attrName = left
            if (ExpressionAttributeNames && left.startsWith('#')) {
              attrName = ExpressionAttributeNames[left]
            }
            let attrValue
            if (ExpressionAttributeValues && right.startsWith(':')) {
              attrValue = ExpressionAttributeValues[right]
            } else {
              attrValue = right
            }
            item[attrName] = attrValue
          }
        }
      }

      // Handle REMOVE operations
      const removeMatch = UpdateExpression.match(
        /REMOVE\s+(.+?)(?=\s+SET|\s+ADD|$)/i
      )
      if (removeMatch) {
        const attrs = removeMatch[1].split(',').map((s: string) => s.trim())
        for (const attr of attrs) {
          let attrName = attr
          if (ExpressionAttributeNames && attr.startsWith('#')) {
            attrName = ExpressionAttributeNames[attr]
          }
          delete item[attrName]
        }
      }

      // Handle ADD operations (for numbers)
      const addMatch = UpdateExpression.match(
        /ADD\s+(.+?)(?=\s+SET|\s+REMOVE|$)/i
      )
      if (addMatch) {
        const parts = addMatch[1].trim().split(/\s+/)
        if (parts.length >= 2) {
          const left = parts[0]
          const right = parts[1]
          let attrName = left
          if (ExpressionAttributeNames && left.startsWith('#')) {
            attrName = ExpressionAttributeNames[left]
          }
          let addValue
          if (ExpressionAttributeValues && right.startsWith(':')) {
            addValue = ExpressionAttributeValues[right]
          }
          if (addValue && addValue.N) {
            const currentVal = item[attrName]?.N
              ? parseInt(item[attrName].N)
              : 0
            item[attrName] = { N: String(currentVal + parseInt(addValue.N)) }
          }
        }
      }
    }

    await this.storage.putItem(TableName, item)

    // Handle ReturnValues
    if (ReturnValues === 'ALL_OLD') {
      return { Attributes: oldItem || {} }
    } else if (ReturnValues === 'UPDATED_NEW') {
      return { Attributes: item }
    } else if (ReturnValues === 'ALL_NEW') {
      return { Attributes: item }
    } else if (ReturnValues === 'UPDATED_OLD') {
      return { Attributes: oldItem || {} }
    }

    return { Attributes: item }
  }

  async handleDeleteItem(body: any) {
    const { TableName, Key, ReturnValues } = body

    const item = await this.storage.deleteItem(TableName, Key)

    if (item) {
      // Return attributes by default (ALL_OLD behavior)
      return { Attributes: item }
    }

    return {}
  }

  async handleScan(body: any) {
    const {
      TableName,
      Limit,
      FilterExpression,
      ExpressionAttributeValues,
      ExpressionAttributeNames,
      ExclusiveStartKey,
    } = body

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

    const result: any = {
      Items: items,
      Count: items.length,
      ScannedCount: scannedCount,
    }

    if (lastEvaluatedKey) {
      result.LastEvaluatedKey = lastEvaluatedKey
    }

    return result
  }

  async handleQuery(body: any) {
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
      const sortKeyName = table.keySchema[1]!.AttributeName
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

    const result: any = {
      Items: items,
      Count: items.length,
      ScannedCount: scannedCount,
    }

    if (lastEvaluatedKey) {
      result.LastEvaluatedKey = lastEvaluatedKey
    }

    return result
  }

  async handleDeleteTable(body: any) {
    const { TableName } = body

    await this.storage.deleteTable(TableName)
    return { TableDescription: { TableName, TableStatus: 'DELETING' } }
  }

  async handleBatchGetItem(body: any) {
    const { RequestItems } = body
    const responses: any = {}

    for (const tableName in RequestItems) {
      const { Keys } = RequestItems[tableName]
      const items = await this.storage.batchGet(tableName, Keys)
      responses[tableName] = items
    }

    return { Responses: responses }
  }

  async handleBatchWriteItem(body: any) {
    const { RequestItems } = body

    for (const tableName in RequestItems) {
      const requests = RequestItems[tableName]
      const puts: DynamoDBItem[] = []
      const deletes: DynamoDBItem[] = []

      for (const request of requests) {
        if (request.PutRequest) {
          puts.push(request.PutRequest.Item)
        } else if (request.DeleteRequest) {
          deletes.push(request.DeleteRequest.Key)
        }
      }

      await this.storage.batchWrite(tableName, puts, deletes)
    }

    return {}
  }

  async handleTransactWriteItems(body: any) {
    const { TransactItems, ClientRequestToken } = body

    if (!TransactItems || !Array.isArray(TransactItems)) {
      throw {
        name: 'ValidationException',
        message: 'TransactItems is required',
      }
    }

    if (TransactItems.length > 100) {
      throw {
        name: 'ValidationException',
        message: 'Transaction cannot contain more than 100 items',
      }
    }

    // Convert DynamoDB API format to storage format
    const items: TransactWriteItem[] = TransactItems.map((item: any) => {
      if (item.Put) {
        return {
          Put: {
            tableName: item.Put.TableName,
            item: item.Put.Item,
            conditionExpression: item.Put.ConditionExpression,
            expressionAttributeNames: item.Put.ExpressionAttributeNames,
            expressionAttributeValues: item.Put.ExpressionAttributeValues,
            returnValuesOnConditionCheckFailure:
              item.Put.ReturnValuesOnConditionCheckFailure,
          },
        }
      } else if (item.Update) {
        return {
          Update: {
            tableName: item.Update.TableName,
            key: item.Update.Key,
            updateExpression: item.Update.UpdateExpression,
            conditionExpression: item.Update.ConditionExpression,
            expressionAttributeNames: item.Update.ExpressionAttributeNames,
            expressionAttributeValues: item.Update.ExpressionAttributeValues,
            returnValuesOnConditionCheckFailure:
              item.Update.ReturnValuesOnConditionCheckFailure,
          },
        }
      } else if (item.Delete) {
        return {
          Delete: {
            tableName: item.Delete.TableName,
            key: item.Delete.Key,
            conditionExpression: item.Delete.ConditionExpression,
            expressionAttributeNames: item.Delete.ExpressionAttributeNames,
            expressionAttributeValues: item.Delete.ExpressionAttributeValues,
            returnValuesOnConditionCheckFailure:
              item.Delete.ReturnValuesOnConditionCheckFailure,
          },
        }
      } else if (item.ConditionCheck) {
        return {
          ConditionCheck: {
            tableName: item.ConditionCheck.TableName,
            key: item.ConditionCheck.Key,
            conditionExpression: item.ConditionCheck.ConditionExpression,
            expressionAttributeNames:
              item.ConditionCheck.ExpressionAttributeNames,
            expressionAttributeValues:
              item.ConditionCheck.ExpressionAttributeValues,
            returnValuesOnConditionCheckFailure:
              item.ConditionCheck.ReturnValuesOnConditionCheckFailure,
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
    } catch (error: any) {
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

  async handleTransactGetItems(body: any) {
    const { TransactItems } = body

    if (!TransactItems || !Array.isArray(TransactItems)) {
      throw {
        name: 'ValidationException',
        message: 'TransactItems is required',
      }
    }

    if (TransactItems.length > 100) {
      throw {
        name: 'ValidationException',
        message: 'Transaction cannot contain more than 100 items',
      }
    }

    // Convert DynamoDB API format to storage format
    const items: TransactGetItem[] = TransactItems.map((item: any) => {
      if (!item.Get) {
        throw { name: 'ValidationException', message: 'Get is required' }
      }

      return {
        tableName: item.Get.TableName,
        key: item.Get.Key,
        projectionExpression: item.Get.ProjectionExpression,
        expressionAttributeNames: item.Get.ExpressionAttributeNames,
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
  items: any[],
  filterExpression: string,
  expressionAttributeValues: any,
  expressionAttributeNames: any
): any[] {
  return items.filter((item: any) => {
    // Simple filter for equality: "#attr = :value"
    const eqMatch = filterExpression.match(/([#\w]+)\s*=\s*(:\w+)/)
    if (eqMatch && eqMatch[1] && eqMatch[2]) {
      let attrName = eqMatch[1]
      const valueRef = eqMatch[2]

      if (expressionAttributeNames && attrName.startsWith('#')) {
        const resolved = expressionAttributeNames[attrName]
        if (resolved) attrName = resolved
      }

      const filterValue = expressionAttributeValues?.[valueRef]
      if (filterValue) {
        return JSON.stringify(item[attrName]) === JSON.stringify(filterValue)
      }
    }

    // Simple filter for greater than: "#attr > :value"
    const gtMatch = filterExpression.match(/([#\w]+)\s*>\s*(:\w+)/)
    if (gtMatch && gtMatch[1] && gtMatch[2]) {
      let attrName = gtMatch[1]
      const valueRef = gtMatch[2]

      if (expressionAttributeNames && attrName.startsWith('#')) {
        const resolved = expressionAttributeNames[attrName]
        if (resolved) attrName = resolved
      }

      const filterValue = expressionAttributeValues?.[valueRef]
      if (filterValue && filterValue.N && item[attrName]?.N) {
        return parseInt(item[attrName].N) > parseInt(filterValue.N)
      }
    }

    return true
  })
}

// Helper to extract key from item
function extractKey(table: TableSchema, item: DynamoDBItem): any {
  const key: any = {}
  for (const keySchema of table.keySchema) {
    const attrName = keySchema.AttributeName
    key[attrName] = item[attrName]
  }
  return key
}
