// MetadataStore: Manages table schemas independently from shards
// In DO architecture, this would be a single Durable Object

import { Database } from 'bun:sqlite'
import type { KeySchemaElement } from '@aws-sdk/client-dynamodb'
import type { DynamoDBItem, TableSchema } from './types.ts'
import * as fs from 'fs'

interface TableSchemaRow {
  table_name: string
  key_schema: string
  attribute_definitions: string
  created_at: number
}

function ensureAttributeName(
  schema: KeySchemaElement,
  context: string
): string {
  if (!schema.AttributeName) {
    throw new Error(`Key schema missing AttributeName for ${context}`)
  }
  return schema.AttributeName
}

export class MetadataStore {
  private db: Database
  private cache: Map<string, TableSchema> = new Map()

  constructor(dataDir: string) {
    // Create data directory if it doesn't exist
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true })
    }

    this.db = new Database(`${dataDir}/metadata.db`)

    // Create metadata table
    this.db.run(`
      CREATE TABLE IF NOT EXISTS table_schemas (
        table_name TEXT PRIMARY KEY,
        key_schema TEXT NOT NULL,
        attribute_definitions TEXT NOT NULL,
        created_at INTEGER NOT NULL
      )
    `)

    // Load all schemas into cache
    this.loadSchemas()
  }

  private loadSchemas() {
    const schemas = this.db
      .query<TableSchemaRow, []>('SELECT * FROM table_schemas')
      .all()

    for (const schema of schemas) {
      this.cache.set(schema.table_name, {
        tableName: schema.table_name,
        keySchema: JSON.parse(schema.key_schema),
        attributeDefinitions: JSON.parse(schema.attribute_definitions),
      })
    }
  }

  async createTable(schema: TableSchema): Promise<void> {
    if (this.cache.has(schema.tableName)) {
      throw new Error(`Table already exists: ${schema.tableName}`)
    }

    const keySchemaJson = JSON.stringify(schema.keySchema)
    const attrDefsJson = JSON.stringify(schema.attributeDefinitions)

    this.db.run(
      `INSERT INTO table_schemas
       (table_name, key_schema, attribute_definitions, created_at)
       VALUES (?, ?, ?, ?)`,
      [schema.tableName, keySchemaJson, attrDefsJson, Date.now()]
    )

    this.cache.set(schema.tableName, schema)
  }

  async describeTable(tableName: string): Promise<TableSchema | null> {
    return this.cache.get(tableName) || null
  }

  async listTables(): Promise<string[]> {
    return Array.from(this.cache.keys())
  }

  async deleteTable(tableName: string): Promise<void> {
    this.db.run('DELETE FROM table_schemas WHERE table_name = ?', [tableName])
    this.cache.delete(tableName)
  }

  // Helper to get partition key attribute name from schema
  getPartitionKeyName(tableName: string): string | null {
    const schema = this.cache.get(tableName)
    if (!schema) return null

    const partitionKeyAttr = schema.keySchema.find(
      (k: KeySchemaElement) => k.KeyType === 'HASH'
    )
    return partitionKeyAttr?.AttributeName || null
  }

  // Helper to get sort key attribute name from schema
  getSortKeyName(tableName: string): string | null {
    const schema = this.cache.get(tableName)
    if (!schema) return null

    const sortKeyAttr = schema.keySchema.find(
      (k: KeySchemaElement) => k.KeyType === 'RANGE'
    )
    return sortKeyAttr?.AttributeName || null
  }

  // Helper to extract key from item based on table schema
  extractKey(tableName: string, item: DynamoDBItem): DynamoDBItem {
    const schema = this.cache.get(tableName)
    if (!schema) {
      throw new Error(`Table not found: ${tableName}`)
    }

    const key: DynamoDBItem = {}
    for (const keySchema of schema.keySchema) {
      const attrName = ensureAttributeName(
        keySchema,
        `table ${tableName} key extraction`
      )
      if (!(attrName in item)) {
        throw new Error(`Key attribute missing: ${attrName}`)
      }
      const value = item[attrName]
      if (value === undefined) {
        throw new Error(`Key attribute missing: ${attrName}`)
      }
      key[attrName] = value
    }
    return key
  }

  // Helper to get partition key value from item
  getPartitionKeyValue(tableName: string, item: DynamoDBItem): string {
    const partitionKeyName = this.getPartitionKeyName(tableName)
    if (!partitionKeyName) {
      throw new Error(`No partition key found for table: ${tableName}`)
    }

    const value = item[partitionKeyName]
    if (value === undefined) {
      throw new Error(`Partition key attribute missing: ${partitionKeyName}`)
    }

    return JSON.stringify(value)
  }

  // Helper to get sort key value from item (returns empty string if table has no sort key)
  getSortKeyValue(tableName: string, item: DynamoDBItem): string {
    const sortKeyName = this.getSortKeyName(tableName)
    if (!sortKeyName) {
      return '' // Table doesn't have a sort key
    }

    const value = item[sortKeyName]
    if (value === undefined) {
      throw new Error(`Sort key attribute missing: ${sortKeyName}`)
    }

    return JSON.stringify(value)
  }

  // Helper to extract both partition and sort key values as strings for storage
  extractKeyValues(
    tableName: string,
    item: DynamoDBItem
  ): { partitionKeyValue: string; sortKeyValue: string } {
    const partitionKeyValue = this.getPartitionKeyValue(tableName, item)
    const sortKeyValue = this.getSortKeyValue(tableName, item)
    return { partitionKeyValue, sortKeyValue }
  }

  // Helper to extract key values from a key object (not full item)
  extractKeyValuesFromKey(
    tableName: string,
    key: DynamoDBItem
  ): { partitionKeyValue: string; sortKeyValue: string } {
    const partitionKeyName = this.getPartitionKeyName(tableName)
    const sortKeyName = this.getSortKeyName(tableName)

    if (!partitionKeyName) {
      throw new Error(`No partition key found for table: ${tableName}`)
    }

    const partitionValue = key[partitionKeyName]
    if (partitionValue === undefined) {
      throw new Error(`Partition key attribute missing: ${partitionKeyName}`)
    }

    const partitionKeyValue = JSON.stringify(partitionValue)
    const sortKeyValue =
      sortKeyName && key[sortKeyName] !== undefined
        ? JSON.stringify(key[sortKeyName])
        : ''

    return { partitionKeyValue, sortKeyValue }
  }

  close() {
    this.db.close()
  }
}
