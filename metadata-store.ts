// MetadataStore: Manages table schemas independently from shards
// In DO architecture, this would be a single Durable Object

import { Database } from "bun:sqlite";
import type { TableSchema } from "./types.ts";
import * as fs from "fs";

export class MetadataStore {
  private db: Database;
  private cache: Map<string, TableSchema> = new Map();

  constructor(dataDir: string) {
    // Create data directory if it doesn't exist
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }

    this.db = new Database(`${dataDir}/metadata.db`);

    // Create metadata table
    this.db.run(`
      CREATE TABLE IF NOT EXISTS table_schemas (
        table_name TEXT PRIMARY KEY,
        key_schema TEXT NOT NULL,
        attribute_definitions TEXT NOT NULL,
        created_at INTEGER NOT NULL
      )
    `);

    // Load all schemas into cache
    this.loadSchemas();
  }

  private loadSchemas() {
    const schemas = this.db
      .query("SELECT * FROM table_schemas")
      .all() as any[];

    for (const schema of schemas) {
      this.cache.set(schema.table_name, {
        tableName: schema.table_name,
        keySchema: JSON.parse(schema.key_schema),
        attributeDefinitions: JSON.parse(schema.attribute_definitions),
      });
    }
  }

  async createTable(schema: TableSchema): Promise<void> {
    if (this.cache.has(schema.tableName)) {
      throw new Error(`Table already exists: ${schema.tableName}`);
    }

    const keySchemaJson = JSON.stringify(schema.keySchema);
    const attrDefsJson = JSON.stringify(schema.attributeDefinitions);

    this.db.run(
      `INSERT INTO table_schemas
       (table_name, key_schema, attribute_definitions, created_at)
       VALUES (?, ?, ?, ?)`,
      [schema.tableName, keySchemaJson, attrDefsJson, Date.now()]
    );

    this.cache.set(schema.tableName, schema);
  }

  async describeTable(tableName: string): Promise<TableSchema | null> {
    return this.cache.get(tableName) || null;
  }

  async listTables(): Promise<string[]> {
    return Array.from(this.cache.keys());
  }

  async deleteTable(tableName: string): Promise<void> {
    this.db.run("DELETE FROM table_schemas WHERE table_name = ?", [tableName]);
    this.cache.delete(tableName);
  }

  // Helper to get partition key attribute name from schema
  getPartitionKeyName(tableName: string): string | null {
    const schema = this.cache.get(tableName);
    if (!schema) return null;

    const partitionKeyAttr = schema.keySchema.find(
      (k: any) => k.KeyType === "HASH"
    );
    return partitionKeyAttr?.AttributeName || null;
  }

  // Helper to extract key from item based on table schema
  extractKey(tableName: string, item: any): any {
    const schema = this.cache.get(tableName);
    if (!schema) {
      throw new Error(`Table not found: ${tableName}`);
    }

    const key: any = {};
    for (const keySchema of schema.keySchema) {
      const attrName = keySchema.AttributeName;
      if (!(attrName in item)) {
        throw new Error(`Key attribute missing: ${attrName}`);
      }
      key[attrName] = item[attrName];
    }
    return key;
  }

  // Helper to get partition key value from item
  getPartitionKeyValue(tableName: string, item: any): string {
    const partitionKeyName = this.getPartitionKeyName(tableName);
    if (!partitionKeyName) {
      throw new Error(`No partition key found for table: ${tableName}`);
    }

    if (!(partitionKeyName in item)) {
      throw new Error(`Partition key attribute missing: ${partitionKeyName}`);
    }

    return JSON.stringify(item[partitionKeyName]);
  }

  close() {
    this.db.close();
  }
}
