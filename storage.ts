// Storage abstraction interface for DynamoDB-compatible storage

export interface DynamoDBItem {
  [key: string]: any;
}

export interface TableSchema {
  tableName: string;
  keySchema: any[];
  attributeDefinitions: any[];
}

export interface TransactWriteItem {
  Put?: {
    tableName: string;
    item: DynamoDBItem;
    conditionExpression?: string;
    expressionAttributeNames?: Record<string, string>;
    expressionAttributeValues?: Record<string, any>;
    returnValuesOnConditionCheckFailure?: "ALL_OLD" | "NONE";
  };
  Update?: {
    tableName: string;
    key: DynamoDBItem;
    updateExpression: string;
    conditionExpression?: string;
    expressionAttributeNames?: Record<string, string>;
    expressionAttributeValues?: Record<string, any>;
    returnValuesOnConditionCheckFailure?: "ALL_OLD" | "NONE";
  };
  Delete?: {
    tableName: string;
    key: DynamoDBItem;
    conditionExpression?: string;
    expressionAttributeNames?: Record<string, string>;
    expressionAttributeValues?: Record<string, any>;
    returnValuesOnConditionCheckFailure?: "ALL_OLD" | "NONE";
  };
  ConditionCheck?: {
    tableName: string;
    key: DynamoDBItem;
    conditionExpression: string;
    expressionAttributeNames?: Record<string, string>;
    expressionAttributeValues?: Record<string, any>;
    returnValuesOnConditionCheckFailure?: "ALL_OLD" | "NONE";
  };
}

export interface TransactGetItem {
  tableName: string;
  key: DynamoDBItem;
  projectionExpression?: string;
  expressionAttributeNames?: Record<string, string>;
}

export interface CancellationReason {
  Code?: string;
  Message?: string;
  Item?: DynamoDBItem;
}

export class TransactionCancelledError extends Error {
  override name = "TransactionCanceledException" as const;
  cancellationReasons: CancellationReason[];

  constructor(message: string, cancellationReasons: CancellationReason[]) {
    super(message);
    this.cancellationReasons = cancellationReasons;
  }
}

export interface StorageBackend {
  // Table operations
  listTables(): Promise<string[]>;
  createTable(schema: TableSchema): Promise<void>;
  describeTable(tableName: string): Promise<TableSchema | null>;
  deleteTable(tableName: string): Promise<void>;
  getTableItemCount(tableName: string): Promise<number>;

  // Item operations
  putItem(tableName: string, item: DynamoDBItem): Promise<void>;
  getItem(tableName: string, key: DynamoDBItem): Promise<DynamoDBItem | null>;
  deleteItem(tableName: string, key: DynamoDBItem): Promise<DynamoDBItem | null>;

  // Scan/Query operations
  scan(tableName: string, limit?: number, exclusiveStartKey?: any): Promise<{
    items: DynamoDBItem[];
    lastEvaluatedKey?: any;
  }>;

  query(tableName: string, keyCondition: (item: DynamoDBItem) => boolean, limit?: number, exclusiveStartKey?: any): Promise<{
    items: DynamoDBItem[];
    lastEvaluatedKey?: any;
  }>;

  // Batch operations
  batchGet(tableName: string, keys: DynamoDBItem[]): Promise<DynamoDBItem[]>;
  batchWrite(tableName: string, puts: DynamoDBItem[], deletes: DynamoDBItem[]): Promise<void>;

  // Transaction operations
  transactWrite(items: TransactWriteItem[], clientRequestToken?: string): Promise<void>;
  transactGet(items: TransactGetItem[]): Promise<Array<DynamoDBItem | null>>;
}
