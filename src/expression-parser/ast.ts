// AST (Abstract Syntax Tree) node types for DynamoDB expressions

import type { DynamoDBItem } from '../types.ts'
import type { AttributeValue } from '@aws-sdk/client-dynamodb'

// ============================================================================
// Condition Expression AST
// ============================================================================

export type ConditionExpression =
  | ComparisonExpression
  | LogicalExpression
  | FunctionExpression
  | NotExpression
  | BetweenExpression
  | InExpression

export interface ComparisonExpression {
  type: 'comparison'
  operator: '=' | '<>' | '<' | '>' | '<=' | '>='
  left: AttributePath
  right: Value
}

export interface LogicalExpression {
  type: 'logical'
  operator: 'AND' | 'OR'
  left: ConditionExpression
  right: ConditionExpression
}

export interface NotExpression {
  type: 'not'
  operand: ConditionExpression
}

export interface FunctionExpression {
  type: 'function'
  name:
    | 'attribute_exists'
    | 'attribute_not_exists'
    | 'begins_with'
    | 'contains'
    | 'size'
    | 'attribute_type'
  args: (AttributePath | Value)[]
}

export interface BetweenExpression {
  type: 'between'
  value: AttributePath
  lower: Value
  upper: Value
}

export interface InExpression {
  type: 'in'
  value: AttributePath
  list: Value[]
}

// ============================================================================
// Update Expression AST
// ============================================================================

export interface UpdateExpression {
  set?: SetAction[]
  remove?: RemoveAction[]
  add?: AddAction[]
  delete?: DeleteAction[]
}

export interface SetAction {
  type: 'set'
  path: AttributePath
  value: SetValue
}

export type SetValue =
  | Value
  | ArithmeticExpression
  | IfNotExistsExpression
  | ListAppendExpression

export interface ArithmeticExpression {
  type: 'arithmetic'
  operator: '+' | '-'
  left: AttributePath | Value
  right: AttributePath | Value
}

export interface IfNotExistsExpression {
  type: 'if_not_exists'
  path: AttributePath
  defaultValue: Value
}

export interface ListAppendExpression {
  type: 'list_append'
  list1: AttributePath | Value
  list2: AttributePath | Value
}

export interface RemoveAction {
  type: 'remove'
  path: AttributePath
}

export interface AddAction {
  type: 'add'
  path: AttributePath
  value: Value
}

export interface DeleteAction {
  type: 'delete'
  path: AttributePath
  value: Value
}

// ============================================================================
// Common Types
// ============================================================================

export interface AttributePath {
  type: 'attribute_path'
  name: string // Resolved attribute name (after applying expressionAttributeNames)
}

export interface Value {
  type: 'value'
  value: AttributeValue | string | number | boolean | null
}

// ============================================================================
// Evaluation Context
// ============================================================================

export interface EvaluationContext {
  item: DynamoDBItem | null
  expressionAttributeNames?: Record<string, string>
  expressionAttributeValues?: Record<string, AttributeValue>
}
