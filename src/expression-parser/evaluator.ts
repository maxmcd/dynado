// Evaluator: Execute AST against DynamoDB items

import type {
  ConditionExpression,
  ComparisonExpression,
  LogicalExpression,
  NotExpression,
  FunctionExpression,
  BetweenExpression,
  InExpression,
  AttributePath,
  Value,
  EvaluationContext,
} from './ast.ts'
import type { DynamoDBItem } from '../types.ts'
import type { AttributeValue } from '@aws-sdk/client-dynamodb'
import type { ComparisonOperator } from './ast.ts'

type AttributeValueLike =
  | AttributeValue
  | string
  | number
  | boolean
  | null
  | AttributeValueLike[]
  | { [key: string]: AttributeValueLike }

export function evaluateCondition(
  expression: ConditionExpression,
  context: EvaluationContext
): boolean {
  switch (expression.type) {
    case 'comparison':
      return evaluateComparison(expression, context)
    case 'logical':
      return evaluateLogical(expression, context)
    case 'not':
      return !evaluateCondition(expression.operand, context)
    case 'function':
      return evaluateFunction(expression, context)
    case 'between':
      return evaluateBetween(expression, context)
    case 'in':
      return evaluateIn(expression, context)
    default: {
      const _exhaustive: never = expression
      throw new Error(
        `Unknown expression type: ${
          typeof _exhaustive === 'object' && _exhaustive
            ? ((_exhaustive as { type?: string }).type ?? 'unknown')
            : 'unknown'
        }`
      )
    }
  }
}

function evaluateComparison(
  expr: ComparisonExpression,
  context: EvaluationContext
): boolean {
  const leftValue = getAttributeValue(expr.left, context)
  const rightValue = resolveValue(expr.right, context)

  if (leftValue === undefined) return false

  switch (expr.operator) {
    case '=':
      return JSON.stringify(leftValue) === JSON.stringify(rightValue)
    case '<>':
      return JSON.stringify(leftValue) !== JSON.stringify(rightValue)
    case '<':
      return compareValues(leftValue, rightValue) < 0
    case '>':
      return compareValues(leftValue, rightValue) > 0
    case '<=':
      return compareValues(leftValue, rightValue) <= 0
    case '>=':
      return compareValues(leftValue, rightValue) >= 0
    default:
      throw new Error(`Unknown operator: ${expr.operator}`)
  }
}

function evaluateLogical(
  expr: LogicalExpression,
  context: EvaluationContext
): boolean {
  switch (expr.operator) {
    case 'AND':
      return (
        evaluateCondition(expr.left, context) &&
        evaluateCondition(expr.right, context)
      )
    case 'OR':
      return (
        evaluateCondition(expr.left, context) ||
        evaluateCondition(expr.right, context)
      )
    default:
      throw new Error(`Unknown logical operator: ${expr.operator}`)
  }
}

function evaluateFunction(
  expr: FunctionExpression,
  context: EvaluationContext
): boolean {
  switch (expr.name) {
    case 'attribute_exists': {
      const path = expr.args[0] as AttributePath
      const attrName = resolveAttributeName(path.name, context)
      return context.item !== null && attrName in context.item
    }

    case 'attribute_not_exists': {
      const path = expr.args[0] as AttributePath
      const attrName = resolveAttributeName(path.name, context)
      return context.item === null || !(attrName in context.item)
    }

    case 'begins_with': {
      const path = expr.args[0] as AttributePath
      const valueArg = expr.args[1] as Value

      const attrValue = getAttributeValue(path, context)
      const prefixValue = resolveValue(valueArg, context)

      if (!attrValue || !prefixValue) return false

      const attrStr = getStringValue(attrValue)
      const prefixStr = getStringValue(prefixValue)

      if (attrStr !== null && prefixStr !== null) {
        return attrStr.startsWith(prefixStr)
      }

      return false
    }

    case 'contains': {
      const path = expr.args[0] as AttributePath
      const valueArg = expr.args[1] as Value

      const attrValue = getAttributeValue(path, context)
      const searchValue = resolveValue(valueArg, context)

      if (!attrValue || !searchValue) return false

      // String contains
      const attrStr = getStringValue(attrValue)
      const searchStr = getStringValue(searchValue)

      if (attrStr !== null && searchStr !== null) {
        return attrStr.includes(searchStr)
      }

      // List contains
      const listValues = Array.isArray(attrValue)
        ? attrValue
        : typeof attrValue === 'object' &&
            attrValue !== null &&
            'L' in attrValue &&
            Array.isArray(attrValue.L)
          ? (attrValue.L as AttributeValueLike[])
          : undefined

      if (listValues) {
        return listValues.some(
          (v) => JSON.stringify(v) === JSON.stringify(searchValue)
        )
      }

      return false
    }

    case 'size': {
      const path = expr.args[0] as AttributePath
      const operatorArg = expr.args[1]
      const valueArg = expr.args[2] as Value

      const attrValue = getAttributeValue(path, context)
      const compareValue = resolveValue(valueArg, context)

      if (!attrValue) return false

      let size = 0
      if (typeof attrValue === 'string') {
        size = attrValue.length
      } else if (
        typeof attrValue === 'object' &&
        attrValue !== null &&
        'S' in attrValue &&
        typeof attrValue.S === 'string'
      ) {
        size = attrValue.S.length
      } else if (Array.isArray(attrValue)) {
        size = attrValue.length
      } else if (
        typeof attrValue === 'object' &&
        attrValue !== null &&
        'L' in attrValue &&
        Array.isArray(attrValue.L)
      ) {
        size = attrValue.L.length
      }

      const compareNum = getNumericValue(compareValue)
      if (compareNum === null) {
        return false
      }

      const operator = isComparisonOperator(operatorArg) ? operatorArg : '='

      // operator is the comparison operator token
      switch (operator) {
        case '=':
          return size === compareNum
        case '<>':
          return size !== compareNum
        case '<':
          return size < compareNum
        case '>':
          return size > compareNum
        case '<=':
          return size <= compareNum
        case '>=':
          return size >= compareNum
        default:
          return false
      }
    }

    case 'attribute_type': {
      const path = expr.args[0] as AttributePath
      const typeArg = expr.args[1] as Value

      const attrValue = getAttributeValue(path, context)
      const expectedType = resolveValue(typeArg, context)

      if (!attrValue) return false

      const actualType = getAttributeType(attrValue)
      const expectedTypeStr = getStringValue(expectedType)
      if (!expectedTypeStr) return false

      return actualType === expectedTypeStr
    }

    default:
      throw new Error(`Unknown function: ${expr.name}`)
  }
}

function evaluateBetween(
  expr: BetweenExpression,
  context: EvaluationContext
): boolean {
  const value = getAttributeValue(expr.value, context)
  const lower = resolveValue(expr.lower, context)
  const upper = resolveValue(expr.upper, context)

  if (value === undefined) return false

  return compareValues(value, lower) >= 0 && compareValues(value, upper) <= 0
}

function evaluateIn(expr: InExpression, context: EvaluationContext): boolean {
  const value = getAttributeValue(expr.value, context)

  if (value === undefined) return false

  const valueStr = JSON.stringify(value)

  for (const listItem of expr.list) {
    const itemValue = resolveValue(listItem, context)
    if (JSON.stringify(itemValue) === valueStr) {
      return true
    }
  }

  return false
}

// Helper functions

function resolveAttributeName(
  name: string,
  context: EvaluationContext
): string {
  if (name.startsWith('#')) {
    return context.expressionAttributeNames?.[name] || name
  }
  return name
}

function getAttributeValue(
  path: AttributePath,
  context: EvaluationContext
): AttributeValueLike | undefined {
  if (!context.item) return undefined

  const attrName = resolveAttributeName(path.name, context)
  return context.item[attrName]
}

function resolveValue(
  value: Value,
  context: EvaluationContext
): AttributeValueLike | undefined {
  if (typeof value.value === 'string' && value.value.startsWith(':')) {
    // Expression attribute value
    return context.expressionAttributeValues?.[value.value]
  }
  return value.value
}

function compareValues(
  a: AttributeValueLike | undefined,
  b: AttributeValueLike | undefined
): number {
  // Get numeric values
  const aNum = getNumericValue(a)
  const bNum = getNumericValue(b)

  if (aNum !== null && bNum !== null) {
    return aNum - bNum
  }

  // Get string values
  const aStr = getStringValue(a)
  const bStr = getStringValue(b)

  if (aStr !== null && bStr !== null) {
    return aStr.localeCompare(bStr)
  }

  // Fallback to JSON comparison
  const aJson = JSON.stringify(a)
  const bJson = JSON.stringify(b)
  return aJson.localeCompare(bJson)
}

function getNumericValue(value: AttributeValueLike | undefined): number | null {
  if (value === null || value === undefined) return null

  if (hasNumericAttribute(value)) {
    const num = parseFloat(value.N)
    return isNaN(num) ? null : num
  }

  if (typeof value === 'number') {
    return value
  }

  if (typeof value === 'string') {
    const num = parseFloat(value)
    return isNaN(num) ? null : num
  }

  return null
}

function getStringValue(value: AttributeValueLike | undefined): string | null {
  if (value === null || value === undefined) return null

  if (
    typeof value === 'object' &&
    value !== null &&
    'S' in value &&
    typeof value.S === 'string'
  ) {
    return value.S
  }

  if (typeof value === 'string') {
    return value
  }

  return null
}

function getAttributeType(value: AttributeValueLike): string {
  if (typeof value !== 'object' || value === null) {
    return 'UNKNOWN'
  }
  if ('S' in value && value.S !== undefined) return 'S'
  if ('N' in value && value.N !== undefined) return 'N'
  if ('B' in value && value.B !== undefined) return 'B'
  if ('SS' in value && value.SS !== undefined) return 'SS'
  if ('NS' in value && value.NS !== undefined) return 'NS'
  if ('BS' in value && value.BS !== undefined) return 'BS'
  if ('M' in value && value.M !== undefined) return 'M'
  if ('L' in value && value.L !== undefined) return 'L'
  if ('NULL' in value && value.NULL !== undefined) return 'NULL'
  if ('BOOL' in value && value.BOOL !== undefined) return 'BOOL'
  return 'UNKNOWN'
}

function isComparisonOperator(value: unknown): value is ComparisonOperator {
  return (
    value === '=' ||
    value === '<>' ||
    value === '<' ||
    value === '>' ||
    value === '<=' ||
    value === '>='
  )
}
function hasNumericAttribute(
  value: AttributeValueLike | undefined
): value is { N: string } {
  return (
    typeof value === 'object' &&
    value !== null &&
    'N' in value &&
    typeof (value as { N?: unknown }).N === 'string'
  )
}
