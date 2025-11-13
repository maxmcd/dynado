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
    default:
      throw new Error(`Unknown expression type: ${(expression as any).type}`)
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

      const attrStr = attrValue.S || attrValue
      const prefixStr = prefixValue.S || prefixValue

      if (typeof attrStr === 'string' && typeof prefixStr === 'string') {
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
      const attrStr = attrValue.S || attrValue
      const searchStr = searchValue.S || searchValue

      if (typeof attrStr === 'string' && typeof searchStr === 'string') {
        return attrStr.includes(searchStr)
      }

      // List contains
      if (Array.isArray(attrValue)) {
        return attrValue.some(
          (v) => JSON.stringify(v) === JSON.stringify(searchValue)
        )
      }

      return false
    }

    case 'size': {
      const path = expr.args[0] as AttributePath
      const operator = expr.args[1] as any // operator string
      const valueArg = expr.args[2] as Value

      const attrValue = getAttributeValue(path, context)
      const compareValue = resolveValue(valueArg, context)

      if (!attrValue) return false

      let size = 0
      if (typeof attrValue === 'string' || attrValue.S) {
        size = (attrValue.S || attrValue).length
      } else if (Array.isArray(attrValue)) {
        size = attrValue.length
      } else if (attrValue.L) {
        size = attrValue.L.length
      }

      const compareNum =
        typeof compareValue === 'number'
          ? compareValue
          : parseInt(compareValue?.N || '0')

      // operator is the comparison operator token
      switch (operator.type === 'value' ? operator.value : operator) {
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
      const expectedTypeStr = expectedType.S || expectedType

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
): any {
  if (!context.item) return undefined

  const attrName = resolveAttributeName(path.name, context)
  return context.item[attrName]
}

function resolveValue(value: Value, context: EvaluationContext): any {
  if (typeof value.value === 'string' && value.value.startsWith(':')) {
    // Expression attribute value
    return context.expressionAttributeValues?.[value.value]
  }
  return value.value
}

function compareValues(a: any, b: any): number {
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

function getNumericValue(value: any): number | null {
  if (value === null || value === undefined) return null

  if (value.N) {
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

function getStringValue(value: any): string | null {
  if (value === null || value === undefined) return null

  if (value.S) {
    return value.S
  }

  if (typeof value === 'string') {
    return value
  }

  return null
}

function getAttributeType(value: any): string {
  if (value.S !== undefined) return 'S'
  if (value.N !== undefined) return 'N'
  if (value.B !== undefined) return 'B'
  if (value.SS !== undefined) return 'SS'
  if (value.NS !== undefined) return 'NS'
  if (value.BS !== undefined) return 'BS'
  if (value.M !== undefined) return 'M'
  if (value.L !== undefined) return 'L'
  if (value.NULL !== undefined) return 'NULL'
  if (value.BOOL !== undefined) return 'BOOL'
  return 'UNKNOWN'
}
