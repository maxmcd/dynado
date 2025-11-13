// Update Expression Evaluator

import type {
  UpdateExpression,
  SetAction,
  SetValue,
  RemoveAction,
  AddAction,
  DeleteAction,
  ArithmeticExpression,
  IfNotExistsExpression,
  ListAppendExpression,
  AttributePath,
  Value,
  EvaluationContext,
} from './ast.ts'
import type { DynamoDBItem } from '../types.ts'
import type { AttributeValue } from '@aws-sdk/client-dynamodb'

export function applyUpdateExpression(
  item: DynamoDBItem,
  expression: UpdateExpression,
  context: EvaluationContext
): DynamoDBItem {
  const updatedItem = { ...item }

  // Apply SET actions
  if (expression.set) {
    for (const action of expression.set) {
      applySetAction(updatedItem, action, context)
    }
  }

  // Apply REMOVE actions
  if (expression.remove) {
    for (const action of expression.remove) {
      applyRemoveAction(updatedItem, action, context)
    }
  }

  // Apply ADD actions
  if (expression.add) {
    for (const action of expression.add) {
      applyAddAction(updatedItem, action, context)
    }
  }

  // Apply DELETE actions
  if (expression.delete) {
    for (const action of expression.delete) {
      applyDeleteAction(updatedItem, action, context)
    }
  }

  return updatedItem
}

function applySetAction(
  item: DynamoDBItem,
  action: SetAction,
  context: EvaluationContext
): void {
  const attrName = resolveAttributeName(action.path.name, context)
  const value = evaluateSetValue(item, action.value, context)

  if (value !== undefined) {
    item[attrName] = value
  }
}

function evaluateSetValue(
  item: DynamoDBItem,
  value: SetValue,
  context: EvaluationContext
): AttributeValue | undefined {
  // Arithmetic expression
  if (value.type === 'arithmetic') {
    const expr = value as ArithmeticExpression
    const left = resolveOperand(item, expr.left, context)
    const right = resolveOperand(item, expr.right, context)

    const leftNum = getNumericValue(left)
    const rightNum = getNumericValue(right)

    if (leftNum !== null && rightNum !== null) {
      const result =
        expr.operator === '+' ? leftNum + rightNum : leftNum - rightNum
      return { N: String(result) }
    }

    return undefined
  }

  // if_not_exists function
  if (value.type === 'if_not_exists') {
    const expr = value as IfNotExistsExpression
    const attrName = resolveAttributeName(expr.path.name, context)

    if (attrName in item) {
      return item[attrName]
    }

    return resolveValue(expr.defaultValue, context)
  }

  // list_append function
  if (value.type === 'list_append') {
    const expr = value as ListAppendExpression
    const list1 = resolveOperand(item, expr.list1, context)
    const list2 = resolveOperand(item, expr.list2, context)

    const arr1 = toAttributeValueArray(list1)
    const arr2 = toAttributeValueArray(list2)

    return { L: [...arr1, ...arr2] }
  }

  // Simple value
  if (value.type === 'value') {
    return resolveValue(value as Value, context)
  }

  return undefined
}

function applyRemoveAction(
  item: DynamoDBItem,
  action: RemoveAction,
  context: EvaluationContext
): void {
  const attrName = resolveAttributeName(action.path.name, context)
  delete item[attrName]
}

function applyAddAction(
  item: DynamoDBItem,
  action: AddAction,
  context: EvaluationContext
): void {
  const attrName = resolveAttributeName(action.path.name, context)
  const addValue = resolveValue(action.value, context)

  if (!isNumberAttribute(addValue)) return

  const currentValue = item[attrName]
  const currentNum = isNumberAttribute(currentValue)
    ? parseInt(currentValue.N)
    : 0
  const addNum = parseInt(addValue.N)

  item[attrName] = { N: String(currentNum + addNum) }
}

function applyDeleteAction(
  item: DynamoDBItem,
  action: DeleteAction,
  context: EvaluationContext
): void {
  const attrName = resolveAttributeName(action.path.name, context)
  const deleteValue = resolveValue(action.value, context)

  // DELETE is used for removing elements from sets
  // For now, we'll skip this implementation as it's complex
  // and not used in our current tests
  console.warn(`DELETE action not fully implemented for ${attrName}`)
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

function resolveValue(
  value: Value,
  context: EvaluationContext
): AttributeValue | undefined {
  if (typeof value.value === 'string' && value.value.startsWith(':')) {
    // Expression attribute value
    return context.expressionAttributeValues?.[value.value]
  }
  if (typeof value.value === 'object' && value.value !== null) {
    return value.value as AttributeValue
  }
  return undefined
}

function resolveOperand(
  item: DynamoDBItem,
  operand: AttributePath | Value,
  context: EvaluationContext
): AttributeValue | AttributeValue[] | undefined {
  if (operand.type === 'attribute_path') {
    const attrName = resolveAttributeName(operand.name, context)
    return item[attrName]
  }

  if (operand.type === 'value') {
    return resolveValue(operand, context)
  }

  return undefined
}

function getNumericValue(
  value: AttributeValue | AttributeValue[] | undefined
): number | null {
  if (value === null || value === undefined) return null

  if (Array.isArray(value)) {
    return null
  }

  if (
    typeof value === 'object' &&
    value !== null &&
    'N' in value &&
    value.N !== undefined
  ) {
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

function toAttributeValueArray(
  value: AttributeValue | AttributeValue[] | undefined
): AttributeValue[] {
  if (!value) return []
  if (Array.isArray(value)) {
    return value
  }
  if (
    typeof value === 'object' &&
    value !== null &&
    'L' in value &&
    Array.isArray(value.L)
  ) {
    return value.L as AttributeValue[]
  }
  return []
}

function isNumberAttribute(
  value: AttributeValue | undefined
): value is AttributeValue & { N: string } {
  return (
    typeof value === 'object' &&
    value !== null &&
    'N' in value &&
    typeof value.N === 'string'
  )
}
