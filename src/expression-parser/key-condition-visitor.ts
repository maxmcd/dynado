// Visitor to convert KeyCondition CST to AST

import { keyConditionParser } from './key-condition-parser.ts'
import type { CstNode } from 'chevrotain'

const BaseVisitor = keyConditionParser.getBaseCstVisitorConstructor()

interface TokenNode {
  image: string
}

interface KeyChildren {
  ExpressionAttributeName?: TokenNode[]
  Identifier?: TokenNode[]
}

interface ValueChildren {
  ExpressionAttributeValue: TokenNode[]
}

interface OperatorChildren {
  Equals?: unknown[]
  LessThan?: unknown[]
  GreaterThan?: unknown[]
  LessThanOrEqual?: unknown[]
  GreaterThanOrEqual?: unknown[]
}

interface PartitionKeyConditionCtx {
  key: Array<{ children: KeyChildren }>
  value: Array<{ children: ValueChildren }>
}

interface SortKeyConditionCtx extends PartitionKeyConditionCtx {
  value1?: Array<{ children: ValueChildren }>
  value2?: Array<{ children: ValueChildren }>
  operator?: Array<{ children: OperatorChildren }>
  Between?: unknown[]
  BeginsWith?: unknown[]
}

interface KeyConditionExpressionCtx {
  partitionKeyCondition: CstNode | CstNode[]
  sortKeyCondition?: CstNode | CstNode[]
}

function extractAttributeName(children: KeyChildren): string {
  const token =
    children.ExpressionAttributeName?.[0] || children.Identifier?.[0]
  if (!token) {
    throw new Error('Missing attribute name in key condition')
  }
  return token.image
}

function extractValueRef(children: ValueChildren): string {
  const token = children.ExpressionAttributeValue[0]
  if (!token) {
    throw new Error('Missing attribute value reference')
  }
  return token.image
}

export interface KeyConditionAST {
  partitionKey: {
    attributeName: string
    value: string // Reference like ":pk"
  }
  sortKey?: {
    attributeName: string
    operator: '=' | '<' | '>' | '<=' | '>=' | 'BETWEEN' | 'begins_with'
    value: string // Reference like ":sk"
    value2?: string // For BETWEEN
  }
}

class KeyConditionVisitor extends BaseVisitor {
  constructor() {
    super()
    // Don't validate visitor - we have terminal rules that don't need visiting
  }

  keyConditionExpression(ctx: KeyConditionExpressionCtx): KeyConditionAST {
    const partitionKey = this.visit(ctx.partitionKeyCondition)
    const sortKey = ctx.sortKeyCondition
      ? this.visit(ctx.sortKeyCondition)
      : undefined

    return { partitionKey, sortKey }
  }

  partitionKeyCondition(ctx: PartitionKeyConditionCtx) {
    const keyNode = ctx.key[0]
    const valueNode = ctx.value[0]
    if (!keyNode || !valueNode) {
      throw new Error('Invalid partition key condition')
    }
    const key = keyNode.children
    const value = valueNode.children

    if (!value?.ExpressionAttributeValue?.[0]) {
      throw new Error('Missing partition key value')
    }

    const attributeName = extractAttributeName(key)
    const valueRef = extractValueRef(value)

    return {
      attributeName,
      value: valueRef,
    }
  }

  sortKeyCondition(ctx: SortKeyConditionCtx) {
    // BETWEEN: sk BETWEEN :val1 AND :val2
    if (ctx.Between) {
      const keyNode = ctx.key[0]
      const valueNode = ctx.value1?.[0]
      const valueNode2 = ctx.value2?.[0]
      if (!keyNode || !valueNode || !valueNode2) {
        throw new Error('Invalid BETWEEN sort key condition')
      }
      const key = keyNode.children
      const value = valueNode.children
      const value2 = valueNode2.children

      if (!value?.ExpressionAttributeValue?.[0]) {
        throw new Error('Missing BETWEEN value')
      }
      if (!value2?.ExpressionAttributeValue?.[0]) {
        throw new Error('Missing BETWEEN upper value')
      }

      const attributeName = extractAttributeName(key)
      const valueRef = extractValueRef(value)
      const valueRef2 = extractValueRef(value2)

      return {
        attributeName,
        operator: 'BETWEEN' as const,
        value: valueRef,
        value2: valueRef2,
      }
    }

    // begins_with(sk, :val)
    if (ctx.BeginsWith) {
      const keyNode = ctx.key[0]
      const valueNode = ctx.value[0]
      if (!keyNode || !valueNode) {
        throw new Error('Invalid begins_with condition')
      }
      const key = keyNode.children
      const value = valueNode.children

      if (!value?.ExpressionAttributeValue?.[0]) {
        throw new Error('Missing begins_with value')
      }

      const attributeName = extractAttributeName(key)
      const valueRef = extractValueRef(value)

      return {
        attributeName,
        operator: 'begins_with' as const,
        value: valueRef,
      }
    }

    // Comparison operators: =, <, >, <=, >=
    const keyNode = ctx.key[0]
    const valueNode = ctx.value[0]
    if (!keyNode || !valueNode) {
      throw new Error('Invalid sort key comparison')
    }
    const key = keyNode.children
    const value = valueNode.children

    if (!value?.ExpressionAttributeValue?.[0]) {
      throw new Error('Missing sort key value')
    }

    const attributeName = extractAttributeName(key)
    const valueRef = extractValueRef(value)

    const operator = ctx.operator?.[0]?.children
    if (!operator) {
      throw new Error('Missing sort key operator')
    }
    let op: '=' | '<' | '>' | '<=' | '>=' = '='

    if (operator.Equals) op = '='
    else if (operator.LessThan) op = '<'
    else if (operator.GreaterThan) op = '>'
    else if (operator.LessThanOrEqual) op = '<='
    else if (operator.GreaterThanOrEqual) op = '>='

    return {
      attributeName,
      operator: op,
      value: valueRef,
    }
  }
}

export const keyConditionVisitor = new KeyConditionVisitor()
