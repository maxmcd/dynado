// Visitor to convert KeyCondition CST to AST

import { keyConditionParser } from './key-condition-parser.ts'

const BaseVisitor = keyConditionParser.getBaseCstVisitorConstructor()

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

  keyConditionExpression(ctx: any): KeyConditionAST {
    const partitionKey = this.visit(ctx.partitionKeyCondition)
    const sortKey = ctx.sortKeyCondition
      ? this.visit(ctx.sortKeyCondition)
      : undefined

    return { partitionKey, sortKey }
  }

  partitionKeyCondition(ctx: any) {
    const key = ctx.key[0].children
    const value = ctx.value[0].children

    const attributeName = key.ExpressionAttributeName
      ? key.ExpressionAttributeName[0].image
      : key.Identifier[0].image

    const valueRef = value.ExpressionAttributeValue[0].image

    return {
      attributeName,
      value: valueRef,
    }
  }

  sortKeyCondition(ctx: any) {
    // BETWEEN: sk BETWEEN :val1 AND :val2
    if (ctx.Between) {
      const key = ctx.key[0].children
      const value = ctx.value1[0].children
      const value2 = ctx.value2[0].children

      const attributeName = key.ExpressionAttributeName
        ? key.ExpressionAttributeName[0].image
        : key.Identifier[0].image

      const valueRef = value.ExpressionAttributeValue[0].image
      const valueRef2 = value2.ExpressionAttributeValue[0].image

      return {
        attributeName,
        operator: 'BETWEEN' as const,
        value: valueRef,
        value2: valueRef2,
      }
    }

    // begins_with(sk, :val)
    if (ctx.BeginsWith) {
      const key = ctx.key[0].children
      const value = ctx.value[0].children

      const attributeName = key.ExpressionAttributeName
        ? key.ExpressionAttributeName[0].image
        : key.Identifier[0].image

      const valueRef = value.ExpressionAttributeValue[0].image

      return {
        attributeName,
        operator: 'begins_with' as const,
        value: valueRef,
      }
    }

    // Comparison operators: =, <, >, <=, >=
    const key = ctx.key[0].children
    const value = ctx.value[0].children

    const attributeName = key.ExpressionAttributeName
      ? key.ExpressionAttributeName[0].image
      : key.Identifier[0].image

    const valueRef = value.ExpressionAttributeValue[0].image

    const operator = ctx.operator[0].children
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
