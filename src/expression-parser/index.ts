// Public API for expression parser

import { expressionLexer } from './lexer.ts'
import { conditionParser } from './condition-parser.ts'
import { conditionVisitor } from './condition-visitor.ts'
import { evaluateCondition } from './evaluator.ts'
import { updateParser } from './update-parser.ts'
import { updateVisitor } from './update-visitor.ts'
import { applyUpdateExpression } from './update-evaluator.ts'
import type {
  ConditionExpression,
  UpdateExpression,
  EvaluationContext,
} from './ast.ts'
import type { DynamoDBItem } from '../types.ts'
import type { AttributeValue } from '@aws-sdk/client-dynamodb'

/**
 * Parse and evaluate a DynamoDB ConditionExpression
 */
export function evaluateConditionExpression(
  item: DynamoDBItem | null,
  conditionExpression?: string,
  expressionAttributeNames?: Record<string, string>,
  expressionAttributeValues?: Record<string, AttributeValue>
): boolean {
  // Empty or undefined expression always passes
  if (!conditionExpression || conditionExpression.trim() === '') {
    return true
  }

  try {
    // Lexing
    const lexResult = expressionLexer.tokenize(conditionExpression)

    if (lexResult.errors.length > 0) {
      const error = lexResult.errors[0]
      throw new Error(
        `Lexer error at line ${error?.line}, column ${error?.column}: ${error?.message}`
      )
    }

    // Parsing
    conditionParser.input = lexResult.tokens
    const cst = conditionParser.conditionExpression()

    if (conditionParser.errors.length > 0) {
      const error = conditionParser.errors[0]
      throw new Error(
        `Parser error at token "${error?.token?.image}": ${error?.message}`
      )
    }

    // Convert CST to AST
    const ast = conditionVisitor.visit(cst) as ConditionExpression

    // Evaluate AST
    const context: EvaluationContext = {
      item,
      expressionAttributeNames,
      expressionAttributeValues,
    }

    return evaluateCondition(ast, context)
  } catch (error: unknown) {
    // Provide helpful error message
    const message =
      error instanceof Error ? error.message : 'Unknown evaluation error'
    throw new Error(
      `Failed to evaluate condition expression "${conditionExpression}": ${message}`
    )
  }
}

/**
 * Parse and apply a DynamoDB UpdateExpression to an item
 */
export function applyUpdateExpressionToItem(
  item: DynamoDBItem,
  updateExpression: string,
  expressionAttributeNames?: Record<string, string>,
  expressionAttributeValues?: Record<string, AttributeValue>
): DynamoDBItem {
  if (!updateExpression || updateExpression.trim() === '') {
    return item
  }

  try {
    // Lexing
    const lexResult = expressionLexer.tokenize(updateExpression)

    if (lexResult.errors.length > 0) {
      const error = lexResult.errors[0]
      throw new Error(
        `Lexer error at line ${error?.line}, column ${error?.column}: ${error?.message}`
      )
    }

    // Parsing
    updateParser.input = lexResult.tokens
    const cst = updateParser.updateExpression()

    if (updateParser.errors.length > 0) {
      const error = updateParser.errors[0]
      throw new Error(
        `Parser error at token "${error?.token?.image}": ${error?.message}`
      )
    }

    // Convert CST to AST
    const ast = updateVisitor.visit(cst) as UpdateExpression

    // Apply update
    const context: EvaluationContext = {
      item,
      expressionAttributeNames,
      expressionAttributeValues,
    }

    return applyUpdateExpression(item, ast, context)
  } catch (error: unknown) {
    // Provide helpful error message
    const message =
      error instanceof Error ? error.message : 'Unknown update error'
    throw new Error(
      `Failed to apply update expression "${updateExpression}": ${message}`
    )
  }
}

// Re-export types for convenience
export type {
  ConditionExpression,
  UpdateExpression,
  EvaluationContext,
} from './ast.ts'
