// CST Visitor to convert Chevrotain CST to our AST

import { conditionParser } from './condition-parser.ts'
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
} from './ast.ts'

const BaseVisitor = conditionParser.getBaseCstVisitorConstructor()

class ConditionVisitor extends BaseVisitor {
  constructor() {
    super()
    this.validateVisitor()
  }

  conditionExpression(ctx: any): ConditionExpression {
    return this.visit(ctx.orExpression)
  }

  orExpression(ctx: any): ConditionExpression {
    let result = this.visit(ctx.lhs)

    if (ctx.rhs) {
      for (const rhs of ctx.rhs) {
        result = {
          type: 'logical',
          operator: 'OR',
          left: result,
          right: this.visit(rhs),
        } as LogicalExpression
      }
    }

    return result
  }

  andExpression(ctx: any): ConditionExpression {
    let result = this.visit(ctx.lhs)

    if (ctx.rhs) {
      for (const rhs of ctx.rhs) {
        result = {
          type: 'logical',
          operator: 'AND',
          left: result,
          right: this.visit(rhs),
        } as LogicalExpression
      }
    }

    return result
  }

  notExpression(ctx: any): ConditionExpression {
    const operand = this.visit(ctx.comparisonExpression)

    if (ctx.Not) {
      return {
        type: 'not',
        operand,
      } as NotExpression
    }

    return operand
  }

  comparisonExpression(ctx: any): ConditionExpression {
    // Parenthesized expression
    if (ctx.conditionExpression) {
      return this.visit(ctx.conditionExpression)
    }

    // BETWEEN expression
    if (ctx.Between) {
      return {
        type: 'between',
        value: this.visit(ctx.value),
        lower: this.visit(ctx.lower),
        upper: this.visit(ctx.upper),
      } as BetweenExpression
    }

    // IN expression
    if (ctx.In) {
      return {
        type: 'in',
        value: this.visit(ctx.value),
        list: ctx.listItem.map((item: any) => this.visit(item)),
      } as InExpression
    }

    // Function call
    if (ctx.functionCall) {
      return this.visit(ctx.functionCall)
    }

    // Regular comparison
    if (ctx.comparisonOperator) {
      const operatorToken = this.visit(ctx.comparisonOperator)
      return {
        type: 'comparison',
        operator: operatorToken,
        left: this.visit(ctx.attributePath),
        right: this.visit(ctx.operandValue),
      } as ComparisonExpression
    }

    throw new Error('Unknown comparison expression')
  }

  comparisonOperator(ctx: any): '=' | '<>' | '<' | '>' | '<=' | '>=' {
    if (ctx.Equals) return '='
    if (ctx.NotEquals) return '<>'
    if (ctx.LessThan) return '<'
    if (ctx.GreaterThan) return '>'
    if (ctx.LessThanOrEqual) return '<='
    if (ctx.GreaterThanOrEqual) return '>='
    throw new Error('Unknown comparison operator')
  }

  functionCall(ctx: any): FunctionExpression {
    if (ctx.AttributeExists) {
      return {
        type: 'function',
        name: 'attribute_exists',
        args: [this.visit(ctx.attributePath)],
      }
    }

    if (ctx.AttributeNotExists) {
      return {
        type: 'function',
        name: 'attribute_not_exists',
        args: [this.visit(ctx.attributePath)],
      }
    }

    if (ctx.BeginsWith) {
      return {
        type: 'function',
        name: 'begins_with',
        args: [this.visit(ctx.attributePath), this.visit(ctx.operandValue)],
      }
    }

    if (ctx.Contains) {
      return {
        type: 'function',
        name: 'contains',
        args: [this.visit(ctx.attributePath), this.visit(ctx.operandValue)],
      }
    }

    if (ctx.Size) {
      // size(path) op value - create a special comparison
      return {
        type: 'function',
        name: 'size',
        args: [
          this.visit(ctx.attributePath),
          this.visit(ctx.comparisonOperator),
          this.visit(ctx.operandValue),
        ],
      }
    }

    if (ctx.AttributeType) {
      return {
        type: 'function',
        name: 'attribute_type',
        args: [this.visit(ctx.attributePath), this.visit(ctx.operandValue)],
      }
    }

    throw new Error('Unknown function')
  }

  attributePath(ctx: any): AttributePath {
    const token = ctx.ExpressionAttributeName?.[0] || ctx.Identifier?.[0]
    return {
      type: 'attribute_path',
      name: token.image,
    }
  }

  operandValue(ctx: any): Value {
    if (ctx.ExpressionAttributeValue) {
      return {
        type: 'value',
        value: ctx.ExpressionAttributeValue[0].image,
      }
    }

    if (ctx.NumberLiteral) {
      return {
        type: 'value',
        value: { N: ctx.NumberLiteral[0].image },
      }
    }

    if (ctx.StringLiteral) {
      // Remove quotes
      const str = ctx.StringLiteral[0].image
      return {
        type: 'value',
        value: { S: str.substring(1, str.length - 1) },
      }
    }

    throw new Error('Unknown operand value')
  }
}

export const conditionVisitor = new ConditionVisitor()
