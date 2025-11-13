// CST Visitor to convert Chevrotain CST to our AST

import { conditionParser } from './condition-parser.ts'
import type { CstNode, IToken } from 'chevrotain'
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
  ComparisonOperator,
} from './ast.ts'

const BaseVisitor = conditionParser.getBaseCstVisitorConstructor()

type NodeArray = CstNode[]
type TokenArray = IToken[]

interface ConditionExpressionCtx {
  orExpression: NodeArray
}

interface BinaryExpressionCtx {
  lhs: NodeArray
  rhs?: NodeArray[]
}

interface NotExpressionCtx {
  comparisonExpression: NodeArray
  Not?: TokenArray
}

interface ComparisonExpressionCtx {
  conditionExpression?: NodeArray
  Between?: TokenArray
  value?: NodeArray
  lower?: NodeArray
  upper?: NodeArray
  In?: TokenArray
  listItem?: NodeArray[]
  functionCall?: NodeArray
  comparisonOperator?: NodeArray
  attributePath?: NodeArray
  operandValue?: NodeArray
}

interface ComparisonOperatorCtx {
  Equals?: TokenArray
  NotEquals?: TokenArray
  LessThan?: TokenArray
  GreaterThan?: TokenArray
  LessThanOrEqual?: TokenArray
  GreaterThanOrEqual?: TokenArray
}

interface FunctionCallCtx {
  AttributeExists?: TokenArray
  AttributeNotExists?: TokenArray
  BeginsWith?: TokenArray
  Contains?: TokenArray
  Size?: TokenArray
  AttributeType?: TokenArray
  attributePath: NodeArray
  operandValue?: NodeArray
  comparisonOperator?: NodeArray
}

interface AttributePathCtx {
  ExpressionAttributeName?: TokenArray
  Identifier?: TokenArray
}

interface OperandValueCtx {
  ExpressionAttributeValue?: TokenArray
  NumberLiteral?: TokenArray
  StringLiteral?: TokenArray
}

class ConditionVisitor extends BaseVisitor {
  constructor() {
    super()
    this.validateVisitor()
  }

  conditionExpression(ctx: ConditionExpressionCtx): ConditionExpression {
    return this.visit(ctx.orExpression)
  }

  orExpression(ctx: BinaryExpressionCtx): ConditionExpression {
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

  andExpression(ctx: BinaryExpressionCtx): ConditionExpression {
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

  notExpression(ctx: NotExpressionCtx): ConditionExpression {
    const operand = this.visit(ctx.comparisonExpression)

    if (ctx.Not) {
      return {
        type: 'not',
        operand,
      } as NotExpression
    }

    return operand
  }

  comparisonExpression(ctx: ComparisonExpressionCtx): ConditionExpression {
    // Parenthesized expression
    if (ctx.conditionExpression) {
      return this.visit(ctx.conditionExpression)
    }

    // BETWEEN expression
    if (ctx.Between) {
      const valueNode = ctx.value
      const lowerNode = ctx.lower
      const upperNode = ctx.upper

      if (!valueNode || !lowerNode || !upperNode) {
        throw new Error('Invalid BETWEEN expression')
      }
      return {
        type: 'between',
        value: this.visit(valueNode),
        lower: this.visit(lowerNode),
        upper: this.visit(upperNode),
      } as BetweenExpression
    }

    // IN expression
    if (ctx.In) {
      const valueNode = ctx.value
      const listItems = ctx.listItem
      if (!valueNode || !listItems) {
        throw new Error('Invalid IN expression')
      }
      return {
        type: 'in',
        value: this.visit(valueNode),
        list: listItems.map((item) => this.visit(item)),
      } as InExpression
    }

    // Function call
    if (ctx.functionCall) {
      return this.visit(ctx.functionCall)
    }

    // Regular comparison
    if (ctx.comparisonOperator && ctx.attributePath && ctx.operandValue) {
      const operatorToken = this.visit(
        ctx.comparisonOperator
      ) as ComparisonOperator
      return {
        type: 'comparison',
        operator: operatorToken,
        left: this.visit(ctx.attributePath),
        right: this.visit(ctx.operandValue),
      } as ComparisonExpression
    }

    throw new Error('Unknown comparison expression')
  }

  comparisonOperator(ctx: ComparisonOperatorCtx): ComparisonOperator {
    if (ctx.Equals) return '='
    if (ctx.NotEquals) return '<>'
    if (ctx.LessThan) return '<'
    if (ctx.GreaterThan) return '>'
    if (ctx.LessThanOrEqual) return '<='
    if (ctx.GreaterThanOrEqual) return '>='
    throw new Error('Unknown comparison operator')
  }

  functionCall(ctx: FunctionCallCtx): FunctionExpression {
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
      if (!ctx.operandValue) {
        throw new Error('begins_with requires operand value')
      }
      return {
        type: 'function',
        name: 'begins_with',
        args: [this.visit(ctx.attributePath), this.visit(ctx.operandValue)],
      }
    }

    if (ctx.Contains) {
      if (!ctx.operandValue) {
        throw new Error('contains requires operand value')
      }
      return {
        type: 'function',
        name: 'contains',
        args: [this.visit(ctx.attributePath), this.visit(ctx.operandValue)],
      }
    }

    if (ctx.Size) {
      if (!ctx.operandValue) {
        throw new Error('size() requires operand value')
      }
      // size(path) op value - create a special comparison
      return {
        type: 'function',
        name: 'size',
        args: [
          this.visit(ctx.attributePath),
          ctx.comparisonOperator
            ? (this.visit(ctx.comparisonOperator) as ComparisonOperator)
            : '=',
          this.visit(ctx.operandValue),
        ],
      }
    }

    if (ctx.AttributeType) {
      if (!ctx.operandValue) {
        throw new Error('attribute_type requires operand value')
      }
      return {
        type: 'function',
        name: 'attribute_type',
        args: [this.visit(ctx.attributePath), this.visit(ctx.operandValue)],
      }
    }

    throw new Error('Unknown function')
  }

  attributePath(ctx: AttributePathCtx): AttributePath {
    const token = ctx.ExpressionAttributeName?.[0] || ctx.Identifier?.[0]
    if (!token) {
      throw new Error('Attribute path missing identifier')
    }
    return {
      type: 'attribute_path',
      name: token.image,
    }
  }

  operandValue(ctx: OperandValueCtx): Value {
    if (ctx.ExpressionAttributeValue) {
      if (!ctx.ExpressionAttributeValue[0]) {
        throw new Error('Missing expression attribute value token')
      }
      return {
        type: 'value',
        value: ctx.ExpressionAttributeValue[0].image,
      }
    }

    if (ctx.NumberLiteral) {
      if (!ctx.NumberLiteral[0]) {
        throw new Error('Missing number literal token')
      }
      return {
        type: 'value',
        value: { N: ctx.NumberLiteral[0].image },
      }
    }

    if (ctx.StringLiteral) {
      // Remove quotes
      const token = ctx.StringLiteral[0]
      if (!token) {
        throw new Error('Missing string literal token')
      }
      const str = token.image
      return {
        type: 'value',
        value: { S: str.substring(1, str.length - 1) },
      }
    }

    throw new Error('Unknown operand value')
  }
}

export const conditionVisitor = new ConditionVisitor()
