// CST Visitor for Update Expression

import { updateParser } from './update-parser.ts'
import type { CstNode, IToken } from 'chevrotain'
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
} from './ast.ts'

const BaseVisitor = updateParser.getBaseCstVisitorConstructor()

type NodeArray = CstNode[]
type TokenArray = IToken[]

interface UpdateExpressionCtx {
  setClause?: NodeArray[]
  removeClause?: NodeArray[]
  addClause?: NodeArray[]
  deleteClause?: NodeArray[]
}

interface SetClauseCtx {
  action?: NodeArray
}

interface SetActionCtx {
  path: NodeArray
  value: NodeArray
}

interface SetValueCtx {
  operator?: TokenArray
  left?: NodeArray
  right?: NodeArray
  IfNotExists?: TokenArray
  path?: NodeArray
  default?: NodeArray
  ListAppend?: TokenArray
  list1?: NodeArray
  list2?: NodeArray
  operandValue: NodeArray
}

interface RemoveClauseCtx {
  path: NodeArray
}

interface AddClauseCtx {
  action?: NodeArray
}

interface AddActionCtx {
  path: NodeArray
  value: NodeArray
}

interface DeleteClauseCtx {
  action?: NodeArray
}

interface DeleteActionCtx {
  path: NodeArray
  value: NodeArray
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

class UpdateVisitor extends BaseVisitor {
  constructor() {
    super()
    this.validateVisitor()
  }

  updateExpression(ctx: UpdateExpressionCtx): UpdateExpression {
    const result: UpdateExpression = {}

    if (ctx.setClause) {
      const setActions: SetAction[] = []
      for (const clause of ctx.setClause) {
        const actions = this.visit(clause)
        setActions.push(...actions)
      }
      if (setActions.length > 0) {
        result.set = setActions
      }
    }

    if (ctx.removeClause) {
      const removeActions: RemoveAction[] = []
      for (const clause of ctx.removeClause) {
        const actions = this.visit(clause)
        removeActions.push(...actions)
      }
      if (removeActions.length > 0) {
        result.remove = removeActions
      }
    }

    if (ctx.addClause) {
      const addActions: AddAction[] = []
      for (const clause of ctx.addClause) {
        const actions = this.visit(clause)
        addActions.push(...actions)
      }
      if (addActions.length > 0) {
        result.add = addActions
      }
    }

    if (ctx.deleteClause) {
      const deleteActions: DeleteAction[] = []
      for (const clause of ctx.deleteClause) {
        const actions = this.visit(clause)
        deleteActions.push(...actions)
      }
      if (deleteActions.length > 0) {
        result.delete = deleteActions
      }
    }

    return result
  }

  setClause(ctx: SetClauseCtx): SetAction[] {
    if (!ctx.action) return []
    return ctx.action.map((action) => this.visit(action))
  }

  setAction(ctx: SetActionCtx): SetAction {
    return {
      type: 'set',
      path: this.visit(ctx.path),
      value: this.visit(ctx.value),
    }
  }

  setValue(ctx: SetValueCtx): SetValue {
    // Arithmetic expression
    if (ctx.operator) {
      const operatorToken = ctx.operator[0]
      if (!operatorToken) {
        throw new Error('Arithmetic operator missing symbol')
      }
      const operator = operatorToken.image === '+' ? '+' : '-'
      if (!ctx.left || !ctx.right) {
        throw new Error('Arithmetic set expression missing operands')
      }
      return {
        type: 'arithmetic',
        operator,
        left: this.visit(ctx.left),
        right: this.visit(ctx.right),
      } as ArithmeticExpression
    }

    // if_not_exists function
    if (ctx.IfNotExists) {
      if (!ctx.path || !ctx.default) {
        throw new Error('if_not_exists requires path and default value')
      }
      return {
        type: 'if_not_exists',
        path: this.visit(ctx.path),
        defaultValue: this.visit(ctx.default),
      } as IfNotExistsExpression
    }

    // list_append function
    if (ctx.ListAppend) {
      if (!ctx.list1 || !ctx.list2) {
        throw new Error('list_append requires two list operands')
      }
      return {
        type: 'list_append',
        list1: this.visit(ctx.list1),
        list2: this.visit(ctx.list2),
      } as ListAppendExpression
    }

    // Simple value
    return this.visit(ctx.operandValue)
  }

  removeClause(ctx: RemoveClauseCtx): RemoveAction[] {
    return ctx.path.map((pathCtx) => ({
      type: 'remove',
      path: this.visit(pathCtx),
    }))
  }

  addClause(ctx: AddClauseCtx): AddAction[] {
    if (!ctx.action) return []
    return ctx.action.map((action) => this.visit(action))
  }

  addAction(ctx: AddActionCtx): AddAction {
    return {
      type: 'add',
      path: this.visit(ctx.path),
      value: this.visit(ctx.value),
    }
  }

  deleteClause(ctx: DeleteClauseCtx): DeleteAction[] {
    if (!ctx.action) return []
    return ctx.action.map((action) => this.visit(action))
  }

  deleteAction(ctx: DeleteActionCtx): DeleteAction {
    return {
      type: 'delete',
      path: this.visit(ctx.path),
      value: this.visit(ctx.value),
    }
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
      const literal = ctx.StringLiteral[0]
      if (!literal) {
        throw new Error('Missing string literal token')
      }
      const str = literal.image
      return {
        type: 'value',
        value: { S: str.substring(1, str.length - 1) },
      }
    }

    throw new Error('Unknown operand value')
  }
}

export const updateVisitor = new UpdateVisitor()
