// CST Visitor for Update Expression

import { updateParser } from "./update-parser.ts";
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
} from "./ast.ts";

const BaseVisitor = updateParser.getBaseCstVisitorConstructor();

class UpdateVisitor extends BaseVisitor {
  constructor() {
    super();
    this.validateVisitor();
  }

  updateExpression(ctx: any): UpdateExpression {
    const result: UpdateExpression = {};

    if (ctx.setClause) {
      const setActions: SetAction[] = [];
      for (const clause of ctx.setClause) {
        const actions = this.visit(clause);
        setActions.push(...actions);
      }
      if (setActions.length > 0) {
        result.set = setActions;
      }
    }

    if (ctx.removeClause) {
      const removeActions: RemoveAction[] = [];
      for (const clause of ctx.removeClause) {
        const actions = this.visit(clause);
        removeActions.push(...actions);
      }
      if (removeActions.length > 0) {
        result.remove = removeActions;
      }
    }

    if (ctx.addClause) {
      const addActions: AddAction[] = [];
      for (const clause of ctx.addClause) {
        const actions = this.visit(clause);
        addActions.push(...actions);
      }
      if (addActions.length > 0) {
        result.add = addActions;
      }
    }

    if (ctx.deleteClause) {
      const deleteActions: DeleteAction[] = [];
      for (const clause of ctx.deleteClause) {
        const actions = this.visit(clause);
        deleteActions.push(...actions);
      }
      if (deleteActions.length > 0) {
        result.delete = deleteActions;
      }
    }

    return result;
  }

  setClause(ctx: any): SetAction[] {
    return ctx.action.map((action: any) => this.visit(action));
  }

  setAction(ctx: any): SetAction {
    return {
      type: "set",
      path: this.visit(ctx.path),
      value: this.visit(ctx.value),
    };
  }

  setValue(ctx: any): SetValue {
    // Arithmetic expression
    if (ctx.operator) {
      const operator = ctx.operator[0].image === "+" ? "+" : "-";
      return {
        type: "arithmetic",
        operator,
        left: this.visit(ctx.left),
        right: this.visit(ctx.right),
      } as ArithmeticExpression;
    }

    // if_not_exists function
    if (ctx.IfNotExists) {
      return {
        type: "if_not_exists",
        path: this.visit(ctx.path),
        defaultValue: this.visit(ctx.default),
      } as IfNotExistsExpression;
    }

    // list_append function
    if (ctx.ListAppend) {
      return {
        type: "list_append",
        list1: this.visit(ctx.list1),
        list2: this.visit(ctx.list2),
      } as ListAppendExpression;
    }

    // Simple value
    return this.visit(ctx.operandValue);
  }

  removeClause(ctx: any): RemoveAction[] {
    return ctx.path.map((pathCtx: any) => ({
      type: "remove",
      path: this.visit(pathCtx),
    } as RemoveAction));
  }

  addClause(ctx: any): AddAction[] {
    return ctx.action.map((action: any) => this.visit(action));
  }

  addAction(ctx: any): AddAction {
    return {
      type: "add",
      path: this.visit(ctx.path),
      value: this.visit(ctx.value),
    };
  }

  deleteClause(ctx: any): DeleteAction[] {
    return ctx.action.map((action: any) => this.visit(action));
  }

  deleteAction(ctx: any): DeleteAction {
    return {
      type: "delete",
      path: this.visit(ctx.path),
      value: this.visit(ctx.value),
    };
  }

  attributePath(ctx: any): AttributePath {
    const token = ctx.ExpressionAttributeName?.[0] || ctx.Identifier?.[0];
    return {
      type: "attribute_path",
      name: token.image,
    };
  }

  operandValue(ctx: any): Value {
    if (ctx.ExpressionAttributeValue) {
      return {
        type: "value",
        value: ctx.ExpressionAttributeValue[0].image,
      };
    }

    if (ctx.NumberLiteral) {
      return {
        type: "value",
        value: { N: ctx.NumberLiteral[0].image },
      };
    }

    if (ctx.StringLiteral) {
      // Remove quotes
      const str = ctx.StringLiteral[0].image;
      return {
        type: "value",
        value: { S: str.substring(1, str.length - 1) },
      };
    }

    throw new Error("Unknown operand value");
  }
}

export const updateVisitor = new UpdateVisitor();
