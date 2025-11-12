// Condition Expression Parser using Chevrotain

import { CstParser, CstNode, IToken } from "chevrotain";
import {
  allTokens,
  And,
  Or,
  Not,
  Between,
  In,
  Equals,
  NotEquals,
  LessThan,
  GreaterThan,
  LessThanOrEqual,
  GreaterThanOrEqual,
  LParen,
  RParen,
  Comma,
  ExpressionAttributeName,
  ExpressionAttributeValue,
  Identifier,
  AttributeExists,
  AttributeNotExists,
  BeginsWith,
  Contains,
  Size,
  AttributeType,
  NumberLiteral,
  StringLiteral,
} from "./lexer.ts";

class ConditionExpressionParser extends CstParser {
  constructor() {
    super(allTokens);
    this.performSelfAnalysis();
  }

  // Top-level rule: OR has lowest precedence
  public conditionExpression = this.RULE("conditionExpression", () => {
    this.SUBRULE(this.orExpression);
  });

  // OR expression
  private orExpression = this.RULE("orExpression", () => {
    this.SUBRULE(this.andExpression, { LABEL: "lhs" });
    this.MANY(() => {
      this.CONSUME(Or);
      this.SUBRULE2(this.andExpression, { LABEL: "rhs" });
    });
  });

  // AND expression
  private andExpression = this.RULE("andExpression", () => {
    this.SUBRULE(this.notExpression, { LABEL: "lhs" });
    this.MANY(() => {
      this.CONSUME(And);
      this.SUBRULE2(this.notExpression, { LABEL: "rhs" });
    });
  });

  // NOT expression
  private notExpression = this.RULE("notExpression", () => {
    this.OPTION(() => {
      this.CONSUME(Not);
    });
    this.SUBRULE(this.comparisonExpression);
  });

  // Comparison and other atomic expressions
  private comparisonExpression = this.RULE("comparisonExpression", () => {
    this.OR([
      // Parenthesized expression
      { ALT: () => {
        this.CONSUME(LParen);
        this.SUBRULE(this.conditionExpression);
        this.CONSUME(RParen);
      }},

      // BETWEEN expression
      { ALT: () => {
        this.SUBRULE(this.attributePath, { LABEL: "value" });
        this.CONSUME(Between);
        this.SUBRULE(this.operandValue, { LABEL: "lower" });
        this.CONSUME(And);
        this.SUBRULE2(this.operandValue, { LABEL: "upper" });
      }},

      // IN expression
      { ALT: () => {
        this.SUBRULE2(this.attributePath, { LABEL: "value" });
        this.CONSUME(In);
        this.CONSUME2(LParen);
        this.SUBRULE3(this.operandValue, { LABEL: "listItem" });
        this.MANY(() => {
          this.CONSUME(Comma);
          this.SUBRULE4(this.operandValue, { LABEL: "listItem" });
        });
        this.CONSUME2(RParen);
      }},

      // Function call
      { ALT: () => {
        this.SUBRULE(this.functionCall);
      }},

      // Comparison (attr op value)
      { ALT: () => {
        this.SUBRULE3(this.attributePath);
        this.SUBRULE(this.comparisonOperator);
        this.SUBRULE5(this.operandValue);
      }},
    ]);
  });

  // Comparison operators
  private comparisonOperator = this.RULE("comparisonOperator", () => {
    this.OR([
      { ALT: () => this.CONSUME(Equals) },
      { ALT: () => this.CONSUME(NotEquals) },
      { ALT: () => this.CONSUME(LessThanOrEqual) },
      { ALT: () => this.CONSUME(GreaterThanOrEqual) },
      { ALT: () => this.CONSUME(LessThan) },
      { ALT: () => this.CONSUME(GreaterThan) },
    ]);
  });

  // Function calls
  private functionCall = this.RULE("functionCall", () => {
    this.OR([
      // attribute_exists(path)
      { ALT: () => {
        this.CONSUME(AttributeExists);
        this.CONSUME(LParen);
        this.SUBRULE(this.attributePath);
        this.CONSUME(RParen);
      }},

      // attribute_not_exists(path)
      { ALT: () => {
        this.CONSUME(AttributeNotExists);
        this.CONSUME2(LParen);
        this.SUBRULE2(this.attributePath);
        this.CONSUME2(RParen);
      }},

      // begins_with(path, value)
      { ALT: () => {
        this.CONSUME(BeginsWith);
        this.CONSUME3(LParen);
        this.SUBRULE3(this.attributePath);
        this.CONSUME(Comma);
        this.SUBRULE(this.operandValue);
        this.CONSUME3(RParen);
      }},

      // contains(path, value)
      { ALT: () => {
        this.CONSUME(Contains);
        this.CONSUME4(LParen);
        this.SUBRULE4(this.attributePath);
        this.CONSUME2(Comma);
        this.SUBRULE2(this.operandValue);
        this.CONSUME4(RParen);
      }},

      // size(path) - returns a number, used in comparisons
      { ALT: () => {
        this.CONSUME(Size);
        this.CONSUME5(LParen);
        this.SUBRULE5(this.attributePath);
        this.CONSUME5(RParen);
        this.SUBRULE3(this.comparisonOperator);
        this.SUBRULE3(this.operandValue);
      }},

      // attribute_type(path, type)
      { ALT: () => {
        this.CONSUME(AttributeType);
        this.CONSUME6(LParen);
        this.SUBRULE6(this.attributePath);
        this.CONSUME3(Comma);
        this.SUBRULE4(this.operandValue);
        this.CONSUME6(RParen);
      }},
    ]);
  });

  // Attribute path (for now, just a simple name)
  private attributePath = this.RULE("attributePath", () => {
    this.OR([
      { ALT: () => this.CONSUME(ExpressionAttributeName) },
      { ALT: () => this.CONSUME(Identifier) },
    ]);
  });

  // Operand value (expression attribute value or literal)
  private operandValue = this.RULE("operandValue", () => {
    this.OR([
      { ALT: () => this.CONSUME(ExpressionAttributeValue) },
      { ALT: () => this.CONSUME(NumberLiteral) },
      { ALT: () => this.CONSUME(StringLiteral) },
    ]);
  });
}

// Export singleton instance
export const conditionParser = new ConditionExpressionParser();
