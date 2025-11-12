// KeyCondition Expression Parser using Chevrotain
// Parses expressions like: "pk = :val AND sk < :val2"

import { CstParser } from "chevrotain";
import {
  allTokens,
  And,
  Between,
  Equals,
  LessThan,
  GreaterThan,
  LessThanOrEqual,
  GreaterThanOrEqual,
  ExpressionAttributeName,
  ExpressionAttributeValue,
  Identifier,
  BeginsWith,
  LParen,
  RParen,
  Comma,
} from "./lexer.ts";

class KeyConditionExpressionParser extends CstParser {
  constructor() {
    super(allTokens);
    this.performSelfAnalysis();
  }

  // Top-level rule: partition key condition optionally followed by sort key condition
  public keyConditionExpression = this.RULE("keyConditionExpression", () => {
    // Partition key must be an equality
    this.SUBRULE(this.partitionKeyCondition);

    // Optional sort key condition
    this.OPTION(() => {
      this.CONSUME(And);
      this.SUBRULE(this.sortKeyCondition);
    });
  });

  // Partition key condition: must be equality
  private partitionKeyCondition = this.RULE("partitionKeyCondition", () => {
    this.SUBRULE(this.attributePath, { LABEL: "key" });
    this.CONSUME(Equals);
    this.SUBRULE(this.value, { LABEL: "value" });
  });

  // Sort key condition: supports various comparison operators
  private sortKeyCondition = this.RULE("sortKeyCondition", () => {
    this.OR([
      // BETWEEN: sk BETWEEN :val1 AND :val2
      {
        ALT: () => {
          this.SUBRULE(this.attributePath, { LABEL: "key" });
          this.CONSUME(Between);
          this.SUBRULE(this.value, { LABEL: "value1" });
          this.CONSUME(And);
          this.SUBRULE2(this.value, { LABEL: "value2" });
        },
      },
      // begins_with(sk, :val)
      {
        ALT: () => {
          this.CONSUME(BeginsWith);
          this.CONSUME(LParen);
          this.SUBRULE2(this.attributePath, { LABEL: "key" });
          this.CONSUME(Comma);
          this.SUBRULE3(this.value, { LABEL: "value" });
          this.CONSUME(RParen);
        },
      },
      // Comparison operators: =, <, >, <=, >=
      {
        ALT: () => {
          this.SUBRULE3(this.attributePath, { LABEL: "key" });
          this.SUBRULE(this.comparisonOperator, { LABEL: "operator" });
          this.SUBRULE4(this.value, { LABEL: "value" });
        },
      },
    ]);
  });

  private comparisonOperator = this.RULE("comparisonOperator", () => {
    this.OR([
      { ALT: () => this.CONSUME(Equals) },
      { ALT: () => this.CONSUME(LessThan) },
      { ALT: () => this.CONSUME(GreaterThan) },
      { ALT: () => this.CONSUME(LessThanOrEqual) },
      { ALT: () => this.CONSUME(GreaterThanOrEqual) },
    ]);
  });

  private attributePath = this.RULE("attributePath", () => {
    this.OR([
      { ALT: () => this.CONSUME(ExpressionAttributeName) },
      { ALT: () => this.CONSUME(Identifier) },
    ]);
  });

  private value = this.RULE("value", () => {
    this.CONSUME(ExpressionAttributeValue);
  });
}

// Singleton instance
export const keyConditionParser = new KeyConditionExpressionParser();
