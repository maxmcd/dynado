// Evaluator for KeyCondition expressions

import { expressionLexer } from "./lexer.ts";
import { keyConditionParser } from "./key-condition-parser.ts";
import { keyConditionVisitor, type KeyConditionAST } from "./key-condition-visitor.ts";
import type { DynamoDBItem } from "../types.ts";

// DynamoDB attribute value type
type DynamoDBAttributeValue = any;

function resolveAttributeName(
  name: string,
  expressionAttributeNames?: Record<string, string>
): string {
  if (name.startsWith("#") && expressionAttributeNames) {
    return expressionAttributeNames[name] || name;
  }
  return name;
}

function resolveAttributeValue(
  ref: string,
  expressionAttributeValues?: Record<string, DynamoDBAttributeValue>
): DynamoDBAttributeValue | undefined {
  if (ref.startsWith(":") && expressionAttributeValues) {
    return expressionAttributeValues[ref];
  }
  return undefined;
}

function compareValues(
  itemValue: DynamoDBAttributeValue,
  compareValue: DynamoDBAttributeValue,
  operator: string
): boolean {
  // Handle string comparisons
  if (itemValue.S !== undefined && compareValue.S !== undefined) {
    const a = itemValue.S;
    const b = compareValue.S;

    switch (operator) {
      case "=":
        return a === b;
      case "<":
        return a < b;
      case ">":
        return a > b;
      case "<=":
        return a <= b;
      case ">=":
        return a >= b;
      case "begins_with":
        return a.startsWith(b);
      default:
        return false;
    }
  }

  // Handle number comparisons
  if (itemValue.N !== undefined && compareValue.N !== undefined) {
    const a = parseFloat(itemValue.N);
    const b = parseFloat(compareValue.N);

    switch (operator) {
      case "=":
        return a === b;
      case "<":
        return a < b;
      case ">":
        return a > b;
      case "<=":
        return a <= b;
      case ">=":
        return a >= b;
      default:
        return false;
    }
  }

  return false;
}

export function evaluateKeyCondition(
  item: DynamoDBItem,
  keyConditionExpression: string,
  expressionAttributeNames?: Record<string, string>,
  expressionAttributeValues?: Record<string, DynamoDBAttributeValue>
): boolean {
  if (!keyConditionExpression) {
    return true;
  }

  // Lex and parse
  const lexResult = expressionLexer.tokenize(keyConditionExpression);
  if (lexResult.errors.length > 0) {
    throw new Error(
      `Lexer error: ${lexResult.errors.map((e) => e.message).join(", ")}`
    );
  }

  keyConditionParser.input = lexResult.tokens;
  const cst = keyConditionParser.keyConditionExpression();

  if (keyConditionParser.errors.length > 0) {
    throw new Error(
      `Parser error: ${keyConditionParser.errors.map((e) => e.message).join(", ")}`
    );
  }

  // Visit CST to get AST
  const ast: KeyConditionAST = keyConditionVisitor.visit(cst);

  // Evaluate partition key condition
  const pkName = resolveAttributeName(
    ast.partitionKey.attributeName,
    expressionAttributeNames
  );
  const pkValue = resolveAttributeValue(
    ast.partitionKey.value,
    expressionAttributeValues
  );

  if (!pkValue) {
    throw new Error(
      `Missing value for ${ast.partitionKey.value} in ExpressionAttributeValues`
    );
  }

  const itemPkValue = item[pkName];
  if (!itemPkValue || JSON.stringify(itemPkValue) !== JSON.stringify(pkValue)) {
    return false;
  }

  // If no sort key condition, we're done
  if (!ast.sortKey) {
    return true;
  }

  // Evaluate sort key condition
  const skName = resolveAttributeName(
    ast.sortKey.attributeName,
    expressionAttributeNames
  );
  const skValue = resolveAttributeValue(
    ast.sortKey.value,
    expressionAttributeValues
  );

  if (!skValue) {
    throw new Error(
      `Missing value for ${ast.sortKey.value} in ExpressionAttributeValues`
    );
  }

  const itemSkValue = item[skName];
  if (!itemSkValue) {
    return false;
  }

  // Handle BETWEEN
  if (ast.sortKey.operator === "BETWEEN" && ast.sortKey.value2) {
    const skValue2 = resolveAttributeValue(
      ast.sortKey.value2,
      expressionAttributeValues
    );
    if (!skValue2) {
      throw new Error(
        `Missing value for ${ast.sortKey.value2} in ExpressionAttributeValues`
      );
    }

    const ge = compareValues(itemSkValue, skValue, ">=");
    const le = compareValues(itemSkValue, skValue2, "<=");
    return ge && le;
  }

  // Handle other operators
  return compareValues(itemSkValue, skValue, ast.sortKey.operator);
}
