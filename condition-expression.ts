// Condition expression parser and evaluator for DynamoDB

import type { DynamoDBItem } from "./storage.ts";

export function evaluateConditionExpression(
  item: DynamoDBItem | null,
  conditionExpression?: string,
  expressionAttributeNames?: Record<string, string>,
  expressionAttributeValues?: Record<string, any>
): boolean {
  if (!conditionExpression) return true;

  // Resolve attribute names and values
  const resolveAttributeName = (name: string): string => {
    if (expressionAttributeNames && name.startsWith("#")) {
      return expressionAttributeNames[name] || name;
    }
    return name;
  };

  const resolveAttributeValue = (valueRef: string): any => {
    if (expressionAttributeValues && valueRef.startsWith(":")) {
      return expressionAttributeValues[valueRef];
    }
    return undefined;
  };

  const getAttributeValue = (item: DynamoDBItem | null, attrName: string): any => {
    if (!item) return undefined;
    return item[attrName];
  };

  // Handle logical operators (AND, OR, NOT)
  if (conditionExpression.includes(" AND ")) {
    const parts = splitByLogicalOperator(conditionExpression, " AND ");
    return parts.every(part =>
      evaluateConditionExpression(item, part.trim(), expressionAttributeNames, expressionAttributeValues)
    );
  }

  if (conditionExpression.includes(" OR ")) {
    const parts = splitByLogicalOperator(conditionExpression, " OR ");
    return parts.some(part =>
      evaluateConditionExpression(item, part.trim(), expressionAttributeNames, expressionAttributeValues)
    );
  }

  if (conditionExpression.trim().startsWith("NOT ")) {
    const innerExpression = conditionExpression.trim().substring(4).trim();
    return !evaluateConditionExpression(item, innerExpression, expressionAttributeNames, expressionAttributeValues);
  }

  // Handle function calls
  const expr = conditionExpression.trim();

  // attribute_not_exists(path)
  const attrNotExistsMatch = expr.match(/^attribute_not_exists\s*\(\s*([#\w]+)\s*\)$/);
  if (attrNotExistsMatch && attrNotExistsMatch[1]) {
    const attrName = resolveAttributeName(attrNotExistsMatch[1]);
    return item === null || !(attrName in item);
  }

  // attribute_exists(path)
  const attrExistsMatch = expr.match(/^attribute_exists\s*\(\s*([#\w]+)\s*\)$/);
  if (attrExistsMatch && attrExistsMatch[1]) {
    const attrName = resolveAttributeName(attrExistsMatch[1]);
    return item !== null && attrName in item;
  }

  // begins_with(path, value)
  const beginsWithMatch = expr.match(/^begins_with\s*\(\s*([#\w]+)\s*,\s*(:\w+)\s*\)$/);
  if (beginsWithMatch && beginsWithMatch[1] && beginsWithMatch[2]) {
    const attrName = resolveAttributeName(beginsWithMatch[1]);
    const value = resolveAttributeValue(beginsWithMatch[2]);
    const itemValue = getAttributeValue(item, attrName);

    if (!itemValue || !value) return false;

    // Handle DynamoDB string format
    const itemStr = itemValue.S || itemValue;
    const valueStr = value.S || value;

    if (typeof itemStr === "string" && typeof valueStr === "string") {
      return itemStr.startsWith(valueStr);
    }
    return false;
  }

  // contains(path, value)
  const containsMatch = expr.match(/^contains\s*\(\s*([#\w]+)\s*,\s*(:\w+)\s*\)$/);
  if (containsMatch && containsMatch[1] && containsMatch[2]) {
    const attrName = resolveAttributeName(containsMatch[1]);
    const value = resolveAttributeValue(containsMatch[2]);
    const itemValue = getAttributeValue(item, attrName);

    if (!itemValue || !value) return false;

    // Handle strings
    const itemStr = itemValue.S || itemValue;
    const valueStr = value.S || value;

    if (typeof itemStr === "string" && typeof valueStr === "string") {
      return itemStr.includes(valueStr);
    }

    // Handle lists/arrays
    if (Array.isArray(itemValue)) {
      return itemValue.some(v => JSON.stringify(v) === JSON.stringify(value));
    }

    return false;
  }

  // size(path) comparisons
  const sizeMatch = expr.match(/^size\s*\(\s*([#\w]+)\s*\)\s*([<>=!]+)\s*(:\w+|\d+)$/);
  if (sizeMatch && sizeMatch[1] && sizeMatch[2] && sizeMatch[3]) {
    const attrName = resolveAttributeName(sizeMatch[1]);
    const operator = sizeMatch[2];
    const compareValueStr = sizeMatch[3];
    const compareValue = compareValueStr.startsWith(":")
      ? resolveAttributeValue(compareValueStr)
      : parseInt(compareValueStr);

    const itemValue = getAttributeValue(item, attrName);
    if (!itemValue) return false;

    let size = 0;
    if (typeof itemValue === "string" || itemValue.S) {
      size = (itemValue.S || itemValue).length;
    } else if (Array.isArray(itemValue)) {
      size = itemValue.length;
    } else if (itemValue.L) {
      size = itemValue.L.length;
    }

    return compareValues(size, operator, compareValue);
  }

  // Comparison operators: =, <>, <, >, <=, >=
  const comparisonMatch = expr.match(/^([#\w]+)\s*(<>|<=|>=|<|>|=)\s*(:\w+)$/);
  if (comparisonMatch && comparisonMatch[1] && comparisonMatch[2] && comparisonMatch[3]) {
    const attrName = resolveAttributeName(comparisonMatch[1]);
    const operator = comparisonMatch[2];
    const valueRef = comparisonMatch[3];
    const expectedValue = resolveAttributeValue(valueRef);

    const itemValue = getAttributeValue(item, attrName);

    if (operator === "=") {
      return JSON.stringify(itemValue) === JSON.stringify(expectedValue);
    }

    if (operator === "<>") {
      return JSON.stringify(itemValue) !== JSON.stringify(expectedValue);
    }

    // For numeric comparisons
    const itemNum = getNumericValue(itemValue);
    const expectedNum = getNumericValue(expectedValue);

    if (itemNum !== null && expectedNum !== null) {
      return compareValues(itemNum, operator, expectedNum);
    }

    // For string comparisons
    const itemStr = getStringValue(itemValue);
    const expectedStr = getStringValue(expectedValue);

    if (itemStr !== null && expectedStr !== null) {
      return compareValues(itemStr, operator, expectedStr);
    }

    return false;
  }

  // Default: condition passes (for unsupported expressions)
  console.warn(`Unsupported condition expression: ${conditionExpression}`);
  return true;
}

function splitByLogicalOperator(expression: string, operator: string): string[] {
  // Simple split - doesn't handle nested parentheses
  // For production, would need a proper parser
  const parts: string[] = [];
  let current = "";
  let parenDepth = 0;

  let i = 0;
  while (i < expression.length) {
    if (expression[i] === "(") {
      parenDepth++;
      current += expression[i];
      i++;
    } else if (expression[i] === ")") {
      parenDepth--;
      current += expression[i];
      i++;
    } else if (parenDepth === 0 && expression.substring(i, i + operator.length) === operator) {
      parts.push(current.trim());
      current = "";
      i += operator.length;
    } else {
      current += expression[i];
      i++;
    }
  }

  if (current.trim()) {
    parts.push(current.trim());
  }

  return parts;
}

function compareValues(a: any, operator: string, b: any): boolean {
  switch (operator) {
    case "=":
      return a === b;
    case "<>":
      return a !== b;
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

function getNumericValue(value: any): number | null {
  if (value === null || value === undefined) return null;

  // DynamoDB number format
  if (value.N) {
    const num = parseFloat(value.N);
    return isNaN(num) ? null : num;
  }

  // Plain number
  if (typeof value === "number") {
    return value;
  }

  // String that might be a number
  if (typeof value === "string") {
    const num = parseFloat(value);
    return isNaN(num) ? null : num;
  }

  return null;
}

function getStringValue(value: any): string | null {
  if (value === null || value === undefined) return null;

  // DynamoDB string format
  if (value.S) {
    return value.S;
  }

  // Plain string
  if (typeof value === "string") {
    return value;
  }

  return null;
}
