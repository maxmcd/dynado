// Condition expression parser - now uses proper lexer/parser
// This file maintains backwards compatibility by re-exporting from the new parser

import { evaluateConditionExpression } from "./expression-parser/index.ts";

// Re-export the main function with the same signature
export { evaluateConditionExpression };
