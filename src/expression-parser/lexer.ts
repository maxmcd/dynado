// Lexer for DynamoDB expression language using Chevrotain

import { createToken, Lexer } from 'chevrotain'

// ============================================================================
// Keywords and Operators
// ============================================================================

export const And = createToken({ name: 'And', pattern: /AND\b/ })
export const Or = createToken({ name: 'Or', pattern: /OR\b/ })
export const Not = createToken({ name: 'Not', pattern: /NOT\b/ })
export const Between = createToken({ name: 'Between', pattern: /BETWEEN\b/ })
export const In = createToken({ name: 'In', pattern: /IN\b/ })

// Update expression keywords
export const Set = createToken({ name: 'Set', pattern: /SET\b/ })
export const Remove = createToken({ name: 'Remove', pattern: /REMOVE\b/ })
export const Add = createToken({ name: 'Add', pattern: /ADD\b/ })
export const Delete = createToken({ name: 'Delete', pattern: /DELETE\b/ })

// Function names
export const AttributeExists = createToken({
  name: 'AttributeExists',
  pattern: /attribute_exists\b/,
})
export const AttributeNotExists = createToken({
  name: 'AttributeNotExists',
  pattern: /attribute_not_exists\b/,
})
export const BeginsWith = createToken({
  name: 'BeginsWith',
  pattern: /begins_with\b/,
})
export const Contains = createToken({
  name: 'Contains',
  pattern: /contains\b/,
})
export const Size = createToken({ name: 'Size', pattern: /size\b/ })
export const AttributeType = createToken({
  name: 'AttributeType',
  pattern: /attribute_type\b/,
})
export const IfNotExists = createToken({
  name: 'IfNotExists',
  pattern: /if_not_exists\b/,
})
export const ListAppend = createToken({
  name: 'ListAppend',
  pattern: /list_append\b/,
})

// ============================================================================
// Operators
// ============================================================================

export const Equals = createToken({ name: 'Equals', pattern: /=/ })
export const NotEquals = createToken({ name: 'NotEquals', pattern: /<>/ })
export const LessThanOrEqual = createToken({
  name: 'LessThanOrEqual',
  pattern: /<=/,
})
export const GreaterThanOrEqual = createToken({
  name: 'GreaterThanOrEqual',
  pattern: />=/,
})
export const LessThan = createToken({ name: 'LessThan', pattern: /</ })
export const GreaterThan = createToken({ name: 'GreaterThan', pattern: />/ })
export const Plus = createToken({ name: 'Plus', pattern: /\+/ })
export const Minus = createToken({ name: 'Minus', pattern: /-/ })

// ============================================================================
// Delimiters
// ============================================================================

export const LParen = createToken({ name: 'LParen', pattern: /\(/ })
export const RParen = createToken({ name: 'RParen', pattern: /\)/ })
export const Comma = createToken({ name: 'Comma', pattern: /,/ })

// ============================================================================
// Identifiers and Literals
// ============================================================================

// Expression attribute name placeholder (#name)
export const ExpressionAttributeName = createToken({
  name: 'ExpressionAttributeName',
  pattern: /#[a-zA-Z_][a-zA-Z0-9_]*/,
})

// Expression attribute value placeholder (:value)
export const ExpressionAttributeValue = createToken({
  name: 'ExpressionAttributeValue',
  pattern: /:[a-zA-Z_][a-zA-Z0-9_]*/,
})

// Regular identifier (attribute name without #)
export const Identifier = createToken({
  name: 'Identifier',
  pattern: /[a-zA-Z_][a-zA-Z0-9_]*/,
})

// Number literal
export const NumberLiteral = createToken({
  name: 'NumberLiteral',
  pattern: /-?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?/,
})

// String literal (simplified - doesn't handle escapes)
export const StringLiteral = createToken({
  name: 'StringLiteral',
  pattern: /"([^"\\]|\\.)*"|'([^'\\]|\\.)*'/,
})

// ============================================================================
// Whitespace (skipped)
// ============================================================================

export const WhiteSpace = createToken({
  name: 'WhiteSpace',
  pattern: /\s+/,
  group: Lexer.SKIPPED,
})

// ============================================================================
// All Tokens (order matters for precedence)
// ============================================================================

export const allTokens = [
  WhiteSpace,

  // Keywords (must come before Identifier)
  And,
  Or,
  Not,
  Between,
  In,
  Set,
  Remove,
  Add,
  Delete,

  // Functions (must come before Identifier)
  AttributeExists,
  AttributeNotExists,
  BeginsWith,
  Contains,
  Size,
  AttributeType,
  IfNotExists,
  ListAppend,

  // Operators (multi-char before single-char)
  NotEquals,
  LessThanOrEqual,
  GreaterThanOrEqual,
  Equals,
  LessThan,
  GreaterThan,
  Plus,
  Minus,

  // Delimiters
  LParen,
  RParen,
  Comma,

  // Identifiers and literals
  ExpressionAttributeName,
  ExpressionAttributeValue,
  NumberLiteral,
  StringLiteral,
  Identifier,
]

// Create lexer instance
export const expressionLexer = new Lexer(allTokens)
