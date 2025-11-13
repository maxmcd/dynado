// Update Expression Parser using Chevrotain

import { CstParser } from 'chevrotain'
import {
  allTokens,
  Set,
  Remove,
  Add,
  Delete,
  Equals,
  Plus,
  Minus,
  Comma,
  LParen,
  RParen,
  ExpressionAttributeName,
  ExpressionAttributeValue,
  Identifier,
  NumberLiteral,
  StringLiteral,
  IfNotExists,
  ListAppend,
} from './lexer.ts'

class UpdateExpressionParser extends CstParser {
  constructor() {
    super(allTokens)
    this.performSelfAnalysis()
  }

  // Top-level update expression: can have SET, REMOVE, ADD, DELETE in any order
  public updateExpression = this.RULE('updateExpression', () => {
    this.MANY(() => {
      this.OR([
        { ALT: () => this.SUBRULE(this.setClause) },
        { ALT: () => this.SUBRULE(this.removeClause) },
        { ALT: () => this.SUBRULE(this.addClause) },
        { ALT: () => this.SUBRULE(this.deleteClause) },
      ])
    })
  })

  // SET clause: SET path = value, path2 = value2, ...
  private setClause = this.RULE('setClause', () => {
    this.CONSUME(Set)
    this.SUBRULE(this.setAction, { LABEL: 'action' })
    this.MANY(() => {
      this.CONSUME(Comma)
      this.SUBRULE2(this.setAction, { LABEL: 'action' })
    })
  })

  // Single SET action: path = value
  private setAction = this.RULE('setAction', () => {
    this.SUBRULE(this.attributePath, { LABEL: 'path' })
    this.CONSUME(Equals)
    this.SUBRULE(this.setValue, { LABEL: 'value' })
  })

  // SET value: can be literal, expression attribute value, or function
  private setValue = this.RULE('setValue', () => {
    this.OR([
      // Arithmetic: path + value or path - value
      {
        ALT: () => {
          this.SUBRULE(this.attributePath, { LABEL: 'left' })
          this.OR2([
            { ALT: () => this.CONSUME(Plus, { LABEL: 'operator' }) },
            { ALT: () => this.CONSUME(Minus, { LABEL: 'operator' }) },
          ])
          this.SUBRULE(this.operandValue, { LABEL: 'right' })
        },
      },

      // if_not_exists(path, value)
      {
        ALT: () => {
          this.CONSUME(IfNotExists)
          this.CONSUME(LParen)
          this.SUBRULE2(this.attributePath, { LABEL: 'path' })
          this.CONSUME(Comma)
          this.SUBRULE2(this.operandValue, { LABEL: 'default' })
          this.CONSUME(RParen)
        },
      },

      // list_append(list1, list2)
      {
        ALT: () => {
          this.CONSUME(ListAppend)
          this.CONSUME2(LParen)
          this.OR3([
            {
              ALT: () => this.SUBRULE3(this.attributePath, { LABEL: 'list1' }),
            },
            { ALT: () => this.SUBRULE3(this.operandValue, { LABEL: 'list1' }) },
          ])
          this.CONSUME2(Comma)
          this.OR4([
            {
              ALT: () => this.SUBRULE4(this.attributePath, { LABEL: 'list2' }),
            },
            { ALT: () => this.SUBRULE4(this.operandValue, { LABEL: 'list2' }) },
          ])
          this.CONSUME2(RParen)
        },
      },

      // Simple value
      {
        ALT: () => {
          this.SUBRULE5(this.operandValue)
        },
      },
    ])
  })

  // REMOVE clause: REMOVE path, path2, ...
  private removeClause = this.RULE('removeClause', () => {
    this.CONSUME(Remove)
    this.SUBRULE(this.attributePath, { LABEL: 'path' })
    this.MANY(() => {
      this.CONSUME(Comma)
      this.SUBRULE2(this.attributePath, { LABEL: 'path' })
    })
  })

  // ADD clause: ADD path value, path2 value2, ...
  private addClause = this.RULE('addClause', () => {
    this.CONSUME(Add)
    this.SUBRULE(this.addAction, { LABEL: 'action' })
    this.MANY(() => {
      this.CONSUME(Comma)
      this.SUBRULE2(this.addAction, { LABEL: 'action' })
    })
  })

  // Single ADD action: path value
  private addAction = this.RULE('addAction', () => {
    this.SUBRULE(this.attributePath, { LABEL: 'path' })
    this.SUBRULE(this.operandValue, { LABEL: 'value' })
  })

  // DELETE clause: DELETE path value, path2 value2, ...
  private deleteClause = this.RULE('deleteClause', () => {
    this.CONSUME(Delete)
    this.SUBRULE(this.deleteAction, { LABEL: 'action' })
    this.MANY(() => {
      this.CONSUME(Comma)
      this.SUBRULE2(this.deleteAction, { LABEL: 'action' })
    })
  })

  // Single DELETE action: path value
  private deleteAction = this.RULE('deleteAction', () => {
    this.SUBRULE(this.attributePath, { LABEL: 'path' })
    this.SUBRULE(this.operandValue, { LABEL: 'value' })
  })

  // Attribute path (for now, just a simple name)
  private attributePath = this.RULE('attributePath', () => {
    this.OR([
      { ALT: () => this.CONSUME(ExpressionAttributeName) },
      { ALT: () => this.CONSUME(Identifier) },
    ])
  })

  // Operand value (expression attribute value or literal)
  private operandValue = this.RULE('operandValue', () => {
    this.OR([
      { ALT: () => this.CONSUME(ExpressionAttributeValue) },
      { ALT: () => this.CONSUME(NumberLiteral) },
      { ALT: () => this.CONSUME(StringLiteral) },
    ])
  })
}

// Export singleton instance
export const updateParser = new UpdateExpressionParser()
