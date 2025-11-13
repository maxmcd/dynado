// Tests for condition expression edge cases

import { test, expect, describe } from 'bun:test'
import { evaluateConditionExpression } from './index.ts'
import type { DynamoDBItem } from '../types.ts'

describe('Condition Expression Evaluator', () => {
  describe('Basic Comparisons', () => {
    test('should handle = operator with strings', () => {
      const item: DynamoDBItem = { name: { S: 'Alice' } }
      expect(
        evaluateConditionExpression(item, 'name = :val', undefined, {
          ':val': { S: 'Alice' },
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'name = :val', undefined, {
          ':val': { S: 'Bob' },
        })
      ).toBe(false)
    })

    test('should handle <> operator (not equals)', () => {
      const item: DynamoDBItem = { status: { S: 'active' } }
      expect(
        evaluateConditionExpression(item, 'status <> :val', undefined, {
          ':val': { S: 'inactive' },
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'status <> :val', undefined, {
          ':val': { S: 'active' },
        })
      ).toBe(false)
    })

    test('should handle numeric comparisons', () => {
      const item: DynamoDBItem = { age: { N: '25' } }
      expect(
        evaluateConditionExpression(item, 'age > :val', undefined, {
          ':val': { N: '20' },
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'age < :val', undefined, {
          ':val': { N: '30' },
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'age >= :val', undefined, {
          ':val': { N: '25' },
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'age <= :val', undefined, {
          ':val': { N: '25' },
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'age > :val', undefined, {
          ':val': { N: '30' },
        })
      ).toBe(false)
    })
  })

  describe('Existence Functions', () => {
    test('should handle attribute_exists for existing attribute', () => {
      const item: DynamoDBItem = { name: { S: 'Alice' }, age: { N: '25' } }
      expect(evaluateConditionExpression(item, 'attribute_exists(name)')).toBe(
        true
      )
      expect(evaluateConditionExpression(item, 'attribute_exists(age)')).toBe(
        true
      )
    })

    test('should handle attribute_exists for non-existing attribute', () => {
      const item: DynamoDBItem = { name: { S: 'Alice' } }
      expect(
        evaluateConditionExpression(item, 'attribute_exists(missing)')
      ).toBe(false)
    })

    test('should handle attribute_not_exists for non-existing attribute', () => {
      const item: DynamoDBItem = { name: { S: 'Alice' } }
      expect(
        evaluateConditionExpression(item, 'attribute_not_exists(missing)')
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'attribute_not_exists(name)')
      ).toBe(false)
    })

    test('should handle attribute_not_exists for null item', () => {
      expect(
        evaluateConditionExpression(null, 'attribute_not_exists(id)')
      ).toBe(true)
    })

    test('should handle attribute_exists with expression attribute names', () => {
      const item: DynamoDBItem = { status: { S: 'active' } }
      expect(
        evaluateConditionExpression(item, 'attribute_exists(#s)', {
          '#s': 'status',
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'attribute_not_exists(#s)', {
          '#s': 'status',
        })
      ).toBe(false)
    })
  })

  describe('String Functions', () => {
    test('should handle begins_with function', () => {
      const item: DynamoDBItem = { prefix: { S: 'PROD-12345' } }
      expect(
        evaluateConditionExpression(
          item,
          'begins_with(prefix, :val)',
          undefined,
          {
            ':val': { S: 'PROD' },
          }
        )
      ).toBe(true)
      expect(
        evaluateConditionExpression(
          item,
          'begins_with(prefix, :val)',
          undefined,
          {
            ':val': { S: 'ORDER' },
          }
        )
      ).toBe(false)
    })

    test('should handle begins_with with expression attribute names', () => {
      const item: DynamoDBItem = { itemId: { S: 'USER-001' } }
      expect(
        evaluateConditionExpression(
          item,
          'begins_with(#id, :prefix)',
          { '#id': 'itemId' },
          { ':prefix': { S: 'USER' } }
        )
      ).toBe(true)
    })

    test('should handle contains function with strings', () => {
      const item: DynamoDBItem = {
        description: { S: 'This is a test description' },
      }
      expect(
        evaluateConditionExpression(
          item,
          'contains(description, :val)',
          undefined,
          {
            ':val': { S: 'test' },
          }
        )
      ).toBe(true)
      expect(
        evaluateConditionExpression(
          item,
          'contains(description, :val)',
          undefined,
          {
            ':val': { S: 'missing' },
          }
        )
      ).toBe(false)
    })

    test('should handle contains with missing attribute', () => {
      const item: DynamoDBItem = { name: { S: 'Alice' } }
      expect(
        evaluateConditionExpression(
          item,
          'contains(missing, :val)',
          undefined,
          {
            ':val': { S: 'test' },
          }
        )
      ).toBe(false)
    })
  })

  describe('Size Function', () => {
    test('should handle size function with strings', () => {
      const item: DynamoDBItem = { name: { S: 'Alice' } }
      expect(
        evaluateConditionExpression(item, 'size(name) = :len', undefined, {
          ':len': 5,
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'size(name) > :len', undefined, {
          ':len': 3,
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'size(name) < :len', undefined, {
          ':len': 10,
        })
      ).toBe(true)
    })

    test('should handle size with expression attribute names', () => {
      const item: DynamoDBItem = { userName: { S: 'Bob' } }
      expect(
        evaluateConditionExpression(
          item,
          'size(#name) = :len',
          { '#name': 'userName' },
          { ':len': 3 }
        )
      ).toBe(true)
    })

    test('should handle size with missing attribute', () => {
      const item: DynamoDBItem = { name: { S: 'Alice' } }
      expect(
        evaluateConditionExpression(item, 'size(missing) > :len', undefined, {
          ':len': 0,
        })
      ).toBe(false)
    })
  })

  describe('Logical Operators', () => {
    test('should handle AND operator', () => {
      const item: DynamoDBItem = { age: { N: '25' }, status: { S: 'active' } }
      expect(
        evaluateConditionExpression(
          item,
          'age > :minAge AND status = :status',
          undefined,
          { ':minAge': { N: '18' }, ':status': { S: 'active' } }
        )
      ).toBe(true)
      expect(
        evaluateConditionExpression(
          item,
          'age > :minAge AND status = :status',
          undefined,
          { ':minAge': { N: '18' }, ':status': { S: 'inactive' } }
        )
      ).toBe(false)
    })

    test('should handle OR operator', () => {
      const item: DynamoDBItem = { role: { S: 'admin' } }
      expect(
        evaluateConditionExpression(
          item,
          'role = :admin OR role = :moderator',
          undefined,
          { ':admin': { S: 'admin' }, ':moderator': { S: 'moderator' } }
        )
      ).toBe(true)
      expect(
        evaluateConditionExpression(
          item,
          'role = :user OR role = :guest',
          undefined,
          { ':user': { S: 'user' }, ':guest': { S: 'guest' } }
        )
      ).toBe(false)
    })

    test('should handle NOT operator', () => {
      const item: DynamoDBItem = { deleted: { S: 'false' } }
      expect(
        evaluateConditionExpression(item, 'NOT deleted = :true', undefined, {
          ':true': { S: 'true' },
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'NOT deleted = :false', undefined, {
          ':false': { S: 'false' },
        })
      ).toBe(false)
    })

    test('should handle complex AND/OR combinations', () => {
      const item: DynamoDBItem = {
        age: { N: '25' },
        role: { S: 'admin' },
        active: { S: 'true' },
      }
      expect(
        evaluateConditionExpression(
          item,
          'age > :age AND (role = :admin OR role = :mod)',
          undefined,
          {
            ':age': { N: '18' },
            ':admin': { S: 'admin' },
            ':mod': { S: 'moderator' },
          }
        )
      ).toBe(true)
    })
  })

  describe('Expression Attribute Names', () => {
    test('should resolve expression attribute names', () => {
      const item: DynamoDBItem = { status: { S: 'active' } }
      expect(
        evaluateConditionExpression(
          item,
          '#s = :val',
          { '#s': 'status' },
          { ':val': { S: 'active' } }
        )
      ).toBe(true)
    })

    test('should handle reserved keywords with expression attribute names', () => {
      const item: DynamoDBItem = { name: { S: 'Alice' }, data: { S: 'test' } }
      expect(
        evaluateConditionExpression(
          item,
          '#name = :n AND #data = :d',
          { '#name': 'name', '#data': 'data' },
          { ':n': { S: 'Alice' }, ':d': { S: 'test' } }
        )
      ).toBe(true)
    })
  })

  describe('Edge Cases', () => {
    test('should handle null item', () => {
      expect(
        evaluateConditionExpression(null, 'attribute_not_exists(id)')
      ).toBe(true)
      expect(evaluateConditionExpression(null, 'attribute_exists(id)')).toBe(
        false
      )
    })

    test('should handle empty condition expression', () => {
      const item: DynamoDBItem = { name: { S: 'Alice' } }
      expect(evaluateConditionExpression(item, undefined)).toBe(true)
      expect(evaluateConditionExpression(item, '')).toBe(true)
    })

    test('should throw error for unsupported expressions', () => {
      const item: DynamoDBItem = { name: { S: 'Alice' } }
      expect(() => {
        evaluateConditionExpression(item, 'UNSUPPORTED_FUNCTION(name)')
      }).toThrow() // Parser will throw a detailed error message
    })

    test('should handle comparison with missing attribute', () => {
      const item: DynamoDBItem = { name: { S: 'Alice' } }
      expect(
        evaluateConditionExpression(item, 'missing = :val', undefined, {
          ':val': { S: 'test' },
        })
      ).toBe(false)
    })

    test('should handle numeric string comparisons', () => {
      const item: DynamoDBItem = { version: { N: '2' } }
      expect(
        evaluateConditionExpression(item, 'version > :v', undefined, {
          ':v': { N: '1' },
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'version < :v', undefined, {
          ':v': { N: '3' },
        })
      ).toBe(true)
    })

    test('should handle zero values', () => {
      const item: DynamoDBItem = { counter: { N: '0' } }
      expect(
        evaluateConditionExpression(item, 'counter = :zero', undefined, {
          ':zero': { N: '0' },
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'counter >= :zero', undefined, {
          ':zero': { N: '0' },
        })
      ).toBe(true)
    })

    test('should handle negative numbers', () => {
      const item: DynamoDBItem = { temperature: { N: '-5' } }
      expect(
        evaluateConditionExpression(item, 'temperature < :zero', undefined, {
          ':zero': { N: '0' },
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'temperature > :neg', undefined, {
          ':neg': { N: '-10' },
        })
      ).toBe(true)
    })

    test('should handle floating point numbers', () => {
      const item: DynamoDBItem = { price: { N: '19.99' } }
      expect(
        evaluateConditionExpression(item, 'price < :max', undefined, {
          ':max': { N: '20.00' },
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'price > :min', undefined, {
          ':min': { N: '19.00' },
        })
      ).toBe(true)
    })

    test('should handle empty strings', () => {
      const item: DynamoDBItem = { emptyField: { S: '' } }
      expect(
        evaluateConditionExpression(item, 'emptyField = :empty', undefined, {
          ':empty': { S: '' },
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(
          item,
          'size(emptyField) = :zero',
          undefined,
          { ':zero': 0 }
        )
      ).toBe(true)
    })

    test('should handle special characters in strings', () => {
      const item: DynamoDBItem = { special: { S: 'Hello!@#$%^&*()' } }
      expect(
        evaluateConditionExpression(item, 'special = :val', undefined, {
          ':val': { S: 'Hello!@#$%^&*()' },
        })
      ).toBe(true)
    })

    test('should handle unicode characters', () => {
      const item: DynamoDBItem = { unicode: { S: '你好世界' } }
      expect(
        evaluateConditionExpression(item, 'unicode = :val', undefined, {
          ':val': { S: '你好世界' },
        })
      ).toBe(true)
      expect(
        evaluateConditionExpression(item, 'size(unicode) = :len', undefined, {
          ':len': 4,
        })
      ).toBe(true)
    })

    test('should handle whitespace in expressions', () => {
      const item: DynamoDBItem = { name: { S: 'Alice' } }
      expect(
        evaluateConditionExpression(item, '  name   =   :val  ', undefined, {
          ':val': { S: 'Alice' },
        })
      ).toBe(true)
    })

    test('should handle nested expression attribute names', () => {
      const item: DynamoDBItem = { user: { name: { S: 'Alice' } } }
      // This is a simplified test - full nested attribute support would require more work
      expect(evaluateConditionExpression(item, 'attribute_exists(user)')).toBe(
        true
      )
    })
  })

  describe('Complex Scenarios', () => {
    test('should handle multiple conditions with different operators', () => {
      const item: DynamoDBItem = {
        age: { N: '30' },
        name: { S: 'Alice' },
        status: { S: 'active' },
      }
      expect(
        evaluateConditionExpression(
          item,
          'age >= :minAge AND age <= :maxAge AND status = :active',
          undefined,
          {
            ':minAge': { N: '25' },
            ':maxAge': { N: '35' },
            ':active': { S: 'active' },
          }
        )
      ).toBe(true)
    })

    test('should handle mix of existence and comparison checks', () => {
      const item: DynamoDBItem = {
        confirmed: { S: 'true' },
        email: { S: 'alice@example.com' },
      }
      expect(
        evaluateConditionExpression(
          item,
          'attribute_exists(email) AND confirmed = :true',
          undefined,
          { ':true': { S: 'true' } }
        )
      ).toBe(true)
    })

    test('should handle NOT with function calls', () => {
      const item: DynamoDBItem = { id: { S: '123' } }
      expect(
        evaluateConditionExpression(item, 'NOT attribute_not_exists(id)')
      ).toBe(true)
    })

    test('should short-circuit AND operator', () => {
      const item: DynamoDBItem = { status: { S: 'inactive' } }
      // First condition fails, second shouldn't be evaluated (but we can't really test that)
      expect(
        evaluateConditionExpression(
          item,
          'status = :active AND attribute_exists(missing)',
          undefined,
          { ':active': { S: 'active' } }
        )
      ).toBe(false)
    })

    test('should short-circuit OR operator', () => {
      const item: DynamoDBItem = { role: { S: 'admin' } }
      // First condition succeeds, second shouldn't matter
      expect(
        evaluateConditionExpression(
          item,
          'role = :admin OR attribute_exists(missing)',
          undefined,
          { ':admin': { S: 'admin' } }
        )
      ).toBe(true)
    })
  })
})
