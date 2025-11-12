# Testing with DynamoDB Local

The TypeScript test suite can run against either the dynado server or the official DynamoDB Local container for validation.

## Usage

### Test against dynado (default)

```bash
bun test
```

### Test against DynamoDB Local for validation

```bash
TEST_DYNAMODB_LOCAL=true bun test
```

## How it works

The same test suite (`index.test.ts`) runs against either implementation:

1. `test-db-setup.ts` exports `startTestDB()` which conditionally starts:

   - **Dynado server** (default) - Fast, runs in-process
   - **DynamoDB Local container** (when `TEST_DYNAMODB_LOCAL=true`) - Validates API compatibility

2. All tests use the same DynamoDB client, just pointed at different endpoints

3. Tests are written to be compatible with both implementations

## Requirements

- Docker must be running to use `TEST_DYNAMODB_LOCAL=true`
- First run will pull the `amazon/dynamodb-local:3.1.0` image (~500MB)
- Testcontainers package handles container lifecycle automatically
