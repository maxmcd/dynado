package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	client    *dynamodb.Client
	serverCmd *exec.Cmd
)

func TestMain(m *testing.M) {
	// Start the server
	ctx := context.Background()
	if err := startServer(ctx); err != nil {
		panic(err)
	}

	// Wait for server to be ready
	time.Sleep(2 * time.Second)

	// Create DynamoDB client
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:           "http://localhost:8000",
					SigningRegion: "us-east-1",
				}, nil
			},
		)),
	)
	if err != nil {
		panic(err)
	}

	client = dynamodb.NewFromConfig(cfg)

	// Run tests
	code := m.Run()

	// Cleanup
	stopServer()
	os.Exit(code)
}

func startServer(ctx context.Context) error {
	// Set environment variables for test
	os.Setenv("SHARD_COUNT", "4")
	os.Setenv("DATA_DIR", "./test-data-go")
	os.Setenv("PORT", "8000")

	// Start the Bun server
	serverCmd = exec.Command("bun", "run", "index.ts")
	serverCmd.Env = os.Environ()

	// Capture output for debugging
	serverCmd.Stdout = os.Stdout
	serverCmd.Stderr = os.Stderr

	return serverCmd.Start()
}

func stopServer() {
	if serverCmd != nil && serverCmd.Process != nil {
		serverCmd.Process.Kill()
		serverCmd.Wait()
	}
	// Cleanup test data
	os.RemoveAll("./test-data-go")
}

func TestTableOperations(t *testing.T) {
	ctx := context.Background()
	tableName := "GoTestTable"

	// Create table
	t.Run("CreateTable", func(t *testing.T) {
		_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String(tableName),
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String("id"),
					KeyType:       types.KeyTypeHash,
				},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{
					AttributeName: aws.String("id"),
					AttributeType: types.ScalarAttributeTypeS,
				},
			},
		})
		if err != nil {
			t.Fatalf("CreateTable failed: %v", err)
		}
	})

	// List tables
	t.Run("ListTables", func(t *testing.T) {
		result, err := client.ListTables(ctx, &dynamodb.ListTablesInput{})
		if err != nil {
			t.Fatalf("ListTables failed: %v", err)
		}

		found := false
		for _, name := range result.TableNames {
			if name == tableName {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Table %s not found in list", tableName)
		}
	})

	// Describe table
	t.Run("DescribeTable", func(t *testing.T) {
		result, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			t.Fatalf("DescribeTable failed: %v", err)
		}
		if *result.Table.TableName != tableName {
			t.Errorf("Expected table name %s, got %s", tableName, *result.Table.TableName)
		}
	})

	// Delete table
	t.Run("DeleteTable", func(t *testing.T) {
		_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			t.Fatalf("DeleteTable failed: %v", err)
		}
	})
}

func TestItemOperations(t *testing.T) {
	ctx := context.Background()
	tableName := "GoTestItems"

	// Create table first
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	defer client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})

	// PutItem
	t.Run("PutItem", func(t *testing.T) {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"id":    &types.AttributeValueMemberS{Value: "test-1"},
				"name":  &types.AttributeValueMemberS{Value: "Test Item"},
				"count": &types.AttributeValueMemberN{Value: "42"},
			},
		})
		if err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	})

	// GetItem
	t.Run("GetItem", func(t *testing.T) {
		result, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "test-1"},
			},
		})
		if err != nil {
			t.Fatalf("GetItem failed: %v", err)
		}
		if result.Item == nil {
			t.Fatal("Item not found")
		}
		nameVal := result.Item["name"].(*types.AttributeValueMemberS)
		if nameVal.Value != "Test Item" {
			t.Errorf("Expected name 'Test Item', got '%s'", nameVal.Value)
		}
	})

	// UpdateItem
	t.Run("UpdateItem", func(t *testing.T) {
		_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "test-1"},
			},
			UpdateExpression: aws.String("SET #count = :val"),
			ExpressionAttributeNames: map[string]string{
				"#count": "count",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":val": &types.AttributeValueMemberN{Value: "100"},
			},
		})
		if err != nil {
			t.Fatalf("UpdateItem failed: %v", err)
		}

		// Verify update
		result, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "test-1"},
			},
		})
		if err != nil {
			t.Fatalf("GetItem after update failed: %v", err)
		}
		countVal := result.Item["count"].(*types.AttributeValueMemberN)
		if countVal.Value != "100" {
			t.Errorf("Expected count 100, got %s", countVal.Value)
		}
	})

	// DeleteItem
	t.Run("DeleteItem", func(t *testing.T) {
		_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "test-1"},
			},
		})
		if err != nil {
			t.Fatalf("DeleteItem failed: %v", err)
		}

		// Verify deletion
		result, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "test-1"},
			},
		})
		if err != nil {
			t.Fatalf("GetItem after delete failed: %v", err)
		}
		if result.Item != nil {
			t.Error("Item should have been deleted")
		}
	})
}

func TestBatchOperations(t *testing.T) {
	ctx := context.Background()
	tableName := "GoTestBatch"

	// Create table first
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	defer client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})

	// BatchWriteItem
	t.Run("BatchWriteItem", func(t *testing.T) {
		_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: {
					{
						PutRequest: &types.PutRequest{
							Item: map[string]types.AttributeValue{
								"id":    &types.AttributeValueMemberS{Value: "batch-1"},
								"value": &types.AttributeValueMemberS{Value: "First"},
							},
						},
					},
					{
						PutRequest: &types.PutRequest{
							Item: map[string]types.AttributeValue{
								"id":    &types.AttributeValueMemberS{Value: "batch-2"},
								"value": &types.AttributeValueMemberS{Value: "Second"},
							},
						},
					},
					{
						PutRequest: &types.PutRequest{
							Item: map[string]types.AttributeValue{
								"id":    &types.AttributeValueMemberS{Value: "batch-3"},
								"value": &types.AttributeValueMemberS{Value: "Third"},
							},
						},
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("BatchWriteItem failed: %v", err)
		}
	})

	// BatchGetItem
	t.Run("BatchGetItem", func(t *testing.T) {
		result, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
			RequestItems: map[string]types.KeysAndAttributes{
				tableName: {
					Keys: []map[string]types.AttributeValue{
						{"id": &types.AttributeValueMemberS{Value: "batch-1"}},
						{"id": &types.AttributeValueMemberS{Value: "batch-2"}},
						{"id": &types.AttributeValueMemberS{Value: "batch-3"}},
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("BatchGetItem failed: %v", err)
		}
		items := result.Responses[tableName]
		if len(items) != 3 {
			t.Errorf("Expected 3 items, got %d", len(items))
		}
	})
}

func TestScanAndQuery(t *testing.T) {
	ctx := context.Background()
	tableName := "GoTestScan"

	// Create table first
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	defer client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})

	// Add test items
	for i := 1; i <= 5; i++ {
		client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"id":       &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i)},
				"category": &types.AttributeValueMemberS{Value: "test"},
			},
		})
	}

	// Scan
	t.Run("Scan", func(t *testing.T) {
		result, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if result.Count != 5 {
			t.Errorf("Expected 5 items, got %d", result.Count)
		}
	})

	// Query
	t.Run("Query", func(t *testing.T) {
		result, err := client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String("id = :id"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":id": &types.AttributeValueMemberS{Value: "item-1"},
			},
		})
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Count != 1 {
			t.Errorf("Expected 1 item, got %d", result.Count)
		}
	})
}

func TestTransactions(t *testing.T) {
	ctx := context.Background()
	tableName := "GoTestTransact"

	// Create table first
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	defer client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})

	// TransactWriteItems
	t.Run("TransactWriteItems", func(t *testing.T) {
		_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Put: &types.Put{
						TableName: aws.String(tableName),
						Item: map[string]types.AttributeValue{
							"id":    &types.AttributeValueMemberS{Value: "txn-1"},
							"value": &types.AttributeValueMemberS{Value: "First"},
						},
					},
				},
				{
					Put: &types.Put{
						TableName: aws.String(tableName),
						Item: map[string]types.AttributeValue{
							"id":    &types.AttributeValueMemberS{Value: "txn-2"},
							"value": &types.AttributeValueMemberS{Value: "Second"},
						},
					},
				},
				{
					Put: &types.Put{
						TableName: aws.String(tableName),
						Item: map[string]types.AttributeValue{
							"id":    &types.AttributeValueMemberS{Value: "txn-3"},
							"value": &types.AttributeValueMemberS{Value: "Third"},
						},
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("TransactWriteItems failed: %v", err)
		}

		// Verify all items were created
		result, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "txn-1"},
			},
		})
		if err != nil || result.Item == nil {
			t.Error("Transaction item not found")
		}
	})

	// TransactGetItems
	t.Run("TransactGetItems", func(t *testing.T) {
		result, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
			TransactItems: []types.TransactGetItem{
				{
					Get: &types.Get{
						TableName: aws.String(tableName),
						Key: map[string]types.AttributeValue{
							"id": &types.AttributeValueMemberS{Value: "txn-1"},
						},
					},
				},
				{
					Get: &types.Get{
						TableName: aws.String(tableName),
						Key: map[string]types.AttributeValue{
							"id": &types.AttributeValueMemberS{Value: "txn-2"},
						},
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("TransactGetItems failed: %v", err)
		}
		if len(result.Responses) != 2 {
			t.Errorf("Expected 2 responses, got %d", len(result.Responses))
		}
	})

	// Transaction with condition
	t.Run("TransactWriteWithCondition", func(t *testing.T) {
		// First, put an item
		client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"id":    &types.AttributeValueMemberS{Value: "cond-1"},
				"value": &types.AttributeValueMemberS{Value: "Original"},
			},
		})

		// Try to put with attribute_not_exists - should fail
		_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Put: &types.Put{
						TableName: aws.String(tableName),
						Item: map[string]types.AttributeValue{
							"id":    &types.AttributeValueMemberS{Value: "cond-1"},
							"value": &types.AttributeValueMemberS{Value: "New"},
						},
						ConditionExpression: aws.String("attribute_not_exists(id)"),
					},
				},
			},
		})
		if err == nil {
			t.Error("Transaction should have failed due to condition")
		}

		// Verify original item is unchanged
		result, _ := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: "cond-1"},
			},
		})
		valueVal := result.Item["value"].(*types.AttributeValueMemberS)
		if valueVal.Value != "Original" {
			t.Errorf("Item should be unchanged, got value: %s", valueVal.Value)
		}
	})
}
