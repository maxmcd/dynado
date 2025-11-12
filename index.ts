import { DB } from "./src";
export { DB } from "./src";

// Start server if run directly
if (import.meta.main) {
  const db = new DB();
  console.log(
    `DynamoDB-compatible server running at http://localhost:${db.config.port}`
  );
  console.log(
    `Using sharded SQLite storage with ${db.config.shardCount} shards`
  );
}
