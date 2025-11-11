// Configuration for storage backend

export interface Config {
  shardCount: number;
  dataDir: string;
  port: number;
}

export function getConfig(): Config {
  const shardCount = parseInt(process.env.SHARD_COUNT || "4");
  const dataDir = process.env.DATA_DIR || "./data";
  const port = parseInt(process.env.PORT || "8000");

  return {
    shardCount,
    dataDir,
    port,
  };
}
