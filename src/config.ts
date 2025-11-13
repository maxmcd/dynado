// Configuration for storage backend

export interface Config {
  shardCount: number
  dataDir: string
  port: number
}

export function createConfig(params?: {
  shardCount?: number
  dataDir?: string
  port?: number
}): Config {
  return {
    shardCount: params?.shardCount ?? 4,
    dataDir: params?.dataDir ?? './data',
    port: params?.port ?? 8000,
  }
}

// Helper for reading from environment variables (used in Bun/Node.js)
export function getConfigFromEnv(): Config {
  const shardCount = process.env.SHARD_COUNT
    ? parseInt(process.env.SHARD_COUNT)
    : 4
  const dataDir = process.env.DATA_DIR || './data'
  const port = process.env.PORT ? parseInt(process.env.PORT) : 8000

  return createConfig({
    shardCount,
    dataDir,
    port,
  })
}
