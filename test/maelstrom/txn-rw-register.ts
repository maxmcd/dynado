import readline from 'node:readline'
import { stdin, stdout, stderr } from 'node:process'
import path from 'node:path'
import fs from 'node:fs/promises'

import {
  DynamoDBClient,
  CreateTableCommand,
  DescribeTableCommand,
  GetItemCommand,
  PutItemCommand,
  TransactWriteItemsCommand,
  type AttributeValue,
  type TransactWriteItem,
} from '@aws-sdk/client-dynamodb'
import { DB } from '../../src/index.ts'
import { createConfig } from '../../src/config.ts'

type TxnOp = ['r' | 'w', unknown, unknown | null]

type MaelstromMessage = {
  src: string
  dest: string
  body: {
    type: string
    msg_id?: number
    node_id?: string
    node_ids?: string[]
    txn?: TxnOp[]
  }
}

type OutboundMessage = {
  src: string
  dest: string
  body: Record<string, unknown>
}

const TABLE_NAME = 'txn_rw'
const KEY_ATTR = 'pk'
const VALUE_ATTR = 'val'

const dataRoot =
  process.env.MAELSTROM_DATA_DIR ?? path.join(process.cwd(), '.maelstrom-data')
const nodeDataDir = path.join(dataRoot, `node-${process.pid}`)
const shardCount = parseInt(process.env.MAELSTROM_SHARD_COUNT ?? '1', 10)

const db = new DB(
  createConfig({
    shardCount,
    dataDir: nodeDataDir,
    port: 0,
  })
)
const endpoint = `http://127.0.0.1:${db.server.port}`
const client = new DynamoDBClient({
  endpoint,
  region: 'local',
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
})

let tableReady: Promise<void> | null = null
async function ensureTableExists() {
  if (!tableReady) {
    tableReady = (async () => {
      try {
        await client.send(new DescribeTableCommand({ TableName: TABLE_NAME }))
      } catch (error) {
        if ((error as { name?: string }).name !== 'ResourceNotFoundException') {
          throw error
        }
        await client.send(
          new CreateTableCommand({
            TableName: TABLE_NAME,
            KeySchema: [{ AttributeName: KEY_ATTR, KeyType: 'HASH' }],
            AttributeDefinitions: [
              { AttributeName: KEY_ATTR, AttributeType: 'S' },
            ],
            BillingMode: 'PAY_PER_REQUEST',
          })
        )
      }
    })()
  }
  await tableReady
}

let nodeId: string | null = null
let nextMsgId = 0

const rl = readline.createInterface({
  input: stdin,
  crlfDelay: Infinity,
})

let queue = Promise.resolve()

rl.on('line', (line) => {
  queue = queue
    .then(() => handleLine(line))
    .catch((error) => {
      stderr.write(
        `maelstrom handler error: ${
          error instanceof Error ? error.stack : String(error)
        }\n`
      )
      process.exit(1)
    })
})

rl.on('close', () => {
  queue
    .catch((error) => {
      stderr.write(
        `maelstrom queue flush error: ${
          error instanceof Error ? error.stack : String(error)
        }\n`
      )
    })
    .finally(() => {
      db.server.stop().catch(() => {})
      fs.rm(nodeDataDir, { recursive: true, force: true }).catch(() => {})
      process.exit(0)
    })
})

async function handleLine(line: string) {
  if (!line.trim()) return
  const message = JSON.parse(line) as MaelstromMessage
  await handleMessage(message)
}

async function handleMessage(message: MaelstromMessage) {
  const { body } = message
  switch (body.type) {
    case 'init':
      await ensureTableExists()
      nodeId = body.node_id ?? null
      reply(message, { type: 'init_ok' })
      return
    case 'topology':
      reply(message, { type: 'topology_ok' })
      return
    case 'txn':
      await handleTxn(message)
      return
    default:
      reply(message, {
        type: 'error',
        code: 10,
        text: `unsupported message type: ${body.type}`,
      })
  }
}

async function handleTxn(message: MaelstromMessage) {
  await ensureTableExists()
  const txnOps = message.body.txn
  if (!Array.isArray(txnOps)) {
    reply(message, {
      type: 'error',
      code: 12,
      text: 'txn body must include txn array',
    })
    return
  }

  const stagedWrites = new Map<
    string,
    { attr: AttributeValue; rawKey: unknown; rawValue: unknown }
  >()
  const readCache = new Map<string, unknown | null>()
  const responseOps: TxnOp[] = []

  for (const original of txnOps) {
    const [kind, key, value] = original
    if (kind !== 'r' && kind !== 'w') {
      reply(message, {
        type: 'error',
        code: 12,
        text: `unknown txn operation: ${JSON.stringify(original)}`,
      })
      return
    }
    const keyStr = keyToString(key)
    if (kind === 'r') {
      let observed: unknown | null
      if (stagedWrites.has(keyStr)) {
        observed = decodeValue(stagedWrites.get(keyStr)!.attr)
      } else if (readCache.has(keyStr)) {
        observed = readCache.get(keyStr) ?? null
      } else {
        const response = await client.send(
          new GetItemCommand({
            TableName: TABLE_NAME,
            Key: buildKeyItemFromString(keyStr),
            ConsistentRead: true,
          })
        )
        observed =
          response.Item && response.Item[VALUE_ATTR]
            ? decodeValue(response.Item[VALUE_ATTR]!)
            : null
        readCache.set(keyStr, observed)
      }
      responseOps.push(['r', key, observed])
    } else {
      if (value === null || value === undefined) {
        reply(message, {
          type: 'error',
          code: 12,
          text: 'write operations require a value',
        })
        return
      }
      const attr = encodeValue(value)
      stagedWrites.set(keyStr, { attr, rawKey: key, rawValue: value })
      readCache.set(keyStr, value)
      responseOps.push(['w', key, value])
    }
  }

  if (stagedWrites.size > 0) {
    const transactItems: TransactWriteItem[] = Array.from(
      stagedWrites.entries()
    ).map(([keyStr, write]) => ({
      Put: {
        TableName: TABLE_NAME,
        Item: buildFullItemFromString(keyStr, write.attr),
      },
    }))

    for (let i = 0; i < transactItems.length; i += 25) {
      const chunk = transactItems.slice(i, i + 25)
      await client.send(
        new TransactWriteItemsCommand({
          TransactItems: chunk,
        })
      )
    }
  }

  reply(message, {
    type: 'txn_ok',
    txn: responseOps,
  })
}

function buildKeyItemFromString(key: string): Record<string, AttributeValue> {
  return {
    [KEY_ATTR]: { S: key },
  }
}

function buildFullItemFromString(
  key: string,
  valueAttr: AttributeValue
): Record<string, AttributeValue> {
  return {
    [KEY_ATTR]: { S: key },
    [VALUE_ATTR]: valueAttr,
  }
}

function keyToString(key: unknown): string {
  return typeof key === 'string' ? key : JSON.stringify(key)
}

function encodeValue(value: unknown): AttributeValue {
  return { S: JSON.stringify(value) }
}

function decodeValue(attr: AttributeValue): unknown {
  if ('S' in attr && typeof attr.S === 'string') {
    try {
      return JSON.parse(attr.S)
    } catch {
      return attr.S
    }
  }
  return null
}

function reply(request: MaelstromMessage, body: Record<string, unknown>) {
  const responseBody = {
    ...body,
    in_reply_to: request.body.msg_id,
    msg_id: nextMsgId++,
  }
  send({
    src: nodeId ?? 'unknown',
    dest: request.src,
    body: responseBody,
  })
}

function send(message: OutboundMessage) {
  stdout.write(`${JSON.stringify(message)}\n`)
}
