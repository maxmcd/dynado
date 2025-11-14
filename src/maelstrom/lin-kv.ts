import readline from 'node:readline'
import { stdin, stdout, stderr } from 'node:process'
import path from 'node:path'

import type { AttributeValue } from '@aws-sdk/client-dynamodb'
import { ShardedSQLiteStorage } from '../storage-sqlite.ts'
import type { DynamoDBItem, TransactWriteItem } from '../storage.ts'
import { TransactionCancelledError } from '../storage.ts'

type MaelstromMessage = {
  src: string
  dest: string
  body: {
    type: string
    msg_id?: number
    in_reply_to?: number
    key?: unknown
    value?: unknown
    from?: unknown
    to?: unknown
    node_id?: string
    node_ids?: string[]
  }
}

type OutboundMessage = {
  src: string
  dest: string
  body: Record<string, unknown>
}

const TABLE_NAME = 'lin_kv'
const KEY_ATTR = 'pk'
const VALUE_ATTR = 'val'

const dataRoot =
  process.env.MAELSTROM_DATA_DIR ?? path.join(process.cwd(), '.maelstrom-data')
const nodeDataDir = path.join(dataRoot, `node-${process.pid}`)
const shardCount = parseInt(process.env.MAELSTROM_SHARD_COUNT ?? '1', 10)

const storage = new ShardedSQLiteStorage({
  shardCount,
  dataDir: nodeDataDir,
})

let tableReady: Promise<void> | null = null
async function ensureTableExists() {
  if (!tableReady) {
    tableReady = (async () => {
      const existing = await storage.describeTable(TABLE_NAME)
      if (!existing) {
        await storage.createTable({
          tableName: TABLE_NAME,
          keySchema: [{ AttributeName: KEY_ATTR, KeyType: 'HASH' }],
          attributeDefinitions: [
            { AttributeName: KEY_ATTR, AttributeType: 'S' },
          ],
        })
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
      storage.close()
      process.exit(0)
    })
})

async function handleLine(line: string) {
  if (!line.trim()) {
    return
  }
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
    case 'read':
      await handleRead(message)
      return
    case 'write':
      await handleWrite(message)
      return
    case 'cas':
      await handleCas(message)
      return
    default:
      reply(message, {
        type: 'error',
        code: 10,
        text: `unsupported message type: ${body.type}`,
      })
  }
}

async function handleRead(message: MaelstromMessage) {
  await ensureTableExists()
  const { key, msg_id } = message.body
  const keyItem = buildKeyItem(key)
  const item = await storage.getItem(TABLE_NAME, keyItem)
  if (!item || !item[VALUE_ATTR]) {
    sendError(message, 20, 'key does not exist')
    return
  }
  reply(message, {
    type: 'read_ok',
    value: decodeValue(item[VALUE_ATTR]),
    in_reply_to: msg_id,
  })
}

async function handleWrite(message: MaelstromMessage) {
  await ensureTableExists()
  const { key, value } = message.body
  if (key === undefined) {
    sendError(message, 12, 'write requires key')
    return
  }
  if (value === undefined) {
    sendError(message, 12, 'write requires value')
    return
  }
  await storage.putItem(TABLE_NAME, buildFullItem(key, value))
  reply(message, { type: 'write_ok' })
}

async function handleCas(message: MaelstromMessage) {
  await ensureTableExists()
  const { key, from, to } = message.body
  if (key === undefined) {
    sendError(message, 12, 'cas requires key')
    return
  }
  if (from === undefined || to === undefined) {
    sendError(message, 12, 'cas requires both from and to')
    return
  }
  const keyAttr = buildKeyItem(key)
  const conditionNames = { '#v': VALUE_ATTR }
  const fromAttr = encodeValue(from)
  const toAttr = encodeValue(to)

  const items: TransactWriteItem[] = [
    {
      ConditionCheck: {
        tableName: TABLE_NAME,
        key: keyAttr,
        conditionExpression: '#v = :expected',
        expressionAttributeNames: conditionNames,
        expressionAttributeValues: {
          ':expected': fromAttr,
        },
        returnValuesOnConditionCheckFailure: 'ALL_OLD',
      },
    },
    {
      Update: {
        tableName: TABLE_NAME,
        key: keyAttr,
        updateExpression: 'SET #v = :next',
        expressionAttributeNames: conditionNames,
        expressionAttributeValues: {
          ':next': toAttr,
        },
      },
    },
  ]

  try {
    await storage.transactWrite(items)
    reply(message, { type: 'cas_ok' })
  } catch (error) {
    if (error instanceof TransactionCancelledError) {
      const reason = error.cancellationReasons?.find((r) => r.Code !== 'None')
      if (reason?.Code === 'ConditionalCheckFailed') {
        if (!reason.Item || !reason.Item[VALUE_ATTR]) {
          sendError(message, 20, 'key does not exist')
          return
        }
        const observed = decodeValue(reason.Item[VALUE_ATTR])
        sendError(
          message,
          22,
          `compare-and-set failed: expected ${JSON.stringify(
            from
          )}, observed ${JSON.stringify(observed)}`
        )
        return
      }
    }
    throw error
  }
}

function reply(request: MaelstromMessage, body: Record<string, unknown>) {
  const inReplyTo = request.body.msg_id
  const responseBody = {
    ...body,
    in_reply_to: inReplyTo,
    msg_id: nextMsgId++,
  }
  send({
    src: nodeId ?? 'unknown',
    dest: request.src,
    body: responseBody,
  })
}

function sendError(message: MaelstromMessage, code: number, text: string) {
  reply(message, { type: 'error', code, text })
}

function send(message: OutboundMessage) {
  stdout.write(`${JSON.stringify(message)}\n`)
}

function buildKeyItem(key: unknown): DynamoDBItem {
  return {
    [KEY_ATTR]: { S: keyToString(key) },
  }
}

function buildFullItem(key: unknown, value: unknown): DynamoDBItem {
  return {
    ...buildKeyItem(key),
    [VALUE_ATTR]: encodeValue(value),
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
