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
  TransactionCanceledException,
  type AttributeValue,
  type TransactWriteItem,
} from '@aws-sdk/client-dynamodb'
import { DB } from '../../src/index.ts'
import { createConfig } from '../../src/config.ts'

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
  const response = await client.send(
    new GetItemCommand({
      TableName: TABLE_NAME,
      Key: keyItem,
      ConsistentRead: true,
    })
  )
  if (!response.Item || !response.Item[VALUE_ATTR]) {
    sendError(message, 20, 'key does not exist')
    return
  }
  reply(message, {
    type: 'read_ok',
    value: decodeValue(response.Item[VALUE_ATTR]!),
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
  await client.send(
    new PutItemCommand({
      TableName: TABLE_NAME,
      Item: buildFullItem(key, value),
    })
  )
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
        TableName: TABLE_NAME,
        Key: keyAttr,
        ConditionExpression: '#v = :expected',
        ExpressionAttributeNames: conditionNames,
        ExpressionAttributeValues: {
          ':expected': fromAttr,
        },
        ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
      },
    },
    {
      Update: {
        TableName: TABLE_NAME,
        Key: keyAttr,
        UpdateExpression: 'SET #v = :next',
        ExpressionAttributeNames: conditionNames,
        ExpressionAttributeValues: {
          ':next': toAttr,
        },
      },
    },
  ]

  try {
    await client.send(
      new TransactWriteItemsCommand({
        TransactItems: items,
      })
    )
    reply(message, { type: 'cas_ok' })
  } catch (error) {
    if (error instanceof TransactionCanceledException) {
      const reason = error.CancellationReasons?.find((r) => r.Code !== 'None')
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

function buildKeyItem(key: unknown): Record<string, AttributeValue> {
  return {
    [KEY_ATTR]: { S: keyToString(key) },
  }
}

function buildFullItem(
  key: unknown,
  value: unknown
): Record<string, AttributeValue> {
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
