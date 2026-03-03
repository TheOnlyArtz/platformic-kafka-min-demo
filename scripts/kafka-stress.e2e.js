#!/usr/bin/env node

const { Kafka, CompressionTypes } = require('kafkajs')
const { performance } = require('node:perf_hooks')
const { investigator } = require('@tsgs/icd-types')

const BROKERS = (process.env.E2E_KAFKA_BROKERS ?? '0.0.0.0:19092')
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean)

const CLIENT_ID = process.env.E2E_CLIENT_ID ?? 'kafka-stress-e2e'
const TOPIC =
  process.env.E2E_TOPIC ?? `kafka.benchmark.downlink`
const TAIL_NUMBER = process.env.E2E_TAIL_NUMBER ?? '9999'

const TOTAL_MESSAGES = 5000
const BATCH_SIZE = 100
const IN_FLIGHT = 100
const LOG_EVERY = 1000
const ACKS = 1
const COMPRESSION = 'none'

const COMPRESSION_TYPES = {
  none: CompressionTypes.None,
  gzip: CompressionTypes.GZIP,
  snappy: CompressionTypes.Snappy,
  lz4: CompressionTypes.LZ4,
  zstd: CompressionTypes.ZSTD,
}

function objectifyBigInts(value) {
  if (typeof value === 'bigint') {
    return { type: 'BigInt', value: value.toString() }
  }

  if (Array.isArray(value)) {
    return value.map((item) => objectifyBigInts(item))
  }

  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, objectifyBigInts(entry)]))
  }

  return value
}

const BASE_DATA = objectifyBigInts(new investigator.PayloadReportData())

function buildPayload() {
  return {
    header: {
      type: 'investigator',
      tailNumber: TAIL_NUMBER,
      messageTypeId: investigator.IcdOpcodes.OP_PayloadReport,
      timestamp: Math.floor(Date.now() / 1000),
    },
    data: BASE_DATA,
  }
}

function toCompressionType() {
  return COMPRESSION_TYPES[COMPRESSION] ?? CompressionTypes.None
}

async function run() {
  if (!Number.isFinite(TOTAL_MESSAGES) || TOTAL_MESSAGES <= 0) {
    throw new Error('E2E_TOTAL_MESSAGES must be a positive number')
  }

  if (!Number.isFinite(BATCH_SIZE) || BATCH_SIZE <= 0) {
    throw new Error('E2E_BATCH_SIZE must be a positive number')
  }

  if (!Number.isFinite(IN_FLIGHT) || IN_FLIGHT <= 0) {
    throw new Error('E2E_IN_FLIGHT must be a positive number')
  }

  const kafka = new Kafka({ clientId: CLIENT_ID, brokers: BROKERS })
  const producer = kafka.producer({ allowAutoTopicCreation: true })

  console.log('Kafka stress test starting...')
  console.log(`Brokers: ${BROKERS.join(', ')}`)
  console.log(`Topic: ${TOPIC}`)
  console.log(`TailNumber: ${TAIL_NUMBER}`)
  console.log(`Total: ${TOTAL_MESSAGES} | Batch: ${BATCH_SIZE} | InFlight: ${IN_FLIGHT}`)

  const compressionType = toCompressionType()
  const start = performance.now()
  let sent = 0
  let totalBytes = 0
  const inFlight = []

  await producer.connect()

  try {
    for (let offset = 0; offset < TOTAL_MESSAGES; offset += BATCH_SIZE) {
      const count = Math.min(BATCH_SIZE, TOTAL_MESSAGES - offset)
      const messages = new Array(count)
      let batchBytes = 0

      for (let index = 0; index < count; index += 1) {
        const payload = buildPayload()
        const value = JSON.stringify(payload)
        batchBytes += Buffer.byteLength(value)
        messages[index] = {
          key: null,
          value,
        }
      }

      totalBytes += batchBytes
      sent += count

      const sendPromise = producer.send({
        topic: TOPIC,
        messages,
        acks: ACKS,
        compression: compressionType,
      })

      inFlight.push(sendPromise)

      if (sent % LOG_EVERY < count) {
        const elapsedSeconds = (performance.now() - start) / 1000
        const rate = elapsedSeconds > 0 ? (sent / elapsedSeconds).toFixed(1) : '0'
        console.log(`Sent ${sent}/${TOTAL_MESSAGES} messages (${rate} msg/s)`)
      }

      if (inFlight.length >= IN_FLIGHT) {
        await Promise.all(inFlight)
        inFlight.length = 0
      }
    }

    if (inFlight.length > 0) {
      await Promise.all(inFlight)
    }
  } finally {
    await producer.disconnect()
  }

  const durationSeconds = (performance.now() - start) / 1000
  const msgRate = durationSeconds > 0 ? (TOTAL_MESSAGES / durationSeconds).toFixed(1) : '0'
  const mbRate = durationSeconds > 0 ? (totalBytes / (1024 * 1024 * durationSeconds)).toFixed(2) : '0'

  console.log('Kafka stress test finished.')
  console.log(`Duration: ${durationSeconds.toFixed(2)}s`)
  console.log(`Throughput: ${msgRate} msg/s`)
  console.log(`Payload rate: ${mbRate} MB/s`)
}

if (require.main === module) {
  run().catch((error) => {
    console.error('Kafka stress test failed:', error)
    process.exitCode = 1
  })
}

module.exports = {
  run,
}
