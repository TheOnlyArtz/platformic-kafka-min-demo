import { Producer, stringSerializers } from '@platformatic/kafka'

const SOURCE_TOPIC = 'platformic.minimum.demo'
const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:19092').split(',')
const TOTAL_MESSAGES = Number(process.env.COUNT ?? 100000)
const BATCH_SIZE = Number(process.env.BATCH_SIZE ?? 1000)
const PAYLOAD_SIZE = Number(process.env.PAYLOAD_SIZE ?? 128)

async function main(): Promise<void> {
  const producer = new Producer<string, string, string, string>({
    clientId: 'platformic-benchmark-producer',
    bootstrapBrokers: BROKERS,
    serializers: stringSerializers,
  })

  const payload = 'x'.repeat(PAYLOAD_SIZE)
  const startedAt = Date.now()

  let sent = 0

  while (sent < TOTAL_MESSAGES) {
    const batchSize = Math.min(BATCH_SIZE, TOTAL_MESSAGES - sent)
    const startIndex = sent

    const messages = Array.from({ length: batchSize }, (_, index) => {
      const id = startIndex + index

      return {
        topic: SOURCE_TOPIC,
        key: String(id),
        value: JSON.stringify({ id, payload, ts: Date.now() }),
      }
    })

    await producer.send({ messages })
    sent += batchSize

    if (sent % 10000 === 0 || sent === TOTAL_MESSAGES) {
      const elapsedSeconds = (Date.now() - startedAt) / 1000
      const rate = Math.round(sent / elapsedSeconds)
      console.log(`Sent ${sent}/${TOTAL_MESSAGES} messages (${rate}/s)`)
    }
  }

  await producer.close()
}

void main()
