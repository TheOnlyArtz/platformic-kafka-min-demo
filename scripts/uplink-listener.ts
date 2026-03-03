import { Consumer, stringDeserializers } from '@platformatic/kafka'

const UPLINK_TOPIC = 'platformic.uplink.demo'
const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:19092').split(',')
const GROUP_ID = process.env.GROUP_ID ?? 'platformic-uplink-listener-group'
const EXPECTED_COUNT = Number(process.env.EXPECTED_COUNT ?? 0)

async function main(): Promise<void> {
  const consumer = new Consumer<string, string, string, string>({
    groupId: GROUP_ID,
    clientId: 'platformic-uplink-listener',
    bootstrapBrokers: BROKERS,
    deserializers: stringDeserializers,
  })

  const stream = await consumer.consume({
    autocommit: true,
    topics: [UPLINK_TOPIC],
    sessionTimeout: 10000,
    heartbeatInterval: 500,
  })

  console.log(`Listening on ${UPLINK_TOPIC}`)

  let received = 0
  const startedAt = Date.now()

  for await (const _message of stream) {
    received += 1

    if (received % 10000 === 0) {
      const elapsedSeconds = (Date.now() - startedAt) / 1000
      const rate = Math.round(received / elapsedSeconds)
      console.log(`Received ${received} messages (${rate}/s)`)
    }

    if (EXPECTED_COUNT > 0 && received >= EXPECTED_COUNT) {
      const elapsedSeconds = (Date.now() - startedAt) / 1000
      const rate = Math.round(received / elapsedSeconds)
      console.log(`Reached EXPECTED_COUNT=${EXPECTED_COUNT} (${rate}/s)`)
      break
    }
  }

  await consumer.close()
}

void main()
