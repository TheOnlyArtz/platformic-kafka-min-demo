import { ConfigModule } from '@nestjs/config'

export interface AppConfig {
  readonly http: {
    readonly port: number
  }
  readonly kafka: {
    readonly brokers: string[]
    readonly groupId: string
    readonly sourceTopic: 'platformic.minimum.demo'
    readonly uplinkTopic: 'platformic.uplink.demo'
  }
}

function parseBrokers(rawBrokers: string | undefined): string[] {
  const brokers = (rawBrokers ?? 'localhost:19092')
    .split(',')
    .map((broker) => broker.trim())
    .filter((broker) => broker.length > 0)

  return brokers.length > 0 ? brokers : ['localhost:19092']
}

export function configuration(): AppConfig {
  return {
    http: {
      port: Number(process.env.PORT ?? 3000),
    },
    kafka: {
      brokers: parseBrokers(process.env.KAFKA_BROKERS),
      groupId: process.env.KAFKA_GROUP_ID ?? 'platformic-minimum-demo-group',
      sourceTopic: 'platformic.minimum.demo',
      uplinkTopic: 'platformic.uplink.demo',
    },
  }
}

export const configModule = ConfigModule.forRoot({
  isGlobal: true,
  cache: true,
  load: [configuration],
})
