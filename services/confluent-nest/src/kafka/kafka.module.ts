import { AdminClient, type GlobalConfig, Producer } from '@confluentinc/kafka-javascript'
import { Logger, Module } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import { AppConfig } from '../configuration'
import { KAFKA_BASE_CONFIG, KAFKA_CLIENT } from './kafka.constants'
import { KafkaProducerService } from './kafka-producer.service'
import { connectClient, disconnectClient, listTopics } from './kafka-rdkafka-promises'
import { resolveKafkaSslOptions } from './kafka-ssl'

const KAFKA_REQUEST_TIMEOUT_MS = 60000
const KAFKA_RETRY_INITIAL_TIME_MS = 300
const KAFKA_RETRY_MAX_TIME_MS = 30000
const KAFKA_POLL_INTERVAL_MS = 1000

function resolveKafkaBrokers(kafkaConfig: AppConfig['kafka']): string[] {
  const brokers = kafkaConfig.brokers.map((broker: string) => broker.trim())

  if (brokers.length === 0) {
    throw new Error('Invalid kafka.brokers configuration: at least one broker is required')
  }

  return brokers
}

function createKafkaClientConfig(kafkaConfig: AppConfig['kafka']): GlobalConfig {
  const brokers = resolveKafkaBrokers(kafkaConfig)
  const sslOptions = resolveKafkaSslOptions(kafkaConfig.ssl)

  return {
    'bootstrap.servers': brokers.join(','),
    'client.id': kafkaConfig.groupId,
    'socket.timeout.ms': KAFKA_REQUEST_TIMEOUT_MS,
    'retry.backoff.ms': KAFKA_RETRY_INITIAL_TIME_MS,
    'retry.backoff.max.ms': KAFKA_RETRY_MAX_TIME_MS,
    ...sslOptions.clientOptions,
  }
}

@Module({
  providers: [
    {
      provide: KAFKA_BASE_CONFIG,
      useFactory: (configService: ConfigService) => {
        const logger = new Logger('KafkaModule')
        const kafkaConfig = configService.get('kafka') as AppConfig['kafka']
        const brokers = resolveKafkaBrokers(kafkaConfig)
        const sslOptions = resolveKafkaSslOptions(kafkaConfig.ssl)

        logger.log(`Kafka brokers configured: ${brokers.join(', ')}`)
        if (sslOptions.enabled) {
          const certPath = kafkaConfig.ssl?.certPath?.trim() ?? 'missing'
          const keyPath = kafkaConfig.ssl?.keyPath?.trim() ?? 'missing'
          logger.log(
            `Kafka SSL enabled (certPath=${certPath}, keyPath=${keyPath})`,
          )
        } else {
          logger.log('Kafka SSL disabled')
        }

        return createKafkaClientConfig(kafkaConfig)
      },
      inject: [ConfigService],
    },
    {
      provide: KAFKA_CLIENT,
      useFactory: async (baseConfig: GlobalConfig) => {
        const logger = new Logger('KafkaModule')
        const producer = new Producer(baseConfig)

        try {
          await connectClient(producer)
          producer.setPollInterval(KAFKA_POLL_INTERVAL_MS)

          const admin = AdminClient.createFrom(producer)
          await listTopics(admin, KAFKA_REQUEST_TIMEOUT_MS)
          admin.disconnect()
          logger.log('Kafka connection established successfully')
        } catch (error) {
          await disconnectClient(producer).catch(() => undefined)
          logger.error(
            'Kafka connection failed during startup',
            error instanceof Error ? error.stack : undefined,
          )
          throw error
        }

        return producer
      },
      inject: [KAFKA_BASE_CONFIG],
    },
    {
      provide: Producer,
      useExisting: KAFKA_CLIENT,
    },
    KafkaProducerService,
  ],
  exports: [KAFKA_BASE_CONFIG, KAFKA_CLIENT, Producer, KafkaProducerService],
})
export class KafkaModule {}
