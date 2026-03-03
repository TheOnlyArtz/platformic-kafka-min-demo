import { Admin, Consumer, GroupProtocols, Producer, stringSerializers } from '@platformatic/kafka'
import { Logger, Module } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import { AppConfig } from '../configuration'
import { KafkaClientOptions, toKafkaBaseOptions } from './kafka-client-options'
import {
  KAFKA_CLIENT,
  KAFKA_CLIENT_OPTIONS,
  KAFKA_CONSUMER_CLIENT,
  KAFKA_PRODUCER_CLIENT,
} from './kafka.constants'
import { KafkaProducerService } from './kafka-producer.service'
import { resolveKafkaSslOptions } from './kafka-ssl'

const KAFKA_REQUEST_TIMEOUT_MS = 60000
const KAFKA_CONNECTION_TIMEOUT_MS = 10000
const KAFKA_RETRY_INITIAL_TIME_MS = 300
const KAFKA_RETRY_COUNT = 8
const KAFKA_MAX_IN_FLIGHTS = 5

function resolveKafkaBrokers(kafkaConfig: AppConfig['kafka']): string[] {
  const brokers = kafkaConfig.brokers
    .map((broker: string) => broker.trim())
    .filter((broker: string) => broker.length > 0)

  return brokers.length > 0 ? brokers : []
}

@Module({
  providers: [
    {
      provide: KAFKA_CLIENT_OPTIONS,
      useFactory: async (configService: ConfigService): Promise<KafkaClientOptions> => {
        const logger = new Logger('KafkaModule')
        const kafkaConfig = configService.get('kafka') as AppConfig['kafka']
        const brokers = resolveKafkaBrokers(kafkaConfig)
        const sslConfig = kafkaConfig.ssl
        let tls: KafkaClientOptions['tls']

        logger.log(`Kafka brokers configured: ${brokers.join(', ')}`)
        try {
          tls = resolveKafkaSslOptions(sslConfig)
        } catch (error) {
          logger.error(
            `Kafka SSL configuration error${error instanceof Error ? `: ${error.message}` : ''}`,
            error instanceof Error ? error.stack : undefined,
          )
          throw error
        }

        if (tls) {
          const certPath = sslConfig?.certPath?.trim() ?? 'missing'
          const keyPath = sslConfig?.keyPath?.trim() ?? 'missing'
          logger.log(`Kafka SSL enabled (certPath=${certPath}, keyPath=${keyPath})`)
        }

        const options: KafkaClientOptions = {
          bootstrapBrokers: brokers,
          groupId: `${kafkaConfig.groupId}-group`,
          tls,
          requestTimeout: KAFKA_REQUEST_TIMEOUT_MS,
          connectTimeout: KAFKA_CONNECTION_TIMEOUT_MS,
          retries: KAFKA_RETRY_COUNT,
          retryDelay: KAFKA_RETRY_INITIAL_TIME_MS,
          maxInflights: KAFKA_MAX_IN_FLIGHTS,
          producerClientId: `${kafkaConfig.groupId}-producer`,
          consumerClientId: `${kafkaConfig.groupId}-consumer`,
          adminClientId: `${kafkaConfig.groupId}-admin`,
        }

        const admin = new Admin(toKafkaBaseOptions(options, options.adminClientId))

        try {
          await admin.listTopics()
          logger.log('Kafka connection established successfully')
        } catch (error) {
          logger.error(
            'Kafka connection failed during startup',
            error instanceof Error ? error.stack : undefined,
          )
          throw error
        } finally {
          await admin.close().catch(() => undefined)
        }

        return options
      },
      inject: [ConfigService],
    },
    {
      provide: KAFKA_CLIENT,
      useExisting: KAFKA_CLIENT_OPTIONS,
    },
    {
      provide: KAFKA_PRODUCER_CLIENT,
      useFactory: (options: KafkaClientOptions) => {
        return new Producer<string, string, string, string>({
          ...toKafkaBaseOptions(options, options.producerClientId),
          serializers: stringSerializers,
          idempotent: false,
        })
      },
      inject: [KAFKA_CLIENT_OPTIONS],
    },
    {
      provide: KAFKA_CONSUMER_CLIENT,
      useFactory: (options: KafkaClientOptions) => {
        return new Consumer({
          ...toKafkaBaseOptions(options, options.consumerClientId),
          autocommit: 5000,
          groupId: options.groupId,
        })
      },
      inject: [KAFKA_CLIENT_OPTIONS],
    },
    {
      provide: KafkaProducerService,
      useFactory: (producer: Producer<string, string, string, string>) => {
        return new KafkaProducerService(producer)
      },
      inject: [KAFKA_PRODUCER_CLIENT],
    },
  ],
  exports: [
    KAFKA_CLIENT,
    KAFKA_CLIENT_OPTIONS,
    KAFKA_CONSUMER_CLIENT,
    KAFKA_PRODUCER_CLIENT,
    KafkaProducerService,
  ],
})
export class KafkaModule {}
