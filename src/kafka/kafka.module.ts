import { Consumer, Producer, stringDeserializers, stringSerializers } from '@platformatic/kafka'
import { Logger, Module } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import type { AppConfig } from '../configuration'
import {
  KAFKA_CLIENT_OPTIONS,
  KAFKA_CONSUMER_CLIENT,
  KAFKA_PRODUCER_CLIENT,
} from './kafka.constants'
import { KafkaProducerService } from './kafka-producer.service'
import type { KafkaClientOptions } from './kafka.types'

@Module({
  providers: [
    {
      provide: KAFKA_CLIENT_OPTIONS,
      useFactory: (configService: ConfigService): KafkaClientOptions => {
        const kafkaConfig = configService.getOrThrow<AppConfig['kafka']>('kafka')
        const options: KafkaClientOptions = {
          bootstrapBrokers: kafkaConfig.brokers,
          groupId: kafkaConfig.groupId,
          producerClientId: `${kafkaConfig.groupId}-producer`,
          consumerClientId: `${kafkaConfig.groupId}-consumer`,
        }

        return options
      },
      inject: [ConfigService],
    },
    {
      provide: KAFKA_PRODUCER_CLIENT,
      useFactory: (options: KafkaClientOptions) => {
        return new Producer<string, string, string, string>({
          clientId: options.producerClientId,
          bootstrapBrokers: options.bootstrapBrokers,
          serializers: stringSerializers,
          idempotent: false,
        })
      },
      inject: [KAFKA_CLIENT_OPTIONS],
    },
    {
      provide: KAFKA_CONSUMER_CLIENT,
      useFactory: (options: KafkaClientOptions) => {
        return new Consumer<string, string, string, string>({
          groupId: options.groupId,
          clientId: options.consumerClientId,
          bootstrapBrokers: options.bootstrapBrokers,
          deserializers: stringDeserializers,
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
    Logger,
  ],
  exports: [KAFKA_CLIENT_OPTIONS, KAFKA_PRODUCER_CLIENT, KAFKA_CONSUMER_CLIENT, KafkaProducerService],
})
export class KafkaModule {}
