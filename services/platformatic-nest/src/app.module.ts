import { Module } from '@nestjs/common'
import type { Consumer } from '@platformatic/kafka'
import { configModule } from './configuration'
import { DemoConsumer } from './demo/demo.consumer'
import { DemoService } from './demo/demo.service'
import { KafkaInboundConsumerService } from './kafka/kafka-inbound-consumer.service'
import { KAFKA_CONSUMER_CLIENT } from './kafka/kafka.constants'
import { KafkaModule } from './kafka/kafka.module'

@Module({
  imports: [configModule, KafkaModule],
  providers: [
    DemoService,
    DemoConsumer,
    {
      provide: KafkaInboundConsumerService,
      useFactory: (
        consumer: Consumer<Buffer, Buffer, Buffer, Buffer>,
        demoConsumer: DemoConsumer,
      ) => {
        return new KafkaInboundConsumerService(
          consumer,
          demoConsumer,
        )
      },
      inject: [
        KAFKA_CONSUMER_CLIENT,
        DemoConsumer,
      ],
    },
  ],
})
export class AppModule {}
