import { Logger, Module } from '@nestjs/common'
import type { Consumer } from '@platformatic/kafka'
import { AppController } from './app.controller'
import { configModule } from './configuration'
import { DemoConsumer } from './demo/demo.consumer'
import { DemoModule } from './demo/demo.module'
import { KAFKA_CONSUMER_CLIENT } from './kafka/kafka.constants'
import { KafkaInboundConsumerService } from './kafka/kafka-inbound-consumer.service'
import { KafkaModule } from './kafka/kafka.module'

@Module({
  imports: [configModule, KafkaModule, DemoModule],
  controllers: [AppController],
  providers: [
    Logger,
    {
      provide: KafkaInboundConsumerService,
      useFactory: (
        consumer: Consumer<string, string, string, string>,
        demoConsumer: DemoConsumer,
      ) => {
        return new KafkaInboundConsumerService(consumer, demoConsumer)
      },
      inject: [KAFKA_CONSUMER_CLIENT, DemoConsumer],
    },
  ],
})
export class AppModule {}
