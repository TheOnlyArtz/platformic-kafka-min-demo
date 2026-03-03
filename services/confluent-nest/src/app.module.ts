import { Module } from '@nestjs/common'
import { configModule } from './configuration'
import { DemoConsumer } from './demo/demo.consumer'
import { DemoService } from './demo/demo.service'
import { KafkaConsumerRunnerService } from './kafka/kafka-consumer-runner.service'
import { KafkaModule } from './kafka/kafka.module'

@Module({
  imports: [configModule, KafkaModule],
  providers: [
    DemoService,
    DemoConsumer,
    KafkaConsumerRunnerService,
  ],
})
export class AppModule {}
