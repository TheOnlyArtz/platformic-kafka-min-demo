import { Module } from '@nestjs/common'
import { KafkaModule } from '../kafka/kafka.module'
import { DemoConsumer } from './demo.consumer'
import { DemoController } from './demo.controller'
import { DemoService } from './demo.service'

@Module({
  imports: [KafkaModule],
  controllers: [DemoController],
  providers: [DemoService, DemoConsumer],
  exports: [DemoConsumer],
})
export class DemoModule {}
