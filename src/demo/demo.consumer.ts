import { Injectable } from '@nestjs/common'
import type { KafkaTopicHandler, KafkaTopicMessageHandler } from '../kafka/kafka-topic-handler'
import { DemoService } from './demo.service'

const SOURCE_TOPIC = 'platformic.minimum.demo'

@Injectable()
export class DemoConsumer implements KafkaTopicHandler {
  constructor(private readonly demoService: DemoService) {}

  getTopicHandlers(): ReadonlyMap<string, KafkaTopicMessageHandler> {
    return new Map<string, KafkaTopicMessageHandler>([
      [SOURCE_TOPIC, (message) => this.onSourceMessage(message)],
    ])
  }

  private async onSourceMessage(message: Parameters<KafkaTopicMessageHandler>[0]): Promise<void> {
    void this.demoService.handleInboundMessage(message)
  }
}
