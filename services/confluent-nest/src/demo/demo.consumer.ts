import { Injectable } from '@nestjs/common'
import { config } from '../configuration'
import { KafkaTopicHandler } from '../kafka/kafka-topic-handler'
import { DemoInboundMessage, DemoService } from './demo.service'

const DEFAULT_DOWNLINK_TOPIC = 'kafka.benchmark.downlink'

@Injectable()
export class DemoConsumer {
  constructor(private readonly demoService: DemoService) {}

  getTopicHandlers(): ReadonlyArray<KafkaTopicHandler<DemoInboundMessage>> {
    const downlinkTopic = config.kafka.downlinkTopic ?? DEFAULT_DOWNLINK_TOPIC
    return [
      {
        topic: downlinkTopic,
        parse: (rawValue: string) => JSON.parse(rawValue) as DemoInboundMessage,
        handle: (payload: DemoInboundMessage) => this.onDownlink(payload),
      },
    ]
  }

  onDownlink(payload: DemoInboundMessage): void {
    this.demoService.handleInboundMessage(payload)
  }
}
