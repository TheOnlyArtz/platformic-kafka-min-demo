import { Injectable } from '@nestjs/common'
import { config } from '../configuration'
import { KafkaTopicHandler } from '../kafka/kafka-topic-handler'
import { DemoInboundMessage, DemoService } from './demo.service'

const DEFAULT_DOWNLINK_TOPIC = 'kafka.benchmark.downlink'

@Injectable()
export class DemoConsumer implements KafkaTopicHandler {
  constructor(private readonly demoService: DemoService) {}

  getTopicHandlers(): ReadonlyMap<string, (payload: unknown) => void> {
    const downlinkTopic = config.kafka.downlinkTopic ?? DEFAULT_DOWNLINK_TOPIC
    return new Map<string, (payload: unknown) => void>([
      [downlinkTopic, (payload) => this.onDownlink(payload as DemoInboundMessage)],
    ])
  }

  onDownlink(payload: DemoInboundMessage): void {
    this.demoService.handleInboundMessage(payload)
  }
}
