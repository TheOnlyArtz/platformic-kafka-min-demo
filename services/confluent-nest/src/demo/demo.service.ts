import { Injectable } from '@nestjs/common'
import { config } from '../configuration'
import { KafkaProducerService } from '../kafka/kafka-producer.service'

export type DemoInboundMessage = Record<string, unknown>

const DEFAULT_UPLINK_TOPIC = 'kafka.benchmark.uplink'

@Injectable()
export class DemoService {
  constructor(private readonly kafkaProducer: KafkaProducerService) {}

  handleInboundMessage(payload: DemoInboundMessage): void {
    const uplinkTopic = config.kafka.uplinkTopic ?? DEFAULT_UPLINK_TOPIC
    void this.kafkaProducer.send({
      topic: uplinkTopic,
      messages: [
        {
          value: JSON.stringify(payload),
        },
      ],
    })
  }
}
