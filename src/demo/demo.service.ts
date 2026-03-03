import { Injectable, Logger } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import type { AppConfig } from '../configuration'
import { KafkaProducerService } from '../kafka/kafka-producer.service'
import type { KafkaInboundMessage } from '../kafka/kafka-topic-handler'
import type { DemoSendRequestDto } from './types/demo-send.dto'

@Injectable()
export class DemoService {
  private readonly logger = new Logger(DemoService.name)
  private readonly sourceTopic: AppConfig['kafka']['sourceTopic']
  private readonly uplinkTopic: AppConfig['kafka']['uplinkTopic']
  private forwardedCount = 0
  private readonly startedAt = Date.now()

  constructor(
    private readonly configService: ConfigService,
    private readonly kafkaProducer: KafkaProducerService,
  ) {
    const kafkaConfig = this.configService.getOrThrow<AppConfig['kafka']>('kafka')
    this.sourceTopic = kafkaConfig.sourceTopic
    this.uplinkTopic = kafkaConfig.uplinkTopic
  }

  hi(): string {
    return 'hi'
  }

  async publishSourceMessage(request: DemoSendRequestDto): Promise<void> {
    void this.kafkaProducer.send({
      topic: this.sourceTopic,
      messages: [
        {
          key: request.key,
          value: request.value,
          headers: request.headers,
        },
      ],
    })
  }

  async handleInboundMessage(message: KafkaInboundMessage): Promise<void> {
    void this.kafkaProducer.send({
      topic: this.uplinkTopic,
      messages: [
        {
          key: message.key,
          value: message.value ?? '',
          headers: message.headers,
        },
      ],
    })

    this.forwardedCount += 1

    if (this.forwardedCount % 1000 === 0) {
      const elapsedSeconds = (Date.now() - this.startedAt) / 1000
      const rate = Math.round(this.forwardedCount / elapsedSeconds)
      this.logger.log(`Forwarded ${this.forwardedCount} messages (${rate}/s)`)
    }
  }
}
