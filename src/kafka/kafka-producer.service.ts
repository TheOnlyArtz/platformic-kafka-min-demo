import { type MessageToProduce, Producer } from '@platformatic/kafka'
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common'

export interface KafkaProducerMessage {
  readonly key?: string
  readonly value: string
  readonly headers?: Map<string, string> | Record<string, string>
}

export interface KafkaProducerRecord {
  readonly topic: string
  readonly messages: ReadonlyArray<KafkaProducerMessage>
}

@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaProducerService.name)
  private connectPromise: Promise<void> | null = null
  private connected = false
  private shuttingDown = false

  constructor(private readonly producer: Producer<string, string, string, string>) {}

  async onModuleInit(): Promise<void> {
    await this.connect('startup')
  }

  async onModuleDestroy(): Promise<void> {
    this.shuttingDown = true
    this.connected = false
    await this.producer.close().catch(() => undefined)
  }

  async send(record: KafkaProducerRecord): Promise<void> {
    if (this.shuttingDown) {
      throw new Error('Kafka producer is shutting down')
    }

    await this.connect('send')

    const messages = this.normalizeMessages(record)
    this.producer.send({ messages, acks: -1, repeatOnStaleMetadata: true }).catch(console.error);
  }

  private normalizeMessages(
    record: KafkaProducerRecord,
  ): Array<MessageToProduce<string, string, string, string>> {
    return record.messages.map((message) => {
      return {
        topic: record.topic,
        key: message.key,
        value: message.value,
        headers: message.headers,
      }
    })
  }

  private async connect(reason: 'startup' | 'send'): Promise<void> {
    if (this.connected && this.producer.isConnected()) {
      return
    }

    if (this.connectPromise !== null) {
      await this.connectPromise
      return
    }

    this.connectPromise = (async () => {
      try {
        await this.producer.connectToBrokers()
        this.connected = true
        this.logger.log(`Kafka producer connected (${reason})`)
      } catch (error) {
        this.connected = false
        this.logger.error(
          'Kafka producer connection failed',
          error instanceof Error ? error.stack : undefined,
        )
        throw error
      } finally {
        this.connectPromise = null
      }
    })()

    await this.connectPromise
  }
}
