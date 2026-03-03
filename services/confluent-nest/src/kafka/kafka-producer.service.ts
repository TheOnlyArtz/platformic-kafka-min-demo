import { type MessageHeader, Producer } from '@confluentinc/kafka-javascript'
import { Injectable, OnModuleDestroy } from '@nestjs/common'
import { disconnectClient } from './kafka-rdkafka-promises'

export interface KafkaProducerMessage {
  readonly key?: string | Buffer | null
  readonly value: string | Buffer | null
  readonly partition?: number
  readonly headers?: Readonly<Record<string, string | Buffer>>
}

export interface KafkaProducerRecord {
  readonly topic: string
  readonly messages: ReadonlyArray<KafkaProducerMessage>
}

@Injectable()
export class KafkaProducerService implements OnModuleDestroy {
  private shuttingDown = false

  constructor(private readonly producer: Producer) {}

  getProducer(): Producer {
    if (this.shuttingDown) {
      throw new Error('Kafka producer is shutting down')
    }

    return this.producer
  }

  async send(record: KafkaProducerRecord): Promise<void> {
    if (this.shuttingDown) {
      throw new Error('Kafka producer is shutting down')
    }

    for (const message of record.messages) {
      const headers = this.resolveHeaders(message.headers)
      const value = this.resolveValue(message.value)

      this.producer.produce(
        record.topic,
        message.partition ?? null,
        value,
        message.key ?? null,
        Date.now(),
        undefined,
        headers,
      )
    }

    this.producer.poll()
  }

  async onModuleDestroy(): Promise<void> {
    this.shuttingDown = true
    await disconnectClient(this.producer).catch(() => undefined)
  }

  private resolveValue(value: string | Buffer | null): Buffer | null {
    if (value === null) {
      return null
    }

    if (Buffer.isBuffer(value)) {
      return value
    }

    return Buffer.from(value)
  }

  private resolveHeaders(
    headers: Readonly<Record<string, string | Buffer>> | undefined,
  ): MessageHeader[] | undefined {
    if (!headers) {
      return undefined
    }

    return Object.entries(headers).map(([key, value]) => ({
      [key]: value,
    }))
  }

}
