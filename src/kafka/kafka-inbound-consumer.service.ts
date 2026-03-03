import { setTimeout as delay } from 'node:timers/promises'

import { Consumer, MessagesStream, type Message } from '@platformatic/kafka'
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common'
import { DemoConsumer } from '../demo/demo.consumer'
import type { KafkaTopicHandler, KafkaTopicMessageHandler } from './kafka-topic-handler'

const INITIAL_RETRY_DELAY_MS = 1000
const MAX_RETRY_DELAY_MS = 30000

@Injectable()
export class KafkaInboundConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaInboundConsumerService.name)
  private readonly topicHandlers = new Map<string, KafkaTopicMessageHandler>()
  private stream: MessagesStream<string, string, string, string> | null = null
  private processingLoop: Promise<void> | null = null
  private shuttingDown = false

  constructor(
    private readonly consumer: Consumer<string, string, string, string>,
    demoConsumer: DemoConsumer,
  ) {
    this.registerHandlers(demoConsumer)
  }

  async onModuleInit(): Promise<void> {
    const topics = Array.from(this.topicHandlers.keys())

    if (topics.length === 0) {
      this.logger.warn('Kafka consumer has no topic handlers configured')
      return
    }

    this.processingLoop = this.startConsumption(topics)
    void this.processingLoop.catch((error: unknown) => {
      this.logConsumerError('Kafka consumer loop crashed', error)
    })
  }

  async onModuleDestroy(): Promise<void> {
    this.shuttingDown = true

    if (this.stream !== null) {
      await this.stream.close().catch(() => undefined)
      this.stream = null
    }

    if (this.processingLoop !== null) {
      await this.processingLoop.catch(() => undefined)
      this.processingLoop = null
    }

    await this.consumer.close().catch(() => undefined)
  }

  private registerHandlers(handler: KafkaTopicHandler): void {
    for (const [topic, topicHandler] of handler.getTopicHandlers().entries()) {
      if (this.topicHandlers.has(topic)) {
        throw new Error(`Duplicate Kafka topic handler registration for topic ${topic}`)
      }

      this.topicHandlers.set(topic, topicHandler)
    }
  }

  private async startConsumption(topics: string[]): Promise<void> {
    let retryDelayMs = INITIAL_RETRY_DELAY_MS

    while (!this.shuttingDown) {
      try {
        this.logger.log(`Starting Kafka consume stream for ${topics.join(', ')}`)

        this.stream = await this.consumer.consume({
          autocommit: true,
          topics,
          sessionTimeout: 10000,
          heartbeatInterval: 500,
        })

        this.logger.log('Kafka consumer stream connected')
        retryDelayMs = INITIAL_RETRY_DELAY_MS

        await this.consumeMessages(this.stream)
      } catch (error) {
        this.logConsumerError('Kafka consume stream failed', error)
      } finally {
        if (this.stream !== null) {
          await this.stream.close().catch(() => undefined)
          this.stream = null
        }
      }

      if (this.shuttingDown) {
        return
      }

      this.logger.warn(`Retrying Kafka consume stream in ${retryDelayMs} ms`)
      await delay(retryDelayMs)
      retryDelayMs = Math.min(retryDelayMs * 2, MAX_RETRY_DELAY_MS)
    }
  }

  private async consumeMessages(
    stream: MessagesStream<string, string, string, string>,
  ): Promise<void> {
    for await (const message of stream) {
      void this.processMessage(message)
    }
  }

  private async processMessage(message: Message<string, string, string, string>): Promise<void> {
    const handler = this.topicHandlers.get(message.topic)

    if (!handler) {
      return
    }

    await handler(message)
  }

  private logConsumerError(context: string, error: unknown): void {
    if (error instanceof Error) {
      this.logger.error(context, error.stack)
      return
    }

    this.logger.error(`${context}: ${String(error)}`)
  }
}
