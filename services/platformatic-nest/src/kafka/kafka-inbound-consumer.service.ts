import {setTimeout as delay} from 'node:timers/promises'
import {Consumer, type Message, type MessagesStream, MessagesStreamModes} from '@platformatic/kafka'
import {Injectable, Logger, OnModuleDestroy, OnModuleInit} from '@nestjs/common'
import {DemoConsumer} from '../demo/demo.consumer'
import {deserializeKafkaMessage} from './kafka-deserializer'

const INITIAL_RETRY_DELAY_MS = 1000
const MAX_RETRY_DELAY_MS = 30000

@Injectable()
export class KafkaInboundConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaInboundConsumerService.name)
  private readonly topicHandlers = new Map<string, (payload: unknown) => void>()
  private stream: MessagesStream<Buffer, Buffer, Buffer, Buffer> | null = null
  private processingLoop: Promise<void> | null = null
  private shuttingDown = false

  constructor(
    private readonly consumer: Consumer,
    demoConsumer: DemoConsumer,
  ) {
    this.registerHandlers(demoConsumer.getTopicHandlers())
  }

  async onModuleInit(): Promise<void> {
    const topics = Array.from(this.topicHandlers.keys())
    if (topics.length === 0) {
      this.logger.warn('Kafka consumer has no topic handlers configured')
      return
    }

    if (this.processingLoop !== null) {
      this.logger.warn('Kafka consumer loop is already running')
      return
    }

    this.processingLoop = this.startConsumption(topics)
    void this.processingLoop.catch((error) => {
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

    try {
      await this.consumer.close(true)
    } catch {
    }
  }

  private registerHandlers(handlers: ReadonlyMap<string, (payload: unknown) => void>): void {
    for (const [topic, handler] of handlers.entries()) {
      if (this.topicHandlers.has(topic)) {
        throw new Error(`Duplicate Kafka topic handler registration for topic ${topic}`)
      }

      this.topicHandlers.set(topic, handler)
    }
  }

  private async startConsumption(topics: string[]): Promise<void> {
    let retryDelayMs = INITIAL_RETRY_DELAY_MS

    while (!this.shuttingDown) {
      try {
        this.logger.log(`Starting Kafka consume stream for ${topics.length} topics`)

        this.stream = await this.consumer.consume({
          topics,
          mode: MessagesStreamModes.LATEST,
          fallbackMode: MessagesStreamModes.LATEST,
        })

        this.logger.log(`Kafka consumer subscribed to ${topics.length} topics`)

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

      this.logger.log(`Retrying to connect in ${retryDelayMs/1000} seconds`);
      await delay(retryDelayMs)
      retryDelayMs = Math.min(retryDelayMs * 2, MAX_RETRY_DELAY_MS)
    }
  }

  private async consumeMessages(
    stream: MessagesStream<Buffer, Buffer, Buffer, Buffer>,
  ): Promise<void> {
    for await (const message of stream) {
      void this.processMessage(message);
    }
  }

  private async processMessage(message: Message): Promise<void> {
    const handler = this.topicHandlers.get(message.topic)
    if (!handler) {
      return
    }

    const payload = deserializeKafkaMessage(message.topic, message.value)
    if (payload === null) {
      return
    }

    handler(payload)
  }

  private logConsumerError(context: string, error: unknown): void {
    if (error instanceof AggregateError) {
      this.logger.error(`${context}: ${error.message}`, error.stack)
      for (const nestedError of error.errors) {
        const message = nestedError instanceof Error ? nestedError.message : String(nestedError)
        const stack = nestedError instanceof Error ? nestedError.stack : undefined
        this.logger.error(`${context} nested: ${message}`, stack)
      }
      return
    }

    this.logger.error(context, error instanceof Error ? error.stack : undefined)
  }
}
