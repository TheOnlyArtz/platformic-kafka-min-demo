import {
  type ConsumerGlobalConfig,
  type ConsumerTopicConfig,
  KafkaConsumer,
  type Message,
  type MessageHeader,
  type TopicPartition,
} from '@confluentinc/kafka-javascript'
import { Inject, Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import { AppConfig } from '../configuration'
import { DemoConsumer } from '../demo/demo.consumer'
import { KAFKA_BASE_CONFIG } from './kafka.constants'
import { connectClient, disconnectClient } from './kafka-rdkafka-promises'
import { KafkaMessageMetadata, KafkaTopicHandler } from './kafka-topic-handler'

const KAFKA_AUTO_COMMIT_INTERVAL_MS = 5000
const KAFKA_AUTO_OFFSET_RESET: ConsumerTopicConfig['auto.offset.reset'] = 'latest'
const KAFKA_ASSIGNMENT_STRATEGY = 'roundrobin'
const KAFKA_MAX_IN_FLIGHT = 500
const KAFKA_RESUME_IN_FLIGHT = 400

@Injectable()
export class KafkaConsumerRunnerService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumerRunnerService.name)
  private readonly handlersByTopic = new Map<string, KafkaTopicHandler<unknown>>()
  @Inject(KAFKA_BASE_CONFIG)
  private readonly kafkaBaseConfig!: ConsumerGlobalConfig
  private consumer: KafkaConsumer | null = null
  private inFlightCount = 0
  private pausedForBackpressure = false

  constructor(
    private readonly configService: ConfigService,
    private readonly demoConsumer: DemoConsumer,
  ) {
    this.registerHandlers(this.demoConsumer.getTopicHandlers())
  }

  async onApplicationBootstrap(): Promise<void> {
    const kafkaConfig = this.configService.get('kafka') as AppConfig['kafka']
    const subscribedTopics = [...this.handlersByTopic.keys()]

    if (subscribedTopics.length === 0) {
      this.logger.warn('Skipping Kafka consumer startup: no topic handlers registered')
      return
    }

    const consumerConfig: ConsumerGlobalConfig = {
      ...this.kafkaBaseConfig,
      'group.id': kafkaConfig.groupId,
      'enable.auto.commit': true,
      'auto.commit.interval.ms': KAFKA_AUTO_COMMIT_INTERVAL_MS,
      'partition.assignment.strategy': KAFKA_ASSIGNMENT_STRATEGY,
    }
    const consumerTopicConfig: ConsumerTopicConfig = {
      'auto.offset.reset': KAFKA_AUTO_OFFSET_RESET,
    }

    this.consumer = this.createConsumer(consumerConfig, consumerTopicConfig)
    this.consumer.on('event.error', (error) => {
      this.logger.error(
        `Kafka consumer error: ${error.message}`,
        error.stack,
      )
    })
    this.consumer.on('rebalance', (_error, assignments) => {
      this.logger.log(
        `Kafka consumer rebalance assignments: ${this.stringifyAssignments(assignments)}`,
      )
    })
    this.consumer.on('data', (message) => {
      this.dispatchMessage(message)
    })

    await connectClient(this.consumer)
    this.consumer.subscribe(subscribedTopics)
    this.consumer.consume()

    this.logger.log(`Kafka consumer subscribed to ${subscribedTopics.length} topics`)
  }

  async onModuleDestroy(): Promise<void> {
    if (!this.consumer) {
      return
    }

    this.consumer.unsubscribe()
    await disconnectClient(this.consumer).catch(() => undefined)
  }

  private registerHandlers(handlers: ReadonlyArray<KafkaTopicHandler<unknown>>): void {
    for (const handler of handlers) {
      if (this.handlersByTopic.has(handler.topic)) {
        throw new Error(`Duplicate Kafka topic handler registration for topic ${handler.topic}`)
      }

      this.handlersByTopic.set(handler.topic, handler)
    }
  }

  protected createConsumer(
    consumerConfig: ConsumerGlobalConfig,
    consumerTopicConfig: ConsumerTopicConfig,
  ): KafkaConsumer {
    return new KafkaConsumer(consumerConfig, consumerTopicConfig)
  }

  private dispatchMessage(message: Message): void {
    if (this.inFlightCount >= KAFKA_MAX_IN_FLIGHT) {
      this.pauseForBackpressure()
    }

    this.inFlightCount += 1

    const { topic, partition, offset } = message
    const handler = this.handlersByTopic.get(topic)
    if (!handler) {
      this.logger.warn(`No Kafka topic handler found for topic ${topic}`)
      this.onMessageSettled()
      return
    }

    if (message.value === null) {
      this.logger.warn(`Kafka message has empty payload (topic=${topic}, partition=${partition})`)
      this.onMessageSettled()
      return
    }

    const metadata: KafkaMessageMetadata = {
      topic,
      partition,
      offset: String(offset),
      correlationId: this.resolveCorrelationId(message.headers),
    }

    try {
      const rawValue = message.value.toString('utf8')
      const payload = handler.parse(rawValue)

      void Promise.resolve(handler.handle(payload, metadata))
        .catch((error) => {
          this.logger.error(
            `Kafka handler failed (topic=${topic}, partition=${partition}, offset=${offset})`,
            error instanceof Error ? error.stack : undefined,
          )
        })
        .finally(() => {
          this.onMessageSettled()
        })
    } catch (error) {
      this.logger.error(
        `Kafka dispatch failed (topic=${topic}, partition=${partition}, offset=${offset})`,
        error instanceof Error ? error.stack : undefined,
      )
      this.onMessageSettled()
    }
  }

  private onMessageSettled(): void {
    this.inFlightCount = Math.max(0, this.inFlightCount - 1)
    if (this.pausedForBackpressure && this.inFlightCount <= KAFKA_RESUME_IN_FLIGHT) {
      this.resumeAfterBackpressure()
    }
  }

  private pauseForBackpressure(): void {
    if (this.pausedForBackpressure || !this.consumer) {
      return
    }

    const assignments = this.getTopicAssignments()
    if (assignments.length === 0) {
      return
    }

    this.consumer.pause(assignments)
    this.pausedForBackpressure = true
    this.logger.warn(`Kafka consumer paused due to backpressure (inFlight=${this.inFlightCount})`)
  }

  private resumeAfterBackpressure(): void {
    if (!this.pausedForBackpressure || !this.consumer) {
      return
    }

    const assignments = this.getTopicAssignments()
    if (assignments.length === 0) {
      return
    }

    this.consumer.resume(assignments)
    this.pausedForBackpressure = false
    this.logger.log(`Kafka consumer resumed after backpressure (inFlight=${this.inFlightCount})`)
  }

  private getTopicAssignments(): TopicPartition[] {
    if (!this.consumer) {
      return []
    }

    return this.consumer.assignments().map((assignment) => ({
      topic: assignment.topic,
      partition: assignment.partition,
    }))
  }

  private stringifyAssignments(assignments: ReadonlyArray<TopicPartition>): string {
    if (assignments.length === 0) {
      return 'none'
    }

    return assignments.map((assignment) => `${assignment.topic}[${assignment.partition}]`).join(',')
  }

  private resolveCorrelationId(headers: MessageHeader[] | undefined): string | undefined {
    if (!headers) {
      return undefined
    }

    for (const header of headers) {
      const correlationId = header.correlationId ?? header['x-request-id']
      if (correlationId !== undefined) {
        return this.normalizeHeaderValue(correlationId)
      }
    }

    return undefined
  }

  private normalizeHeaderValue(value: Buffer | string): string {
    if (typeof value === 'string') {
      return value
    }

    return value.toString('utf8')
  }
}
