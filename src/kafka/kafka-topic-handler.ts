import type { Message } from '@platformatic/kafka'

export type KafkaInboundMessage = Message<string, string, string, string>

export type KafkaTopicMessageHandler = (message: KafkaInboundMessage) => Promise<void> | void

export interface KafkaTopicHandler {
  getTopicHandlers(): ReadonlyMap<string, KafkaTopicMessageHandler>
}
