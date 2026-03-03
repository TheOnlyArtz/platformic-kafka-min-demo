export type KafkaTopicMessageHandler = (payload: unknown) => void

export interface KafkaTopicHandler {
  getTopicHandlers(): ReadonlyMap<string, KafkaTopicMessageHandler>
}
