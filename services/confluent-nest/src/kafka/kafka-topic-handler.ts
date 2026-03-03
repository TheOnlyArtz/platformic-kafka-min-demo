export interface KafkaMessageMetadata {
  readonly topic: string
  readonly partition: number
  readonly offset: string
  readonly correlationId?: string
}

export interface KafkaTopicHandler<TPayload> {
  readonly topic: string
  parse(rawValue: string): TPayload
  handle(payload: TPayload, metadata?: KafkaMessageMetadata): void | Promise<void>
}
