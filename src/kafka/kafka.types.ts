export interface KafkaClientOptions {
  readonly bootstrapBrokers: string[]
  readonly groupId: string
  readonly producerClientId: string
  readonly consumerClientId: string
}
