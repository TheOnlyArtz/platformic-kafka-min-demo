import type { BaseOptions, ConnectionOptions } from '@platformatic/kafka'

export interface KafkaClientOptions {
  readonly bootstrapBrokers: string[]
  readonly groupId: string
  readonly tls?: ConnectionOptions['tls']
  readonly requestTimeout: number
  readonly connectTimeout: number
  readonly retries: number
  readonly retryDelay: number
  readonly maxInflights: number
  readonly producerClientId: string
  readonly consumerClientId: string
  readonly adminClientId: string
}

export function toKafkaBaseOptions(
  options: KafkaClientOptions,
  clientId: string,
): BaseOptions {
  return {
    clientId,
    bootstrapBrokers: options.bootstrapBrokers,
    tls: options.tls,
    timeout: options.requestTimeout,
    connectTimeout: options.connectTimeout,
    retries: options.retries,
    retryDelay: options.retryDelay,
    maxInflights: options.maxInflights,
    autocreateTopics: true,
  }
}
