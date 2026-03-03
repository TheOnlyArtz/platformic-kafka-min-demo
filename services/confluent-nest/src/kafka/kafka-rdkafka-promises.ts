import { KafkaConsumer, type IAdminClient, type LibrdKafkaError, type Producer } from '@confluentinc/kafka-javascript'

type ConnectableClient = KafkaConsumer | Producer

function toError(error: LibrdKafkaError | null | undefined): Error | null {
  if (!error) {
    return null
  }

  return new Error(error.message)
}

export function connectClient(client: ConnectableClient): Promise<void> {
  return new Promise((resolve, reject) => {
    client.connect(undefined, (error) => {
      const resolvedError = toError(error)
      if (resolvedError) {
        reject(resolvedError)
        return
      }

      resolve()
    })
  })
}

export function disconnectClient(client: ConnectableClient): Promise<void> {
  return new Promise((resolve, reject) => {
    client.disconnect((error) => {
      const resolvedError = toError(error)
      if (resolvedError) {
        reject(resolvedError)
        return
      }

      resolve()
    })
  })
}

export function listTopics(adminClient: IAdminClient, timeout: number): Promise<ReadonlyArray<string>> {
  return new Promise((resolve, reject) => {
    adminClient.listTopics({ timeout }, (error, topics) => {
      const resolvedError = toError(error)
      if (resolvedError) {
        reject(resolvedError)
        return
      }

      resolve(topics ?? [])
    })
  })
}
