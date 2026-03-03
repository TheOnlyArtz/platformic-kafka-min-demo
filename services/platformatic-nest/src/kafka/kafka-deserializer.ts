import { DemoInboundMessage } from '../demo/demo.service'

export function deserializeKafkaMessage(topic: string, value: Buffer): unknown | null {
  const rawValue = value.toString()

  try {
    return JSON.parse(rawValue) as DemoInboundMessage
  } catch {
    return null
  }
}
