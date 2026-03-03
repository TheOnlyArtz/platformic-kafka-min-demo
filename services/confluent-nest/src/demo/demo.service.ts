import { Injectable } from '@nestjs/common'

export type DemoInboundMessage = Record<string, unknown>

@Injectable()
export class DemoService {
  handleInboundMessage(_payload: DemoInboundMessage): void {
  }
}
