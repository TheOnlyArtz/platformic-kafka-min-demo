import { Body, Controller, Get, Post } from '@nestjs/common'
import { DemoService } from './demo.service'
import type { DemoSendRequestDto, DemoSendResponseDto } from './types/demo-send.dto'

@Controller({ version: '1', path: 'demo' })
export class DemoController {
  constructor(private readonly demoService: DemoService) {}

  @Get('hi')
  hi(): string {
    return this.demoService.hi()
  }

  @Post('send')
  async send(@Body() request: DemoSendRequestDto): Promise<DemoSendResponseDto> {
    await this.demoService.publishSourceMessage(request)

    return { status: 'queued' }
  }
}
