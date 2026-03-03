import { Controller, Get } from '@nestjs/common'

@Controller({ path: 'health', version: '1' })
export class AppController {
  @Get()
  health(): { status: 'ok' } {
    return { status: 'ok' }
  }
}
