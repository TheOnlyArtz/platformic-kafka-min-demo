import 'reflect-metadata'

import { VersioningType } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import { NestFactory } from '@nestjs/core'
import { AppModule } from './app.module'
import type { AppConfig } from './configuration'

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule)

  app.enableShutdownHooks()
  app.enableVersioning({
    type: VersioningType.URI,
    prefix: 'api/v',
  })

  const configService = app.get(ConfigService)
  const port = configService.getOrThrow<AppConfig['http']['port']>('http.port')

  await app.listen(port)
  console.log(`Application listening on http://localhost:${port}`)
}

void bootstrap()
