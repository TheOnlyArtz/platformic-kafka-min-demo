import { readFileSync } from 'node:fs'
import { ConfigModule } from '@nestjs/config'
import configSchema from '../config/config.json'

const CONFIG_FILE_PATH = `${process.cwd()}/config/config.json`

export type AppConfig = typeof configSchema

export function configuration(): AppConfig {
  const rawConfig = JSON.parse(readFileSync(CONFIG_FILE_PATH, 'utf8')) as unknown
  return rawConfig as AppConfig
}

export const configModule = ConfigModule.forRoot({
  isGlobal: true,
  cache: true,
  load: [configuration],
})

export const config = configuration()
