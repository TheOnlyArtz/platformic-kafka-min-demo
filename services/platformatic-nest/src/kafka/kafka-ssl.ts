import { readFileSync } from 'node:fs'
import type { ConnectionOptions } from '@platformatic/kafka'

export type KafkaSslConfig = Readonly<{
  certPath?: string
  keyPath?: string
}>

const readFile = (path: string): string => readFileSync(path, 'utf8')

const normalizePath = (value?: string): string | undefined => {
  const trimmed = value?.trim()
  return trimmed ? trimmed : undefined
}

export function resolveKafkaSslOptions(
  sslConfig: KafkaSslConfig | undefined,
  readFileContents: (path: string) => string = readFile,
): ConnectionOptions['tls'] | undefined {
  if (!sslConfig) {
    return undefined
  }

  const certPath = normalizePath(sslConfig.certPath)
  const keyPath = normalizePath(sslConfig.keyPath)

  if (!certPath && !keyPath) {
    return undefined
  }

  if (!certPath || !keyPath) {
    throw new Error(
      `Invalid kafka.ssl configuration: certPath and keyPath must be provided together (certPath: ${
        certPath ?? 'missing'
      }, keyPath: ${keyPath ?? 'missing'})`,
    )
  }

  const ssl: ConnectionOptions['tls'] = {
    cert: readFileContents(certPath),
    key: readFileContents(keyPath),
  }

  console.log({ssl})
  return ssl
}
