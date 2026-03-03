export type KafkaSslConfig = Readonly<{
  certPath?: string
  keyPath?: string
}>

export interface ResolvedKafkaSslOptions {
  readonly enabled: boolean
  readonly clientOptions: Readonly<Record<string, string>>
}

const normalizePath = (value?: string): string | undefined => {
  const trimmed = value?.trim()
  return trimmed ? trimmed : undefined
}

export function resolveKafkaSslOptions(
  sslConfig: KafkaSslConfig | undefined,
): ResolvedKafkaSslOptions {
  if (!sslConfig) {
    return {
      enabled: false,
      clientOptions: {},
    }
  }

  const certPath = normalizePath(sslConfig.certPath)
  const keyPath = normalizePath(sslConfig.keyPath)
  if (!certPath && !keyPath) {
    return {
      enabled: false,
      clientOptions: {},
    }
  }

  if (!certPath || !keyPath) {
    throw new Error(
      `Invalid kafka.ssl configuration: certPath and keyPath must be provided together (certPath: ${
        certPath ?? 'missing'
      }, keyPath: ${keyPath ?? 'missing'})`,
    )
  }

  return {
    enabled: true,
    clientOptions: {
      'security.protocol': 'ssl',
      'ssl.certificate.location': certPath,
      'ssl.key.location': keyPath,
    },
  }
}
