export interface DemoSendRequestDto {
  readonly key?: string
  readonly value: string
  readonly headers?: Record<string, string>
}

export interface DemoSendResponseDto {
  readonly status: 'queued'
}
