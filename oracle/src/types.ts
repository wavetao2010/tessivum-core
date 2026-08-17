export const schemaVersion = 'tessivum.conformance/v1' as const

export type Domain = 'lifecycle' | 'service' | 'event' | 'loader'
export type EventName =
  | 'fiber-created'
  | 'fiber-state-changed'
  | 'service-provided'
  | 'service-removed'
  | 'listener-added'
  | 'listener-removed'
  | 'event-dispatched'
  | 'effect-created'
  | 'effect-disposed'
  | 'plugin-error'
  | 'config-committed'
  | 'config-rolled-back'

export interface TraceEvent {
  event: EventName
  subject?: string
  from?: string
  to?: string
  label?: string
  phase?: string
  value?: unknown
  error?: string
}

export interface Fixture {
  schemaVersion: typeof schemaVersion
  name: string
  domain: Domain
  scenario: string
  input?: Record<string, unknown>
  expectedTrace: TraceEvent[]
}

export type ResultStatus = 'PASS' | 'MISMATCH' | 'UNSUPPORTED_SCENARIO' | 'INVALID_FIXTURE' | 'ORACLE_ERROR'

export interface OracleResult {
  fixture: string
  status: ResultStatus
  trace?: TraceEvent[]
  error?: {
    code: string
    fixture: string
    event?: number
    message: string
    expected?: TraceEvent
    actual?: TraceEvent
  }
}
