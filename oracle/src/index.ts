import { execute } from './scenarios.ts'
import { schemaVersion, type Fixture, type OracleResult, type TraceEvent } from './types.ts'

const eventNames = new Set<TraceEvent['event']>([
  'fiber-created', 'fiber-state-changed', 'service-provided', 'service-removed',
  'listener-added', 'listener-removed', 'event-dispatched', 'effect-created',
  'effect-disposed', 'plugin-error', 'config-committed', 'config-rolled-back',
])
const domains = new Set(['lifecycle', 'service', 'event', 'loader'])
const traceKeys = new Set(['event', 'subject', 'from', 'to', 'label', 'phase', 'value', 'error'])

function validFixture(value: unknown): value is Fixture {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false
  const fixture = value as Record<string, unknown>
  if (fixture.schemaVersion !== schemaVersion || typeof fixture.name !== 'string' || !fixture.name || typeof fixture.scenario !== 'string' || !fixture.scenario || !domains.has(fixture.domain as string) || !Array.isArray(fixture.expectedTrace)) return false
  if (fixture.input !== undefined && (!fixture.input || typeof fixture.input !== 'object' || Array.isArray(fixture.input))) return false
  return fixture.expectedTrace.every((event) => {
    if (!event || typeof event !== 'object' || Array.isArray(event)) return false
    const record = event as Record<string, unknown>
    return eventNames.has(record.event as TraceEvent['event']) && Object.keys(record).every(key => traceKeys.has(key))
  })
}

function invalid(code: string, message: string): OracleResult {
  return { fixture: '<input>', status: 'INVALID_FIXTURE', error: { code, fixture: '<input>', message } }
}

async function main() {
  const path = process.argv[2]
  let input: unknown
  try {
    input = JSON.parse(path ? await Bun.file(path).text() : await Bun.stdin.text())
  } catch {
    return invalid('INVALID_JSON', 'fixture input is not valid JSON')
  }
  if (!validFixture(input)) return invalid('INVALID_FIXTURE', `fixture must match ${schemaVersion}`)
  return execute(input)
}

const result = await main()
process.stdout.write(`${JSON.stringify(result)}\n`)
process.exitCode = result.status === 'PASS' ? 0 : 1
