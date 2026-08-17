import type { Fixture, OracleResult, TraceEvent } from './types.ts'

type Disposer = () => void | Promise<void>

interface Fiber {
  uid: number | null
  name: string
  state: number
  dispose(): Promise<void>
}

interface Context {
  on(name: 'internal/plugin', listener: (fiber: Fiber) => void): Disposer
  on(name: 'internal/status', listener: (fiber: Fiber, from: number) => void): Disposer
  on(name: string, listener: (...args: unknown[]) => void): Disposer
  emit(name: string): void
  plugin(plugin: { name: string, apply(context: Context): unknown }): Fiber
  effect(execute: () => Disposer, label: string): Disposer
  provide(name: string, value: object): Disposer
  isolate(name: string): Context
  get(name: string): unknown
}

type ContextConstructor = new () => Context
// Static imports would make tsc typecheck Cordis's vendored source.
const cordisPath = [
  '/Users/chan/Documents/my_project/SCHarness/upstream',
  'deepseek-harness/vendor/cordis/src/index.ts',
].join('/')
const { Context } = await import(cordisPath) as { Context: ContextConstructor }

const states = ['PENDING', 'LOADING', 'ACTIVE', 'FAILED', 'DISPOSED', 'UNLOADING']
type Input = Record<string, unknown>

function text(value: unknown, fallback: string) { return typeof value === 'string' && value ? value : fallback }
function stable(value: unknown): unknown {
  if (value instanceof Error) return value.message
  if (Array.isArray(value)) return value.map(stable)
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value as Record<string, unknown>).filter(([, item]) => item !== undefined).sort(([left], [right]) => left.localeCompare(right)).map(([key, item]) => [key, stable(item)]))
  return value
}

class Trace {
  readonly events: TraceEvent[] = []

  constructor(root: Context) {
    root.on('internal/plugin', (fiber) => {
      if (fiber.uid !== null) this.add({ event: 'fiber-created', subject: fiber.name })
    })
    root.on('internal/status', (fiber, from) => this.add({ event: 'fiber-state-changed', subject: fiber.name, from: states[from], to: states[fiber.state] }))
  }

  add(event: TraceEvent) { this.events.push(stable(event) as TraceEvent) }
}

async function lifecycle(root: Context, trace: Trace, input: Input) {
  const effects = input.effects
  const label = Array.isArray(effects) && typeof effects[0] === 'string' && effects[0] ? effects[0] : 'effect-dispose'
  const fiber = root.plugin({
    name: 'effect-dispose',
    apply(ctx) {
      trace.add({ event: 'effect-created', subject: 'effect-dispose.effect', label })
      ctx.effect(() => () => trace.add({ event: 'effect-disposed', subject: 'effect-dispose.effect', label }), label)
    },
  })
  await fiber
  await fiber.dispose()
}

function provider(trace: Trace, name: string, label: string, value: object) {
  return {
    name: `isolate-realm.${label}`,
    apply(ctx: Context) {
      const remove = ctx.provide(name, value)
      trace.add({ event: 'service-provided', subject: name, label })
      return async () => {
        await remove()
        trace.add({ event: 'service-removed', subject: name, label })
      }
    },
  }
}

async function service(root: Context, trace: Trace, input: Input) {
  const name = text(input.service, 'storage')
  const first = text(input.first, 'root')
  const second = text(input.second, 'isolated')
  const rootValue = { realm: first }
  const isolatedValue = { realm: second }
  const rootProvider = root.plugin(provider(trace, name, first, rootValue))
  await rootProvider
  const isolated = root.isolate(name)
  const isolatedProvider = isolated.plugin(provider(trace, name, second, isolatedValue))
  await isolatedProvider
  if (root.get(name) !== rootValue || isolated.get(name) !== isolatedValue) throw new Error(`service realm visibility failed for ${name}`)
  await isolatedProvider.dispose()
  await rootProvider.dispose()
}

function isLabeledListener(value: unknown): value is { label: string } {
  return !!value && typeof value === 'object' && 'label' in value && typeof value.label === 'string'
}

function listeners(input: Input) {
  const supplied = input.listeners
  if (Array.isArray(supplied) && supplied.every(isLabeledListener)) return supplied.map(item => item.label)
  return ['first', 'second']
}

async function events(root: Context, trace: Trace, input: Input) {
  const mode = text(input.mode, 'emit')
  if (mode !== 'emit') throw new Error('emit scenario requires emit mode')
  const name = text(input.event, 'emit')
  const labels = listeners(input)
  const disposers = labels.map((label) => {
    const dispose = root.on(name, () => trace.add({ event: 'event-dispatched', subject: name, label, phase: mode }))
    trace.add({ event: 'listener-added', subject: name, label })
    return dispose
  })
  root.emit(name)
  for (let index = disposers.length - 1; index >= 0; index--) {
    await disposers[index]!()
    trace.add({ event: 'listener-removed', subject: name, label: labels[index]! })
  }
}

export async function execute(fixture: Fixture): Promise<OracleResult> {
  const supported = fixture.domain === 'lifecycle' && fixture.scenario === 'effect-dispose'
    || fixture.domain === 'service' && fixture.scenario === 'isolate-realm'
    || fixture.domain === 'event' && fixture.scenario === 'emit'
  if (!supported) return { fixture: fixture.name, status: 'UNSUPPORTED_SCENARIO', error: { code: 'UNSUPPORTED_SCENARIO', fixture: fixture.name, message: `${fixture.domain}/${fixture.scenario} is not supported by the Cordis oracle` } }
  const root = new Context()
  const trace = new Trace(root)
  try {
    if (fixture.domain === 'lifecycle') await lifecycle(root, trace, fixture.input ?? {})
    else if (fixture.domain === 'service') await service(root, trace, fixture.input ?? {})
    else await events(root, trace, fixture.input ?? {})
  } catch (error) {
    return { fixture: fixture.name, status: 'ORACLE_ERROR', trace: trace.events, error: { code: 'SCENARIO_ERROR', fixture: fixture.name, message: String((error as Error).message ?? error) } }
  }
  for (let index = 0; index < Math.max(fixture.expectedTrace.length, trace.events.length); index++) {
    const expected = fixture.expectedTrace[index]
    const actual = trace.events[index]
    if (JSON.stringify(stable(expected)) !== JSON.stringify(stable(actual))) return { fixture: fixture.name, status: 'MISMATCH', trace: trace.events, error: { code: 'TRACE_MISMATCH', fixture: fixture.name, event: index, message: `trace mismatch at event ${index}`, expected, actual } }
  }
  return { fixture: fixture.name, status: 'PASS', trace: trace.events }
}
