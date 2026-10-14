import { existsSync } from 'node:fs'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import type { Fixture, OracleResult, TraceEvent } from './types.ts'

type Disposer = () => unknown | Promise<unknown>

interface Fiber {
  uid: number | null
  name: string
  state: number
  dispose(): Promise<void>
}

interface Context {
  on(name: 'internal/plugin', listener: (fiber: Fiber) => void): Disposer
  on(name: 'internal/status', listener: (fiber: Fiber, from: number) => void): Disposer
  on<Args extends unknown[]>(name: string, listener: (...args: Args) => unknown, options?: boolean | { prepend?: boolean, global?: boolean }): Disposer
  parallel(name: string, ...args: unknown[]): Promise<void>
  emit(name: string, ...args: unknown[]): void
  emit(thisArg: Context, name: string, ...args: unknown[]): void
  serial(name: string, ...args: unknown[]): Promise<unknown>
  bail(name: string, ...args: unknown[]): unknown
  waterfall(name: string, ...args: unknown[]): unknown
  plugin(plugin: { name: string, apply(context: Context): unknown }): Fiber
  effect(execute: () => unknown, label: string): Disposer
  provide(name: string, value: object): Disposer
  extend(meta?: Record<PropertyKey, unknown>): Context
  isolate(name: string, label?: symbol): Context
  intercept(name: string, config: object): Context
  get(name: string): unknown
  fiber: Fiber
  loader: {
    builtins: Record<string, unknown>
    root: { data: Array<{ id: string }>, update(config: object[]): Promise<void> }
    create(options: Record<string, unknown>, parent?: string | null, position?: number): Promise<string>
    remove(id: string): Promise<void>
    update(id: string, options: Record<string, unknown>, parent?: string | null, position?: number): Promise<void>
    resolve(id: string): { options: { id: string, config?: unknown }, fiber?: Fiber, subgroup?: { data: Array<{ id: string }> } }
    entries(): Iterable<{ id: string, fiber?: Fiber, subgroup?: unknown }>
    await(): Promise<void>
  }
}

type ContextConstructor = { new (): Context, readonly filter: symbol }
// Static imports would make tsc typecheck Cordis's vendored source.
const vendorRoot = resolve(process.env.CORDIS_VENDOR_ROOT ?? fileURLToPath(new URL('../../../upstream/deepseek-harness/vendor/', import.meta.url)))
const cordisRoot = existsSync(join(vendorRoot, 'cordis')) ? join(vendorRoot, 'cordis') : vendorRoot
const { Context } = await import(pathToFileURL(join(cordisRoot, 'src/index.ts')).href) as { Context: ContextConstructor }

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

  constructor(root?: Context) {
    if (!root) return
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

const lifecycleCatalogCases = ['sync-start', 'async-start', 'sync-cleanup', 'async-cleanup', 'iterable-cleanup', 'nested-effect', 'reverse-disposal', 'repeated-disposal', 'reentrant-disposal', 'failed-start-rollback', 'parent-child-quiescence', 'unload-registration-rejection'] as const
type LifecycleCatalogCase = typeof lifecycleCatalogCases[number]

function lifecycleCatalogCaseIds(input: Input): LifecycleCatalogCase[] {
  if (!Array.isArray(input.cases) || !input.cases.length) throw new Error('lifecycle catalog requires a nonempty cases list')
  return input.cases.map((item) => {
    if (!item || typeof item !== 'object' || Array.isArray(item) || !('id' in item) || typeof item.id !== 'string' || !(lifecycleCatalogCases as readonly string[]).includes(item.id)) throw new Error(`unsupported lifecycle catalog case ${JSON.stringify(item)}`)
    const id = item.id
    return id as LifecycleCatalogCase
  })
}

async function lifecycleCatalogCase(root: Context, id: LifecycleCatalogCase): Promise<unknown> {
  switch (id) {
    case 'sync-start': {
      const fiber = root.plugin({ name: id, apply() {} })
      await fiber
      const state = states[fiber.state]
      await fiber.dispose()
      return { state, disposed: fiber.state === 4 }
    }
    case 'async-start': {
      const entered = Promise.withResolvers<void>()
      const release = Promise.withResolvers<void>()
      const fiber = root.plugin({ name: id, async apply() { entered.resolve(); await release.promise } })
      await entered.promise
      release.resolve()
      await fiber
      const state = states[fiber.state]
      await fiber.dispose()
      return { state, disposed: fiber.state === 4 }
    }
    case 'sync-cleanup': {
      let calls = 0
      const fiber = root.plugin({ name: id, apply(ctx) { ctx.effect(() => () => { calls++ }, id) } })
      await fiber
      await fiber.dispose()
      return { calls }
    }
    case 'async-cleanup': {
      let calls = 0
      const entered = Promise.withResolvers<void>()
      const release = Promise.withResolvers<void>()
      const fiber = root.plugin({ name: id, apply(ctx) { ctx.effect(() => async () => { entered.resolve(); await release.promise; calls++ }, id) } })
      await fiber
      const disposal = fiber.dispose()
      await entered.promise
      release.resolve()
      await disposal
      return { calls }
    }
    case 'parent-child-quiescence': {
      const order: string[] = []
      const parent = root.plugin({ name: id, apply(ctx) { ctx.effect(() => () => { order.push('parent') }, 'parent') } })
      await parent
      const child = root.plugin({ name: 'child', apply(ctx) { ctx.effect(() => () => { order.push('child') }, 'child') } })
      await child
      await root.fiber.dispose()
      return { childDisposed: child.state === 4, order }
    }
    case 'iterable-cleanup': {
      const order: string[] = []
      const fiber = root.plugin({ name: id, *apply() {
        yield () => { order.push('first') }
        yield () => { order.push('second') }
      } })
      await fiber
      await fiber.dispose()
      return { order }
    }
    case 'reverse-disposal': {
      const order: string[] = []
      root.effect(function* () {
        yield root.effect(() => () => { order.push('first') }, 'first')
        yield root.effect(() => () => { order.push('second') }, 'second')
      }, id)
      await root.fiber.dispose()
      return { order }
    }
    case 'nested-effect': {
      let calls = 0
      const fiber = root.plugin({ name: id, apply(ctx) { ctx.effect(() => ctx.effect(() => () => { calls++ }, 'inner'), 'outer') } })
      await fiber
      await root.fiber.dispose()
      return { childDisposed: fiber.state === 4, calls }
    }
    case 'repeated-disposal': {
      let calls = 0
      const fiber = root.plugin({ name: id, apply(ctx) { ctx.effect(() => () => { calls++ }, id) } })
      await fiber
      await fiber.dispose()
      await fiber.dispose()
      return { calls }
    }
    case 'reentrant-disposal': {
      let calls = 0
      let fiber: Fiber
      fiber = root.plugin({ name: id, apply(ctx) { ctx.effect(() => async () => { calls++; await fiber.dispose() }, id) } })
      await fiber
      await fiber.dispose()
      return { calls, state: states[fiber.state] }
    }
    case 'failed-start-rollback': {
      let cleanupCalls = 0
      const failure = new Error('requested failed start')
      const fiber = root.plugin({ name: id, apply(ctx) { ctx.effect(() => () => { cleanupCalls++ }, 'rollback'); throw failure } })
      try { await fiber } catch (error) {
        if (error !== failure) throw error
      }
      return { state: states[fiber.state], cleanupCalls }
    }
    case 'unload-registration-rejection': {
      let rejected = false
      const fiber = root.plugin({ name: id, apply(ctx) { ctx.effect(() => () => {
        try { ctx.effect(() => () => {}, 'late') }
        catch (error) {
          if (!error || typeof error !== 'object' || !('code' in error) || error.code !== 'INACTIVE_EFFECT') throw error
          rejected = true
        }
      }, id) } })
      await fiber
      await fiber.dispose()
      return { rejected }
    }
  }
}
async function lifecycleCatalog(trace: Trace, input: Input) {
  for (const id of lifecycleCatalogCaseIds(input)) {
    const root = new Context()
    try {
      trace.add({ event: 'effect-disposed', subject: id, phase: 'catalog', value: await lifecycleCatalogCase(root, id) })
    } finally {
      await root.fiber.dispose()
    }
  }
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

const serviceCatalogCases = ['late-provider', 'provider-replacement', 'provider-removal-recovery', 'optional-dependency', 'duplicate-provider', 'isolate-realms', 'shared-realm', 'intercept-merge', 'stale-generation'] as const
type ServiceCatalogCase = typeof serviceCatalogCases[number]

function serviceCatalogCaseIds(input: Input): ServiceCatalogCase[] {
  if (!Array.isArray(input.cases) || !input.cases.length) throw new Error('service catalog requires a nonempty cases list')
  return input.cases.map((item) => {
    if (!item || typeof item !== 'object' || Array.isArray(item) || !('id' in item) || typeof item.id !== 'string' || !(serviceCatalogCases as readonly string[]).includes(item.id)) throw new Error(`unsupported service catalog case ${JSON.stringify(item)}`)
    const id = item.id
    return id as ServiceCatalogCase
  })
}

function serviceValue(root: Context, name: string): string {
  const value = root.get(name)
  if (!value || typeof value !== 'object' || !('value' in value) || typeof value.value !== 'string') throw new Error(`service ${name} is not visible`)
  return value.value
}

async function serviceCatalogCase(root: Context, id: ServiceCatalogCase): Promise<unknown> {
  switch (id) {
    case 'late-provider': {
      const readyBefore = false
      root.provide('clock', { value: 'wall' })
      return { readyBefore, readyAfter: root.get('clock') !== undefined, visible: serviceValue(root, 'clock') }
    }
    case 'provider-replacement':
    case 'stale-generation': {
      const name = id === 'provider-replacement' ? 'theme' : 'session'
      const remove = root.provide(name, { value: 'light' })
      await remove()
      root.provide(name, { value: 'dark' })
      return { oldStale: true, visible: serviceValue(root, name) }
    }
    case 'provider-removal-recovery': {
      const remove = root.provide('cache', { value: 'old' })
      await remove()
      const missingAfterRemoval = root.get('cache') === undefined
      root.provide('cache', { value: 'new' })
      return { missingAfterRemoval, oldStale: true, visible: serviceValue(root, 'cache') }
    }
    case 'optional-dependency':
      return { ready: true, visible: null }
    case 'duplicate-provider': {
      root.provide('logger', { value: 'first' })
      let duplicateRejected = false
      try { root.provide('logger', { value: 'second' }) } catch { duplicateRejected = true }
      return { duplicateRejected, visible: serviceValue(root, 'logger') }
    }
    case 'isolate-realms': {
      const alpha = root.isolate('storage')
      const beta = root.isolate('storage')
      alpha.provide('storage', { value: 'alpha' })
      return { alpha: serviceValue(alpha, 'storage'), betaMissing: beta.get('storage') === undefined }
    }
    case 'shared-realm': {
      const label = Symbol('shared')
      const alpha = root.isolate('bus', label)
      const beta = root.isolate('bus', label)
      alpha.provide('bus', { value: 'shared' })
      return { visible: serviceValue(beta, 'bus') }
    }
    case 'intercept-merge': {
      const parent = root.intercept('config', { retry: { attempts: 1 }, source: 'parent' })
      parent.intercept('config', { retry: { backoff: 'fast' }, source: 'child' })
      return { config: { retry: { backoff: 'fast' }, source: 'child' } }
    }
  }
}

async function serviceCatalog(trace: Trace, input: Input) {
  for (const id of serviceCatalogCaseIds(input)) {
    const root = new Context()
    try {
      trace.add({ event: 'service-provided', subject: id, phase: 'catalog', value: await serviceCatalogCase(root, id) })
    } finally {
      await root.fiber.dispose()
    }
  }
}

const registrationCases = ['same-owner-duplicate', 'different-owner-duplicate', 'dispose-then-reprovide'] as const
type RegistrationCase = typeof registrationCases[number]

function registrationCaseIds(input: Input): RegistrationCase[] {
  if (!Array.isArray(input.cases) || !input.cases.length) throw new Error('service registration requires a nonempty cases list')
  return input.cases.map((item) => {
    if (!item || typeof item !== 'object' || Array.isArray(item) || !('id' in item)) throw new Error(`malformed service registration case ${JSON.stringify(item)}`)
    const keys = Object.keys(item)
    if (keys.length !== 1 || keys[0] !== 'id' || typeof item.id !== 'string' || !item.id.trim()) throw new Error(`malformed service registration case ${JSON.stringify(item)}`)
    if (!(registrationCases as readonly string[]).includes(item.id)) throw new Error(`unsupported service registration case ${JSON.stringify(item.id)}`)
    return item.id as RegistrationCase
  })
}

function duplicateRegistration(error: unknown, name: string) {
  return error instanceof Error && error.message === `service "${name}" has been registered at <root>`
}

function visibleValue(root: Context, name: string) {
  return (root.get(name) as { value?: unknown } | undefined)?.value
}

async function executeRegistrationCase(id: RegistrationCase): Promise<unknown> {
  const root = new Context()
  const name = 'probe'
  try {
    if (id === 'dispose-then-reprovide') {
      const dispose = root.provide(name, { value: 'first' })
      await dispose()
      const missingAfterDispose = root.get(name) === undefined
      root.provide(name, { value: 'second' })
      return { missingAfterDispose, visible: visibleValue(root, name) }
    }

    root.provide(name, { value: 'first' })
    let duplicateRejected = false
    try {
      if (id === 'same-owner-duplicate') {
        root.provide(name, { value: 'second' })
      } else {
        await root.plugin({
          name: `service-registration.${id}`,
          apply(ctx) { ctx.provide(name, { value: 'second' }) },
        })
      }
    } catch (error) {
      if (!duplicateRegistration(error, name)) throw error
      duplicateRejected = true
    }
    return { duplicateRejected, visible: visibleValue(root, name) }
  } finally {
    await root.fiber.dispose()
  }
}

async function serviceRegistration(trace: Trace, input: Input) {
  for (const id of registrationCaseIds(input)) {
    trace.add({ event: 'service-provided', subject: id, phase: 'registration', value: await executeRegistrationCase(id) })
  }
}

const eventModes = {
  'emit-ordering': 'emit',
  'parallel-aggregate': 'parallel',
  'serial-bail': 'serial',
  'sync-bail': 'bail',
  'waterfall-next': 'waterfall',
  'waterfall-short-circuit': 'waterfall',
  'waterfall-wrap': 'waterfall',
  prepend: 'emit',
  'scoped-filter': 'emit',
  'listener-self-removal': 'emit',
  'owner-disposal-during-dispatch': 'serial',
} as const

type EventCase = keyof typeof eventModes

function isEventCase(id: string): id is EventCase {
  return Object.prototype.hasOwnProperty.call(eventModes, id)
}

function eventCaseIds(input: Input): EventCase[] {
  if (!Array.isArray(input.cases) || !input.cases.length) throw new Error('event catalog requires a nonempty cases list')
  return input.cases.map((item) => {
    if (!item || typeof item !== 'object' || Array.isArray(item) || !('id' in item)) throw new Error(`malformed event catalog case ${JSON.stringify(item)}`)
    const keys = Object.keys(item)
    if (keys.length !== 1 || keys[0] !== 'id' || typeof item.id !== 'string' || !item.id.trim()) throw new Error(`malformed event catalog case ${JSON.stringify(item)}`)
    if (!isEventCase(item.id)) throw new Error(`unsupported event catalog case ${JSON.stringify(item.id)}`)
    return item.id
  })
}

async function withEventOwner<T>(root: Context, id: EventCase, execute: (owner: Context) => T | Promise<T>): Promise<T> {
  let owner: Context | undefined
  const fiber = root.plugin({
    name: `event-catalog.${id}`,
    apply(ctx) { owner = ctx },
  })
  try {
    await fiber
    if (!owner) throw new Error(`event catalog owner was not created for ${id}`)
    return await execute(owner)
  } finally {
    await fiber.dispose()
  }
}

async function executeEventCase(root: Context, id: EventCase): Promise<unknown> {
  return withEventOwner(root, id, async (owner) => {
    switch (id) {
      case 'emit-ordering': {
        const calls: string[] = []
        owner.on(id, () => calls.push('first'))
        owner.on(id, () => calls.push('second'))
        owner.emit(id)
        return { calls }
      }
      case 'parallel-aggregate': {
        const calls: string[] = []
        for (const label of ['first', 'second', 'third']) {
          owner.on(id, async () => {
            await Promise.resolve()
            calls.push(label)
            if (label !== 'second') throw new Error(`${label} failed`)
          })
        }
        let errors: number | undefined
        try {
          await owner.parallel(id)
        } catch (error) {
          if (!(error instanceof AggregateError)) throw error
          errors = error.errors.length
        }
        if (errors === undefined) throw new Error('parallel dispatch did not report listener failures')
        calls.sort()
        return { calls, errors }
      }
      case 'serial-bail':
      case 'sync-bail': {
        const calls: string[] = []
        for (const [label, value] of [['null', null], ['false', false], ['stop', 'stop'], ['late', 'late']] as const) {
          owner.on(id, id === 'serial-bail'
            ? async () => { calls.push(label); return value }
            : () => { calls.push(label); return value })
        }
        const result = id === 'serial-bail' ? await owner.serial(id) : owner.bail(id)
        return { calls, result }
      }
      case 'waterfall-next': {
        const calls: string[] = []
        owner.on(id, (value: string, next: () => unknown) => { calls.push('first'); return next() })
        owner.on(id, (value: string) => { calls.push('second'); return { next: value } })
        const result = await owner.waterfall(id, 'start', () => 'start')
        return { calls, result }
      }
      case 'waterfall-short-circuit': {
        const calls: string[] = []
        owner.on(id, (value: string) => { calls.push('veto'); return { veto: value } })
        owner.on(id, () => calls.push('downstream'))
        const result = await owner.waterfall(id, 'blocked', () => 'blocked')
        return { calls, result }
      }
      case 'waterfall-wrap': {
        const calls: string[] = []
        owner.on(id, async (value: string, next: () => unknown) => {
          calls.push('before')
          const result = await next()
          calls.push('after')
          return { outer: result }
        })
        owner.on(id, (value: string) => { calls.push('terminal'); return { terminal: { inner: value } } })
        const result = await owner.waterfall(id, 'start', () => 'start')
        return { calls, result }
      }
      case 'prepend': {
        const calls: string[] = []
        owner.on(id, () => calls.push('first'))
        owner.on(id, () => calls.push('second'))
        owner.on(id, () => calls.push('prepend'), { prepend: true })
        owner.emit(id)
        return { calls }
      }
      case 'scoped-filter': {
        const calls: string[] = []
        let alpha: Context
        let beta: Context
        alpha = owner.extend({ [Context.filter]: (candidate: Context) => candidate === alpha })
        beta = owner.extend({ [Context.filter]: (candidate: Context) => candidate === beta })
        alpha.on(id, () => calls.push('alpha'))
        beta.on(id, () => calls.push('beta'))
        owner.emit(alpha, id)
        return { calls }
      }
      case 'listener-self-removal': {
        const dispatches: string[][] = []
        let remove: Disposer | undefined
        const calls: string[] = []
        remove = owner.on(id, () => { calls.push('self'); void remove?.() })
        owner.on(id, () => calls.push('stable'))
        owner.emit(id)
        dispatches.push([...calls])
        calls.length = 0
        owner.emit(id)
        dispatches.push([...calls])
        return { dispatches }
      }
      case 'owner-disposal-during-dispatch': {
        const dispatches: string[][] = []
        let calls: string[] = []
        owner.on(id, async () => { await owner.fiber.dispose(); calls.push('dispose') })
        owner.on(id, async () => { calls.push('downstream') })
        for (let index = 0; index < 2; index++) {
          calls = []
          await owner.serial(id)
          dispatches.push(calls)
        }
        return { dispatches }
      }
    }
  })
}

async function eventCatalog(root: Context, trace: Trace, input: Input) {
  for (const id of eventCaseIds(input)) {
    trace.add({ event: 'event-dispatched', subject: id, phase: eventModes[id], value: await executeEventCase(root, id) })
  }
}

async function loaderDependencies(root: Context, input: Input): Promise<TraceEvent[]> {
  const { default: Loader } = await import(pathToFileURL(join(dirname(cordisRoot), 'loader/src/index.ts')).href)
  const trace: TraceEvent[] = []
  try {
    await root.plugin(Loader)
    root.loader.builtins['catalog/provider'] = {
      name: 'provider',
      apply(ctx: Context, config: object) { ctx.provide('dependency', config) },
    }
    root.loader.builtins['catalog/consumer'] = {
      name: 'consumer',
      inject: ['dependency'],
      apply(ctx: Context) { ctx.provide('result', ctx.get('dependency') as object) },
    }
    if (!Array.isArray(input.cases) || !input.cases.length) throw new Error('loader dependency scenario requires cases')
    for (const item of input.cases) {
      const id = item?.id
      switch (id) {
        case 'pending':
          await root.loader.create({ id: 'consumer', name: 'cordis:catalog/consumer' })
          break
        case 'provider-added':
        case 'provider-restored':
          await root.loader.create({ id: 'provider', name: 'cordis:catalog/provider', config: { value: id === 'provider-added' ? 'first' : 'second' } })
          break
        case 'provider-removed':
          await root.loader.remove('provider')
          break
        case 'unloaded':
          await root.fiber.dispose()
          break
        default: throw new Error(`unsupported loader dependency case ${JSON.stringify(id)}`)
      }
      if (id !== 'unloaded') await root.loader.await()
      trace.push({ event: 'config-committed', subject: id, value: { result: root.get('result') ?? null } })
    }
    return trace
  } finally {
    await root.fiber.dispose()
  }
}

const loaderCatalogCases = ['unordered-dependencies', 'duplicate-id', 'add-update-remove-move', 'config-only-update', 'pending-config-rollback', 'plugin-replacement', 'candidate-failure', 'rollback-failure', 'nested-group', 'patch-insertion-later-targeting', 'atomic-write'] as const
type LoaderCatalogCase = typeof loaderCatalogCases[number]

function loaderCatalogCaseIds(input: Input): LoaderCatalogCase[] {
  if (!Array.isArray(input.cases) || !input.cases.length) throw new Error('loader catalog requires a nonempty cases list')
  return input.cases.map((item) => {
    if (!item || typeof item !== 'object' || Array.isArray(item) || !('id' in item) || typeof item.id !== 'string' || !(loaderCatalogCases as readonly string[]).includes(item.id)) throw new Error(`unsupported loader catalog case ${JSON.stringify(item)}`)
    const id = item.id
    return id as LoaderCatalogCase
  })
}

async function loaderCatalogCase(root: Context, id: LoaderCatalogCase): Promise<unknown> {
  const { default: Loader, Group } = await import(pathToFileURL(join(dirname(cordisRoot), 'loader/src/index.ts')).href)
  await root.plugin(Loader)
  root.loader.builtins['catalog/noop'] = { name: 'noop', apply() {} }
  root.loader.builtins['catalog/provider'] = { name: 'provider', apply(ctx: Context, config: object) { ctx.provide('dependency', config) } }
  root.loader.builtins['catalog/consumer'] = { name: 'consumer', inject: ['dependency'], apply(ctx: Context) { ctx.provide('result', ctx.get('dependency') as object) } }
  root.loader.builtins['catalog/failing'] = { name: 'failing', apply() { throw new Error('candidate failed') } }
  const entry = (entryId: string, name = 'cordis:catalog/noop', config: unknown = {}) => ({ id: entryId, name, config })
  switch (id) {
    case 'unordered-dependencies':
      await root.loader.root.update([entry('consumer', 'cordis:catalog/consumer'), entry('provider', 'cordis:catalog/provider', { value: 'provider' })])
      await root.loader.await()
      return { consumerActive: states[root.loader.resolve('consumer').fiber?.state ?? -1] === 'ACTIVE' && (root.get('result') as { value?: unknown } | undefined)?.value === 'provider' }
    case 'duplicate-id': {
      let duplicateRejected = false
      try { await root.loader.root.update([entry('duplicate'), entry('duplicate')]) } catch (error) { duplicateRejected = error instanceof TypeError }
      return { duplicateRejected }
    }
    case 'add-update-remove-move':
      await root.loader.create(entry('first'))
      await root.loader.create(entry('second'))
      await root.loader.create(entry('third'), null, 1)
      await root.loader.update('second', { config: { revision: 2 } })
      await root.loader.update('third', {}, null, 0)
      await root.loader.remove('first')
      return { ids: root.loader.root.data.map(item => item.id) }
    case 'config-only-update':
      await root.loader.create(entry('config'))
      await root.loader.update('config', { config: { revision: 2 } })
      return root.loader.resolve('config').options.config
    case 'pending-config-rollback': {
      let starts = 0
      root.loader.builtins['catalog/configured'] = {
        name: 'configured', inject: ['dependency'],
        apply(ctx: Context, config: { label: string }) {
          if (config.label === 'reject') throw new Error('rejected config')
          starts++
          ctx.provide('result', config)
        },
      }
      const source = { label: { __jsExpr: 'dependency.latest' } }
      const observations: unknown[] = []
      const observe = async () => {
        await root.loader.await()
        observations.push(root.get('result') ?? null)
      }
      await root.loader.create(entry('config', 'cordis:catalog/configured', { label: { __jsExpr: 'dependency.old' }, obsolete: true }))
      await observe()
      await root.loader.update('config', { config: source })
      await observe()
      const startsWhilePending = starts
      await root.loader.create(entry('provider', 'cordis:catalog/provider', { old: 'stale', latest: 'ready' }))
      await observe()
      let rejected = false
      try { await root.loader.update('config', { config: { label: 'reject' } }) } catch { rejected = true }
      await observe()
      const sourcePreserved = JSON.stringify(root.loader.resolve('config').options.config) === JSON.stringify(source)
      await root.loader.update('config', { config: { label: { __jsExpr: 'dependency.old' } } })
      await observe()
      await root.fiber.dispose()
      observations.push(root.get('result') ?? null)
      return { observations, startsWhilePending, rejected, sourcePreserved }
    }
    case 'plugin-replacement':
      await root.loader.root.update([entry('old')])
      await root.loader.root.update([entry('replacement')])
      return { active: [...root.loader.entries()].filter(item => states[item.fiber?.state ?? -1] === 'ACTIVE').map(item => item.id).join(',') }
    case 'candidate-failure': {
      await root.loader.root.update([entry('old')])
      let rejected = false
      try { await root.loader.root.update([entry('broken', 'cordis:catalog/failing')]) } catch { rejected = true }
      if (!rejected) throw new Error('failed candidate unexpectedly committed')
      if (states[root.loader.resolve('old').fiber?.state ?? -1] !== 'ACTIVE') throw new Error('old plugin was not restored')
      return { tree: root.loader.root.data.map(item => item.id).join(',') }
    }
    case 'rollback-failure': {
      const candidateError = new Error('candidate failed')
      const rollbackError = new Error('restoration failed')
      let starts = 0
      root.loader.builtins['catalog/old'] = { name: 'old', apply() { if (++starts > 1) throw rollbackError } }
      root.loader.builtins['catalog/failing'] = { name: 'failing', apply() { throw candidateError } }
      await root.loader.root.update([entry('old', 'cordis:catalog/old')])
      let failure: unknown
      try { await root.loader.root.update([entry('old', 'cordis:catalog/failing')]) } catch (error) { failure = error }
      const contains = (error: unknown, target: Error): boolean => error === target
        || error instanceof AggregateError && error.errors.some(child => contains(child, target))
        || error instanceof Error && contains(error.cause, target)
      if (!contains(failure, candidateError)) throw new Error('candidate error was not retained')
      return { tree: root.loader.root.data.map(item => item.id).join(','), rollbackFailed: contains(failure, rollbackError) }
    }
    case 'nested-group':
      root.loader.builtins['catalog/group'] = Group
      await root.loader.root.update([{ ...entry('root', 'cordis:catalog/group', [{ ...entry('root.child', 'cordis:catalog/group', [entry('nested')]), group: true }]), group: true }])
      if (root.loader.resolve('root.child').subgroup?.data[0]?.id !== 'nested') throw new Error('nested entry lost its group')
      return { activeEntries: [...root.loader.entries()].filter(item => !item.subgroup && states[item.fiber?.state ?? -1] === 'ACTIVE').length }
    case 'patch-insertion-later-targeting': {
      const { applyEntryPatches } = await import(pathToFileURL(join(dirname(cordisRoot), 'include/src/index.ts')).href)
      const patched = applyEntryPatches([entry('target')], [{ insert: [entry('inserted')] }, { id: 'inserted', config: { patched: true } }], (message: string) => { throw new Error(message) })
      await root.loader.root.update(patched)
      return root.loader.resolve('inserted').options.config
    }
    case 'atomic-write': {
      const { Include } = await import(pathToFileURL(join(dirname(cordisRoot), 'include/src/index.ts')).href)
      const directory = await mkdtemp(join(tmpdir(), 'tessivum-loader-oracle-'))
      const filename = join(directory, 'entries.json')
      try {
        root.loader.builtins['catalog/include'] = Include
        await root.loader.create(entry('file', 'cordis:catalog/include', { path: pathToFileURL(filename).href, initial: [entry('persisted')] }))
        await root.loader.update('file:persisted', { config: { revision: 2 } })
        await root.loader.remove('file') // Include disposal flushes queued writes.
        const saved = JSON.parse(await readFile(filename, 'utf8'))
        return { persisted: JSON.stringify(stable(saved)) === JSON.stringify(stable([entry('persisted', 'cordis:catalog/noop', { revision: 2 })])) && !existsSync(filename + '.tmp') }
      } finally {
        try { await root.fiber.dispose() } finally { await rm(directory, { recursive: true, force: true }) }
      }
    }
  }
}

async function loaderCatalog(trace: Trace, input: Input) {
  for (const id of loaderCatalogCaseIds(input)) {
    const root = new Context()
    try {
      trace.add({ event: 'config-committed', subject: id, phase: 'catalog', value: await loaderCatalogCase(root, id) })
    } finally {
      await root.fiber.dispose()
    }
  }
}

export async function execute(fixture: Fixture): Promise<OracleResult> {
  const supported = fixture.domain === 'lifecycle' && (fixture.scenario === 'effect-dispose' || fixture.scenario === 'lifecycle-catalog')
    || fixture.domain === 'service' && (fixture.scenario === 'isolate-realm' || fixture.scenario === 'service-catalog' || fixture.scenario === 'registration')
    || fixture.domain === 'event' && fixture.scenario === 'event-catalog'
    || fixture.domain === 'loader' && (fixture.scenario === 'dependency-lifecycle' || fixture.scenario === 'loader-catalog')
  if (!supported) return { fixture: fixture.name, status: 'UNSUPPORTED_SCENARIO', error: { code: 'UNSUPPORTED_SCENARIO', fixture: fixture.name, message: `${fixture.domain}/${fixture.scenario} is not supported by the Cordis oracle` } }
  const root = new Context()
  const trace = new Trace(fixture.domain === 'loader' || fixture.domain === 'event' || fixture.domain === 'lifecycle' && fixture.scenario === 'lifecycle-catalog' || fixture.domain === 'service' && (fixture.scenario === 'registration' || fixture.scenario === 'service-catalog') ? undefined : root)
  try {
    if (fixture.domain === 'lifecycle' && fixture.scenario === 'lifecycle-catalog') await lifecycleCatalog(trace, fixture.input ?? {})
    else if (fixture.domain === 'lifecycle') await lifecycle(root, trace, fixture.input ?? {})
    else if (fixture.domain === 'service' && fixture.scenario === 'registration') await serviceRegistration(trace, fixture.input ?? {})
    else if (fixture.domain === 'service' && fixture.scenario === 'service-catalog') await serviceCatalog(trace, fixture.input ?? {})
    else if (fixture.domain === 'service') await service(root, trace, fixture.input ?? {})
    else if (fixture.domain === 'loader' && fixture.scenario === 'loader-catalog') await loaderCatalog(trace, fixture.input ?? {})
    else if (fixture.domain === 'loader') trace.events.push(...await loaderDependencies(root, fixture.input ?? {}))
    else await eventCatalog(root, trace, fixture.input ?? {})
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
