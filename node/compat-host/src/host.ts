import { existsSync } from 'node:fs'
import { basename, isAbsolute, join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { FrameDecoder, ProtocolError, defaultMaxFrameBytes, encodeFrame, protocolVersion, type Frame } from './protocol.ts'

type RecordValue = Record<string, unknown>
type Disposer = () => unknown

type Fiber = {
  uid: number | null
  name: string
  state: number
  config: unknown
  inertia?: Promise<void>
  dispose(): Promise<void>
  update(config: unknown, noSave?: boolean): unknown
  await(): Promise<Fiber>
  getEffects(): unknown
}

type Context = Record<string, any> & { fiber: Fiber }
type LoaderEntry = { fiber?: Fiber; options: RecordValue }
type Loader = {
  create(options: RecordValue): Promise<string>
  update(id: string, options: RecordValue): Promise<void>
  remove(id: string): Promise<void>
  resolve(id: string): LoaderEntry
  entries(): Iterable<LoaderEntry>
}

type PluginRecord = { fiber: Fiber; entry?: LoaderEntry }
type Registration = { dispose: Disposer; name?: string }
type Incoming = { controller: AbortController; settled: boolean }
type Outgoing = { resolve(value: unknown): void; reject(reason: unknown): void }

const fiberStates = ['PENDING', 'LOADING', 'ACTIVE', 'FAILED', 'DISPOSED', 'UNLOADING']
const operationKinds = new Set([
  'plugin.load', 'plugin.update', 'plugin.dispose', 'plugin.snapshot',
  'service.call', 'service.provide', 'service.remove',
  'event.subscribe', 'event.emit', 'event.callback', 'registration.dispose',
])
const knownKinds = new Set(['hello', 'ready', 'response', 'error', 'cancel', 'exit', 'heartbeat', 'log', ...operationKinds])
const rawStdoutWrite = process.stdout.write.bind(process.stdout) as (chunk: Uint8Array) => boolean

class BridgeError extends Error {
  constructor(readonly code: string, message: string, readonly details?: unknown) {
    super(message)
    this.name = 'BridgeError'
  }
}

function object(value: unknown, label = 'payload'): RecordValue {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new BridgeError('INVALID_PAYLOAD', `${label} must be an object`)
  return value as RecordValue
}

function text(value: unknown, label: string) {
  if (typeof value !== 'string' || !value) throw new BridgeError('INVALID_PAYLOAD', `${label} must be a non-empty string`)
  return value
}

function optionalText(value: unknown) {
  return typeof value === 'string' && value ? value : undefined
}

function abortIfNeeded(signal: AbortSignal) {
  if (signal.aborted) throw new BridgeError('CANCELLED', 'request cancelled')
}

function settled(result: unknown) {
  return Promise.resolve(result)
}

/** Converts host values to bounded JSON so diagnostics never break the transport. */
function json(value: unknown, depth = 0, seen = new WeakSet<object>()): unknown {
  if (value === undefined) return null
  if (value === null || typeof value === 'boolean') return value
  if (typeof value === 'number') return Number.isFinite(value) ? value : String(value)
  if (typeof value === 'string') return value.length > 16_384 ? `${value.slice(0, 16_384)}…` : value
  if (typeof value === 'bigint') return value.toString()
  if (typeof value === 'function') return `[Function ${(value as Function).name || 'anonymous'}]`
  if (value instanceof Error) return { name: value.name, message: value.message, stack: value.stack?.slice(0, 16_384) }
  if (depth >= 8) return '[MaxDepth]'
  if (typeof value === 'object') {
    if (seen.has(value)) return '[Circular]'
    seen.add(value)
    if (Array.isArray(value)) return value.slice(0, 128).map(item => json(item, depth + 1, seen))
    const output: RecordValue = {}
    for (const [key, item] of Object.entries(value as RecordValue).slice(0, 128)) output[key] = json(item, depth + 1, seen)
    return output
  }
  return String(value)
}

function failure(error: unknown) {
  if (error instanceof BridgeError || error instanceof ProtocolError) {
    return { code: error.code, message: error.message, ...(error instanceof BridgeError && error.details === undefined ? {} : { details: json(error instanceof BridgeError ? error.details : undefined) }) }
  }
  if (error instanceof Error) return { code: 'HOST_ERROR', message: error.message, details: json(error) }
  return { code: 'HOST_ERROR', message: String(error), details: json(error) }
}

function maxFrameBytes() {
  const raw = process.env.CORDIS_NODE_MAX_FRAME_BYTES
  if (raw === undefined) return defaultMaxFrameBytes
  if (!/^\d+$/.test(raw)) throw new BridgeError('INVALID_FRAME_LIMIT', 'CORDIS_NODE_MAX_FRAME_BYTES must be a positive integer')
  const value = Number(raw)
  if (!Number.isSafeInteger(value) || value < 1 || value > 0xffff_ffff) throw new BridgeError('INVALID_FRAME_LIMIT', 'CORDIS_NODE_MAX_FRAME_BYTES must be a positive u32')
  return value
}

function vendorRoot() {
  const configured = process.env.CORDIS_VENDOR_ROOT
  const candidates = [
    configured,
    resolve(process.cwd(), 'upstream/deepseek-harness/vendor'),
    resolve(process.cwd(), '../upstream/deepseek-harness/vendor'),
  ].filter((value): value is string => !!value)
  for (const candidate of candidates) {
    const root = basename(candidate) === 'cordis' ? resolve(candidate, '..') : resolve(candidate)
    if (existsSync(join(root, 'cordis', 'src', 'index.ts')) && existsSync(join(root, 'cosmokit', 'src', 'index.ts'))) return root
  }
  throw new BridgeError('CORDIS_NOT_FOUND', 'set CORDIS_VENDOR_ROOT to the checked-out vendor directory or cordis package directory')
}

function installVendorResolver(vendor: string) {
  if (typeof Bun === 'undefined' || typeof Bun.plugin !== 'function') throw new BridgeError('BUN_REQUIRED', 'compat-host must run under Bun')
  const aliases: Record<string, string> = {
    '@deepseek-ai/cordis': join(vendor, 'cordis', 'src', 'index.ts'),
    '@deepseek-ai/cosmokit': join(vendor, 'cosmokit', 'src', 'index.ts'),
    '@deepseek-ai/cordis-plugin-loader': join(vendor, 'loader', 'src', 'index.ts'),
    cordis: join(vendor, 'cordis', 'src', 'index.ts'),
    cosmokit: join(vendor, 'cosmokit', 'src', 'index.ts'),
    '@cordisjs/plugin-loader': join(vendor, 'loader', 'src', 'index.ts'),
  }
  Bun.plugin({
    name: 'tessivum-vendored-cordis',
    setup(build) {
      for (const [specifier, path] of Object.entries(aliases)) {
        build.onResolve({ filter: new RegExp(`^${specifier.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`) }, () => ({ path }))
      }
    },
  })
}

function moduleTarget(payload: RecordValue) {
  const packageInfo = payload.package === undefined ? undefined : object(payload.package, 'package')
  const direct = optionalText(payload.specifier) ?? optionalText(payload.module)
  const specifier = optionalText(packageInfo?.specifier) ?? direct
  const location = optionalText(packageInfo?.location) ?? optionalText(payload.location)
  if (location && (location.startsWith('file:') || isAbsolute(location) || location.startsWith('.'))) return location
  if (specifier) return specifier
  if (location) return location
  throw new BridgeError('INVALID_PAYLOAD', 'plugin package must provide specifier or location')
}

function importTarget(target: string) {
  if (target.startsWith('file:')) return target
  if (isAbsolute(target)) return pathToFileURL(target).href
  if (target.startsWith('.')) return pathToFileURL(resolve(process.cwd(), target)).href
  return target
}

function exportName(payload: RecordValue) {
  const entry = payload.entry === undefined ? undefined : object(payload.entry, 'entry')
  const options = entry?.options && typeof entry.options === 'object' && !Array.isArray(entry.options) ? entry.options as RecordValue : undefined
  return optionalText(payload.export) ?? optionalText(entry?.export) ?? optionalText(options?.export)
}

function configOf(payload: RecordValue) {
  if (Object.hasOwn(payload, 'config')) return payload.config
  const entry = payload.entry === undefined ? undefined : object(payload.entry, 'entry')
  if (Object.hasOwn(entry ?? {}, 'config')) return entry!.config
  const options = entry?.options && typeof entry.options === 'object' && !Array.isArray(entry.options) ? entry.options as RecordValue : undefined
  return options?.config
}

export class CompatHost {
  private readonly maxFrameBytes = maxFrameBytes()
  private readonly vendor = vendorRoot()
  private generation = 0n
  private phase: 'new' | 'ready' | 'closing' | 'closed' = 'new'
  private root: Context | undefined
  private loader: Loader | undefined
  private readonly plugins = new Map<string, PluginRecord>()
  private readonly registrations = new Map<string, Registration>()
  private readonly callbacks = new Map<string, (...args: unknown[]) => unknown>()
  private readonly incoming = new Map<string, Incoming>()
  private readonly outgoing = new Map<string, Outgoing>()
  private nextRequestId = 1n
  private shutdownTask: Promise<void> | undefined
  private sequence = Promise.resolve()

  constructor() {
    installVendorResolver(this.vendor)
    this.redirectConsole()
  }

  private redirectConsole() {
    const logger = (level: string, args: unknown[]) => this.log({ level, message: args.map(item => typeof item === 'string' ? item : json(item)) })
    for (const name of ['log', 'info', 'debug', 'warn', 'error'] as const) {
      console[name] = (...args: unknown[]) => logger(name === 'log' ? 'info' : name, args)
    }
    for (const [stream, level] of [[process.stdout, 'info'], [process.stderr, 'error']] as const) {
      const replacement = ((chunk: unknown, ...rest: unknown[]) => {
        logger(level, [Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk)])
        const callback = rest.find(item => typeof item === 'function') as (() => void) | undefined
        callback?.()
        return true
      }) as typeof stream.write
      stream.write = replacement
    }
  }

  private write(kind: string, payload: unknown, requestId?: bigint) {
    rawStdoutWrite(encodeFrame({ protocolVersion, connectionGeneration: this.generation, kind, ...(requestId === undefined ? {} : { requestId }), payload: json(payload) }, this.maxFrameBytes))
  }

  private log(payload: unknown) {
    try {
      this.write('log', payload)
    } catch {
      // Nothing safe remains if even a bounded log frame cannot be serialized.
    }
  }

  private respond(frame: Frame, payload: unknown) {
    if (frame.requestId === undefined) throw new BridgeError('INVALID_REQUEST_ID', `${frame.kind} requires requestId`)
    this.write('response', payload, frame.requestId)
  }

  private respondError(frame: Frame, error: unknown) {
    if (frame.requestId === undefined) {
      this.log({ level: 'error', error: failure(error) })
      return
    }
    this.write('error', failure(error), frame.requestId)
  }

  private async initialize() {
    if (this.root) return
    const cordis = await import(pathToFileURL(join(this.vendor, 'cordis', 'src', 'index.ts')).href) as { Context: new () => Context }
    const loaderModule = await import(pathToFileURL(join(this.vendor, 'loader', 'src', 'index.ts')).href) as { Loader: new (ctx: Context, config?: RecordValue) => Loader }
    this.root = new cordis.Context()
    this.root.baseUrl = pathToFileURL(`${process.cwd()}/`).href
    this.root.logger.exporter({
      colors: false,
      export: (message: RecordValue) => this.log({
        level: message.type,
        name: message.name,
        message: (message.args as unknown[] | undefined)?.map(value => json(value)) ?? [],
      }),
    })
    this.loader = new loaderModule.Loader(this.root, { baseUrl: this.root.baseUrl })
  }

  receive(frame: Frame) {
    if (frame.kind === 'cancel') return this.cancel(frame)
    this.sequence = this.sequence.then(() => this.dispatch(frame)).catch(error => this.fatal(error))
    return undefined
  }

  private async dispatch(frame: Frame) {
    if (!knownKinds.has(frame.kind)) return this.fault(frame, new BridgeError('UNKNOWN_KIND', `unknown message kind ${frame.kind}`))
    if (this.phase === 'new') {
      if (frame.kind !== 'hello') return this.fault(frame, new BridgeError('HANDSHAKE_REQUIRED', 'hello is required before other messages'))
      if (frame.requestId !== undefined) return this.fault(frame, new BridgeError('INVALID_REQUEST_ID', 'hello must not have requestId'))
      this.generation = frame.connectionGeneration
      await this.initialize()
      this.phase = 'ready'
      this.write('ready', { protocolVersion, maxFrameBytes: this.maxFrameBytes, vendoredCordis: true })
      return
    }
    if (frame.connectionGeneration !== this.generation) return this.fault(frame, new BridgeError('GENERATION_MISMATCH', 'frame belongs to another connection generation'))
    if (this.phase !== 'ready') return
    if (frame.kind === 'hello' || frame.kind === 'ready') return this.fault(frame, new BridgeError('HANDSHAKE_COMPLETE', 'handshake already completed'))
    if (frame.kind === 'response' || frame.kind === 'error') return this.resolveOutgoing(frame)
    if (frame.kind === 'heartbeat') {
      if (frame.requestId !== undefined) return this.fault(frame, new BridgeError('INVALID_REQUEST_ID', 'heartbeat must not have requestId'))
      this.write('heartbeat', { ok: true })
      return
    }
    if (frame.kind === 'log') {
      if (frame.requestId !== undefined) return this.fault(frame, new BridgeError('INVALID_REQUEST_ID', 'log must not have requestId'))
      this.log(frame.payload)
      return
    }
    if (frame.kind === 'exit') {
      await this.shutdown()
      if (frame.requestId !== undefined) this.respond(frame, { drained: true })
      this.write('exit', { drained: true })
      process.stdin.pause()
      process.exitCode = 0
      queueMicrotask(() => process.exit())
      return
    }
    if (!operationKinds.has(frame.kind)) return this.fault(frame, new BridgeError('UNKNOWN_KIND', `unknown message kind ${frame.kind}`))
    if (frame.requestId === undefined || frame.requestId === 0n) return this.fault(frame, new BridgeError('INVALID_REQUEST_ID', `${frame.kind} requires nonzero requestId`))
    const key = frame.requestId.toString()
    if (this.incoming.has(key)) return this.fault(frame, new BridgeError('DUPLICATE_REQUEST_ID', `request ${key} is already active`))
    const request: Incoming = { controller: new AbortController(), settled: false }
    this.incoming.set(key, request)
    try {
      const value = await this.operation(frame, request.controller.signal)
      this.log({ level: 'debug', message: ['dispatch:operation-done', frame.kind, frame.requestId.toString()] })
      abortIfNeeded(request.controller.signal)
      if (!request.settled) {
        this.respond(frame, value)
        request.settled = true
        this.log({ level: 'debug', message: ['dispatch:response-written', frame.kind, frame.requestId.toString()] })
      }
    } catch (error) {
      if (!request.settled) {
        this.respondError(frame, error)
        request.settled = true
      }
    } finally {
      if (this.incoming.get(key) === request) this.incoming.delete(key)
    }
  }

  private resolveOutgoing(frame: Frame) {
    if (frame.requestId === undefined) throw new BridgeError('INVALID_REQUEST_ID', `${frame.kind} requires requestId`)
    const request = this.outgoing.get(frame.requestId.toString())
    if (!request) throw new BridgeError('UNKNOWN_REQUEST_ID', `unknown outgoing request ${frame.requestId}`)
    this.outgoing.delete(frame.requestId.toString())
    if (frame.kind === 'response') request.resolve(frame.payload)
    else request.reject(Object.assign(new BridgeError('REMOTE_ERROR', 'remote callback failed'), { details: frame.payload }))
  }

  private cancel(frame: Frame) {
    if (this.phase !== 'ready' || frame.connectionGeneration !== this.generation) return this.fault(frame, new BridgeError('GENERATION_MISMATCH', 'cancel belongs to another connection generation'))
    const legacy = frame.payload && typeof frame.payload === 'object' && !Array.isArray(frame.payload) ? frame.payload as RecordValue : undefined
    const target = frame.requestId ?? (typeof legacy?.requestId === 'number' && Number.isSafeInteger(legacy.requestId) ? BigInt(legacy.requestId) : undefined)
    if (target === undefined || target === 0n) return this.fault(frame, new BridgeError('INVALID_REQUEST_ID', 'cancel requires the target requestId'))
    const key = target.toString()
    const incoming = this.incoming.get(key)
    if (incoming && !incoming.settled) {
      incoming.settled = true
      incoming.controller.abort()
      this.write('error', { code: 'CANCELLED', message: 'request cancelled' }, target)
    }
    const outgoing = this.outgoing.get(key)
    if (outgoing) {
      this.outgoing.delete(key)
      outgoing.reject(new BridgeError('CANCELLED', 'remote request cancelled'))
    }
  }

  private async operation(frame: Frame, signal: AbortSignal): Promise<unknown> {
    switch (frame.kind) {
      case 'plugin.load': return this.loadPlugin(object(frame.payload), signal)
      case 'plugin.update': return this.updatePlugin(object(frame.payload), signal)
      case 'plugin.dispose': return this.disposePlugin(object(frame.payload), signal)
      case 'plugin.snapshot': return this.snapshotPlugin(object(frame.payload))
      case 'service.call': return this.callService(object(frame.payload), signal)
      case 'service.provide': return this.provideService(object(frame.payload), signal)
      case 'service.remove': return this.removeService(object(frame.payload), signal)
      case 'event.subscribe': return this.subscribe(object(frame.payload), signal)
      case 'event.emit': return this.emit(object(frame.payload), signal)
      case 'event.callback': return this.callback(object(frame.payload), signal)
      case 'registration.dispose': return this.disposeRegistration(object(frame.payload), signal)
      default: throw new BridgeError('UNKNOWN_KIND', `unknown message kind ${frame.kind}`)
    }
  }

  private context() {
    if (!this.root) throw new BridgeError('HANDSHAKE_REQUIRED', 'host is not initialized')
    return this.root
  }

  private async loadPlugin(payload: RecordValue, signal: AbortSignal) {
    const id = text(payload.pluginId ?? payload.id, 'pluginId')
    if (this.plugins.has(id)) throw new BridgeError('DUPLICATE_PLUGIN', `plugin ${id} is already loaded`)
    const target = moduleTarget(payload)
    const module = await import(/* @vite-ignore */importTarget(target)) as RecordValue
    abortIfNeeded(signal)
    const chosen = exportName(payload)
    const plugin = chosen ? module[chosen] : module.default ?? module
    if (!plugin) throw new BridgeError('PLUGIN_EXPORT_NOT_FOUND', `plugin export ${chosen ?? 'default'} was not found`)
    const config = configOf(payload)
    const entry = payload.entry === undefined ? undefined : object(payload.entry, 'entry')
    const useLoader = payload.loader === true || entry?.loader === true
    if (useLoader) {
      const loader = this.loader
      if (!loader) throw new BridgeError('LOADER_UNAVAILABLE', 'loader is unavailable')
      const options: RecordValue = { id, name: target, ...(config === undefined ? {} : { config }) }
      const inject = entry?.options && typeof entry.options === 'object' && !Array.isArray(entry.options) ? (entry.options as RecordValue).inject : undefined
      if (inject !== undefined) options.inject = inject
      const entryId = await loader.create(options)
      try {
        abortIfNeeded(signal)
        const loaderEntry = loader.resolve(entryId)
        if (!loaderEntry.fiber) throw new BridgeError('PLUGIN_LOAD_FAILED', `loader did not create fiber for ${id}`)
        await loaderEntry.fiber.await()
        abortIfNeeded(signal)
        this.plugins.set(id, { fiber: loaderEntry.fiber, entry: loaderEntry })
      } catch (error) {
        await loader.remove(entryId)
        throw error
      }
    } else {
      const fiber = this.context().plugin(plugin, config) as Fiber
      try {
        await fiber.await()
        abortIfNeeded(signal)
      } catch (error) {
        await settled(fiber.dispose()).catch(() => undefined)
        throw error
      }
      if (signal.aborted) {
        await settled(fiber.dispose()).catch(() => undefined)
        abortIfNeeded(signal)
      }
      this.plugins.set(id, { fiber })
    }
    return this.snapshot(id)
  }

  private plugin(payload: RecordValue) {
    const id = text(payload.pluginId ?? payload.id, 'pluginId')
    const plugin = this.plugins.get(id)
    if (!plugin) throw new BridgeError('UNKNOWN_PLUGIN', `plugin ${id} is not loaded`)
    return [id, plugin] as const
  }

  private async updatePlugin(payload: RecordValue, signal: AbortSignal) {
    const [id, plugin] = this.plugin(payload)
    if (!Object.hasOwn(payload, 'config')) throw new BridgeError('INVALID_PAYLOAD', 'plugin.update requires config')
    abortIfNeeded(signal)
    if (plugin.entry) await this.loader!.update(id, { config: payload.config })
    else await settled(plugin.fiber.update(payload.config))
    abortIfNeeded(signal)
    if (plugin.entry?.fiber) plugin.fiber = plugin.entry.fiber
    await plugin.fiber.await()
    return this.snapshot(id)
  }

  private async disposePlugin(payload: RecordValue, signal: AbortSignal) {
    const [id, plugin] = this.plugin(payload)
    if (plugin.entry) await this.loader!.remove(id)
    else await plugin.fiber.dispose()
    this.plugins.delete(id)
    abortIfNeeded(signal)
    return { pluginId: id, disposed: true }
  }

  private snapshotPlugin(payload: RecordValue) {
    const id = optionalText(payload.pluginId ?? payload.id)
    if (!id && payload.loader === true) return { entries: [...this.loader!.entries()].map(entry => this.fiberSnapshot(entry.fiber)) }
    if (!id) throw new BridgeError('INVALID_PAYLOAD', 'plugin.snapshot requires pluginId')
    return this.snapshot(id)
  }

  private snapshot(id: string) {
    const plugin = this.plugins.get(id)
    if (!plugin) throw new BridgeError('UNKNOWN_PLUGIN', `plugin ${id} is not loaded`)
    return { pluginId: id, ...this.fiberSnapshot(plugin.fiber) }
  }

  private fiberSnapshot(fiber: Fiber | undefined) {
    if (!fiber) return { state: 'DISPOSED', settled: true, effects: [] }
    return {
      uid: fiber.uid,
      name: fiber.name,
      state: fiberStates[fiber.state] ?? 'UNKNOWN',
      config: json(fiber.config),
      settled: !fiber.inertia,
      effects: json(fiber.getEffects()),
    }
  }

  private remoteService(payload: RecordValue) {
    const serviceId = optionalText(payload.serviceId) ?? optionalText(payload.id) ?? optionalText(payload.name)
    return new Proxy(Object.create(null), {
      get: (_target, property) => {
        if (property === 'then') return undefined
        if (typeof property !== 'string') return undefined
        return (...args: unknown[]) => this.requestRemote('service.call', { serviceId, name: payload.name, method: property, args })
      },
    })
  }

  private async provideService(payload: RecordValue, signal: AbortSignal) {
    const name = text(payload.name ?? payload.service, 'service name')
    const registrationId = optionalText(payload.registrationId) ?? optionalText(payload.id) ?? `service:${name}`
    if (this.registrations.has(registrationId)) throw new BridgeError('DUPLICATE_REGISTRATION', `registration ${registrationId} already exists`)
    abortIfNeeded(signal)
    const value = Object.hasOwn(payload, 'value') ? payload.value : this.remoteService(payload)
    const dispose = this.context().provide(name, value) as Disposer
    this.registrations.set(registrationId, { dispose, name })
    return { registrationId, name }
  }

  private async removeService(payload: RecordValue, signal: AbortSignal) {
    const registrationId = optionalText(payload.registrationId) ?? optionalText(payload.id) ?? `service:${text(payload.name ?? payload.service, 'service name')}`
    const registration = this.registrations.get(registrationId)
    if (!registration) throw new BridgeError('UNKNOWN_REGISTRATION', `registration ${registrationId} is not active`)
    await settled(registration.dispose())
    this.registrations.delete(registrationId)
    abortIfNeeded(signal)
    return { registrationId, removed: true }
  }

  private async callService(payload: RecordValue, signal: AbortSignal) {
    const name = text(payload.name ?? payload.service, 'service name')
    const args = payload.args === undefined ? [] : payload.args
    if (!Array.isArray(args)) throw new BridgeError('INVALID_PAYLOAD', 'service.call args must be an array')
    const service = this.context().get(name)
    if (service === undefined) throw new BridgeError('SERVICE_UNAVAILABLE', `service ${name} is unavailable`)
    const method = optionalText(payload.method)
    const callable = method ? service[method] : service
    if (typeof callable !== 'function') {
      if (method || args.length) throw new BridgeError('SERVICE_METHOD_NOT_FOUND', `service ${name}${method ? `.${method}` : ''} is not callable`)
      return json(callable)
    }
    abortIfNeeded(signal)
    const result = await settled(Reflect.apply(callable, service, args))
    abortIfNeeded(signal)
    return json(result)
  }

  private async subscribe(payload: RecordValue, signal: AbortSignal) {
    const event = text(payload.event ?? payload.name, 'event')
    const callbackId = text(payload.callbackId, 'callbackId')
    const registrationId = optionalText(payload.registrationId) ?? optionalText(payload.id) ?? `event:${event}:${callbackId}`
    if (this.registrations.has(registrationId)) throw new BridgeError('DUPLICATE_REGISTRATION', `registration ${registrationId} already exists`)
    const options = payload.options && typeof payload.options === 'object' && !Array.isArray(payload.options) ? payload.options : undefined
    const listener = (...args: unknown[]) => this.requestRemote('event.callback', { callbackId, event, args })
    abortIfNeeded(signal)
    const dispose = this.context().on(event, listener, options) as Disposer
    this.registrations.set(registrationId, { dispose, name: event })
    return { registrationId, event, callbackId }
  }

  private async emit(payload: RecordValue, signal: AbortSignal) {
    const event = text(payload.event ?? payload.name, 'event')
    const mode = optionalText(payload.mode) ?? 'emit'
    if (!['emit', 'parallel', 'serial', 'bail', 'waterfall'].includes(mode)) throw new BridgeError('INVALID_PAYLOAD', `unsupported event mode ${mode}`)
    const args = payload.args === undefined ? [] : payload.args
    if (!Array.isArray(args)) throw new BridgeError('INVALID_PAYLOAD', 'event.emit args must be an array')
    abortIfNeeded(signal)
    const context = this.context()
    const result = mode === 'waterfall'
      ? context.waterfall(event, ...args, () => payload.next ?? payload.result)
      : await settled(context[mode](event, ...args))
    abortIfNeeded(signal)
    return json(result)
  }

  private async callback(payload: RecordValue, signal: AbortSignal) {
    const callbackId = text(payload.callbackId, 'callbackId')
    const callback = this.callbacks.get(callbackId)
    if (!callback) throw new BridgeError('UNKNOWN_CALLBACK', `callback ${callbackId} is not registered`)
    const args = payload.args === undefined ? [] : payload.args
    if (!Array.isArray(args)) throw new BridgeError('INVALID_PAYLOAD', 'event.callback args must be an array')
    const result = await settled(callback(...args))
    abortIfNeeded(signal)
    return json(result)
  }

  private async disposeRegistration(payload: RecordValue, signal: AbortSignal) {
    const id = text(payload.registrationId ?? payload.id, 'registrationId')
    const registration = this.registrations.get(id)
    if (!registration) throw new BridgeError('UNKNOWN_REGISTRATION', `registration ${id} is not active`)
    await settled(registration.dispose())
    this.registrations.delete(id)
    abortIfNeeded(signal)
    return { registrationId: id, disposed: true }
  }

  private requestRemote(kind: string, payload: unknown) {
    if (this.phase !== 'ready') return Promise.reject(new BridgeError('HOST_CLOSING', 'host is not ready'))
    const requestId = this.nextRequestId++
    return new Promise<unknown>((resolve, reject) => {
      this.outgoing.set(requestId.toString(), { resolve, reject })
      try {
        this.write(kind, payload, requestId)
      } catch (error) {
        this.outgoing.delete(requestId.toString())
        reject(error)
      }
    })
  }

  private async shutdown() {
    if (this.shutdownTask) return this.shutdownTask
    this.phase = 'closing'
    this.shutdownTask = (async () => {
      for (const request of this.incoming.values()) request.controller.abort()
      for (const request of this.outgoing.values()) request.reject(new BridgeError('HOST_CLOSING', 'host is shutting down'))
      this.outgoing.clear()
      const disposals = [
        ...[...this.plugins.entries()].map(([id, plugin]) => plugin.entry ? this.loader!.remove(id) : plugin.fiber.dispose()),
        ...[...this.registrations.values()].map(registration => settled(registration.dispose())),
      ]
      await Promise.allSettled(disposals)
      this.plugins.clear()
      this.registrations.clear()
      if (this.root) await Promise.allSettled([this.root.fiber.dispose()])
      this.phase = 'closed'
    })()
    return this.shutdownTask
  }
  async stop(error?: unknown) {
    if (error !== undefined) return this.fatal(error)
    await this.sequence.catch(() => undefined)
    await this.shutdown()
    process.stdin.pause()
  }


  private async fault(frame: Frame, error: unknown) {
    this.respondError(frame, error)
    await this.shutdown()
    process.stdin.pause()
    process.exitCode = 1
    queueMicrotask(() => process.exit())
  }

  private async fatal(error: unknown) {
    this.log({ level: 'error', error: failure(error) })
    await this.shutdown()
    process.stdin.pause()
    process.exitCode = 1
    queueMicrotask(() => process.exit())
  }
}

export function createCompatHost() {
  return new CompatHost()
}

export function createDecoder() {
  return new FrameDecoder(maxFrameBytes())
}
