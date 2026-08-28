import { Buffer } from 'node:buffer'
import { randomBytes } from 'node:crypto'
import { type IncomingMessage } from 'node:http'
import { existsSync, statSync } from 'node:fs'
import { basename, isAbsolute, join, resolve } from 'node:path'
import { PassThrough, Readable, type Duplex } from 'node:stream'
import { pathToFileURL } from 'node:url'
import { FrameDecoder, ProtocolError, defaultMaxFrameBytes, encodeFrame, frameKinds, protocolVersion, type Frame } from './protocol.ts'

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
type LoaderEntry = {
  fiber?: Fiber
  options: RecordValue
  update(options: RecordValue, create?: boolean, force?: boolean): Promise<void>
}
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

type RouteKind = 'exact' | 'prefix'
type Route = {
  id: string
  kind: RouteKind
  path: string
  owner?: string
  handler(request: Readable, response: PassThrough): unknown
  registered: boolean
  removed: boolean
  pending: Promise<unknown>
}
type UpgradeRoute = {
  id: string
  path: string
  owner?: string
  handler(request: IncomingMessage, socket: Duplex, head: Buffer): unknown
  registered: boolean
  removed: boolean
  pending: Promise<unknown>
}
type PnpmOperation = {
  stdout: PassThrough
  stderr: PassThrough
}
type Profile = { name: string; dir: string }
type RemoteRequest = { requestId: bigint; promise: Promise<unknown> }
type UpgradeSocketData = {
  open(socket: Bun.ServerWebSocket<UpgradeSocketData>): void
  close(socket: Bun.ServerWebSocket<UpgradeSocketData>, code: number, reason: string): void
  message(socket: Bun.ServerWebSocket<UpgradeSocketData>, message: string | Buffer): void
  drain(socket: Bun.ServerWebSocket<UpgradeSocketData>): void
  ping(socket: Bun.ServerWebSocket<UpgradeSocketData>, data: Buffer): void
  pong(socket: Bun.ServerWebSocket<UpgradeSocketData>, data: Buffer): void
}
type SessionSnapshot = { id: string; header: RecordValue; events: unknown[] }
type AgentSnapshot = { live: boolean; status?: string; options: RecordValue }

const maxRouteRequestBytes = 2 * 1024 * 1024
const maxRouteResponseBytes = 8 * 1024 * 1024
const maxRouteHeaders = 128
const maxRouteHeaderBytes = 32 * 1024
const maxPnpmOutputChunkBytes = 64 * 1024
const maxNodeRequestId = BigInt(Number.MAX_SAFE_INTEGER - 1)
const hopByHopHeaders = new Set(['connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization', 'te', 'trailer', 'transfer-encoding', 'upgrade'])
const headerName = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/

const fiberStates = ['PENDING', 'LOADING', 'ACTIVE', 'FAILED', 'DISPOSED', 'UNLOADING']
const operationKinds = new Set([
  'plugin.load', 'plugin.update', 'plugin.dispose', 'plugin.snapshot',
  'service.call', 'service.provide', 'service.remove',
  'event.subscribe', 'event.emit', 'event.callback', 'registration.dispose', 'web.route.request',
])
const knownKinds = new Set<string>(frameKinds)
const rawStdoutWrite = process.stdout.write.bind(process.stdout) as (chunk: Uint8Array) => boolean
const bunInternals = Symbol.for('::bunternal::')
const rawStderrWrite = process.stderr.write.bind(process.stderr) as (chunk: string) => boolean

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
  const raw = process.env.TESSIVUM_BRIDGE_MAX_FRAME_SIZE
  if (raw === undefined) return defaultMaxFrameBytes
  if (!/^[1-9]\d*$/.test(raw)) throw new BridgeError('INVALID_FRAME_LIMIT', 'TESSIVUM_BRIDGE_MAX_FRAME_SIZE must be a positive decimal integer')
  const value = Number(raw)
  if (!Number.isSafeInteger(value) || value > 12 * 1024 * 1024) throw new BridgeError('INVALID_FRAME_LIMIT', 'TESSIVUM_BRIDGE_MAX_FRAME_SIZE must not exceed 12 MiB')
  return value
}

function profileFromEnvironment(): Profile | undefined {
  const name = process.env.TESSIVUM_PROFILE_NAME
  const dir = process.env.TESSIVUM_PROFILE_DIR
  if (name === undefined && dir === undefined) return undefined
  if (name === undefined || dir === undefined) throw new BridgeError('INVALID_PROFILE', 'TESSIVUM_PROFILE_NAME and TESSIVUM_PROFILE_DIR must be set together')
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(name)) throw new BridgeError('INVALID_PROFILE', 'TESSIVUM_PROFILE_NAME is invalid')
  if (!isAbsolute(dir) || dir.includes('\0') || !existsSync(dir) || !statSync(dir).isDirectory()) throw new BridgeError('INVALID_PROFILE', 'TESSIVUM_PROFILE_DIR must name an existing absolute directory')
  return { name, dir: resolve(dir) }
}

function routePath(value: unknown) {
  const path = text(value, 'route path')
  if (!path.startsWith('/') || path.includes('\0') || path.includes('?') || path.includes('#') || path.includes('\\')) throw new BridgeError('INVALID_ROUTE', 'route path is invalid')
  let decoded: string
  try {
    decoded = decodeURIComponent(path)
  } catch {
    throw new BridgeError('INVALID_ROUTE', 'route path has invalid escapes')
  }
  if (decoded.split('/').some(segment => segment === '.' || segment === '..')) throw new BridgeError('INVALID_ROUTE', 'route path traversal is forbidden')
  if (!['/dsh-market', '/sidebar'].some(root => path === root || path.startsWith(`${root}/`))) throw new BridgeError('INVALID_ROUTE', 'route path is outside the supported compatibility roots')
  return path
}

function frameBody(value: unknown, limit: number, label: string) {
  if (typeof value !== 'string') throw new BridgeError('INVALID_PAYLOAD', `${label} must be base64 text`)
  const source = value
  if (source.length % 4 || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(source)) throw new BridgeError('INVALID_PAYLOAD', `${label} is not canonical base64`)
  const body = Buffer.from(source, 'base64')
  if (body.byteLength > limit || body.toString('base64') !== source) throw new BridgeError('PAYLOAD_TOO_LARGE', `${label} exceeds ${limit} bytes`)
  return body
}

function headers(value: unknown) {
  if (!Array.isArray(value) || value.length > maxRouteHeaders) throw new BridgeError('INVALID_HEADERS', 'headers must contain at most 128 pairs')
  let bytes = 0
  return value.map((pair, index) => {
    if (!Array.isArray(pair) || pair.length !== 2 || typeof pair[0] !== 'string' || typeof pair[1] !== 'string') throw new BridgeError('INVALID_HEADERS', `header ${index} must be a name/value pair`)
    const [name, headerValue] = pair
    const lower = name.toLowerCase()
    if (!headerName.test(name) || /[\r\n]/.test(headerValue) || hopByHopHeaders.has(lower)) throw new BridgeError('INVALID_HEADERS', `header ${name} is invalid`)
    bytes += Buffer.byteLength(name) + Buffer.byteLength(headerValue)
    if (bytes > maxRouteHeaderBytes) throw new BridgeError('INVALID_HEADERS', 'headers exceed 32 KiB')
    return [name, headerValue] as [string, string]
  })
}

function responseOutput(stream: PassThrough, limit: number) {
  const chunks: Buffer[] = []
  let bytes = 0
  let ended = false
  const append = (chunk: unknown, encoding?: BufferEncoding) => {
    if (ended) throw new BridgeError('LATE_RESPONSE', 'response already ended')
    const body = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk), encoding)
    bytes += body.byteLength
    if (bytes > limit) throw new BridgeError('PAYLOAD_TOO_LARGE', `response exceeds ${limit} bytes`)
    chunks.push(body)
  }
  const end = stream.end.bind(stream)
  stream.write = ((chunk: unknown, encoding?: BufferEncoding | (() => void), callback?: () => void) => {
    append(chunk, typeof encoding === 'string' ? encoding : undefined)
    if (typeof encoding === 'function') encoding()
    callback?.()
    return true
  }) as typeof stream.write
  stream.end = ((chunk?: unknown, encoding?: BufferEncoding | (() => void), callback?: () => void) => {
    if (chunk !== undefined) append(chunk, typeof encoding === 'string' ? encoding : undefined)
    ended = true
    if (typeof encoding === 'function') encoding()
    callback?.()
    return end()
  }) as typeof stream.end
  return { body: () => Buffer.concat(chunks), ended: () => ended }
}

function vendorRoot() {
  const configured = process.env.CORDIS_VENDOR_ROOT
  const candidates = [
    configured,
    resolve(process.cwd(), 'upstream/deepseek-harness/vendor'),
    resolve(process.cwd(), '../upstream/deepseek-harness/vendor'),
    resolve(process.cwd(), '../../../upstream/deepseek-harness/vendor'),
  ].filter((value): value is string => !!value)
  for (const candidate of candidates) {
    const root = basename(candidate) === 'cordis' ? resolve(candidate, '..') : resolve(candidate)
    if (existsSync(join(root, 'cordis', 'lib', 'index.js')) && existsSync(join(root, 'cosmokit', 'lib', 'index.js')) && existsSync(join(root, 'loader', 'lib', 'index.js'))) return root
  }
  throw new BridgeError('CORDIS_NOT_FOUND', 'set CORDIS_VENDOR_ROOT to the checked-out vendor directory or cordis package directory')
}

function installVendorResolver(vendor: string) {
  if (typeof Bun === 'undefined' || typeof Bun.plugin !== 'function') throw new BridgeError('BUN_REQUIRED', 'compat-host must run under Bun')
  const aliases: Record<string, string> = {
    '@deepseek-ai/cordis': join(vendor, 'cordis', 'lib', 'index.js'),
    '@deepseek-ai/cosmokit': join(vendor, 'cosmokit', 'lib', 'index.js'),
    '@deepseek-ai/cordis-plugin-loader': join(vendor, 'loader', 'lib', 'index.js'),
    cordis: join(vendor, 'cordis', 'lib', 'index.js'),
    cosmokit: join(vendor, 'cosmokit', 'lib', 'index.js'),
    '@cordisjs/plugin-loader': join(vendor, 'loader', 'lib', 'index.js'),
  }
  const hostRoot = process.env.TESSIVUM_HOST_MODULE_ROOT
  if (hostRoot) {
    const hostAliases: Record<string, string> = {
      '@deepseek-ai/dsh-settings': join(hostRoot, '@deepseek-ai', 'dsh-settings', 'lib', 'index.js'),
      '@deepseek-ai/schemastery': join(hostRoot, '@deepseek-ai', 'schemastery', 'lib', 'index.mjs'),
      '@deepseek-ai/dsh-tools': join(hostRoot, '@deepseek-ai', 'dsh-tools', 'index.js'),
      '@deepseek-ai/dsh-llm': join(hostRoot, '@deepseek-ai', 'dsh-llm', 'index.js'),
      '@deepseek-ai/dsh-subagent/descriptor': join(hostRoot, '@deepseek-ai', 'dsh-subagent', 'descriptor.js'),
    }
    for (const path of Object.values(hostAliases)) {
      if (!existsSync(path)) throw new BridgeError('HOST_MODULES_NOT_FOUND', `Host compatibility module is missing: ${path}`)
    }
    Object.assign(aliases, hostAliases)
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
  private readonly profile = profileFromEnvironment()
  private generation = 0n
  private phase: 'new' | 'ready' | 'closing' | 'closed' = 'new'
  private root: Context | undefined
  private loader: Loader | undefined
  private readonly plugins = new Map<string, PluginRecord>()
  private readonly registrations = new Map<string, Registration>()
  private readonly callbacks = new Map<string, (...args: any[]) => unknown>()
  private readonly incoming = new Map<string, Incoming>()
  private readonly outgoing = new Map<string, Outgoing>()
  private nextRequestId = 2n
  private readonly cancelledOutgoing = new Set<string>()
  private shutdownTask: Promise<void> | undefined
  private sequence = Promise.resolve()
  private readonly routes = new Map<string, Route>()
  private readonly routeTasks = new Set<Promise<unknown>>()
  private routeFailure: unknown
  private readonly pnpmOperations = new Map<string, PnpmOperation>()
  private nextRouteId = 1n
  private nextOperationId = 1n
  private readonly upgrades = new Map<string, UpgradeRoute>()
  private readonly upgradeToken = randomBytes(32).toString('hex')
  private upgradeServer: Bun.Server<UpgradeSocketData> | undefined
  private upgradePort: Promise<number> | undefined
  private settingsFiber: Fiber | undefined
  private nextToolId = 1n
  private readonly sessions = new Map<string, SessionSnapshot>()
  private readonly agents = new Map<string, AgentSnapshot>()
  private readonly agentQueues = new Map<string, Promise<void>>()
  private readonly sessionPreloads = new Map<string, Promise<void>>()
  private nextAgentId = 1n
  private activeInvokes = 0
  private loadingPlugin: string | undefined

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
    rawStdoutWrite(encodeFrame({ protocolVersion, connectionGeneration: this.generation, kind, ...(requestId === undefined ? {} : { requestId }), payload }, this.maxFrameBytes))
  }

  private log(payload: unknown) {
    try {
      this.write('log', json(payload))
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

  private trackRoute(task: Promise<unknown>) {
    this.routeTasks.add(task)
    void task.then(
      () => this.routeTasks.delete(task),
      error => {
        this.routeTasks.delete(task)
        this.routeFailure ??= error
      },
    )
    return task
  }

  private registerRoute(value: unknown) {
    const definition = object(value, 'route registration')
    const kind = text(definition.kind, 'route kind')
    if (kind !== 'exact' && kind !== 'prefix') throw new BridgeError('INVALID_ROUTE', 'route kind must be exact or prefix')
    const path = routePath(definition.path)
    if (typeof definition.handler !== 'function') throw new BridgeError('INVALID_ROUTE', 'route handler must be a function')
    if ([...this.routes.values()].some(route => !route.removed && route.kind === kind && route.path === path)) throw new BridgeError('DUPLICATE_ROUTE', `route ${kind} ${path} already exists`)
    const route: Route = {
      id: `${this.generation}:route:${this.nextRouteId++}`,
      kind,
      path,
      owner: this.loadingPlugin,
      handler: definition.handler as Route['handler'],
      registered: false,
      removed: false,
      pending: Promise.resolve(),
    }
    route.pending = this.trackRoute(this.requestRemote('web.route.register', { routeId: route.id, kind, path }).then(() => { route.registered = true }))
    void route.pending.catch(() => undefined)
    this.routes.set(route.id, route)
    return () => this.removeRoute(route)
  }

  private removeRoute(route: Route) {
    if (route.removed) return
    route.removed = true
    this.routes.delete(route.id)
    if (this.phase !== 'ready') return
    route.pending = this.trackRoute(route.pending.then(() => route.registered ? this.requestRemote('web.route.unregister', { routeId: route.id }) : undefined))
    void route.pending.catch(() => undefined)
  }

  private async removeRoutes(owner?: string) {
    for (const route of [...this.routes.values()]) if (owner === undefined || route.owner === owner) this.removeRoute(route)
    for (const route of [...this.upgrades.values()]) if (owner === undefined || route.owner === owner) this.removeUpgrade(route)
    await this.flushRoutes()
  }

  private async flushRoutes() {
    const failure = this.routeFailure
    this.routeFailure = undefined
    if (failure !== undefined) throw failure
    await Promise.all([...this.routeTasks])
    if (this.routeFailure !== undefined) {
      const error = this.routeFailure
      this.routeFailure = undefined
      throw error
    }
  }

  private startUpgradeServer() {
    if (this.upgradePort) return this.upgradePort
    const server = Bun.serve<UpgradeSocketData>({
      hostname: '127.0.0.1',
      port: 0,
      fetch: async (request, server) => {
        if (request.headers.get('x-tessivum-upgrade-token') !== this.upgradeToken) {
          return new Response(null, { status: 403 })
        }
        const url = new URL(request.url)
        const route = [...this.upgrades.values()].find(candidate => !candidate.removed && candidate.path === url.pathname)
        if (!route) return new Response(null, { status: 404 })
        await this.preloadSession(url.searchParams.toString(), Buffer.alloc(0))
        if (route.removed) return new Response(null, { status: 410 })

        let upgraded = false
        let destroyed = false
        const upgradeFacade = {
          upgrade: (inner: Request, options: any) => {
            upgraded = server.upgrade(inner, options)
            return upgraded
          },
        }
        const socket = {
          readable: true,
          writable: true,
          destroyed: false,
          server: { [bunInternals]: upgradeFacade },
          [bunInternals]: request,
          destroy() {
            destroyed = true
            return this
          },
        } as unknown as Duplex
        const nodeRequest = {
          method: request.method,
          url: `${url.pathname}${url.search}`,
          headers: Object.fromEntries(request.headers),
          socket,
        } as unknown as IncomingMessage
        route.handler(nodeRequest, socket, Buffer.alloc(0))
        return upgraded ? undefined : new Response(null, { status: destroyed ? 403 : 400 })
      },
      websocket: {
        open: socket => socket.data.open(socket),
        close: (socket, code, reason) => socket.data.close(socket, code, reason),
        message: (socket, message) => socket.data.message(socket, message),
        drain: socket => socket.data.drain(socket),
        ping: (socket, data) => socket.data.ping(socket, data),
        pong: (socket, data) => socket.data.pong(socket, data),
      },
    })
    const port = server.port
    if (port === undefined) {
      server.stop(true)
      throw new BridgeError('UPGRADE_LISTEN_FAILED', 'upgrade backend did not bind a TCP port')
    }
    const ready = Promise.resolve(port)
    this.upgradeServer = server
    this.upgradePort = ready
    return ready
  }

  private registerUpgrade(value: unknown) {
    const definition = object(value, 'upgrade registration')
    const path = routePath(definition.path)
    if (typeof definition.handler !== 'function') throw new BridgeError('INVALID_ROUTE', 'upgrade handler must be a function')
    if ([...this.upgrades.values()].some(route => !route.removed && route.path === path)) throw new BridgeError('DUPLICATE_ROUTE', `upgrade route ${path} already exists`)
    const route: UpgradeRoute = {
      id: `${this.generation}:upgrade:${this.nextRouteId++}`,
      path,
      owner: this.loadingPlugin,
      handler: definition.handler as UpgradeRoute['handler'],
      registered: false,
      removed: false,
      pending: Promise.resolve(),
    }
    route.pending = this.trackRoute(this.startUpgradeServer().then(port => this.requestRemote('web.upgrade.register', {
      routeId: route.id,
      path,
      port,
      token: this.upgradeToken,
    })).then(() => { route.registered = true }))
    void route.pending.catch(() => undefined)
    this.upgrades.set(route.id, route)
    return () => this.removeUpgrade(route)
  }

  private removeUpgrade(route: UpgradeRoute) {
    if (route.removed) return
    route.removed = true
    this.upgrades.delete(route.id)
    if (this.phase !== 'ready') return
    route.pending = this.trackRoute(route.pending.then(() => route.registered ? this.requestRemote('web.upgrade.unregister', { routeId: route.id }) : undefined))
    void route.pending.catch(() => undefined)
  }

  private async preloadSession(query: string, body: Buffer) {
    let sessionId = new URLSearchParams(query).get('sessionId') ?? undefined
    if (sessionId === undefined && body.byteLength > 0) {
      try {
        const payload = JSON.parse(body.toString('utf8')) as RecordValue
        sessionId = optionalText(payload.sessionId) ?? optionalText(payload.rootSessionId) ?? optionalText(payload.childId)
      } catch {
        return
      }
    }
    if (sessionId === undefined) return
    const active = this.sessionPreloads.get(sessionId)
    if (active !== undefined) return active
    const pending = this.loadSession(sessionId).finally(() => {
      if (this.sessionPreloads.get(sessionId) === pending) this.sessionPreloads.delete(sessionId)
    })
    this.sessionPreloads.set(sessionId, pending)
    await pending
  }

  private async loadSession(sessionId: string) {
    let sessionResult: RecordValue
    try {
      sessionResult = object(await this.requestRemote('service.call', {
        service: 'sessions@1',
        method: 'snapshot',
        params: { session: sessionId },
      }), 'session snapshot result')
    } catch (error) {
      if (error instanceof BridgeError && (error.details as RecordValue | undefined)?.code === 'SESSION_NOT_FOUND') return
      throw error
    }
    const agentResult = object(await this.requestRemote('service.call', {
      service: 'agents@1',
      method: 'inspectCompat',
      params: { session: sessionId },
    }), 'agent snapshot result')
    this.captureSession(object(sessionResult.session, 'session snapshot'))
    this.captureAgent(sessionId, agentResult)
  }

  private captureSession(value: RecordValue) {
    const events = value.events
    if (!Array.isArray(events)) throw new BridgeError('INVALID_SESSION', 'session snapshot events must be an array')
    const session: SessionSnapshot = {
      id: text(value.id, 'session snapshot id'),
      header: object(value.header, 'session snapshot header'),
      events,
    }
    this.sessions.set(session.id, session)
    if (this.sessions.size > 128) this.sessions.delete(this.sessions.keys().next().value!)
  }

  private captureAgent(sessionId: string, value: RecordValue) {
    const options = value.options === null || value.options === undefined ? {} : object(value.options, 'agent options')
    this.agents.set(sessionId, {
      live: value.live === true,
      ...(typeof value.status === 'string' ? { status: value.status } : {}),
      options,
    })
    if (value.session && typeof value.session === 'object' && !Array.isArray(value.session)) this.captureSession(value.session as RecordValue)
  }

  private queueAgent(sessionId: string, method: string, params: RecordValue) {
    const previous = this.agentQueues.get(sessionId) ?? Promise.resolve()
    const pending = previous.catch(() => undefined).then(async () => {
      await this.requestRemote('service.call', { service: 'agents@1', method, params })
    })
    this.agentQueues.set(sessionId, pending)
    void pending.catch(error => this.log({ level: 'error', message: ['compat agent operation failed', json(error)] }))
  }

  private agent(sessionId: string) {
    const session = this.sessions.get(sessionId)
    const state = this.agents.get(sessionId)
    if (session === undefined || state?.live !== true) return undefined
    const send = (message: unknown, target: 'followup' | 'steer' | 'inject', wakeup: boolean) => {
      this.queueAgent(sessionId, 'sendCompat', { session: sessionId, message, target, wakeup })
    }
    return {
      id: sessionId,
      session,
      options: state.options,
      status: state.status ?? 'idle',
      inbox: {},
      ctx: this.context(),
      send,
      followup: (message: unknown) => send(message, 'followup', true),
      steer: (message: unknown) => send(message, 'steer', true),
      inject: (message: unknown) => send(message, 'inject', false),
      cancel: (cause: unknown, options?: RecordValue) => {
        this.queueAgent(sessionId, 'cancelCompat', {
          session: sessionId,
          cause,
          keepInbox: options?.keepInbox === true,
        })
      },
      whenIdle: () => this.agentQueues.get(sessionId) ?? Promise.resolve(),
    }
  }

  private agentLabel(options: RecordValue) {
    const seed = Array.isArray(options.seed) ? options.seed : []
    for (let index = seed.length - 1; index >= 0; index--) {
      const event = seed[index]
      if (!event || typeof event !== 'object' || Array.isArray(event)) continue
      const value = event as RecordValue
      if (value.type !== 'subagent/descriptor' || !value.data || typeof value.data !== 'object' || Array.isArray(value.data)) continue
      const label = optionalText((value.data as RecordValue).label)
      if (label !== undefined) return label
    }
    return 'Side chat'
  }

  private async createAgent(value: unknown) {
    const options = object(value, 'agent create options')
    const meta = options.meta === undefined ? {} : object(options.meta, 'agent create metadata')
    const sessionId = text(options.sessionId, 'agent session id')
    const parentSession = text(meta.parentSession, 'agent parent session id')
    const agentOptions = object(options.agentOptions, 'agent options')
    const registrationId = `${this.generation}:agent:${this.nextAgentId++}`
    const result = object(await this.requestRemote('service.call', {
      service: 'agents@1',
      method: 'createCompat',
      params: {
        registrationId,
        parentSession,
        childSession: sessionId,
        ...(optionalText(meta.agentPreset) === undefined ? {} : { agentMode: meta.agentPreset }),
        label: this.agentLabel(options),
        options: agentOptions,
        createdAt: Date.now(),
      },
    }), 'agent create result')
    this.captureAgent(sessionId, result)
    const agent = this.agent(sessionId)
    if (agent === undefined) throw new BridgeError('AGENT_NOT_FOUND', 'created agent was not published')
    return {
      agent,
      dispose: async () => {
        await this.requestRemote('service.call', {
          service: 'agents@1',
          method: 'disposeCompat',
          params: { registrationId, session: sessionId },
        })
        this.agents.set(sessionId, { live: false, options: agentOptions })
      },
    }
  }

  private async resumeAgent(value: unknown) {
    const options = object(value, 'agent resume options')
    const sessionId = text(options.resumeSessionId, 'agent resume session id')
    const session = this.sessions.get(sessionId)
    if (session === undefined) throw new BridgeError('SESSION_NOT_FOUND', `session "${sessionId}" is unavailable`)
    const parentSession = text(session.header.parentSession, 'agent parent session id')
    const parent = object(await this.requestRemote('service.call', {
      service: 'agents@1',
      method: 'inspectCompat',
      params: { session: parentSession },
    }), 'parent agent snapshot')
    const agentOptions = object(parent.options, 'parent agent options')
    const registrationId = `${this.generation}:agent:${this.nextAgentId++}`
    const result = object(await this.requestRemote('service.call', {
      service: 'agents@1',
      method: 'resumeCompat',
      params: {
        registrationId,
        parentSession,
        childSession: sessionId,
        ...(optionalText(session.header.agentMode) === undefined ? {} : { agentMode: session.header.agentMode }),
        options: agentOptions,
        createdAt: typeof session.header.createdAt === 'number' ? session.header.createdAt : Date.now(),
      },
    }), 'agent resume result')
    this.captureAgent(sessionId, result)
    const agent = this.agent(sessionId)
    if (agent === undefined) throw new BridgeError('AGENT_NOT_FOUND', 'resumed agent was not published')
    return {
      agent,
      dispose: async () => {
        await this.requestRemote('service.call', {
          service: 'agents@1',
          method: 'disposeCompat',
          params: { registrationId, session: sessionId },
        })
        this.agents.set(sessionId, { live: false, options: agentOptions })
      },
    }
  }

  private registerTool(value: unknown) {
    const tool = object(value, 'tool definition')
    const name = text(tool.name, 'tool name')
    const description = text(tool.description, 'tool description')
    if (typeof tool.execute !== 'function') throw new BridgeError('INVALID_TOOL', `tool ${name} has no execute function`)
    const output = object(tool.output, 'tool output')
    if (typeof output.render !== 'function') throw new BridgeError('INVALID_TOOL', `tool ${name} has no output renderer`)
    const execute = tool.execute as (arguments_: unknown, context: RecordValue) => unknown
    const render = output.render as (arguments_: unknown, value: unknown) => unknown
    const id = this.nextToolId++
    const registrationId = `${this.generation}:tool:${id}`
    const callbackId = `${registrationId}:execute`
    this.callbacks.set(callbackId, async (payload, signal) => {
      const request = object(payload, 'tool callback')
      const context = object(request.context, 'tool callback context')
      const sessionId = text(context.session, 'tool callback session')
      const session = this.sessions.get(sessionId) ?? { id: sessionId, header: {}, events: [] }
      const value = await execute(request.arguments, {
        signal: signal ?? new AbortController().signal,
        agent: { session },
        call: context.call,
      })
      return { content: render(request.arguments, value), isError: false, meta: { value } }
    })
    let registered = false
    let disposed = false
    const remove = () => this.requestRemote('registration.dispose', { registrationId })
    const pending = this.requestRemote('service.provide', {
      service: 'tools@1',
      method: 'register',
      params: { registrationId, callbackId, name, description, parameters: tool.parameters },
    }).then(() => {
      registered = true
      if (disposed) return remove()
    })
    this.trackRoute(pending)
    return () => {
      if (disposed) return
      disposed = true
      this.callbacks.delete(callbackId)
      if (registered) this.trackRoute(remove())
    }
  }

  private installCompatibilityServices() {
    this.context().provide('webServer', {
      register: (definition: unknown) => this.registerRoute(definition),
      registerUpgrade: (definition: unknown) => this.registerUpgrade(definition),
    })
    this.context().provide('sessions', { get: (id: string) => this.sessions.get(id) })
    this.context().provide('agents', {
      get: (id: string) => this.agent(id),
      create: (options: unknown) => this.createAgent(options),
      resume: (options: unknown) => this.resumeAgent(options),
    })
    this.context().provide('webRuntime', { trustedHosts: Object.freeze([]) })
    this.context().provide('tools', { register: (tool: unknown) => this.registerTool(tool) })
    if (!this.profile) return
    this.context().provide('desktopProfiles', { current: { name: this.profile.name, dir: this.profile.dir } })
    this.context().provide('desktopPnpm', { runPlugin: (args: unknown, invokingDir: unknown, signal?: AbortSignal) => this.runPlugin(args, invokingDir, signal) })
  }

  private async ensureSettingsProvider() {
    if (this.settingsFiber) return this.settingsFiber.await()
    const root = process.env.TESSIVUM_HOST_MODULE_ROOT
    if (!root) return
    const target = join(root, '@deepseek-ai', 'dsh-settings', 'lib', 'index.js')
    const settingsModule = await import(pathToFileURL(target).href) as any
    const host = this
    class NativeSettingsProvider extends settingsModule.SettingsProvider {
      get writable() {
        return true
      }

      async load() {
        return object(await host.requestRemote('service.call', {
          service: 'settings@1',
          method: 'loadDocument',
          params: {},
        }), 'settings document')
      }

      async persist(namespace: string, value: unknown) {
        await host.requestRemote('service.call', {
          service: 'settings@1',
          method: 'persistUnregistered',
          params: { namespace, value },
        })
      }
    }
    const fiber = this.context().plugin(NativeSettingsProvider) as Fiber
    this.settingsFiber = fiber
    await fiber.await()
  }

  private async invokeRoute(payload: RecordValue, signal: AbortSignal) {
    const route = this.routes.get(text(payload.routeId, 'routeId'))
    if (!route || route.removed || !route.registered) throw new BridgeError('UNKNOWN_ROUTE', 'route is no longer active')
    const method = text(payload.method, 'method')
    if (!headerName.test(method)) throw new BridgeError('INVALID_ROUTE_REQUEST', 'method is invalid')
    const path = routePath(payload.path)
    const query = payload.query === undefined || payload.query === null ? '' : payload.query
    if (typeof query !== 'string') throw new BridgeError('INVALID_ROUTE_REQUEST', 'query must be a string')
    if (query.includes('\0') || query.includes('#')) throw new BridgeError('INVALID_ROUTE_REQUEST', 'query is invalid')
    const requestHeaders = headers(payload.headers)
    const body = frameBody(payload.bodyBase64, maxRouteRequestBytes, 'bodyBase64')
    await this.preloadSession(query, body)
    abortIfNeeded(signal)
    const request = Readable.from(body) as Readable & RecordValue
    request.method = method
    request.url = query ? `${path}?${query}` : path
    request.httpVersion = '1.1'
    request.rawHeaders = requestHeaders.flat()
    Object.defineProperty(request, 'socket', { enumerable: true, value: Object.freeze({ remoteAddress: '127.0.0.1' }) })
    request.headers = requestHeaders.reduce<Record<string, string>>((all, [name, value]) => {
      const key = name.toLowerCase()
      all[key] = all[key] === undefined ? value : `${all[key]}, ${value}`
      return all
    }, {})
    const response = new PassThrough() as PassThrough & RecordValue
    let statusCode = 200
    const output = responseOutput(response, maxRouteResponseBytes)
    Object.defineProperty(response, 'statusCode', {
      enumerable: true,
      get: () => statusCode,
      set: (value: unknown) => {
        if (output.ended()) throw new BridgeError('LATE_RESPONSE', 'response already ended')
        statusCode = value as number
      },
    })
    const responseHeaders = new Map<string, [string, string]>()
    response.setHeader = (name: string, value: unknown) => {
      if (output.ended()) throw new BridgeError('LATE_RESPONSE', 'response already ended')
      const pair = headers([[name, Array.isArray(value) ? value.map(String).join(', ') : String(value)]])[0]!
      responseHeaders.set(pair[0].toLowerCase(), pair)
      return response
    }
    response.getHeader = (name: string) => responseHeaders.get(name.toLowerCase())?.[1]
    response.getHeaderNames = () => [...responseHeaders.keys()]
    response.hasHeader = (name: string) => responseHeaders.has(name.toLowerCase())
    response.removeHeader = (name: string) => {
      if (output.ended()) throw new BridgeError('LATE_RESPONSE', 'response already ended')
      responseHeaders.delete(name.toLowerCase())
    }
    response.writeHead = (status: number, statusOrHeaders?: string | Record<string, unknown>, maybeHeaders?: Record<string, unknown>) => {
      if (output.ended()) throw new BridgeError('LATE_RESPONSE', 'response already ended')
      response.statusCode = status
      const values = typeof statusOrHeaders === 'object' ? statusOrHeaders : maybeHeaders
      if (values) for (const [name, value] of Object.entries(values)) (response.setHeader as (name: string, value: unknown) => unknown)(name, value)
      return response
    }
    await settled(route.handler(request, response))
    abortIfNeeded(signal)
    if (!output.ended()) throw new BridgeError('INCOMPLETE_RESPONSE', 'route handler returned without ending its response')
    if (!Number.isInteger(statusCode) || statusCode < 100 || statusCode > 599) throw new BridgeError('INVALID_RESPONSE', 'response status is invalid')
    return { status: statusCode, headers: [...responseHeaders.values()], bodyBase64: output.body().toString('base64') }
  }

  private async initialize() {
    if (this.root) return
    // The vendor root is selected at runtime, so these module URLs cannot be static imports.
    const cordis = await import(pathToFileURL(join(this.vendor, 'cordis', 'lib', 'index.js')).href) as { Context: new () => Context }
    const loaderModule = await import(pathToFileURL(join(this.vendor, 'loader', 'lib', 'index.js')).href) as { Loader: new (ctx: Context, config?: RecordValue) => Loader }
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
    this.installCompatibilityServices()
  }

  private dispatchInvoke(frame: Frame) {
    if (this.phase !== 'ready' || frame.connectionGeneration !== this.generation) return this.fault(frame, new BridgeError('GENERATION_MISMATCH', 'route invoke belongs to another connection generation'))
    if (frame.requestId === undefined || frame.requestId === 0n) return this.fault(frame, new BridgeError('INVALID_REQUEST_ID', 'web.route.request requires nonzero requestId'))
    const key = frame.requestId.toString()
    if (this.incoming.has(key)) return this.fault(frame, new BridgeError('DUPLICATE_REQUEST_ID', `request ${key} is already active`))
    if (this.activeInvokes >= 64) {
      this.respondError(frame, new BridgeError('QUEUE_FULL', 'too many active route invocations'))
      return
    }
    const request: Incoming = { controller: new AbortController(), settled: false }
    this.incoming.set(key, request)
    this.activeInvokes++
    void (async () => {
      try {
        const value = await this.invokeRoute(object(frame.payload), request.controller.signal)
        abortIfNeeded(request.controller.signal)
        if (!request.settled) {
          this.respond(frame, value)
          request.settled = true
        }
      } catch (error) {
        if (!request.settled) {
          this.respondError(frame, error)
          request.settled = true
        }
      } finally {
        if (this.incoming.get(key) === request) this.incoming.delete(key)
        this.activeInvokes--
      }
    })().catch(error => this.fatal(error))
  }

  receive(frame: Frame) {
    if (frame.kind === 'cancel') return this.cancel(frame)
    if (frame.kind === 'pnpm.output') return this.output(frame)
    if (frame.kind === 'web.route.request') return this.dispatchInvoke(frame)
    if (frame.kind === 'heartbeat') {
      if (this.phase !== 'ready' || frame.connectionGeneration !== this.generation) return this.fault(frame, new BridgeError('GENERATION_MISMATCH', 'heartbeat belongs to another connection generation'))
      if (frame.requestId !== undefined) return this.fault(frame, new BridgeError('INVALID_REQUEST_ID', 'heartbeat must not have requestId'))
      this.write('heartbeat', { ok: true })
      return
    }
    if (frame.kind === 'response' || frame.kind === 'error') {
      if (this.phase !== 'ready' || frame.connectionGeneration !== this.generation) {
        return this.fault(frame, new BridgeError('GENERATION_MISMATCH', 'response belongs to another connection generation'))
      }
      try {
        return this.resolveOutgoing(frame)
      } catch (error) {
        return this.fatal(error)
      }
    }
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
      this.write('ready', { protocolVersion, maxFrameBytes: this.maxFrameBytes, vendoredCordis: true, capabilities: ['web.route/v1', 'web.upgrade/v1'] })
      return
    }
    if (frame.connectionGeneration !== this.generation) return this.fault(frame, new BridgeError('GENERATION_MISMATCH', 'frame belongs to another connection generation'))
    if (this.phase !== 'ready') return
    if (frame.kind === 'hello' || frame.kind === 'ready') return this.fault(frame, new BridgeError('HANDSHAKE_COMPLETE', 'handshake already completed'))
    if (frame.kind === 'log') {
      if (frame.requestId !== undefined) return this.fault(frame, new BridgeError('INVALID_REQUEST_ID', 'log must not have requestId'))
      this.log(frame.payload)
      return
    }
    if (frame.kind === 'exit') {
      await this.shutdown()
      if (frame.requestId !== undefined) this.respond(frame, { drained: true })
      process.stdin.pause()
      process.exitCode = 0
      queueMicrotask(() => process.exit())
      return
    }
    if (frame.kind === 'pnpm.output') {
      if (frame.requestId !== undefined) return this.fault(frame, new BridgeError('INVALID_REQUEST_ID', 'pnpm.output must not carry requestId'))
      this.pnpmOutput(object(frame.payload))
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
    const key = frame.requestId.toString()
    const request = this.outgoing.get(key)
    if (!request) {
      if (this.cancelledOutgoing.delete(key)) return
      throw new BridgeError('UNKNOWN_REQUEST_ID', `unknown outgoing request ${frame.requestId}`)
    }
    this.outgoing.delete(key)
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
      this.cancelledOutgoing.add(key)
      outgoing.reject(new BridgeError('CANCELLED', 'remote request cancelled'))
    }
  }

  private output(frame: Frame) {
    if (this.phase !== 'ready' || frame.connectionGeneration !== this.generation) return this.fault(frame, new BridgeError('GENERATION_MISMATCH', 'pnpm output belongs to another connection generation'))
    if (frame.requestId !== undefined) return this.fault(frame, new BridgeError('INVALID_REQUEST_ID', 'pnpm.output must not carry requestId'))
    this.pnpmOutput(object(frame.payload))
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
      case 'web.route.request': return this.invokeRoute(object(frame.payload), signal)
      default: throw new BridgeError('UNKNOWN_KIND', `unknown message kind ${frame.kind}`)
    }
  }

  private context() {
    if (!this.root) throw new BridgeError('HANDSHAKE_REQUIRED', 'host is not initialized')
    return this.root
  }

  private exposeLoaderName(entry: LoaderEntry, name: string | undefined, target: string) {
    if (!name || name === target) return
    const update = entry.update.bind(entry)
    entry.update = async (options, create, force) => {
      entry.options.name = target
      try {
        await update(options, create, force)
      } finally {
        entry.options.name = name
      }
    }
    entry.options.name = name
  }

  private async loadPlugin(payload: RecordValue, signal: AbortSignal) {
    const id = text(payload.pluginId ?? payload.id, 'pluginId')
    if (this.plugins.has(id)) throw new BridgeError('DUPLICATE_PLUGIN', `plugin ${id} is already loaded`)
    const target = moduleTarget(payload)
    await this.ensureSettingsProvider()
    abortIfNeeded(signal)
    const module = await import(/* @vite-ignore */importTarget(target)) as RecordValue
    abortIfNeeded(signal)
    const chosen = exportName(payload)
    const plugin = chosen ? module[chosen] : module.default ?? module
    if (!plugin) throw new BridgeError('PLUGIN_EXPORT_NOT_FOUND', `plugin export ${chosen ?? 'default'} was not found`)
    const config = configOf(payload)
    const entry = payload.entry === undefined ? undefined : object(payload.entry, 'entry')
    const nested = entry?.options && typeof entry.options === 'object' && !Array.isArray(entry.options)
      ? entry.options as RecordValue
      : undefined
    const useLoader = payload.loader === true || entry !== undefined
    this.loadingPlugin = id
    try {
    try {
    if (useLoader) {
      const loader = this.loader
      if (!loader) throw new BridgeError('LOADER_UNAVAILABLE', 'loader is unavailable')
      const name = optionalText(entry?.name) ?? optionalText(nested?.name)
      const options: RecordValue = { id, name: target, ...(config === undefined ? {} : { config }) }
      const inject = entry && Object.hasOwn(entry, 'inject') ? entry.inject : nested?.inject
      if (inject !== undefined) options.inject = inject
      const entryId = await loader.create(options)
      try {
        abortIfNeeded(signal)
        const loaderEntry = loader.resolve(entryId)
        if (!loaderEntry.fiber) throw new BridgeError('PLUGIN_LOAD_FAILED', `loader did not create fiber for ${id}`)
        await loaderEntry.fiber.await()
        this.exposeLoaderName(loaderEntry, name, target)
        this.context().emit('internal/plugin', loaderEntry.fiber)
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
    } finally {
      this.loadingPlugin = undefined
    }
    await this.flushRoutes()
    return this.snapshot(id)
    } catch (error) {
      const active = this.plugins.get(id)
      if (active) await settled(active.entry ? this.loader!.remove(id) : active.fiber.dispose()).catch(() => undefined)
      this.plugins.delete(id)
      await this.removeRoutes(id).catch(() => undefined)
      throw error
    }
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
    await this.removeRoutes(id)
    this.plugins.delete(id)
    abortIfNeeded(signal)
    return { pluginId: id, disposed: true }
  }

  private snapshotPlugin(payload: RecordValue) {
    const id = optionalText(payload.pluginId ?? payload.id)
    if (!id && payload.loader === true) return {
      entries: [...this.loader!.entries()].map(entry => ({
        options: json(entry.options),
        ...this.fiberSnapshot(entry.fiber),
      })),
    }
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
    const result = Object.hasOwn(payload, 'payload')
      ? await settled(callback(payload.payload, signal))
      : await settled(callback(...args))
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

  private pnpmOutput(payload: RecordValue) {
    const operationId = text(payload.operationId, 'operationId')
    const operation = this.pnpmOperations.get(operationId)
    if (!operation) return
    const stream = text(payload.stream, 'stream')
    if (stream !== 'stdout' && stream !== 'stderr') throw new BridgeError('INVALID_PNPM_OUTPUT', 'stream must be stdout or stderr')
    const chunk = frameBody(payload.chunkBase64, maxPnpmOutputChunkBytes, 'chunkBase64')
    if (stream === 'stdout') operation.stdout.write(chunk)
    else operation.stderr.write(chunk)
  }

  private runPlugin(args: unknown, invokingDir: unknown, signal?: AbortSignal) {
    if (!this.profile) throw new BridgeError('PROFILE_UNAVAILABLE', 'desktopPnpm requires a configured profile')
    if (!Array.isArray(args) || !args.length || args.some(arg => typeof arg !== 'string' || !arg || arg.includes('\0'))) throw new BridgeError('INVALID_PNPM_ARGS', 'pnpm args must be non-empty text')
    const directory = text(invokingDir, 'invokingDir')
    if (!isAbsolute(directory) || directory.includes('\0')) throw new BridgeError('INVALID_PNPM_DIR', 'invokingDir must be absolute')
    const operationId = `${this.generation}:pnpm:${this.nextOperationId++}`
    const operation: PnpmOperation = {
      stdout: new PassThrough(),
      stderr: new PassThrough(),
    }
    this.pnpmOperations.set(operationId, operation)
    const remote = this.beginRemote('pnpm.run', { operationId, args, invokingDir: directory })
    let abort: (() => void) | undefined
    const finish = () => {
      if (abort) signal?.removeEventListener('abort', abort)
      operation.stdout.end()
      operation.stderr.end()
      this.pnpmOperations.delete(operationId)
    }
    const done = remote.promise.then(value => {
      const result = object(value, 'pnpm result')
      if ((result.exitCode !== null && !Number.isInteger(result.exitCode)) || (result.signal !== undefined && result.signal !== null && typeof result.signal !== 'string')) throw new BridgeError('INVALID_PNPM_RESULT', 'pnpm result is invalid')
      finish()
      return { exitCode: result.exitCode, signal: result.signal ?? null }
    }, error => {
      finish()
      throw error
    })
    const cancel = () => this.cancelRemote(remote.requestId)
    if (signal) {
      abort = cancel
      signal.addEventListener('abort', abort, { once: true })
      if (signal.aborted) cancel()
    }
    return { stdout: operation.stdout, stderr: operation.stderr, done, cancel }
  }

  private beginRemote(kind: string, payload: unknown): RemoteRequest {
    if (this.phase !== 'ready') return { requestId: 0n, promise: Promise.reject(new BridgeError('HOST_CLOSING', 'host is not ready')) }
    if (this.nextRequestId > maxNodeRequestId) return { requestId: 0n, promise: Promise.reject(new BridgeError('REQUEST_IDS_EXHAUSTED', 'bridge request ids are exhausted')) }
    const requestId = this.nextRequestId
    this.nextRequestId += 2n
    const promise = new Promise<unknown>((resolve, reject) => {
      this.outgoing.set(requestId.toString(), { resolve, reject })
      try {
        this.write(kind, json(payload), requestId)
      } catch (error) {
        this.outgoing.delete(requestId.toString())
        reject(error)
      }
    })
    return { requestId, promise }
  }

  private cancelRemote(requestId: bigint) {
    const pending = this.outgoing.get(requestId.toString())
    if (!pending) return false
    this.outgoing.delete(requestId.toString())
    this.cancelledOutgoing.add(requestId.toString())
    pending.reject(new BridgeError('CANCELLED', 'remote request cancelled'))
    try {
      this.write('cancel', { requestId }, requestId)
    } catch {
      // The local terminal result still wins when the peer is gone.
    }
    return true
  }

  private requestRemote(kind: string, payload: unknown) {
    return this.beginRemote(kind, payload).promise
  }

  private async shutdown() {
    if (this.shutdownTask) return this.shutdownTask
    this.phase = 'closing'
    this.shutdownTask = (async () => {
      for (const request of this.incoming.values()) request.controller.abort()
      for (const request of this.outgoing.values()) request.reject(new BridgeError('HOST_CLOSING', 'host is shutting down'))
      this.outgoing.clear()
      this.cancelledOutgoing.clear()
      this.routes.clear()
      this.upgrades.clear()
      for (const operation of this.pnpmOperations.values()) {
        operation.stdout.end()
        operation.stderr.end()
      }
      this.pnpmOperations.clear()
      const disposals = [
        ...[...this.plugins.entries()].map(([id, plugin]) => plugin.entry ? this.loader!.remove(id) : plugin.fiber.dispose()),
        ...[...this.registrations.values()].map(registration => settled(registration.dispose())),
      ]
      await Promise.allSettled(disposals)
      this.plugins.clear()
      this.registrations.clear()
      if (this.root) await Promise.allSettled([this.root.fiber.dispose()])
      if (this.upgradeServer) await this.upgradeServer.stop(true)
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
    const record = failure(error)
    this.log({ level: 'error', error: record })
    rawStderrWrite(`[tessivum compat-host] ${JSON.stringify(record)}\n`)
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
