import assert from 'node:assert/strict'
import test from 'node:test'

import { CompatHost } from './host.ts'
import { protocolVersion, type Frame } from './protocol.ts'

const { WebSocket, WebSocketServer } = await import(String('ws'))

type HostLifecycle = {
  readonly product: Readonly<{ name: string; command: string }>
  restart(): Promise<unknown>
}

type TestContext = {
  provide(name: string, value: unknown): () => void
  get(name: string): unknown
  fiber: { dispose(): Promise<void> }
}

function host() {
  const value = Object.create(CompatHost.prototype) as any
  Object.assign(value, {
    generation: 5n,
    phase: 'ready',
    routes: new Map(),
    routeTasks: new Set(),
    upgrades: new Map(),
    upgradeToken: 'a'.repeat(64),
    routeFailure: undefined,
    pnpmOperations: new Map(),
    outgoing: new Map(),
    cancelledOutgoing: new Set(),
    incoming: new Map(),
    activeInvokes: 0,
    nextRequestId: 2n,
    callbacks: new Map(),
    sessions: new Map(),
    agents: new Map(),
    agentQueues: new Map(),
    sessionPreloads: new Map(),
    nextAgentId: 1n,
    root: {},
    nextRouteId: 1n,
    nextToolId: 1n,
    nextOperationId: 1n,
    profile: { name: 'web', dir: process.cwd() },
    plugins: new Map(),
    registrations: new Map(),
    write: () => undefined,
  })
  return value
}

function context(): TestContext {
  const services = new Map<string, unknown>()
  const disposers = new Set<() => void>()
  return {
    provide(name: string, value: unknown) {
      services.set(name, value)
      const dispose = () => {
        if (services.get(name) === value) services.delete(name)
        disposers.delete(dispose)
      }
      disposers.add(dispose)
      return dispose
    },
    get(name: string) {
      return services.get(name)
    },
    fiber: {
      async dispose() {
        for (const dispose of [...disposers]) dispose()
      },
    },
  }
}

async function withHostLifecycle<T>(value: string | undefined, callback: () => T | Promise<T>) {
  const previous = process.env.TESSIVUM_HOST_LIFECYCLE
  if (value === undefined) delete process.env.TESSIVUM_HOST_LIFECYCLE
  else process.env.TESSIVUM_HOST_LIFECYCLE = value
  try {
    return await callback()
  } finally {
    if (previous === undefined) delete process.env.TESSIVUM_HOST_LIFECYCLE
    else process.env.TESSIVUM_HOST_LIFECYCLE = previous
  }
}

function hostLifecycle(root: TestContext): HostLifecycle {
  const service = root.get('hostLifecycle')
  if (!service || typeof service !== 'object' || !('product' in service) || !('restart' in service) || typeof service.restart !== 'function') {
    throw new Error('host lifecycle was not provided')
  }
  return service as HostLifecycle
}

test('host lifecycle is absent unless precisely enabled', async () => {
  for (const flag of [undefined, '', '0', 'true']) {
    await withHostLifecycle(flag, () => {
      const value = host()
      const root = context()
      value.root = root
      value.installCompatibilityServices()
      assert.equal(root.get('hostLifecycle'), undefined)
    })
  }
})

test('host lifecycle forwards its fixed restart request', async () => {
  await withHostLifecycle('1', async () => {
    const value = host()
    const root = context()
    const success = Object.freeze({ accepted: true })
    const calls: [string, unknown][] = []
    value.root = root
    value.requestRemote = (kind: string, payload: unknown) => {
      calls.push([kind, payload])
      return Promise.resolve(success)
    }
    value.installCompatibilityServices()

    const lifecycle = hostLifecycle(root)
    assert.equal(Object.isFrozen(lifecycle.product), true)
    assert.deepEqual(lifecycle.product, { name: 'Tessivum', command: 'tessivum web' })
    assert.equal(await lifecycle.restart(), success)
    assert.deepEqual(calls, [['service.call', { service: 'hostLifecycle@1', method: 'restart', params: {} }]])
  })
})

test('host lifecycle preserves structured remote errors', async () => {
  await withHostLifecycle('1', async () => {
    const value = host()
    const root = context()
    value.root = root
    value.installCompatibilityServices()
    const lifecycle = hostLifecycle(root)
    for (const remote of [
      Object.freeze({ code: 'HOST_BUSY', message: 'restart already in progress' }),
      Object.freeze({ code: 'SERVICE_UNAVAILABLE', message: 'host lifecycle is unavailable' }),
    ]) {
      value.requestRemote = () => Promise.reject(remote)
      await assert.rejects(lifecycle.restart(), error => error === remote)
    }
  })
})

test('host lifecycle is disposed with its owning context', async () => {
  await withHostLifecycle('1', async () => {
    const value = host()
    const root = context()
    value.root = root
    value.installCompatibilityServices()
    const lifecycle = hostLifecycle(root)

    await value.shutdown()

    assert.equal(root.get('hostLifecycle'), undefined)
    await assert.rejects(lifecycle.restart(), error => typeof error === 'object' && error !== null && 'code' in error && error.code === 'HOST_CLOSING')
  })
})

test('canonical service calls dispatch object and permitted positional params', async () => {
  const value = host()
  const root = context()
  const params = Object.freeze({ id: 'tool-1' })
  const legacyParams = Object.freeze(['legacy-1', 'details'])
  let objectArgs: unknown[] | undefined
  root.provide('tools', {
    register(...args: unknown[]) {
      objectArgs = args
      return { accepted: true }
    },
  })
  root.provide('sessions', { get: (...args: unknown[]) => args })
  root.provide('legacy.function', { inspect: (...args: unknown[]) => args })
  value.root = root

  assert.deepEqual(await value.operation({ protocolVersion, connectionGeneration: 5n, kind: 'service.call', requestId: 1n, payload: { service: 'tools@1', method: 'register', params } } as Frame, new AbortController().signal), { accepted: true })
  assert.deepEqual(objectArgs, [params])
  assert.deepEqual(await value.operation({ protocolVersion, connectionGeneration: 5n, kind: 'service.call', requestId: 3n, payload: { service: 'sessions@1', method: 'get', params: ['session-1'] } } as Frame, new AbortController().signal), ['session-1'])
  assert.deepEqual(await value.operation({ protocolVersion, connectionGeneration: 5n, kind: 'service.call', requestId: 5n, payload: { service: 'legacy.function', method: 'inspect', params: legacyParams } } as Frame, new AbortController().signal), legacyParams)
  await assert.rejects(
    value.operation({ protocolVersion, connectionGeneration: 5n, kind: 'service.call', requestId: 7n, payload: { service: 'tools@1', method: 'register', params: ['tool-1'] } } as Frame, new AbortController().signal),
    error => typeof error === 'object' && error !== null && 'code' in error && error.code === 'INVALID_PAYLOAD',
  )
})

test('remote service calls pack canonical args and retain request correlation', async () => {
  const value = host()
  const writes: [string, unknown, bigint | undefined][] = []
  value.write = (kind: string, payload: unknown, requestId?: bigint) => writes.push([kind, payload, requestId])
  const params = { name: 'sidebar_open' }
  const remote = value.remoteService({ service: 'tools@1' })
  const zero = remote.flush()
  const object = remote.register(params)
  const multiple = remote.update('sidebar_open', { source: 'test' })

  assert.deepEqual(writes, [
    ['service.call', { service: 'tools@1', method: 'flush', params: {} }, 2n],
    ['service.call', { service: 'tools@1', method: 'register', params }, 4n],
    ['service.call', { service: 'tools@1', method: 'update', params: ['sidebar_open', { source: 'test' }] }, 6n],
  ])
  value.resolveOutgoing({ protocolVersion, connectionGeneration: 5n, kind: 'response', requestId: 2n, payload: { flushed: true } } as Frame)
  value.resolveOutgoing({ protocolVersion, connectionGeneration: 5n, kind: 'response', requestId: 4n, payload: { registrationId: 'tool-1' } } as Frame)
  value.resolveOutgoing({ protocolVersion, connectionGeneration: 5n, kind: 'response', requestId: 6n, payload: { updated: true } } as Frame)
  assert.deepEqual(await zero, { flushed: true })
  assert.deepEqual(await object, { registrationId: 'tool-1' })
  assert.deepEqual(await multiple, { updated: true })
  assert.equal(value.outgoing.size, 0)
})

test('route registration flushes registration then removal', async () => {
  const value = host()
  const calls: [string, unknown][] = []
  value.requestRemote = (kind: string, payload: unknown) => {
    calls.push([kind, payload])
    return Promise.resolve({})
  }
  const dispose = value.registerRoute({ kind: 'exact', path: '/dream-skin/api', handler: () => undefined })
  await value.flushRoutes()
  dispose()
  await value.flushRoutes()
  assert.deepEqual(calls.map(([kind]) => kind), ['web.route.register', 'web.route.unregister'])
})

test('route registration rejects unsupported namespaces', () => {
  const value = host()
  assert.throws(() => value.registerRoute({ kind: 'prefix', path: '/', handler: () => undefined }), /supported compatibility roots/)
  assert.throws(() => value.registerRoute({ kind: 'prefix', path: '/api/plugin', handler: () => undefined }), /supported compatibility roots/)
  assert.throws(() => value.registerRoute({ kind: 'prefix', path: '/dream-skin-evil', handler: () => undefined }), /supported compatibility roots/)
})

test('upgrade registration publishes and removes its loopback backend', async () => {
  const value = host()
  const calls: [string, any][] = []
  value.startUpgradeServer = () => Promise.resolve(43123)
  value.requestRemote = (kind: string, payload: unknown) => {
    calls.push([kind, payload])
    return Promise.resolve({})
  }
  const dispose = value.registerUpgrade({ path: '/sidebar/ws/test', handler: () => undefined })
  await value.flushRoutes()
  assert.equal(calls[0]?.[0], 'web.upgrade.register')
  assert.equal(calls[0]?.[1].port, 43123)
  assert.equal(calls[0]?.[1].token, 'a'.repeat(64))
  dispose()
  await value.flushRoutes()
  assert.equal(calls[1]?.[0], 'web.upgrade.unregister')
})
test('upgrade backend carries ws noServer connections', async () => {
  const value = host()
  value.preloadSession = () => Promise.resolve()
  const wss = new WebSocketServer({ noServer: true })
  value.upgrades.set('test', {
    id: 'test', path: '/sidebar/ws/test', registered: true, removed: false, pending: Promise.resolve(),
    handler: (request: any, socket: any, head: Buffer) => {
      wss.handleUpgrade(request, socket, head, (client: any) => client.send('ready'))
    },
  })
  const port = await value.startUpgradeServer()
  const client = new WebSocket(`ws://127.0.0.1:${port}/sidebar/ws/test`, {
    headers: { 'x-tessivum-upgrade-token': 'a'.repeat(64) },
  })

  const message = await new Promise<string>((resolve, reject) => {
    client.once('message', (data: any) => resolve(data.toString()))
    client.once('error', reject)
  })
  assert.equal(message, 'ready')
  client.close()
  value.upgradeServer.stop(true)
})

test('tool facade registers an executable native callback', async () => {
  const value = host()
  const calls: [string, any][] = []
  value.requestRemote = (kind: string, payload: unknown) => {
    calls.push([kind, payload])
    return Promise.resolve({})
  }
  const dispose = value.registerTool({
    name: 'sidebar_open',
    description: 'Open the sidebar',
    parameters: { type: 'object', properties: {} },
    output: { render: (_args: unknown, result: unknown) => [{ type: 'text', text: String(result) }] },
    execute: async () => 'opened',
  })
  await value.flushRoutes()
  const registration = calls[0]?.[1].params
  const callback = value.callbacks.get(registration.callbackId)
  assert.deepEqual(await callback({ context: { session: 's', call: 'c' }, arguments: {} }, new AbortController().signal), {
    content: [{ type: 'text', text: 'opened' }],
    isError: false,
    meta: { value: 'opened' },
  })
  dispose()
  await value.flushRoutes()
  assert.equal(calls[1]?.[0], 'registration.dispose')
})

test('nested callback responses bypass the serialized plugin request', async () => {
  const value = host()
  const responses: bigint[] = []
  value.sequence = Promise.resolve()
  value.respond = (frame: Frame) => responses.push(frame.requestId!)
  value.operation = async () => value.beginRemote('web.route.register', { routeId: 'route' }).promise
  value.receive({ protocolVersion, connectionGeneration: 5n, kind: 'plugin.load', requestId: 1n, payload: {} } as Frame)
  await Promise.resolve()
  await Promise.resolve()
  value.receive({ protocolVersion, connectionGeneration: 5n, kind: 'response', requestId: 2n, payload: {} } as Frame)
  await Promise.resolve()
  await Promise.resolve()
  assert.deepEqual(responses, [1n])
})

test('heartbeats bypass a pending serialized plugin operation', async () => {
  const value = host()
  const { promise, resolve } = Promise.withResolvers<unknown>()
  const writes: string[] = []
  value.sequence = Promise.resolve()
  value.operation = () => promise
  value.write = (kind: string) => writes.push(kind)
  value.receive({ protocolVersion, connectionGeneration: 5n, kind: 'plugin.load', requestId: 1n, payload: {} } as Frame)
  await Promise.resolve()
  value.receive({ protocolVersion, connectionGeneration: 5n, kind: 'heartbeat', payload: {} } as Frame)
  assert.deepEqual(writes, ['heartbeat'])
  resolve({})
  await value.sequence
})


test('route invoke adapts a bounded request and completed response', async () => {
  const value = host()
  value.routes.set('5:route:1', {
    id: '5:route:1', kind: 'exact', path: '/dsh-market/test', registered: true, removed: false, pending: Promise.resolve(),
    handler: (request: any, response: any) => {
      assert.equal(request.method, 'POST')
      assert.equal(request.url, '/dsh-market/test')
      assert.equal(request.socket.remoteAddress, '127.0.0.1')
      response.writeHead(201, { 'x-result': 'yes' })
      response.end('x'.repeat(20_000))
    },
  })
  const result = await value.invokeRoute({ routeId: '5:route:1', method: 'POST', path: '/dsh-market/test', query: '', headers: [], bodyBase64: '' }, new AbortController().signal)
  assert.equal(result.status, 201)
  assert.deepEqual(result.headers, [['x-result', 'yes']])
  assert.equal(Buffer.from(result.bodyBase64, 'base64').byteLength, 20_000)
})
test('pnpm output streams beyond the consumer rolling-tail size and cancel is correlated', async () => {
  const value = host()
  const frames: [string, bigint | undefined][] = []
  value.write = (kind: string, _payload: unknown, requestId?: bigint) => frames.push([kind, requestId])
  const handle = value.runPlugin(['install', '--ignore-scripts'], process.cwd())
  let bytes = 0
  handle.stdout.on('data', (chunk: Buffer) => { bytes += chunk.byteLength })
  const chunkBase64 = Buffer.alloc(16 * 1024, 'x').toString('base64')
  for (let index = 0; index < 20; index += 1) {
    value.output({ protocolVersion, connectionGeneration: 5n, kind: 'pnpm.output', payload: { operationId: '5:pnpm:1', stream: 'stdout', chunkBase64 } } as Frame)
  }
  assert.equal(bytes, 320 * 1024)
  assert.equal(handle.cancel(), true)
  await assert.rejects(handle.done)
  assert.doesNotThrow(() => value.output({ protocolVersion, connectionGeneration: 5n, kind: 'pnpm.output', payload: { operationId: '5:pnpm:1', stream: 'stdout', chunkBase64: 'b2s=' } } as Frame))
  assert.deepEqual(frames, [['pnpm.run', 2n], ['cancel', 2n]])
})

test('a second route invoke completes while the first is pending', async () => {
  const value = host()
  let release!: () => void
  const pending = new Promise<void>(resolve => { release = resolve })
  const responses: bigint[] = []
  value.invokeRoute = async (payload: Record<string, unknown>) => {
    if (payload.routeId === 'install') await pending
    return { status: 200, headers: [], bodyBase64: '' }
  }
  value.respond = (frame: Frame) => responses.push(frame.requestId!)
  value.receive({ protocolVersion, connectionGeneration: 5n, kind: 'web.route.request', requestId: 1n, payload: { routeId: 'install' } } as Frame)
  value.receive({ protocolVersion, connectionGeneration: 5n, kind: 'web.route.request', requestId: 3n, payload: { routeId: 'cancel' } } as Frame)
  await Promise.resolve()
  await Promise.resolve()
  assert.deepEqual(responses, [3n])
  release()
  await Promise.resolve()
  await Promise.resolve()
  assert.deepEqual(responses, [3n, 1n])
})

test('a nested pnpm run cancels on a concurrent route request', async () => {
  const value = host()
  const frames: [string, bigint | undefined][] = []
  const responses: bigint[] = []
  const errors: bigint[] = []
  let rejectObserved!: () => void
  const rejectedRoute = new Promise<void>(resolve => { rejectObserved = resolve })
  let handle!: { done: Promise<unknown>; cancel(): boolean }
  value.write = (kind: string, _payload: unknown, requestId?: bigint) => frames.push([kind, requestId])
  value.respond = (frame: Frame) => responses.push(frame.requestId!)
  value.respondError = (frame: Frame) => {
    errors.push(frame.requestId!)
    rejectObserved()
  }
  value.invokeRoute = async (payload: Record<string, unknown>) => {
    if (payload.routeId === 'install') {
      handle = value.runPlugin(['install', '--ignore-scripts'], process.cwd())
      await handle.done
    } else {
      handle.cancel()
    }
    return { status: 200, headers: [], bodyBase64: '' }
  }
  value.receive({ protocolVersion, connectionGeneration: 5n, kind: 'web.route.request', requestId: 1n, payload: { routeId: 'install' } } as Frame)
  await Promise.resolve()
  value.receive({ protocolVersion, connectionGeneration: 5n, kind: 'web.route.request', requestId: 3n, payload: { routeId: 'cancel' } } as Frame)
  await rejectedRoute
  assert.deepEqual(frames, [['pnpm.run', 2n], ['cancel', 2n]])
  assert.deepEqual(responses, [3n])
  assert.deepEqual(errors, [1n])
})

test('pnpm completion permits a signal-only exit', async () => {
  const value = host()
  const handle = value.runPlugin(['install', '--ignore-scripts'], process.cwd())
  value.resolveOutgoing({ protocolVersion, connectionGeneration: 5n, kind: 'response', requestId: 2n, payload: { exitCode: null, signal: 'SIGTERM' } } as Frame)
  assert.deepEqual(await handle.done, { exitCode: null, signal: 'SIGTERM' })
})

test('compat agent creation forwards the exact supplied seed', async () => {
  const value = host()
  const seed = [{ type: 'subagent/descriptor', seq: 0, time: 7, data: { label: 'Pinned side chat' } }]
  let params: Record<string, unknown> | undefined
  value.requestRemote = (_kind: string, payload: any) => {
    params = payload.params
    return Promise.resolve({
      sessionId: 'child',
      live: true,
      status: 'idle',
      options: { provider: 'mock', model: 'mock' },
      session: { id: 'child', header: { parentSession: 'parent' }, events: seed },
    })
  }

  await value.createAgent({
    sessionId: 'child',
    meta: { parentSession: 'parent' },
    seed,
    agentOptions: { provider: 'mock', model: 'mock' },
  })

  assert.deepEqual(params?.seed, seed)
  assert.equal(params?.label, 'Pinned side chat')
})
