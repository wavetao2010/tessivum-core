import assert from 'node:assert/strict'
import test from 'node:test'

import { CompatHost } from './host.ts'
import { protocolVersion, type Frame } from './protocol.ts'

function host() {
  const value = Object.create(CompatHost.prototype) as any
  Object.assign(value, {
    generation: 5n,
    phase: 'ready',
    routes: new Map(),
    routeTasks: new Set(),
    routeFailure: undefined,
    pnpmOperations: new Map(),
    outgoing: new Map(),
    cancelledOutgoing: new Set(),
    incoming: new Map(),
    activeInvokes: 0,
    nextRequestId: 2n,
    nextRouteId: 1n,
    nextOperationId: 1n,
    profile: { name: 'web', dir: process.cwd() },
    write: () => undefined,
  })
  return value
}

test('route registration flushes registration then removal', async () => {
  const value = host()
  const calls: [string, unknown][] = []
  value.requestRemote = (kind: string, payload: unknown) => {
    calls.push([kind, payload])
    return Promise.resolve({})
  }
  const dispose = value.registerRoute({ kind: 'exact', path: '/dsh-market/test', handler: () => undefined })
  await value.flushRoutes()
  dispose()
  await value.flushRoutes()
  assert.deepEqual(calls.map(([kind]) => kind), ['web.route.register', 'web.route.unregister'])
})

test('route invoke adapts a bounded request and completed response', async () => {
  const value = host()
  value.routes.set('5:route:1', {
    id: '5:route:1', kind: 'exact', path: '/dsh-market/test', registered: true, removed: false, pending: Promise.resolve(),
    handler: (request: any, response: any) => {
      assert.equal(request.method, 'POST')
      assert.equal(request.url, '/dsh-market/test?a=1')
      assert.equal(request.socket.remoteAddress, '127.0.0.1')
      response.writeHead(201, { 'x-result': 'yes' })
      response.end('ok')
    },
  })
  const result = await value.invokeRoute({ routeId: '5:route:1', method: 'POST', path: '/dsh-market/test', query: 'a=1', headers: [], bodyBase64: '' }, new AbortController().signal)
  assert.deepEqual(result, { status: 201, headers: [['x-result', 'yes']], bodyBase64: 'b2s=' })
})
test('pnpm output streams before completion and cancel is correlated', async () => {
  const value = host()
  const frames: [string, bigint | undefined][] = []
  value.write = (kind: string, _payload: unknown, requestId?: bigint) => frames.push([kind, requestId])
  const handle = value.runPlugin(['install', '--ignore-scripts'], process.cwd())
  value.output({ protocolVersion, connectionGeneration: 5n, kind: 'pnpm.output', payload: { operationId: '5:pnpm:1', stream: 'stdout', chunkBase64: 'b2s=' } } as Frame)
  assert.equal(handle.stdout.read().toString(), 'ok')
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
