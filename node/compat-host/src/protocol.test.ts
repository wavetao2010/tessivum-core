import assert from 'node:assert/strict'
import test from 'node:test'

import { encodeFrame, parseFrame, parseServiceCall, plannedServiceCapabilities, protocolVersion } from './protocol.ts'

test('extension frame names have exact request correlation', () => {
  for (const kind of ['web.route.register', 'web.route.unregister', 'web.route.request', 'pnpm.run'] as const) {
    const frame = parseFrame(`{"protocolVersion":"${protocolVersion}","connectionGeneration":7,"kind":"${kind}","requestId":9,"payload":{}}`)
    assert.equal(frame.kind, kind)
    assert.equal(frame.requestId, 9n)
    assert.equal(JSON.parse(encodeFrame(frame).subarray(4).toString()).kind, kind)
  }
})

test('pnpm output is an uncorrelated notification', () => {
  const frame = parseFrame(`{"protocolVersion":"${protocolVersion}","connectionGeneration":7,"kind":"pnpm.output","payload":{"operationId":"7:pnpm:1","stream":"stdout","chunkBase64":""}}`)
  assert.equal(frame.requestId, undefined)
  assert.throws(() => parseFrame(`{"protocolVersion":"${protocolVersion}","connectionGeneration":7,"kind":"pnpm.output","requestId":9,"payload":{}}`))
})

test('canonical service call DTO round-trips through a frame', () => {
  const payload = { service: 'sessions@1', method: 'snapshot', params: { session: 'session-1' } }
  assert.deepEqual(parseServiceCall(payload), payload)
  const frame = parseFrame(JSON.stringify({ protocolVersion, connectionGeneration: 7, kind: 'service.call', requestId: 9, payload }))
  assert.deepEqual(frame.payload, payload)
  assert.deepEqual(JSON.parse(encodeFrame(frame).subarray(4).toString()).payload, payload)
})

test('service call DTO rejects unknown, legacy, and invalid fields', () => {
  const base = { service: 'sessions@1', method: 'snapshot', params: {} }
  for (const payload of [
    { ...base, extra: true },
    { ...base, args: [] },
    { ...base, name: 'sessions' },
    { ...base, serviceId: 'sessions@1' },
    { ...base, service: '' },
    { ...base, service: 'sessions' },
    { ...base, method: '' },
    { ...base, method: 'not.valid' },
    { ...base, params: null },
  ]) {
    assert.throws(() => parseServiceCall(payload))
    assert.throws(() => parseFrame(JSON.stringify({ protocolVersion, connectionGeneration: 7, kind: 'service.call', requestId: 9, payload })))
  }
})

test('planned service capabilities retain their deterministic identifiers', () => {
  assert.deepEqual(plannedServiceCapabilities, [
    'sessions@1', 'workspaces@1', 'agentModes@1', 'models@1', 'hostSettings@1',
    'commands@1', 'hostEvents@1', 'webListener@1', 'remoteAccess@1',
  ])
})
