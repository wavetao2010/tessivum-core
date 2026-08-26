import assert from 'node:assert/strict'
import test from 'node:test'

import { encodeFrame, parseFrame, protocolVersion } from './protocol.ts'

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
