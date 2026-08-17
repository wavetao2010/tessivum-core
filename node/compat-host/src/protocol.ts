import { Buffer } from 'node:buffer'

export const protocolVersion = 'cordis.node/v1' as const
export const defaultMaxFrameBytes = 1024 * 1024
const maxU64 = (1n << 64n) - 1n

export interface Frame {
  protocolVersion: typeof protocolVersion
  connectionGeneration: bigint
  kind: string
  requestId?: bigint
  payload: unknown
}

export class ProtocolError extends Error {
  constructor(readonly code: string, message: string) {
    super(message)
    this.name = 'ProtocolError'
  }
}

function record(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && !Array.isArray(value)
}

function whitespace(source: string, index: number) {
  while (index < source.length && /[\t\n\r ]/.test(source[index]!)) index++
  return index
}

function stringEnd(source: string, index: number) {
  if (source[index] !== '"') throw new ProtocolError('INVALID_FRAME', 'invalid JSON object key')
  for (index++; index < source.length; index++) {
    if (source[index] === '\\') index++
    else if (source[index] === '"') return index + 1
  }
  throw new ProtocolError('INVALID_FRAME', 'unterminated JSON string')
}

function valueEnd(source: string, index: number) {
  index = whitespace(source, index)
  const first = source[index]
  if (first === '"') return stringEnd(source, index)
  if (first === '{' || first === '[') {
    const closes: string[] = []
    for (; index < source.length; index++) {
      const char = source[index]!
      if (char === '"') index = stringEnd(source, index) - 1
      else if (char === '{') closes.push('}')
      else if (char === '[') closes.push(']')
      else if (char === '}' || char === ']') {
        if (closes.pop() !== char) throw new ProtocolError('INVALID_FRAME', 'unbalanced JSON value')
        if (!closes.length) return index + 1
      }
    }
    throw new ProtocolError('INVALID_FRAME', 'unterminated JSON value')
  }
  while (index < source.length && !/[\t\n\r ,}\]]/.test(source[index]!)) index++
  return index
}

/** Extract a root-level unsigned integer without losing JSON's u64 precision to Number. */
function rootInteger(source: string, sought: string) {
  let index = whitespace(source, 0)
  if (source[index++] !== '{') throw new ProtocolError('INVALID_FRAME', 'frame must be an object')
  let result: string | undefined
  while (true) {
    index = whitespace(source, index)
    if (source[index] === '}') return result
    const keyStart = index
    const keyEnd = stringEnd(source, index)
    let key: unknown
    try {
      key = JSON.parse(source.slice(keyStart, keyEnd))
    } catch {
      throw new ProtocolError('INVALID_FRAME', 'invalid JSON object key')
    }
    index = whitespace(source, keyEnd)
    if (source[index++] !== ':') throw new ProtocolError('INVALID_FRAME', 'invalid JSON object')
    index = whitespace(source, index)
    const start = index
    const end = valueEnd(source, index)
    if (key === sought) {
      if (result !== undefined) throw new ProtocolError('INVALID_FRAME', `duplicate ${sought}`)
      result = source.slice(start, end)
    }
    index = whitespace(source, end)
    if (source[index] === '}') return result
    if (source[index++] !== ',') throw new ProtocolError('INVALID_FRAME', 'invalid JSON object')
  }
}

function u64(source: string | undefined, field: string, required: boolean) {
  if (source === undefined) {
    if (required) throw new ProtocolError('INVALID_FRAME', `missing ${field}`)
    return undefined
  }
  if (!/^(?:0|[1-9]\d{0,19})$/.test(source)) throw new ProtocolError('INVALID_FRAME', `${field} must be an unsigned 64-bit integer`)
  const value = BigInt(source)
  if (value > maxU64) throw new ProtocolError('INVALID_FRAME', `${field} exceeds u64`)
  return value
}

export function parseFrame(source: string): Frame {
  let raw: unknown
  try {
    raw = JSON.parse(source)
  } catch {
    throw new ProtocolError('INVALID_JSON', 'frame body is not valid JSON')
  }
  if (!record(raw)) throw new ProtocolError('INVALID_FRAME', 'frame must be an object')
  const allowed = new Set(['protocolVersion', 'connectionGeneration', 'kind', 'requestId', 'payload'])
  if (Object.keys(raw).some(key => !allowed.has(key))) throw new ProtocolError('INVALID_FRAME', 'frame contains an unknown field')
  if (raw.protocolVersion !== protocolVersion) throw new ProtocolError('PROTOCOL_VERSION', `expected ${protocolVersion}`)
  if (typeof raw.kind !== 'string' || !raw.kind) throw new ProtocolError('INVALID_FRAME', 'kind must be a non-empty string')
  if (!Object.hasOwn(raw, 'payload')) throw new ProtocolError('INVALID_FRAME', 'missing payload')
  const generation = u64(rootInteger(source, 'connectionGeneration'), 'connectionGeneration', true)!
  const requestId = u64(rootInteger(source, 'requestId'), 'requestId', false)
  if (Object.hasOwn(raw, 'requestId') && requestId === undefined) throw new ProtocolError('INVALID_FRAME', 'invalid requestId')
  return { protocolVersion, connectionGeneration: generation, kind: raw.kind, ...(requestId === undefined ? {} : { requestId }), payload: raw.payload }
}

function json(frame: Frame) {
  if (frame.protocolVersion !== protocolVersion) throw new ProtocolError('PROTOCOL_VERSION', `expected ${protocolVersion}`)
  if (!frame.kind) throw new ProtocolError('INVALID_FRAME', 'kind must be non-empty')
  if (frame.connectionGeneration < 0n || frame.connectionGeneration > maxU64) throw new ProtocolError('INVALID_FRAME', 'connectionGeneration exceeds u64')
  if (frame.requestId !== undefined && (frame.requestId <= 0n || frame.requestId > maxU64)) throw new ProtocolError('INVALID_FRAME', 'requestId must be a nonzero u64')
  let payload: string | undefined
  try {
    payload = JSON.stringify(frame.payload)
  } catch {
    throw new ProtocolError('INVALID_FRAME', 'payload is not JSON serializable')
  }
  if (payload === undefined) throw new ProtocolError('INVALID_FRAME', 'payload is not JSON serializable')
  return `{"protocolVersion":"${protocolVersion}","connectionGeneration":${frame.connectionGeneration},"kind":${JSON.stringify(frame.kind)}${frame.requestId === undefined ? '' : `,"requestId":${frame.requestId}`},"payload":${payload}}`
}

export function encodeFrame(frame: Frame, maxFrameBytes = defaultMaxFrameBytes) {
  const body = Buffer.from(json(frame), 'utf8')
  if (body.byteLength > maxFrameBytes) throw new ProtocolError('FRAME_TOO_LARGE', `frame exceeds ${maxFrameBytes} bytes`)
  const output = Buffer.allocUnsafe(4 + body.byteLength)
  output.writeUInt32BE(body.byteLength, 0)
  body.copy(output, 4)
  return output
}

/** Stateful decoder that validates the length before it allocates a frame body. */
export class FrameDecoder {
  private readonly header = Buffer.allocUnsafe(4)
  private headerLength = 0
  private body: Buffer | undefined
  private bodyLength = 0

  constructor(readonly maxFrameBytes = defaultMaxFrameBytes) {
    if (!Number.isSafeInteger(maxFrameBytes) || maxFrameBytes < 1 || maxFrameBytes > 0xffff_ffff) {
      throw new ProtocolError('INVALID_FRAME_LIMIT', 'max frame bytes must be a positive u32')
    }
  }

  push(chunk: Uint8Array) {
    const frames: Frame[] = []
    let offset = 0
    while (offset < chunk.byteLength) {
      if (!this.body) {
        const count = Math.min(4 - this.headerLength, chunk.byteLength - offset)
        this.header.set(chunk.subarray(offset, offset + count), this.headerLength)
        this.headerLength += count
        offset += count
        if (this.headerLength < 4) continue
        const size = this.header.readUInt32BE(0)
        this.headerLength = 0
        if (size > this.maxFrameBytes) throw new ProtocolError('FRAME_TOO_LARGE', `frame exceeds ${this.maxFrameBytes} bytes`)
        this.body = Buffer.allocUnsafe(size)
        this.bodyLength = 0
        if (!size) {
          this.body = undefined
          throw new ProtocolError('INVALID_FRAME', 'empty frame')
        }
      }
      const body = this.body
      const count = Math.min(body.byteLength - this.bodyLength, chunk.byteLength - offset)
      body.set(chunk.subarray(offset, offset + count), this.bodyLength)
      this.bodyLength += count
      offset += count
      if (this.bodyLength !== body.byteLength) continue
      this.body = undefined
      this.bodyLength = 0
      let source: string
      try {
        source = new TextDecoder('utf-8', { fatal: true }).decode(body)
      } catch {
        throw new ProtocolError('INVALID_UTF8', 'frame body is not UTF-8')
      }
      frames.push(parseFrame(source))
    }
    return frames
  }
}
