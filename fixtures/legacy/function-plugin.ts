import { existsSync } from 'node:fs'
import { basename } from 'node:path'

type Config = {
  prefix?: string
  crash?: boolean
}
type BridgeContext = {
  provide(name: string, value: unknown): unknown
  on(name: string, listener: (value: unknown) => unknown): unknown
  on(name: string, listener: (value: unknown, next: () => unknown) => unknown): unknown
  emit(name: string, value: unknown): void
  get(name: string): unknown
}


function nodeInfo() {
  const filename = import.meta.filename
  if (!existsSync(filename)) throw new Error('the Node fixture source must be readable')
  return { file: basename(filename), readable: true }
}

/** Default function plugin plus the Node built-ins acceptance probe. */
export default function functionPlugin(ctx: BridgeContext, config: Config = {}) {
  const prefix = config.prefix ?? 'function'
  const info = nodeInfo()
  if (config.crash) process.exit(91)
  console.log('legacy:function-plugin', info.file)
  const events: unknown[] = []

  ctx.provide('legacy.function', {
    inspect(value: unknown) {
      return { prefix, value, ...info }
    },
    events() {
      return [...events]
    },
  })
  ctx.on('legacy.event', (value: unknown) => {
    events.push(value)
    return { prefix, value }
  })
  ctx.on('legacy.waterfall', (value: unknown, next: () => unknown) => ({
    prefix,
    value,
    next: next(),
  }))

  return async () => {
    await Promise.resolve()
    ctx.emit('legacy.disposed', { kind: 'function', ...info })
  }
}

/** Object-form plugin exercises Cordis's `{ apply() }` resolver. */
export const objectPlugin = {
  name: 'legacy-object-plugin',
  apply(ctx: BridgeContext, config: Config = {}) {
    const prefix = config.prefix ?? 'object'
    ctx.provide('legacy.object', {
      echo(value: unknown) {
        return { prefix, value }
      },
    })
    return () => ctx.emit('legacy.disposed', { kind: 'object' })
  },
}

/** Explicit dependency metadata is consumed before this plugin's apply hook runs. */
export const injectedObjectPlugin = {
  name: 'legacy-injected-object-plugin',
  inject: ['legacy.required'],
  apply(ctx: BridgeContext) {
    const required = ctx.get('legacy.required')
    const value = required && typeof required === 'object' && 'value' in required
      ? required.value
      : null
    ctx.provide('legacy.injected', {
      value() {
        return value
      },
    })
  },
}
