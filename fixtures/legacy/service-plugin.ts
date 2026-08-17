import { Service, type Context } from '@deepseek-ai/cordis'

type Config = {
  prefix?: string
}

/** A genuine Cordis Service subclass registered by its base constructor. */
export class BridgeService extends Service {
  readonly prefix: string
  private eventCount = 0

  constructor(ctx: Context, config: Config = {}) {
    super(ctx, 'legacy.bridge')
    this.prefix = config.prefix ?? 'class'
  }

  inspect(value: unknown) {
    return { prefix: this.prefix, value, eventCount: this.eventCount }
  }

  recordEvent() {
    this.eventCount += 1
    return this.eventCount
  }
}

/** Class-form plugin with an injected dependency, event listener, and disposer. */
export class ClassPlugin extends BridgeService {
  static inject = ['legacy.required']

  constructor(ctx: Context, config: Config = {}) {
    super(ctx, config)
    const required = ctx.get('legacy.required')
    const value = required && typeof required === 'object' && 'value' in required
      ? required.value
      : null
    ctx.provide('legacy.class', {
      required() {
        return value
      },
      inspect: (item: unknown) => this.inspect(item),
    })
    ctx.events.on('legacy.event', () => this.recordEvent())
    ctx.events.on('legacy.waterfall', (item: unknown, next: () => unknown) => ({
      class: this.prefix,
      value: item,
      next: next(),
    }))
  }

  async [Service.init]() {
    await Promise.resolve()
    return async () => {
      await Promise.resolve()
      this.ctx.emit('legacy.disposed', { kind: 'class', prefix: this.prefix })
    }
  }
}

export default ClassPlugin
