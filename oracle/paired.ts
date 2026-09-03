import { existsSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'

const workloadSchema = 'tessivum.core-benchmark-workload/v1' as const
const runtimeSchema = 'tessivum.core-benchmark-runtime/v2' as const
const maxSamples = 100

type Workload = {
  schema: typeof workloadSchema
  scopes: number
  serviceLookups: number
  eventEmits: number
  loaderEntries: number
  rootChildren: number
}

type CordisFiber = PromiseLike<CordisFiber> & {
  ctx: CordisContext
  dispose(): Promise<void>
  uid: number | null
}

type CordisLoader = {
  builtins: Record<string, unknown>
  create(options: Record<string, unknown>): Promise<string>
  update(id: string, options: Record<string, unknown>): Promise<void>
  await(): Promise<void>
}

type CordisContext = {
  fiber: { getEffects(): unknown[] }
  registry: { size: number }
  loader: CordisLoader
  plugin(plugin: unknown, config?: unknown): CordisFiber
  provide(name: string, value: unknown): () => unknown
  get(name: string): unknown
  on(name: string, listener: (...args: unknown[]) => unknown): () => unknown
  emit(name: string, ...args: unknown[]): void
}

type CordisConstructor = new () => CordisContext

type Options = {
  cordisRoot: string
  workload: string
  samples: number
}

type Benchmark = {
  name: string
  unit: string
  operationsPerSample: number
  samples: number[]
  median: number | null
  p95: number | null
  min: number | null
  max: number | null
  status?: 'unavailable'
  note?: string
}

function usage(): never {
  process.stdout.write('Usage: bun paired.ts --workload <path> --samples <1..100> [--cordis-root <vendor-or-cordis-root>]\n')
  process.exit(0)
}

function nextArgument(args: string[], index: number, flag: string) {
  const value = args[index + 1]
  if (!value || value.startsWith('--')) throw new Error(`${flag} requires a value`)
  return value
}

function parseSamples(value: string) {
  if (!/^[1-9]\d*$/.test(value)) throw new Error('--samples must be a positive integer')
  const samples = Number(value)
  if (!Number.isSafeInteger(samples) || samples > maxSamples) throw new Error(`--samples must be in 1..${maxSamples}`)
  return samples
}

function parseOptions(args: string[]): Options {
  let cordisRoot = process.env.CORDIS_VENDOR_ROOT
  let workload: string | undefined
  let samples: number | undefined

  for (let index = 0; index < args.length; index += 1) {
    switch (args[index]) {
      case '--cordis-root':
        cordisRoot = nextArgument(args, index, '--cordis-root')
        index += 1
        break
      case '--workload':
        workload = nextArgument(args, index, '--workload')
        index += 1
        break
      case '--samples':
        samples = parseSamples(nextArgument(args, index, '--samples'))
        index += 1
        break
      case '--help':
      case '-h':
        usage()
      default:
        throw new Error(`unknown argument ${JSON.stringify(args[index])}`)
    }
  }

  if (!cordisRoot) throw new Error('set --cordis-root or CORDIS_VENDOR_ROOT')
  if (!workload) throw new Error('--workload is required')
  if (!samples) throw new Error('--samples is required')
  return { cordisRoot, workload, samples }
}

function isPositiveInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0
}

function parseWorkload(value: unknown): Workload {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('workload must be an object')
  const workload = value as Record<string, unknown>
  const keys = Object.keys(workload).sort()
  const expected = ['eventEmits', 'loaderEntries', 'rootChildren', 'schema', 'scopes', 'serviceLookups']
  if (keys.length !== expected.length || keys.some((key, index) => key !== expected[index])) {
    throw new Error(`workload must contain exactly ${expected.join(', ')}`)
  }
  if (workload.schema !== workloadSchema || !isPositiveInteger(workload.scopes) || !isPositiveInteger(workload.serviceLookups)
    || !isPositiveInteger(workload.eventEmits) || !isPositiveInteger(workload.loaderEntries) || !isPositiveInteger(workload.rootChildren)) {
    throw new Error(`workload must match ${workloadSchema}`)
  }
  if (workload.scopes !== 1000 || workload.serviceLookups !== 256 || workload.eventEmits !== 256
    || workload.loaderEntries !== 16 || workload.rootChildren !== 32) {
    throw new Error('workload must use the frozen values scopes=1000, serviceLookups=256, eventEmits=256, loaderEntries=16, rootChildren=32')
  }
  return workload as Workload
}

async function loadWorkload(path: string) {
  let document: unknown
  try {
    document = JSON.parse(await Bun.file(path).text())
  } catch (error) {
    throw new Error(`cannot parse workload ${path}: ${error instanceof Error ? error.message : String(error)}`)
  }
  return parseWorkload(document)
}

async function loadCordis(root: string): Promise<{ Context: CordisConstructor, Loader: unknown, identity: Record<string, string> }> {
  const base = resolve(root)
  const candidates = [base, join(base, 'cordis')]
  for (const candidate of candidates) {
    const entry = join(candidate, 'lib', 'index.js')
    const manifest = join(candidate, 'package.json')
    if (!existsSync(entry) || !existsSync(manifest)) continue
    const metadata = JSON.parse(await Bun.file(manifest).text()) as Record<string, unknown>
    if (metadata.name !== '@deepseek-ai/cordis' || typeof metadata.version !== 'string') {
      throw new Error(`Cordis manifest at ${manifest} is invalid`)
    }
    const loaderEntry = join(candidate, '..', 'loader', 'lib', 'index.js')
    if (!existsSync(loaderEntry)) throw new Error(`Cordis Loader was not found at ${loaderEntry}`)
    const module = await import(pathToFileURL(entry).href) as { Context?: CordisConstructor }
    const loaderModule = await import(pathToFileURL(loaderEntry).href) as { default?: unknown, Loader?: unknown }
    const Loader = loaderModule.default ?? loaderModule.Loader
    if (typeof module.Context !== 'function') throw new Error(`Cordis entrypoint at ${entry} does not export Context`)
    if (typeof Loader !== 'function') throw new Error(`Cordis Loader entrypoint at ${loaderEntry} has no plugin export`)
    return {
      Context: module.Context,
      Loader,
      identity: {
        name: metadata.name,
        implementation: 'typescript',
        version: metadata.version,
        revision: process.env.DSH_REVISION ?? 'unavailable',
        profile: 'release',
      },
    }
  }
  throw new Error(`Cordis was not found under ${base}; expected cordis/lib/index.js`)
}

function clock() {
  return process.hrtime.bigint()
}

function elapsedNanoseconds(start: bigint) {
  const elapsed = clock() - start
  if (elapsed <= 0n) throw new Error('timer resolution produced a zero-duration sample')
  return Number(elapsed)
}

function operationsPerSecond(operations: number, elapsed: bigint) {
  if (elapsed <= 0n) throw new Error('timer resolution produced a zero-duration sample')
  return Number((BigInt(operations) * 1_000_000_000n) / elapsed)
}

function summarize(name: string, unit: string, operationsPerSample: number, samples: number[], note?: string): Benchmark {
  if (!samples.length || samples.some(sample => !Number.isFinite(sample))) throw new Error(`${name} collected an invalid sample`)
  const sorted = [...samples].sort((left, right) => left - right)
  const p95Index = Math.ceil(sorted.length * 0.95) - 1
  return {
    name,
    unit,
    operationsPerSample,
    samples,
    median: sorted[Math.floor(sorted.length / 2)],
    p95: sorted[p95Index],
    min: sorted[0],
    max: sorted[sorted.length - 1],
    ...(note ? { note } : {}),
  }
}

function unavailable(name: string, unit: string, operationsPerSample: number, note: string): Benchmark {
  return {
    name,
    unit,
    operationsPerSample,
    samples: [],
    median: null,
    p95: null,
    min: null,
    max: null,
    status: 'unavailable',
    note,
  }
}

function ownedScope() {
  return () => undefined
}

async function createOwnedTree(Context: CordisConstructor, scopes: number) {
  const root = new Context()
  const children: CordisFiber[] = []
  const owner = root.plugin((context) => {
    for (let index = 0; index < scopes; index += 1) children.push(context.plugin(ownedScope))
    return () => undefined
  })
  await owner
  await Promise.all(children)
  return { root, owner, children }
}

function residue(root: CordisContext, children: CordisFiber[]) {
  return root.registry.size + root.fiber.getEffects().length + children.filter(child => child.uid !== null).length
}

function assertNoResidue(root: CordisContext, children: CordisFiber[]) {
  const count = residue(root, children)
  if (count) throw new Error(`dispose left ${count} observable Cordis resource(s)`)
  return count
}

async function scopeCreateDispose(Context: CordisConstructor, scopes: number) {
  const root = new Context()
  const children: CordisFiber[] = []
  const start = clock()
  for (let index = 0; index < scopes; index += 1) children.push(root.plugin(ownedScope))
  await Promise.all(children)
  await Promise.all(children.map(child => child.dispose()))
  const elapsed = elapsedNanoseconds(start)
  assertNoResidue(root, children)
  return elapsed
}

async function serviceLookup(Context: CordisConstructor, lookups: number) {
  const root = new Context()
  const removeProvider = root.provide('benchmark.service', { value: 42 })
  const child = root.plugin(() => () => undefined)
  await child
  const context = child.ctx
  const start = clock()
  for (let index = 0; index < lookups; index += 1) {
    const service = context.get('benchmark.service') as { value?: unknown } | undefined
    if (service?.value !== 42) throw new Error('benchmark service disappeared')
  }
  const elapsed = clock() - start
  await child.dispose()
  await removeProvider()
  if (root.registry.size || root.fiber.getEffects().length) throw new Error('service cleanup left an observable Cordis resource')
  return operationsPerSecond(lookups, elapsed)
}

async function eventEmit(Context: CordisConstructor, emits: number) {
  const root = new Context()
  let seen = 0
  const removeListener = root.on('benchmark.event', () => {
    seen += 1
  })
  const start = clock()
  for (let value = 0; value < emits; value += 1) root.emit('benchmark.event', value)
  const elapsed = clock() - start
  await removeListener()
  if (seen !== emits) throw new Error(`benchmark event listener saw ${seen} of ${emits} events`)
  if (root.fiber.getEffects().length) throw new Error('event cleanup left an observable Cordis resource')
  return operationsPerSecond(emits, elapsed)
}

async function loaderFixture(Context: CordisConstructor, Loader: unknown, entries: number) {
  const root = new Context()
  const owner = root.plugin(Loader)
  await owner
  root.loader.builtins['benchmark-noop'] = ownedScope
  const ids = await Promise.all(Array.from({ length: entries }, (_, index) => root.loader.create({
    id: `benchmark-${index}`,
    name: 'cordis:benchmark-noop',
    config: { revision: 1 },
  })))
  await root.loader.await()
  return { root, owner, ids }
}

async function loaderLoad(Context: CordisConstructor, Loader: unknown, entries: number) {
  const root = new Context()
  const owner = root.plugin(Loader)
  await owner
  root.loader.builtins['benchmark-noop'] = ownedScope
  const start = clock()
  await Promise.all(Array.from({ length: entries }, (_, index) => root.loader.create({
    id: `benchmark-${index}`,
    name: 'cordis:benchmark-noop',
    config: { revision: 1 },
  })))
  await root.loader.await()
  const elapsed = elapsedNanoseconds(start)
  await owner.dispose()
  if (root.registry.size || root.fiber.getEffects().length) throw new Error('loader disposal left an observable Cordis resource')
  return elapsed
}

async function loaderUpdate(Context: CordisConstructor, Loader: unknown, entries: number) {
  const { root, owner, ids } = await loaderFixture(Context, Loader, entries)
  const start = clock()
  await root.loader.update(ids[0], { config: { revision: 2 } })
  await root.loader.await()
  const elapsed = elapsedNanoseconds(start)
  await owner.dispose()
  if (root.registry.size || root.fiber.getEffects().length) throw new Error('loader update disposal left an observable Cordis resource')
  return elapsed
}

async function rootDispose(Context: CordisConstructor, scopes: number) {
  const { root, owner, children } = await createOwnedTree(Context, scopes)
  const start = clock()
  await owner.dispose()
  const elapsed = elapsedNanoseconds(start)
  assertNoResidue(root, children)
  return elapsed
}

async function selfPssKiB() {
  const document = await Bun.file('/proc/self/smaps_rollup').text()
  const match = /^Pss:\s+(\d+)\s+kB$/m.exec(document)
  if (!match) throw new Error('/proc/self/smaps_rollup does not contain Pss')
  const value = Number(match[1])
  if (!Number.isSafeInteger(value)) throw new Error('/proc/self/smaps_rollup Pss is invalid')
  return value
}

async function processPssLive(Context: CordisConstructor, scopes: number) {
  const { root, owner, children } = await createOwnedTree(Context, scopes)
  const live = await selfPssKiB()
  await owner.dispose()
  assertNoResidue(root, children)
  return live
}

async function processPssResidue(Context: CordisConstructor, scopes: number) {
  const { root, owner, children } = await createOwnedTree(Context, scopes)
  await owner.dispose()
  assertNoResidue(root, children)
  return selfPssKiB()
}

async function residueAfterDispose(Context: CordisConstructor, scopes: number) {
  const { root, owner, children } = await createOwnedTree(Context, scopes)
  await owner.dispose()
  return assertNoResidue(root, children)
}

async function collect(samples: number, sample: () => Promise<number>) {
  const values: number[] = []
  for (let index = 0; index < samples; index += 1) values.push(await sample())
  return values
}

async function run(options: Options) {
  const workload = await loadWorkload(options.workload)
  const { Context, Loader, identity } = await loadCordis(options.cordisRoot)
  const pssAvailable = process.platform === 'linux'
  const pssNote = 'Linux /proc/self/smaps_rollup is unavailable on this platform'
  const benchmarks: Benchmark[] = [
    summarize('scope_create_dispose', 'ns', workload.scopes, await collect(options.samples, () => scopeCreateDispose(Context, workload.scopes))),
    summarize('service_lookup', 'operations/s', workload.serviceLookups, await collect(options.samples, () => serviceLookup(Context, workload.serviceLookups))),
    summarize('event_emit', 'operations/s', workload.eventEmits, await collect(options.samples, () => eventEmit(Context, workload.eventEmits))),
    summarize('loader_load', 'ns', workload.loaderEntries, await collect(options.samples, () => loaderLoad(Context, Loader, workload.loaderEntries))),
    summarize('loader_update', 'ns', 1, await collect(options.samples, () => loaderUpdate(Context, Loader, workload.loaderEntries))),
    summarize('root_dispose', 'ns', workload.rootChildren, await collect(options.samples, () => rootDispose(Context, workload.rootChildren))),
    pssAvailable
      ? summarize('process_pss_live', 'KiB', workload.scopes, await collect(options.samples, () => processPssLive(Context, workload.scopes)))
      : unavailable('process_pss_live', 'KiB', workload.scopes, pssNote),
    pssAvailable
      ? summarize('process_pss_residue', 'KiB', workload.scopes, await collect(options.samples, () => processPssResidue(Context, workload.scopes)))
      : unavailable('process_pss_residue', 'KiB', workload.scopes, pssNote),
    summarize('residue_after_dispose', 'count', workload.scopes, await collect(options.samples, () => residueAfterDispose(Context, workload.scopes))),
  ]
  return {
    schema: runtimeSchema,
    runtime: identity,
    revisions: {
      product: process.env.TESSIVUM_PRODUCT_REVISION ?? 'unavailable',
      core: process.env.TESSIVUM_CORE_REVISION ?? 'unavailable',
      dsh: process.env.DSH_REVISION ?? 'unavailable',
    },
    workload,
    environment: {
      platform: process.platform,
      arch: process.arch,
      bun: Bun.version,
    },
    benchmarks,
    diagnostics: {
      processPss: pssAvailable
        ? { status: 'available', source: '/proc/self/smaps_rollup' }
        : { status: 'unavailable', reason: pssNote },
    },
  }
}

try {
  const report = await run(parseOptions(process.argv.slice(2)))
  process.stdout.write(`${JSON.stringify(report)}\n`)
} catch (error) {
  process.stderr.write(`${JSON.stringify({ error: error instanceof Error ? error.message : String(error) })}\n`)
  process.exitCode = 1
}

