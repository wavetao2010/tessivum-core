# Changelog

All notable changes to Tessivum Core are documented here.

The repository follows the pre-1.0 compatibility policy in
[`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md). Wire identifiers are versioned
independently of crate/repository releases.

## 0.1.6 — 2026-08-28

### Legacy Node compatibility host

- Forward the exact plugin-supplied `agents.create` seed to the native Agent
  boundary instead of retaining it only in the compatibility host.
- Preserve the existing `cordis.node/v1`, `cordis.plugin/v1`, and Rust API
  surfaces.
- Correlate cancellation frames with their original request IDs and propagate
  Browser `AbortSignal` cancellation through Legacy service and command calls;
  late replies cannot reopen a cancelled request.

### Benchmark evidence

- Add a frozen shared-workload, process-cold comparison between
  `tessivum-core` and the pinned TypeScript Cordis `4.0.1` oracle, including
  Loader update, root disposal, Linux PSS, and post-disposal residue cases.
- Preserve non-Linux PSS as explicit unavailable data rather than fabricated
  zero values; retain raw successes, failures, environment facts, revisions,
  commands, median, p95, min, and max.
- Publish the 30-sample fixed-Linux raw result and median/p95 comparison;
  preserve the former three-sample Darwin record as explicitly historical.

## 0.1.5 — 2026-08-28

### Legacy Node compatibility host

- Expose native-backed `agents` compatibility objects for Side Chat create,
  resume, message delivery, cancellation, disposal, and session preload flows.
- Expose settled Cordis Loader entry names and Fiber lifecycle transitions so
  the Rust Host can report exact active plugin inventory without a second
  Loader.
- Serialize duplicate session preloads and retain Loader cleanup through
  cancellation, crash, restart, and shutdown.
- `cordis.node/v1`, `cordis.plugin/v1`, and the Rust API surface are unchanged.

## 0.1.4 — 2026-08-25

### Legacy Node compatibility host

- Load the compiled pinned Cordis and Loader entries. Packaged deployments no
  longer ask Bun to import the source-only `FiberState` const enum from a
  compiled Cordis module.
- `cordis.node/v1`, `cordis.plugin/v1`, and the Rust API surface are unchanged.

## 0.1.3 — 2026-08-25

### Legacy Node bridge

- Allowed bounded `log` frames while the host is initializing, before its
  `ready` frame. Community packages can now emit Cordis startup diagnostics
  without aborting the bridge handshake.
- Kept every other pre-ready frame invalid; `cordis.node/v1` is unchanged.

## 0.1.2 — 2026-08-25

### Licensing

- Licensed the repository and all four runtime crates under the MIT License.
- Runtime behavior, `cordis.plugin/v1`, `cordis.node/v1`, and
  `tessivum.conformance/v1` are unchanged from `0.1.1`.

## 0.1.1 — 2026-08-24

### Runtimes

- The Bun compatibility host now resolves ordinary `cordis`, `cosmokit`, and
  `@cordisjs/plugin-loader` imports to the pinned DeepSeek Cordis vendor, so
  unchanged community packages share the Host's Cordis module identity.

## 0.1.0 — 2026-08-17

First release of the Core runtime surface.

### Core

- Added scoped asynchronous ownership with `Scope`, `Fiber`, cancellation,
  reverse-order cleanup, rollback of failed setup, cleanup aggregation, and
  lifecycle diagnostics.
- Added immutable `ContextHandle` views, typed generation-checked services,
  dependency readiness subscriptions, private/shared service realms, and
  intercept diagnostics.
- Added typed and dynamic events with `emit`, `parallel`, `serial`, `bail`,
  and waterfall dispatch; registrations are scope-owned and dispatch
  diagnostics are available.
- Added JSON/YAML `EntryTree` parsing and validation, detached entry patches,
  atomic same-directory persistence, restricted configuration expressions, and
  transactional `Loader` replacement/update/unload behavior.

### Runtimes

- Added the in-process `NativePlugin` lifecycle and `NativePluginRuntime` Loader
  adapter. Native descriptors validate dependencies and a closed typed JSON
  configuration schema before lifecycle effects.
- Added `tessivum-extism` and `tessivum-pdk` for the exact
  `cordis.plugin/v1` ABI: five lifecycle exports, JSON request/response/error
  envelopes, manifest/schema checks, capability authorization, Extism limits,
  serialized calls, cancellation, and stop-time binding invalidation.
- Added `tessivum-node-bridge` and the Bun compat-host for the exact
  `cordis.node/v1` transport: bounded length-prefixed frames, handshake,
  generation checks, request/response/error/cancel, supervision, and
  generation-owned cleanup. The bridge runs existing Cordis/npm-style plugins
  in a separate process; it is not a security sandbox.

### Contracts and documentation

- Established `tessivum.conformance/v1` as the fixture schema identifier.
- Established `cordis.plugin/v1` as the WASM ABI and `cordis.node/v1` as the
  legacy Node bridge protocol. Cross-runtime service contracts use their own
  `name@contract-version` identifiers.
- Added public API, protocol, security-review, and compatibility/migration
  documentation in `docs/`.
- Added Native and Rust WASM minimal examples. The Native example exercises
  provider/consumer dependencies, typed events, provider replacement, reload,
  and root disposal; the Rust WASM example exports all v1 lifecycle functions.

### Security notes

- Native plugins are fully trusted host-process code.
- WASM capabilities are deny-by-default and require manifest declaration, host
  grant, and a registered handler.
- Node frame/queue/process controls provide protocol and failure isolation, not
  privilege isolation. Deploy the compat-host with OS/container policy when
  loading code that is not fully trusted.

### Upgrade notes

- This is the first release; there is no prior Tessivum Core release migration.
- Use the canonical Loader fields `package` and `runtime: legacy-node` when
  writing configuration. Parser-only aliases (`module`, `plugin`, `source`, and
  `legacy_node`) are not canonical persisted output.
- Update the Rust bridge and Bun compat-host together, restart the supervisor,
  and discard all old-generation bridge clients and registrations.
- WASM guests must declare `abi: "cordis.plugin/v1"`, export all five lifecycle
  functions, and return the PDK's `{ requestId, result }` or
  `{ requestId, error }` form with a matching request ID. The current host
  ignores extra response fields, so portable guests must not rely on such
  fields for v1 behavior.
