# Changelog

All notable changes to Tessivum Core are documented here.

The repository follows the pre-1.0 compatibility policy in
[`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md). Wire identifiers are versioned
independently of crate/repository releases.

## Unreleased

- Retain Native Loader configuration expressions until declared JSON-valued service dependencies are ready; re-evaluate against current providers on replacement and restoration. Missing providers suspend the consumer, while evaluation, schema, or candidate-update failures preserve the committed instance and effects.
- Add `LoaderRuntime::instantiate_configured` for source configuration import. Eager runtimes still resolve before construction; direct `NativePluginRuntime::instantiate` also accepts source expressions using the default scope. `NativePluginInstance::instantiate` remains the resolved-value primitive. Persisted entries keep their expressions, and arbitrary typed services are not implicitly serialized.
- Preserve fresh factory dependency declarations during transactional updates, augmenting only the required dependencies used by retained service expressions.
- Wait for queued dependency snapshots to settle before returning Native activation results; do not report a previous generation's success/failure during reconfiguration.
- Make repeated Native `activation()` observation idempotent: it no longer requeues an already failed start and hangs Loader rollback. Dependency notifications still schedule reconfiguration. A failing-before/passing-after regression preserves the original error and verifies provisional service cleanup.
- Add the real Native/upstream `loader-pending-config.json` scenario. The user approved latest pending-source semantics on 2026-10-14; raw upstream `MISMATCH` remains visible under `pending-config-uses-latest-source`.
- Replace service-oracle constants with real consumers, Native lifecycle callbacks, retained-reference operations and resolved service intercepts. Preserve Rust generation safety under the explicitly approved `revoked-service-handles-reject-access` difference; its dedicated fixture keeps the upstream mismatch visible.
- Add synchronous emit-error propagation to both event catalogs: remaining listeners do not run after the first error.
- Add `EntryTreeWatcher`, a real bounded JSON/YAML file owner using serialized Loader transactions, configurable polling and joined shutdown/Drop. Missing, invalid, oversized and non-regular sources retain committed state and expose errors; plugin tasks continue running while files are unchanged. Keep the independent public `HmrDriver` package-invalidation boundary.
- Complete Core C1 behavioral acceptance: 130 Rust tests, strict Clippy, six Bun tests, TypeScript, actual dual-runner traces and real Native/frozen-HMR filesystem scenarios. See `docs/CONFORMANCE_REPORT.md` and the C1 machine-readable report; this is not C2, product D04 or release-archive acceptance.

## 0.2.0 — 2026-10-13

Repository source release; all runtime crates remain `publish = false`.
The minor-version bump reflects breaking service-registration and Native Loader readiness semantics;
see [the migration guide](docs/COMPATIBILITY.md#migrating-from-01x-to-020).
All four runtime crates advance together. `cordis.plugin/v1`, `cordis.node/v1`,
and `tessivum.conformance/v1` remain unchanged.

- Make `ContextHandle::provide` strict: duplicate visible keys return `ServiceAlreadyProvided`; add explicit `ContextHandle::replace` with generation-safe replacement and staged commit conflict detection.
- Add service-catalog conformance coverage: every declared dependency, realm, interception, replacement, removal, duplicate, and stale-generation case now runs independently in both runners with strict invalid-case rejection.
- Add lifecycle-catalog conformance coverage: startup, cleanup, nested scopes, rollback, repeated/reentrant disposal, quiescence, and unload registration now execute independently in both runners.
- Preserve managed Native Loader consumers in `Pending` when a required provider is absent; do not mistake a dependency wait for a failed activation transaction. Actual activation failures retain rollback behavior.
- Add paired upstream Loader/Group/Include and Rust Loader catalog conformance: ten cases cover dependency ordering, duplicate IDs, ordered patches, config updates, replacement, candidate and rollback failures, nested groups, later patch targeting, and atomic persistence.
- Execute every declared event catalog case in both conformance runners instead of allowing TS to ignore the case list. Migrate `event/emit` to `event/event-catalog` with 11 observed call/result traces and strict case validation; add an oracle regression for per-case execution and mismatch rejection. Production event APIs are unchanged.
- Bound Bridge pending-request admission, cancel queued compat-host operations,
  and limit session hydration by page count, event count, and bytes. Bound cached
  agent snapshots with oldest-entry eviction. These controls do not make the
  Legacy Node process a security sandbox.

### Release verification

- macOS ARM64: 125 workspace Rust tests; Oracle 5 tests / 151 assertions;
  Oracle and compat-host TypeScript checks. The rebuilt Rust WASM guest passed
  the Native/Extism/Legacy runtime smoke; the Native example completed provider
  replacement and consumer reload with zero remaining resources.
- Before this version-only cutover, Core `cbf9fabc375dab492eca703d93ebe52d164ad1bf`
  passed the 30-sample macOS paired benchmark and the Alpha.29 product probe
  (574 Rust tests, 22 filtered; actual CLI first-run and resume tool round trips).
  These records identify that commit, not an eventual `0.2.0` tag. macOS PSS is
  unavailable; the historical published Linux benchmark is unchanged.
- Candidate `e8283fb7767e4abb7d1c92afa86e7578f6f4e77b` passed the
  [Windows Core CI](https://github.com/wavetao2010/tessivum-core/actions/runs/38229775907),
  including Clippy, Rust tests, and Native/Extism/Legacy runtime execution.
- The same candidate, pinned by Git revision and exact `0.2.0` crate versions,
  passed the Alpha.29 product probe on macOS ARM64: 574 tests, 22 filtered,
  and actual CLI first-run/resume tool round trips with preserved history.
- All six conformance fixtures passed both real Rust and Bun runners on
  2026-10-13 with identical traces. Release assets retain raw conformance output
  and product integration evidence. Changes after the candidate are documentation only.
- Product dependencies still pin Core `0.1.7`; Core Windows CI does not establish
  product Windows or Browser acceptance. Historical Linux benchmark data is
  retained, not presented as a fresh `0.2.0` performance measurement.

## 0.1.8 — 2026-09-24

### Legacy Node compatibility host

- Expose the native `systemPrompt@1` service to Legacy Node plugins through `ctx.systemPrompt.section({ name, order, text })`; registrations are scoped to the plugin and disposed with it.
- Resolve the vendored `@deepseek-ai/schemastery` module through the compatibility host's package resolver.
- Serve Legacy WebSocket upgrade routes through a real Node HTTP upgrade socket so unchanged `ws` `WebSocketServer({ noServer: true })` handlers receive the expected `Duplex` contract.
- Preserve `cordis.node/v1`, `cordis.plugin/v1`, and the documented Rust APIs.

## 0.1.7 — 2026-09-10

### Legacy Node compatibility host

- Hydrate the initiating native session before tool callbacks, check cancellation before plugin execution, and reject missing sessions instead of fabricating empty headers. Cold-session tools now resolve the authoritative workspace without requiring an earlier browser route request. Rust APIs and wire identifiers are unchanged.
- Assemble bounded native session snapshot pages before publishing Legacy route
  context. Preserve prior cached history when a later page fails; agent metadata
  inspection no longer needs to duplicate the session history.

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
