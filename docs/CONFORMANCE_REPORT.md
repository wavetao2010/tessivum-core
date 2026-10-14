# Conformance Report

**Report schema:** `tessivum.differential-report/v1`  
**Recorded:** 2026-08-17T10:19:40Z  
**ABI:** `cordis.plugin/v1` · **bridge:** `cordis.node/v1`  
**Repository source release:** `0.2.0` · all runtime crates remain `publish = false`; this is not a crates.io package release.

The original differential evidence below was recorded for the `0.1.0` release on 2026-08-17. Its hashes and counts are historical snapshots, not hashes of today's fixtures. Later releases through `0.1.4` retained that recorded baseline; the C1 event and Loader catalog cutovers below extend the same `tessivum.conformance/v1` contract without changing production event or Loader APIs.


The machine-readable evidence is [`../fixtures/conformance/differential-report.json`](../fixtures/conformance/differential-report.json).

## 0.2.0 release check — 2026-10-13

All six current fixtures (`lifecycle`, `services`, `service-registration`,
`events`, `loader-dependencies`, and `loader`) passed the actual Rust conformance
binary and Bun oracle with identical traces on macOS ARM64. The release asset
`conformance-0.2.0.json` records each fixture hash and both complete outputs.
Runtime source matches candidate `e8283fb7767e4abb7d1c92afa86e7578f6f4e77b`;
the release finalization changes documentation only. The original differential
report linked above remains historical evidence, not the current fixture hashes.

## Pending configuration and rollback — 2026-10-13

- Candidate: local changes on Core `b69d0ed86f30adb0b2d6138d458593487336aa08`; no new release. Product `2bc3257546121b7348fdef3a7b6dfdacd6c4e4b0` remains pinned to that unpatched Git revision. Product smoke used an isolated temporary Cargo patch for Core/Extism/Node Bridge; its manifest and lock were not changed.
- Added `fixtures/conformance/loader-pending-config.json`, separate from the six equal fixtures. Both runners execute real plugins: load without a provider, replace the entire config while pending, add the provider, reject a candidate, recover, and unload. No plugin starts before readiness; the failed candidate retains the committed source expression.
- Rust observations: `null → null → {label: ready} → {label: ready} → {label: stale} → null`. Frozen Harness `47f943859bef60e4160492346772ded9b24f765a` produces `{label: stale, obsolete: true}` at the first activation instead; the remaining observations match. `git diff -- vendor` was empty and `CORDIS_VENDOR_ROOT` unset. This is **MISMATCH**, not an accepted difference or a full C1/G03 pass. The expected latest-config contract was not changed to match stale upstream behavior.
- The native scenario initially hung during rejected-candidate rollback. A deterministic regression showed that a second `activation()` call requeued a failed instance, leaving it Pending despite ready dependencies. Only the first activation request now schedules work; subsequent calls observe the settled result or queued dependency updates.
- Verification on macOS ARM64: Core workspace/all-targets **129 passed**, strict Clippy passed; existing Bun oracle **5 passed / 151 assertions**, TypeScript check passed; actual Native scenario **100/100** runs within a five-second per-run deadline. Actual product Composition missing-dependency admission, provider replacement/removal/restoration, source-expression preservation, and stop passed with the temporary patch.
- Local evidence: product `test-artifacts/c1-pending-config/` contains both complete traces, the failing regression, successful test/Clippy and product logs, reference and candidate hashes, and stress results. No Windows/Browser acceptance or formal Git-pin upgrade was performed for this local patch.

Reproduce from Core root:

```sh
cargo run --locked -p tessivum-core --bin conformance -- fixtures/conformance/loader-pending-config.json
bun run oracle/src/index.ts fixtures/conformance/loader-pending-config.json
```

The first command returns `PASS`; the second currently exits nonzero with `TRACE_MISMATCH`. Keep that difference visible; do not replace the expected trace or report both as passing.

## Initial C1 admission review — 2026-10-14 (historical)

The user explicitly approved `pending-config-uses-latest-source`: keep the latest pending configuration, including removed-field semantics. The first-activation difference is now accepted in `intentional-differences.json`; the raw oracle still returns `TRACE_MISMATCH`. The earlier “unaccepted” status above is historical.

**At this review C1 was not yet closed.** Inspecting the actual oracle invalidated the earlier broad claim that all service observations were measured: required/optional readiness, stale references and intercept resolution included constants. This finding led to the real-consumer and watcher work recorded below; historical equal output remains historical, not proof of those contracts.

| Gate | Evidence and remaining action |
|---|---|
| G01 service lifecycle | Replace the above oracle constants with real required/optional consumers, stale-reference operations and service intercept resolution; expose any resulting mismatch rather than changing the contract to obtain PASS. |
| G02 lifecycle | Real lifecycle catalog covers the declared startup/disposal minimum; Rust race and cleanup tests add independent invariants. |
| G02 event errors | Existing catalog covers ordering, short-circuit, parallel aggregation and removal. Add real synchronous emit-error propagation comparison; Rust `events.rs` already checks stop-on-error. |
| G03 Loader | Ordered patches, whole-config replacement, failed candidates/restoration errors and atomic file replacement have concrete coverage. Pending-config difference is approved. Live JSON projection evidence is Native-specific, not arbitrary typed-service or JS equivalence. |
| G03 watcher/HMR | `HmrDriver` currently exposes invalidation only; injected mock invalidation is not proof of filesystem watcher shutdown or rapid-change reconciliation. Identify and exercise the actual owner path before closing this requirement. Product profile/Legacy JS wiring remains D04, not a reason to add a JS evaluator to Core. |

Atomic file rename is not a combined memory/disk/external-side-effect transaction and was never required to become one. Product D04, C2 Guest languages and release archive validation are separate scope, not additional claims made by C1.

## C1 behavioral acceptance — 2026-10-14

**Core C1 behavior is accepted with two explicitly user-approved differences.** The complete machine-readable traces and fixture hashes are in [`c1-admission-report.json`](../fixtures/conformance/c1-admission-report.json). The frozen source is Harness `47f943859bef60e4160492346772ded9b24f765a`; vendor code was not modified for these probes.

| Gate | Executed evidence |
|---|---|
| G01 | Six equal service cases now observe real Cordis consumers, actual Native plugin start/stop callbacks, required/optional readiness, realm visibility and `Service.resolveConfig`. Three separate retained-reference cases observe provider replacement/removal/recovery and actual old-reference operations. |
| G02 | Twelve lifecycle and twelve event cases agree, including synchronous emit failure: only the first listener runs and `listener exploded` reaches the caller. Rust race/cleanup tests remain additional invariants. |
| G03 Loader | Ten Loader catalog and five dependency-lifecycle observations agree; the pending-config regression keeps latest-source semantics, rejects a failing candidate, restores committed state and unloads. |
| G03 files | Real Native `EntryTreeWatcher` and frozen Node 22 HMR/Include scenarios exercise actual file writes, invalid/missing-source preservation, latest valid convergence, atomic recreation, in-flight close and post-close quiescence. Rust additionally verifies bounded/non-regular reads, idle Tokio task progress and Drop cleanup. |

`services.json` has six equal cases; `service-retained-references.json` contains the other three. Rust rejects obsolete handles; upstream retained objects still return old values. The user approved this as `revoked-service-handles-reject-access`, without weakening Rust generation protection. It and `pending-config-uses-latest-source` retain Rust `PASS` / upstream `MISMATCH`. Comparison checks every other trace field for equality; neither exception is a blanket oracle bypass.

Verification: macOS ARM64 Core workspace/all-targets **130 passed**, strict Clippy and oracle TypeScript check passed; Bun **6 passed / 151 assertions**. Both actual CLIs ran from an external working directory: six equal fixtures and two precisely bounded accepted differences. Actual Native file smoke observed `initial → initial (rejected) → latest → latest (missing) → recreated → absent (stopped)`; real frozen HMR returned all convergence/close assertions true. Watcher regression first failed on missing-source handling, then passed after preserving the committed tree.

The watcher owns one continuously driven Tokio worker and uses configurable bounded whole-file polling, not JS/module-cache HMR. Start/stop/Drop join synchronously; async hosts must not block a runtime needed by their plugins. Intermediate writes may coalesce; a persistent latest source is reread after in-flight work. Default source limit is 4 MiB; missing or oversized/non-regular input is an observable error, never an empty configuration. Product D04 wiring, C2 Guest languages and release archives remain separate. Product same-revision consumption is recorded in the product implementation checklist, not inferred from Core tests.


## C1 event catalog evidence — 2026-10-10

- Fixture: `fixtures/conformance/events.json`, SHA-256 `d720344784628ae2d4062fb1fce2914661765a3697b56ff210fd8fa84d217e41`.
- Reproduced the previous oracle defect: an unknown event case still returned `PASS` because only the generic emit trace ran. The migrated `event/event-catalog` executes all 11 cases on real vendored Cordis and Rust `EventBus`, producing 11 equal observations.
- Commands from Core root: `bun run oracle/src/index.ts fixtures/conformance/events.json`; `cargo run -p tessivum-core --bin conformance -- fixtures/conformance/events.json`; `cargo test -p tessivum-core --test conformance --test events -- --test-threads=1` (8 passed); `bun test oracle/src/scenarios.test.ts` (1 passed, 37 assertions). `bunx tsc --noEmit` from `oracle/` passed.
- Both actual CLIs rejected each of 11 individually corrupted expected observations at its exact index. Unknown, empty, malformed, and extra-field cases were rejected. The old `event/emit` scenario returned `UNSUPPORTED_SCENARIO` on both sides; no alias remains.
- CLI runs from an external working directory and compared lifecycle, service, event, Loader dependency lifecycle, and Loader catalog traces successfully. The Loader catalog now has ten normalized observations; the earlier unsupported result below is retained only as historical 0.1.0 evidence.


## C1 Loader catalog evidence — 2026-10-10

- Fixture: `fixtures/conformance/loader.json`.
- Both runners executed all ten declared cases and returned identical normalized observations: dependency ordering, duplicate IDs, ordered patches, config updates, replacement, candidate rollback, rollback failure reporting, nested groups, later targeting of inserted entries, and atomic Include persistence.
- The TypeScript oracle uses the vendored Loader, Group, Include, and patch implementation. Rust exercises `Loader::load`, `replace`, `update`, `persist`, and `unload`; no empty-baseline or execution-only exception remains.
- `bunx tsc --noEmit`, the TypeScript oracle, and the Rust conformance CLI passed for the fixture.

## Recorded 0.1.0 oracle evidence

The recorded TypeScript oracle was invoked from `oracle/` with:

```sh
bun run oracle -- ../fixtures/conformance/<fixture>.json
```

| Fixture | SHA-256 | Oracle result | Normalized events |
|---|---|---:|---:|
| `lifecycle.core-catalog` | `8ee0d6dcd58f593700a5c051bf925eac7edbca365175a86c7413b0b4325b1973` | `PASS` | 7 |
| `service.core-catalog` | `4a1dc2e17242d6398f9ea9097dd05e442ab26423d1b5214b5c3de26795cca8dc` | `PASS` | 14 |
| `event.core-catalog` | `4752e8d81375f51678e5f5e34e7ddaafff0bdfa41cc2418342113aab4576279c` | `PASS` | 6 |
| `loader.core-catalog` | `558f11c374fce2867b51b5ff4ee2e4dec34aa83188a66fdaeec3ef6c99432d22` | `UNSUPPORTED_SCENARIO` | — |

The Loader result is historical 0.1.0 evidence: `loader/loader-catalog` was unsupported by the oracle then and its empty baseline recorded no cross-runtime trace. C1 evidence above replaces that limitation.

## Rust runner

`crates/tessivum-core/src/bin/conformance.rs` executes **every** `input.cases` record through public core APIs. It emits `executedCases` in fixture order only after every case succeeds; an unknown, missing, or failing case is `EXECUTION_ERROR`.

| Domain | Declared cases exercised | APIs exercised |
|---|---|---|
| Lifecycle | all 12: start, cleanup, nested/reverse disposal, repeated/reentrant disposal, rollback, quiescence, unload rejection | `Fiber::start`, `Fiber::dispose`, `Scope::child`, `Scope::add_effect`, `Scope::dispose` |
| Service | all 9: late/optional dependency, replacement/removal/recovery, isolated/shared realms, intercepts, stale generations | `ContextHandle::subscribe`, `provide`, `replace`, `get`, `isolate`, `intercept`, `Scope::dispose` |
| Event | all 11: emit, parallel aggregation, serial/synchronous bail, waterfall next/short-circuit/wrap, prepend, filtering, removals | `EventBus::on_dynamic`, `on_dynamic_async`, `on_dynamic_waterfall`, `emit_dynamic`, `parallel_dynamic`, `serial_dynamic`, `bail_dynamic`, `waterfall_dynamic` |
| Loader | all 10: unordered dependencies, duplicates, ordered patches, config updates, replacement, rollback failures, groups, persistence | `Loader::load`, `replace`, `update`, `persist`, `unload` |

Before comparison, the runner recursively canonicalizes JSON object keys, preserves array/event order, and emits only present optional trace fields. Lifecycle, service, and event compare every normalized record in sequence; an extra, missing, or changed record is `TRACE_MISMATCH`. An empty expected trace is rejected as `INVALID_FIXTURE` outside Loader.

The dedicated `service-registration.json` fixture additionally executes three strict cases: same-owner duplicate rejection, different-owner duplicate rejection, and dispose-then-reprovide. Both runners expose one normalized observation per case; `provide` is intentionally non-overwriting and `replace` is the explicit overwrite operation.

Before comparison, the runners recursively canonicalize JSON object keys, preserve array/event order, and emit only present optional trace fields. Lifecycle, service, event, and Loader catalogs compare every normalized record in sequence; an extra, missing, or changed record is `TRACE_MISMATCH`. Empty expected traces are rejected by the runner's fixture-specific validation and cannot be treated as passes.

Event, service, lifecycle, Loader dependency lifecycle, and Loader catalog now compare real observations for every declared case. The Loader catalog has ten observations and the dependency fixture has five; both are differential passes, not execution-only baselines.

The existing runtime-model differences remain accepted: JavaScript `Proxy`/prototype mechanics and arbitrary `!!js` are not Rust core behavior. Legacy `!!js` remains at the Legacy Node boundary; they do not alter the normalized catalog traces.

## Executable full-catalog check

```sh
cargo test -p tessivum-core --test conformance
```

The check starts the conformance binary for six equal fixtures and both approved-difference fixtures, requires Rust `PASS`, and asserts every declared case executed. Empty baselines still fail. Bun tests assert each equal observation, corrupted-baseline rejection, and the exact three upstream retained-reference differences. Raw pending-config and retained-reference oracle commands deliberately remain nonzero `MISMATCH`; the difference ledger does not change runner exit semantics.

## C2 Native and real Rust WASM — 2026-10-14

Validated on macOS ARM64 with Rust 1.97.1 and Extism 1.30.0, based on
`08d87f93fe92c7dbe74446bc7e5f563c687da84a`. This is source acceptance, not a release.

| Contract | Executed evidence |
|---|---|
| Native lifecycle | `tests/native.rs`: 16 passed; dependency waiting/arrival/removal, optional dependencies, candidate rollback, cancellation, cleanup, stale services, post-unload event silence. `native-minimal` ran provider replacement and consumer reload with zero remaining resources. |
| Failed Native activation recovery | `dependency_change_retries_failed_native_activation` timed out before the fix, then passed. The next ready dependency notification now both resets and retries the failed fiber; repeated observation alone still does not retry. |
| Native cancellation handoff | The deterministic both-order token-publication regression fails before and passes after the fix. Publishing a replacement fiber token after disposal began cancels it immediately, outside the control mutex. |
| Rust ABI | `tests/real_guest.rs` uses the compiled Rust guest and real Extism: manifest/ABI rejection, malformed/unknown-field requests, five exports, context and payload, invalid/mismatched/ambiguous responses, redacted guest errors and trap. |
| Host Functions | All eight PDK calls execute over WASM memory with stateful logging, config, arithmetic service, subscription/event and KV handlers. Undeclared permissions, absent grants and revoked grants fail without the corresponding host mutation. |
| Resource/security limits | The same allocation and output succeed with sufficient limits, then fail with lower limits. Infinite-loop timeout and cancellation finish within one second, independently of fuel or the five-second backstop; cancellation follows a real guest readiness callback. A loopback listener stays open throughout the network probe and observes no connection. Default WASI/network settings remain disabled; no sandbox policy was loosened. |
| Update/unload | Committed config changes after update; all five lifecycle exports observed through guest logging; stopped instance and disposed runtime handle reject further work. Product policy generations are separately verified by the consumer. |

Core workspace/all-targets: **132 passed**; the six artifact-dependent matrix tests
are ignored by that general command and **all six passed separately** with the
explicit compiled artifact. Strict Clippy passed. `runtime-smoke` also ran actual
Native, Extism WASM and Legacy Node activation and teardown. The guest SHA-256 is
`97778fbcdd77c079263be5aba4627de10c732a1eebbbd25c069b4e9927e7feb7`.

```sh
cargo build --locked --release --target wasm32-unknown-unknown -p tessivum-wasm-minimal
TESSIVUM_WASM_GUEST="$PWD/target/wasm32-unknown-unknown/release/tessivum_wasm_minimal.wasm" \
  cargo test --locked -p tessivum-extism --test real_guest -- --ignored --nocapture
TESSIVUM_WASM_GUEST="$PWD/target/wasm32-unknown-unknown/release/tessivum_wasm_minimal.wasm" \
  cargo run --locked -p tessivum-runtime-smoke
cargo run --locked -p tessivum-native-minimal
```

CI invokes the matrix after compilation; a missing artifact is a failure, never
a silent success. Native remains fully trusted with cooperative cancellation;
host capability handlers need their own deadlines/quotas. This does not certify
TS/JS, Go, Python, other platforms, product profiles or release archives. Full
local command output, including the initial failing regression and corrected
test/lint failures, is retained in product `test-artifacts/c2-complete/`.

## W2 TypeScript/JavaScript WASM — 2026-10-14

Validated on macOS ARM64 against the C2 source baseline
`38032f769a8a558d2fb18b9cc171ced18bd1c342`, using extism-js release v1.7.0,
Binaryen 133 and the locked TypeScript example dependencies. Reproduction and
compiler archive checksums are in [PROTOCOLS.md](PROTOCOLS.md#building-the-typescriptjavascript-guest).

- The original compiled example loaded under the unchanged no-WASI Host but
  failed init with `PROTOCOL_INVALID` because of its top-level `ok` field.
  The corrected guest uses exact request/response envelopes and arbitrary JSON
  context; invalid requests produce structured guest errors.
- Review exposed a shared error-phase defect: lifecycle Host denials were
  forwarded with phase `call`, producing `PROTOCOL_INVALID` in both real TS
  and Rust guests. The TS lifecycle helper and Rust PDK dispatch boundary now
  assign the invoked export phase. The shared matrix reproduces the original
  failure and verifies denied init plus revoked call/event/update/stop produce
  `GUEST_REJECTED`, without Host mutations or committing rejected config.
- All six shared real-guest matrix tests passed with
  `TESSIVUM_GUEST_LANGUAGE=typescript`, including all eight memory-based Host
  Functions, state mutations, permission denial/revocation, five exports,
  strict envelopes, traps, limits, cancellation, update and stale-handle teardown.
  The TypeScript-only `Date.now()` probe traps; the live loopback network probe
  receives no connection. Production Host limits and WASI/network policy did not change.
- The actual `runtime-smoke` program loaded and disposed Native, compiled TS
  Extism WASM and Legacy Node runtimes. The Rust artifact passed the same six
  checks again; Core workspace/all-targets passed 132 tests and strict Clippy.
- The full suite exposed an existing startup-log fixture race: its peer closed
  immediately after Ready, allowing EOF before handshake observation. The test
  now retains the peer until the client observations finish, matching the adjacent
  response test. No bridge implementation or assertion was weakened.

Accepted TS artifact SHA-256:
`2304cedcd85c87aae2ba12456bdfda8422c042e2d9c8136efa7c2270aa0a1f39`
(2,494,018 bytes); rebuilt Rust artifact:
`82f5012ad10c7627c448d6b65fad48aee915602f91ccb50d112e52e0b8e65ca0`.
Before the phase correction, two identical-source TS builds produced different
hashes (`ae3fea79…` and `9f52226c…`) despite passing the then-existing checks.
The upstream Wizer snapshot is not byte-deterministic. The full build hashes
and failure logs remain in the evidence; only the final artifacts above passed
the expanded lifecycle-denial matrix.

CI now separately compiles TS with checksum-pinned Linux tools and explicitly
runs the matrix. That configuration is not a claim of an observed remote CI run.
Local full logs and the accepted artifact are retained in product
`test-artifacts/w2-complete/`. Product same-candidate consumption is recorded in
its scope checklist. Go/Python, product profiles and release-platform archives
remain outside W2 acceptance.

## W3 Go guest acceptance — 2026-10-14

On macOS ARM64, Go 1.26.9, TinyGo 0.43.0, go-pdk 1.1.3 and Binaryen 133
produced a 308,147-byte guest with SHA-256
`d9fa8934c265e64a15a3f5bab899e8eaf6f8eb03bacb737778a7ca8b22cdc42b`.
All six shared real-guest checks passed, including the new case-variant and null
request-ID rejection cases. The previous struct decoder accepted `RequestId`;
that regression failed before exact-key decoding and passed afterward.
Rust and TypeScript artifacts also passed all six expanded checks.

The real Native/Extism/Legacy runtime smoke passed. Go's memory probe uses a
real PDK linear-memory allocation with page writes and explicit release; the
same 16 MiB request succeeds under defaults and fails at 128 pages. A Go slice
of that size exhausted the unchanged fuel budget while marking GC metadata,
even after bulk-memory lowering; that compiler route was not retained.
No WASI imports or network grants were added. Commands are documented in
[`PROTOCOLS.md`](PROTOCOLS.md#building-the-go-guest). Python and final product
same-candidate acceptance are separate and are not implied by these Go results.

## W3 Python guest acceptance — 2026-10-14

On macOS ARM64, MicroPython 1.26.1 with mpy-cross 1.26.1.post2, WASI SDK 34.0
and Binaryen 133 produced a 223,400-byte artifact with SHA-256
`c57a9f24191596eab8ae7971f7d0c430138b3f30e9ff65b7f54e19fb25adebd4`.
All six shared real-guest checks passed explicitly, followed by the actual
Native/Python-Extism/Legacy runtime smoke. Rust, TypeScript and Go also passed
all six checks with the exception-enabled engine; Host defaults and the original
resource-limit probes remain unchanged, with no sandbox permission added.

The accepted implementation embeds upstream MicroPython and executes compiled
Python bytecode. Runtime source compilation exceeded the default fuel budget;
the retained build compiles ahead of time instead. SDK legacy exception
instructions were translated to standardized exnref instructions and the existing
Extism exception feature enabled. Sustained execution initially trapped with
out-of-bounds memory because conservative GC could not see WASM local roots.
Binaryen `--spill-pointers` fixes that path, and the same timeout/cancellation
matrix now passes. The SDK's implicit wasm-opt step is disabled so stack names
remain available until explicit root spilling and final stripping.

Review regressions first reproduced reuse after interruption and invalid UTF-8
from a valid escaped surrogate pair. A shared export-entry latch now makes hard
Python failures terminal: later exports trap without re-entering the damaged VM;
stop still drops the instance and invalidates authority. Paired surrogate escapes
are normalized, lone surrogates rejected, and escaped literal backslashes preserved.
The matrix covers trap/fuel/timeout/cancel followed by call and stop, plus Unicode
round trips. An additional live-object `gc.collect()` workload and subsequent
event/update/stop pass with an explicit 100,000,000 fuel budget; it exceeded the
default 10,000,000 budget. This extra GC-safety check does not replace or relax
the original default-budget lifecycle/memory/output and reduced-fuel probes.

The real 16 MiB allocation/page-write/free probe uses Extism-owned memory;
it succeeds under defaults and fails at 128 pages. The separate 768 KiB
interpreter heap does not promise large Python bytearrays within the fuel budget.
Files, clock calls, external imports and stdout are unavailable. This is
MicroPython support, not arbitrary CPython/pip/C-extension compatibility.
Toolchain hashes and commands are in
[`PROTOCOLS.md`](PROTOCOLS.md#building-the-python-guest) and the example lock file.
Failed builds and runtime diagnostics are retained in product
`test-artifacts/w3-complete/`; they are not supported alternate build routes.
Product same-candidate acceptance is recorded separately in its scope checklist.
