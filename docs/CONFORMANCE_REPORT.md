# Conformance Report

**Report schema:** `tessivum.differential-report/v1`  
**Recorded:** 2026-08-17T10:19:40Z  
**ABI:** `cordis.plugin/v1` · **bridge:** `cordis.node/v1`  
**Repository source release:** `0.1.4` · all runtime crates remain `publish = false`; this is not a crates.io package release.

The original differential evidence below was recorded for the `0.1.0` release on 2026-08-17. Its hashes and counts are historical snapshots, not hashes of today's fixtures. Later releases through `0.1.4` retained that recorded baseline; the C1 event and Loader catalog cutovers below extend the same `tessivum.conformance/v1` contract without changing production event or Loader APIs.


The machine-readable evidence is [`../fixtures/conformance/differential-report.json`](../fixtures/conformance/differential-report.json).

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

The check starts the conformance binary for all six fixtures, requires `PASS` and successful exit status, and asserts `executedCases` equals each fixture's declared IDs. It also mutates a Lifecycle fixture to an empty expected trace and requires `INVALID_FIXTURE`. The Bun regression executes lifecycle, service, event, and Loader catalog cases independently, rejects changed observations, and rejects invalid case lists.
