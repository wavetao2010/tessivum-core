# Conformance Report

**Report schema:** `tessivum.differential-report/v1`  
**Recorded:** 2026-08-17T10:19:40Z  
**ABI:** `cordis.plugin/v1` · **bridge:** `cordis.node/v1`  
**Repository source release:** `0.1.1` · all runtime crates remain `publish = false`; this is not a crates.io package release.

The differential oracle evidence below was recorded for the `0.1.0` release on
2026-08-17 and remains the compatible `tessivum.conformance/v1` baseline. The
`0.1.1` release reran all 111 Rust workspace tests and refreshed
[`BENCHMARK_RESULTS.json`](BENCHMARK_RESULTS.json) on 2026-08-24; it does not
claim a new TypeScript oracle recording.


The machine-readable evidence is [`../fixtures/conformance/differential-report.json`](../fixtures/conformance/differential-report.json).

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

The Loader result is an oracle limitation: `loader/loader-catalog is not supported by the Cordis oracle`; its empty baseline records that no cross-runtime trace exists.

## Rust runner

`crates/tessivum-core/src/bin/conformance.rs` executes **every** `input.cases` record through public core APIs. It emits `executedCases` in fixture order only after every case succeeds; an unknown, missing, or failing case is `EXECUTION_ERROR`.

| Domain | Declared cases exercised | APIs exercised |
|---|---|---|
| Lifecycle | all 12: start, cleanup, nested/reverse disposal, repeated/reentrant disposal, rollback, quiescence, unload rejection | `Fiber::start`, `Fiber::dispose`, `Scope::child`, `Scope::add_effect`, `Scope::dispose` |
| Service | all 9: late/optional dependency, replacement/removal/recovery, isolated/shared realms, intercepts, stale generations | `ContextHandle::subscribe`, `provide`, `get`, `isolate`, `intercept`, `Scope::dispose` |
| Event | all 11: emit, parallel aggregation, serial/synchronous bail, waterfall next/short-circuit/wrap, prepend, filtering, removals | `EventBus::on_dynamic`, `on_dynamic_async`, `on_dynamic_waterfall`, `emit_dynamic`, `parallel_dynamic`, `serial_dynamic`, `bail_dynamic`, `waterfall_dynamic` |
| Loader | all 10: unordered dependencies, duplicates, ordered patches, config updates, replacement, rollback failures, groups, persistence | `Loader::load`, `replace`, `update`, `persist`, `unload` |

Before comparison, the runner recursively canonicalizes JSON object keys, preserves array/event order, and emits only present optional trace fields. Lifecycle, service, and event compare every normalized record in sequence; an extra, missing, or changed record is `TRACE_MISMATCH`. An empty expected trace is rejected as a mismatch outside Loader.

## Differential result

Lifecycle, service, and event require exact equality with their current `PASS` oracle traces. Loader has an explicitly empty Oracle baseline because its current Oracle result is `UNSUPPORTED_SCENARIO`; after all ten Loader cases execute successfully, Rust reports `PASS` as an execution baseline, never as an accepted `MISMATCH`.

The existing runtime-model differences remain accepted: JavaScript `Proxy`/prototype mechanics and arbitrary `!!js` are not Rust core behavior. Legacy `!!js` remains at the Legacy Node boundary; they do not alter the normalized catalog traces.

## Executable full-catalog check

```sh
cargo test -p tessivum-core --test conformance
```

The release gate starts the conformance binary for all four catalog fixtures, requires `PASS` and successful exit status for every domain, and asserts `executedCases` exactly equals each fixture's declared case IDs. It also mutates a Lifecycle fixture to an empty expected trace and requires `MISMATCH`, so only the explicitly unsupported Loader Oracle baseline receives execution-only `PASS` treatment.
