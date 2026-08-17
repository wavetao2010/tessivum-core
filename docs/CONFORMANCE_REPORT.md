# Conformance Report

**Report schema:** `tessivum.differential-report/v1`  
**Recorded:** 2026-08-17T10:19:40Z  
**ABI:** `cordis.plugin/v1` · **bridge:** `cordis.node/v1`

The machine-readable evidence is [`../fixtures/conformance/differential-report.json`](../fixtures/conformance/differential-report.json).

## Current oracle evidence

The current TypeScript oracle was invoked from `oracle/` with:

```sh
bun run oracle -- ../fixtures/conformance/<fixture>.json
```

| Fixture | SHA-256 | Oracle result | Normalized events |
|---|---|---:|---:|
| `lifecycle.core-catalog` | `8ee0d6dcd58f593700a5c051bf925eac7edbca365175a86c7413b0b4325b1973` | `PASS` | 7 |
| `service.core-catalog` | `4a1dc2e17242d6398f9ea9097dd05e442ab26423d1b5214b5c3de26795cca8dc` | `PASS` | 14 |
| `event.core-catalog` | `4752e8d81375f51678e5f5e34e7ddaafff0bdfa41cc2418342113aab4576279c` | `PASS` | 6 |
| `loader.core-catalog` | `558f11c374fce2867b51b5ff4ee2e4dec34aa83188a66fdaeec3ef6c99432d22` | `UNSUPPORTED_SCENARIO` | — |

The Loader result is an oracle limitation: `loader/loader-catalog is not supported by the Cordis oracle`. It is not evidence of Rust equivalence or failure.

## Rust runner

`crates/tessivum-core/src/bin/conformance.rs` executes each supported catalog scenario using public core APIs:

| Domain | Scenario | APIs exercised |
|---|---|---|
| Lifecycle | `effect-dispose` | `Fiber::start`, `Scope::add_effect`, `Fiber::dispose` |
| Service | `isolate-realm` | `ContextHandle::provide`, `ContextHandle::isolate`, `ContextHandle::get`, `Fiber::dispose` |
| Event | `emit` | `EventBus::on_dynamic`, `EventBus::emit_dynamic`, `ListenerHandle::remove` |
| Loader | `loader-catalog` | `Loader::load`, `Loader::replace`, `Loader::update`, `Loader::persist`, `Loader::unload` |

The Loader catalog executes all ten declared cases: unordered dependencies, duplicate IDs, ordered mutations, config-only updates, replacement, candidate and rollback failures, nested groups, later patch targeting, and atomic persistence.

Before comparison, the runner recursively canonicalizes JSON object keys, preserves array/event order, and emits only present optional trace fields. It compares all normalized records in sequence; an extra, missing, or changed record is `TRACE_MISMATCH`. Supported paths never return `NOT_IMPLEMENTED`.

## Differential result

Lifecycle, service, and event have a current `PASS` oracle trace and are expected to compare equal. Loader is intentionally different:

- The fixture has a reserved empty trace because the current oracle does not execute Loader.
- Rust deliberately emits Loader transaction observations (`config-committed` and `config-rolled-back`) after executing real Loader operations.
- Loader returns `PASS` with its actual Rust transaction trace. Because the Cordis oracle has no Loader implementation, this is an execution result rather than a cross-runtime equality claim.

The existing runtime-model differences remain accepted: JavaScript `Proxy`/prototype mechanics and arbitrary `!!js` are not Rust core behavior. Legacy `!!js` remains at the Legacy Node boundary; they do not alter the normalized catalog traces.

## Executable full-catalog check

```sh
cargo test -p tessivum-core --test conformance
```

The test starts the conformance binary for every catalog fixture and requires `PASS` for lifecycle, service, event, and Loader. On 2026-08-17 the four catalogs were executed successfully; lifecycle, service, and event also match the observed Cordis oracle traces, while Loader retains an explicit oracle comparison gap.
