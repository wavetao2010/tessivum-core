# TypeScript Conformance Oracle

The oracle runs the Harness-vendored Cordis and Loader baseline, normalizes observable behavior, and compares the resulting ordered trace with `expectedTrace`. C1 covers lifecycle, services, events, Loader dependency lifecycle, and the ten-case Loader catalog.

## Run

From the repository root:

```sh
bun run oracle/src/index.ts fixtures/conformance/<fixture>.json
```

Use one fixture per invocation. Supported scenarios are `lifecycle/lifecycle-catalog`, `lifecycle/effect-dispose`, `service/service-catalog`, `service/registration`, `service/isolate-realm`, `event/event-catalog`, `loader/dependency-lifecycle`, and `loader/loader-catalog`. The former `event/emit` scenario remains unsupported, not an alias.

Run the Rust companion against the same fixture:

```sh
cargo run -p tessivum-core --bin conformance -- fixtures/conformance/<fixture>.json
```

`loader-dependencies.json` runs real upstream Loader plugins and Rust Native plugins through waiting, provider arrival, removal, restoration, and unload. `loader.json` runs real upstream Loader/Group/Include cases against Rust's public Loader API: dependencies, duplicate IDs, ordered patches, config updates, replacement, candidate and rollback failures, nested groups, later patch targeting, and atomic persistence. Both compare normalized observations at settled boundaries.

`loader-pending-config.json` separately probes config replacement while dependencies are missing, late activation, failed candidate rollback, recovery, and unload. It requires the latest config and removal of obsolete fields. Rust passes; the frozen upstream returns `TRACE_MISMATCH` on first activation (old config survives). Latest-config semantics were explicitly approved on 2026-10-14 as `pending-config-uses-latest-source`; raw output is not relabeled PASS. See `../docs/CONFORMANCE_REPORT.md` for remaining C1 gates, including unmeasured constants in the historical service oracle.

`events.json` executes all 11 declared cases on both sides: emit order, parallel error aggregation, serial/synchronous bail, waterfall next/veto/wrapping, prepend, scoped filtering, self-removal, and owner disposal during dispatch. Each case emits one `event-dispatched` observation with its case ID, mode, and actual calls/results. Only parallel completion labels are sorted; ordered callbacks and dispatch snapshots retain their order. Empty, malformed, or unknown cases are errors. Run the ignored-case regression with `bun test oracle/src/scenarios.test.ts` from the Core root.

Source resolution is relative to this module by default (`../upstream/deepseek-harness/vendor` from the Core root), not to the working directory. For another checkout location, set `CORDIS_VENDOR_ROOT` to the pinned Harness `vendor` directory (or its `cordis` child). Loader sources are read from the sibling `loader` directory. Install the upstream workspace dependencies first; the override selects a checkout, it does not verify its revision.

## Contract

Fixtures use `schemaVersion: "tessivum.conformance/v1"`, a unique `name`, a supported domain, a scenario, optional scenario input, and an ordered `expectedTrace`. Trace comparison uses fixture-local identifiers and removes runtime IDs, stack traces, object identity, timing, and scheduler-dependent top-level cleanup order.

The event vocabulary is fixed: `fiber-created`, `fiber-state-changed`, `service-provided`, `service-removed`, `listener-added`, `listener-removed`, `event-dispatched`, `effect-created`, `effect-disposed`, `plugin-error`, `config-committed`, and `config-rolled-back`. See [`../docs/CONFORMANCE.md`](../docs/CONFORMANCE.md) for field normalization, Cordis mode rules, source anchors, and the versioned Harness patch ledger.

## Baseline

- Cordis: `47f943859bef60e4160492346772ded9b24f765a`
- DeepSeek Harness: `8cc9e33fab69e2d0476d126baaf2acb24e6a6ab4`
- Harness vendored Cordis: `56b3d4f725681cf4556c1a8695a709cc3b6eed74`

M0 keeps lifecycle behavior equal to that baseline. It deliberately does not copy JavaScript Proxy/prototype behavior and does not evaluate arbitrary `!!js`; future configuration uses safe expressions, with legacy JavaScript delegated to the Legacy Node Loader.
