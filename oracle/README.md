# M0 TypeScript Oracle

The oracle is the M0 reference executor for shared conformance fixtures. It runs the Harness-vendored Cordis baseline, normalizes observable behavior, and compares the resulting ordered trace with `expectedTrace`.

## Run

From the repository root:

```sh
bun run oracle/run.ts fixtures/conformance/<fixture>.json
```

Use one fixture per invocation. A supported `lifecycle`, `service`, or `event` scenario produces the normalized trace or a fixture-specific mismatch. A `loader` fixture returns `UNSUPPORTED_SCENARIO`: Loader is intentionally outside M0.

The Rust companion command is intentionally not an oracle in M0:

```sh
cargo run -p tessivum-core --bin conformance-runner -- fixtures/conformance/<fixture>.json
```

It validates the fixture schema and returns `NOT_IMPLEMENTED`.

## Contract

Fixtures use `schemaVersion: "tessivum.conformance/v1"`, a unique `name`, a supported domain, a scenario, optional scenario input, and an ordered `expectedTrace`. Trace comparison uses fixture-local identifiers and removes runtime IDs, stack traces, object identity, timing, and scheduler-dependent top-level cleanup order.

The event vocabulary is fixed: `fiber-created`, `fiber-state-changed`, `service-provided`, `service-removed`, `listener-added`, `listener-removed`, `event-dispatched`, `effect-created`, `effect-disposed`, `plugin-error`, `config-committed`, and `config-rolled-back`. See [`../docs/CONFORMANCE.md`](../docs/CONFORMANCE.md) for field normalization, Cordis mode rules, source anchors, and the versioned Harness patch ledger.

## Baseline

- Cordis: `47f943859bef60e4160492346772ded9b24f765a`
- DeepSeek Harness: `8cc9e33fab69e2d0476d126baaf2acb24e6a6ab4`
- Harness vendored Cordis: `56b3d4f725681cf4556c1a8695a709cc3b6eed74`

M0 keeps lifecycle behavior equal to that baseline. It deliberately does not copy JavaScript Proxy/prototype behavior and does not evaluate arbitrary `!!js`; future configuration uses safe expressions, with legacy JavaScript delegated to the Legacy Node Loader.
