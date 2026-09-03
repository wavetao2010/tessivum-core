# Benchmarks

`cargo run -p tessivum-bench --release` runs the Core release benchmark executable. It
uses the public Core, Extism, and Node Bridge APIs; it does not use test seams
or synthetic transport responses.

## Prerequisites and command

The WASM cases require the real Rust guest from `examples/wasm-minimal`. Build
that guest for `wasm32-unknown-unknown`, then pass the resulting module to the
benchmark. The Node cases start the checked-in Bun compat host and load the
checked-in legacy function fixture. Bun and the vendored Cordis tree must be
available; set `CORDIS_VENDOR_ROOT` to the vendor directory when the host
cannot discover it from its normal locations.

```sh
cargo build -p tessivum-wasm-minimal --target wasm32-unknown-unknown --release
CORDIS_VENDOR_ROOT=/absolute/path/to/deepseek-harness/vendor \
  cargo run -p tessivum-bench --release -- \
  --wasm target/wasm32-unknown-unknown/release/tessivum_wasm_minimal.wasm
```

The executable writes one JSON document on success. It accepts these bounded
options:

- `--samples`: number of independent samples, from `1` through `100` (default
  `15`);
- `--batch`: operations in each throughput sample, from `1` through `1024`
  (default `256`);
- `--wasm`: required path to the built Rust guest module;
- `--node-host`: Bun executable or path (default `bun`).

A Node request has a two-second request timeout. Host handshake and graceful
shutdown each have a five-second timeout. The Node batch queue, loader tree,
and root-disposal tree are also fixed-size and bounded by the executable.

## Paired Cordis comparison

`fixtures/benchmarks/core-paired.json` freezes the shared workload used by the
Rust driver and the TypeScript Cordis `4.0.1` oracle. The paired runner starts a
fresh process for every sample, alternates A/B launch order, rejects partial or
misordered case sets, retains every failed run, and reports raw samples plus
median, p95, min, and max:

```sh
cargo build --locked --release -p tessivum-bench
python3 scripts/run_paired_benchmarks.py \
  --rust-bin target/release/tessivum-bench \
  --cordis-root ../upstream/deepseek-harness/vendor \
  --workload fixtures/benchmarks/core-paired.json \
  --samples 30 \
  --raw-out core-paired.json
```

The paired cases are Scope create/dispose, Service lookup, Event emit,
16-entry Loader load/update, 32-child root disposal, live and post-disposal
process PSS, and observable residue after disposal. PSS is read only from Linux
`/proc/self/smaps_rollup`; other platforms emit `status: "unavailable"` with no
numeric substitute. Use `tessivum/benchmarks/run-linux-container.sh` for the
pinned Ubuntu 24.04 environment and the full Core + product run.

## Published Alpha.23 result

The checked-in [`BENCHMARK_RESULTS.json`](BENCHMARK_RESULTS.json) is the
30-sample fixed Ubuntu 24.04 arm64 publication run from 2026-09-03. It compares
tessivum-core commit `cedbeb9e1607056845b69e09b825eb7f5be67a69` with
`@deepseek-ai/cordis` 4.0.1 at DeepSeek Harness commit
`47f943859bef60e4160492346772ded9b24f765a`. SHA-256:
`325f9b16352263f17d0b04b629cc22a1c6ec73adbde0eacb6882caf51485d69c`.

| Workload | tessivum-core median / p95 | TypeScript Cordis median / p95 | Median result |
|---|---:|---:|---:|
| 1,000 scope create/dispose cycles | 0.834 / 0.882 ms | 20.021 / 28.363 ms | **24.02× faster** |
| Service lookup | 23.362 / 24.002 M ops/s | 1.111 / 1.186 M ops/s | **21.03× throughput** |
| Event emit | 10.723 / 12.118 M ops/s | 0.404 / 0.432 M ops/s | **26.54× throughput** |
| Load 16 entries | 0.445 / 0.556 ms | 2.154 / 2.431 ms | **4.85× faster** |
| Update 16 entries | 21.085 / 33.537 ms | 0.534 / 0.590 ms | **39.49× slower** |
| Dispose root with 32 children | 0.069 / 0.081 ms | 0.217 / 0.250 ms | **3.15× faster** |
| Peak process PSS | 4.59 / 4.59 MiB | 79.98 / 80.57 MiB | **17.43× lower** |
| Live registrations after disposal | 0 / 0 | 0 / 0 | equal; no residue |

The previous three-sample Rust-only Darwin record remains in
[`BENCHMARK_RESULTS.historical.json`](BENCHMARK_RESULTS.historical.json) for
audit history. It is non-comparative and must not be used as a public claim.
The paired result does not measure complete products, LLM latency, quality or
multi-user saturation.

## Measurements

Every result has `name`, `unit`, `operationsPerSample`, raw `samples`,
`median`, and `p95`. `median` is the upper middle sorted sample; `p95` uses the
nearest-rank sorted sample. The top-level JSON includes `environment.uname`,
`environment.rust`, `environment.profile`, `sampleCount`, `batchSize`, and the
active `cordis.plugin/v1` and `cordis.node/v1` ABI names.

The executable reports these cases:

| Name | Unit | Measured public behavior |
| --- | --- | --- |
| `context_fiber_memory_proxy` | bytes | The shallow `size_of::<ContextHandle>() + size_of::<Fiber>()` layout. It is explicitly a handle-layout proxy, not allocator or RSS profiling. |
| `native_lifecycle` | ns | A direct `NativePluginInstance` start, update, and dispose sequence for a no-op native plugin. |
| `service_lookup` | operations/s | Batched typed `ContextHandle::get` and current `ServiceHandle::with` calls. |
| `event_emit` | operations/s | Batched typed `EventBus::emit` calls to one registered listener; the listener count is checked after each sample. |
| `wasm_cold_call` | ns | Package validation, real `ExtismGuestEngine` instantiation, guest `cordis_init`, and the first `cordis_call`. |
| `wasm_warm_call` | ns | Repeated `cordis_call` calls on one initialized real Extism guest instance. |
| `node_bridge_cold` | ns | Bun compat-host process start and framed Bridge handshake; shutdown is performed after timing. |
| `node_bridge_warm` | operations/s | Calls to the loaded legacy function fixture's `legacy.function.inspect` service over one live Bridge connection. |
| `node_bridge_batch` | operations/s | A bounded set of `BridgeClient::begin_request` service calls followed by correlated waits on one live connection. |
| `loader_tree` | ns | `Loader::load` for a fixed native tree with sixteen entries. |
| `loader_update` | ns | `Loader::update` with `Patch::UpdateConfig` against that loaded tree. |
| `root_dispose` | ns | Disposal of a root `ContextHandle` that owns thirty-two child scopes. |

Setup and cleanup that would make repeated samples unsafe are outside the
reported interval unless they are the stated cold-path behavior. A failed guest,
host, transport, or lifecycle operation stops the executable with a JSON error
instead of emitting a partial benchmark result. The document intentionally
contains no baseline numbers: compare only measurements captured on the target
environment.
