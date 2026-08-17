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
