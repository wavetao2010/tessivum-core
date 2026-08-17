# Legacy Node minimal

The Legacy Node path is the real Bun compat host and the public Rust bridge API. Its framed stdin/stdout protocol is `cordis.node/v1`; it is not a line-oriented command-line interface.

`fixtures/legacy/function-plugin.ts` is the runnable function-form Cordis plugin used by the smoke. It reads its own source through Node APIs, provides `legacy.function`, receives `legacy.event`, and returns an async disposer. The smoke starts `node/compat-host/src/index.ts` with `NodeSupervisor`, loads that plugin through `LegacyNodeRuntime` and `Loader`, verifies the host snapshot, unloads it, and then reaps the host.

## Run the end-to-end smoke

Prerequisites:

- Bun 1.3.14 or newer.
- A Cordis vendor root containing `cordis/src/index.ts` and `cosmokit/src/index.ts`. Set `CORDIS_VENDOR_ROOT` when the host cannot discover it from its checkout.
- Rust's `wasm32-unknown-unknown` target, because the smoke loads the compiled Rust guest through Extism.

```sh
export CORDIS_VENDOR_ROOT=/absolute/path/to/vendor   # omit when auto-discovery works
rustup target add wasm32-unknown-unknown
cargo build -p tessivum-wasm-minimal --target wasm32-unknown-unknown
cargo run -p tessivum-runtime-smoke
```

The default guest path is `target/wasm32-unknown-unknown/debug/tessivum_wasm_minimal.wasm`. To use another compiled guest artifact, provide its exact path:

```sh
TESSIVUM_WASM_GUEST=/absolute/path/to/tessivum_wasm_minimal.wasm \
  cargo run -p tessivum-runtime-smoke
```

A successful run asserts Native start/stop, Extism guest initialization and stop through `cordis.plugin/v1`, Legacy plugin activation and disposal, root-scope disposal, and reaped Node-host ownership.
