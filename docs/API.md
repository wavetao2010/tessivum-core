# Public API

This document describes the Rust APIs exported by the four crates in this
workspace at repository source release `0.1.6`. The public boundary is intentionally small:
Core owns lifecycle, services, events, and the transactional Loader; Native,
WASM, and legacy Node are Loader runtimes. Product-domain APIs do not belong
here.

## Crates and runtime boundaries

| Crate | Public role |
| --- | --- |
| `tessivum-core` | `ContextHandle`, `Scope`, `Fiber`, services, events, configuration, Loader, and in-process Native runtime. |
| `tessivum-extism` | `cordis.plugin/v1` package validation, Extism instance management, limits, and capability authorization. |
| `tessivum-pdk` | Rust guest SDK for `cordis.plugin/v1`. |
| `tessivum-node-bridge` | `cordis.node/v1` framing, checked request client, process supervisor, and legacy Node Loader runtime. |

All four runtime manifests currently declare version `0.1.6`, license `MIT`, and `publish = false`.
They are released together as a repository source release, not as independently
published crates.io registry packages.

## Core ownership and lifecycle

`Scope` is the ownership root. `Scope::root()` creates an active scope;
`child()` creates a child only while its parent is active. Register each
resource through `Scope::add_effect(label, BoxDisposer)`. An effect is a
one-shot, asynchronous `FnOnce` that returns `Result<(), CoreError>`.

`Scope::dispose().await` is idempotent. The first caller changes the scope to
`Unloading`, cancels its `CancellationToken`, then disposes child scopes and
local effects in reverse registration order. Other callers await the same
completion (a reentrant call from that scope's own cleanup returns without
waiting). Cleanup continues after individual failures and returns
`CoreError::Cleanup` only after all registered cleanup has been attempted.
When a scope is no longer active, new child scopes, effects, service providers,
and event listeners return `CoreError::InvalidState`.

`Fiber::new(&parent, name)` owns a child scope and begins in `Pending`.
`Fiber::start` runs a setup future with that scope. It transitions through
`Loading` to `Active`; a setup error transitions to `Failed` and rolls back all
resources registered by setup. `Fiber::dispose` disposes that child scope and
finishes in `Disposed`. `FiberState` values are `Pending`, `Loading`, `Active`,
`Failed`, `Disposed`, and `Unloading`.

Use `Scope::effects()` and the identifiers `ScopeId`, `FiberId`, `ResourceId`,
and `Generation` for diagnostics. They are runtime identities, not portable
configuration keys. `CancellationToken::{cancel,is_cancelled,cancelled}` is a
cooperative signal; cancellation does not forcibly stop arbitrary Rust code.

## Contexts and services

A `ContextHandle` is an immutable view over a scope, a shared service registry,
and an event bus:

```rust
use tessivum_core::{ContextHandle, ServiceKey};

let context = ContextHandle::root();
let key = ServiceKey::new("example.greeting", "native/v1");
let provider = context.provide(key.clone(), String::from("hello"))?;
let greeting = context
    .get::<String>(&key)?
    .expect("provider was registered");
assert_eq!(greeting.with(|value| value.as_str())?, "hello");
drop(provider);
```

`provide<T>` stores a `Send + Sync + 'static` native value and returns a
`ServiceHandle<T>`. The provider is removed automatically when the owning
context scope disposes. `get<T>` returns `Ok(None)` when absent and
`CoreError::ServiceTypeMismatch` for a key registered with a different native
type. A handle is generation-checked: call `is_current()` or
`with(|value| ...)`; `with` returns `CoreError::StaleServiceHandle` if the
provider was removed or replaced. Do not retain a handle across provider
replacement.

`ServiceKey::new(name, contract_version)` identifies a service contract; its
stable diagnostic form is `name@contract_version`. Version a cross-runtime
service contract here independently of the Core, WASM ABI, and Node bridge
versions.

`ContextHandle::child()` changes ownership only. `isolate(service, None)`
selects a private realm for that service; `isolate(service, Some(label))`
selects a realm shared by views with the same `RealmLabel`. `intercept` adds a
JSON intercept value to the immutable view; `intercept_config` shallow-merges
object intercepts from ancestor to descendant, so the later view wins on the
same key. `snapshot`/`snapshots` return `ServiceSnapshot` diagnostics,
including availability, generation, selected realm, and intercept chain.

For readiness, use `Dependency::{Required, Optional}` and
`ContextHandle::subscribe`. The callback receives `DependencySnapshot` and is
called immediately, then whenever availability changes. Only required entries
contribute to `snapshot.ready`. The subscription is an effect of the context
scope and can also be removed early with `unsubscribe()`.

## Events

`ContextHandle::events()` exposes its scoped `EventBus`. Native event payloads
stay typed with `EventKey<T>`; dynamic boundaries use `DynamicEvent` and
`EventValue`, both `serde_json::Value`.

- Register synchronous listeners with `on`, `once`, `on_dynamic`, or
  `once_dynamic`; register async listeners with the corresponding `*_async`
  method. Registration belongs to the `owner: &Scope` supplied to the call.
- `EventOptions { prepend, global }` places a listener before ordinary
  registrations or makes it visible from every context/realm. Otherwise the
  registration uses the current event filter.
- `emit` invokes eligible synchronous listeners in registration order.
  `bail` returns the first effective value (anything other than JSON `null` or
  `false`). It rejects async and waterfall listeners.
- `parallel` starts eligible listeners, awaits all of them, and aggregates
  failures. `serial` awaits each listener in order and returns the first
  effective value. Both accept async registrations; their return values are
  JSON values.
- `on_waterfall` and `on_dynamic_waterfall` receive a `WaterfallNext<T>`.
  Progress occurs only when its one-shot `next(payload)` is called.

`ListenerHandle::remove()` prevents future dispatches; a dispatch already using
its listener snapshot remains deterministic. Scope disposal also removes the
listener. `diagnostics()` and `take_diagnostics()` expose
`DispatchDiagnostic` records with event name, dispatch mode, listener count,
duration, and terminal outcome.

## Configuration and Loader

`ConfigScope` is the complete, explicit input to configuration evaluation:
`environment`, `platform`, `architecture`, `profiles`, and `services`.
`evaluate_config` resolves embedded `{ "$expr": "..." }` leaves using the
restricted `ConfigExpression` language. It does not execute code or read
undeclared environment variables.

`EntryTree` is the persisted Loader model. It is JSON or YAML with top-level
`entries` and nested `groups`; every entry has the canonical `package` field
and flattened `EntryOptions`:

```yaml
entries:
  - package: acme/greeting
    id: greeting
    runtime: native
    config: { enabled: true }
    inject: [example.clock]
    isolate: [example.cache]
    intercept: { level: info }
```

`runtime` is `native`, `wasm`, or `legacy-node`. `disabled: true` suppresses an
entry; a disabled group suppresses all descendants without removing persisted
configuration. Entry IDs and group IDs are non-empty identifiers and entry IDs
are unique across the full tree. Parsing accepts `module`, `plugin`, and
`source` as input aliases for `package`, and `legacy_node` as an input alias
for `legacy-node`; serialization uses only the canonical forms. `!!js` YAML is
rejected by Core.

Use `parse_entry_tree`, `EntryTree::validate`, `EntryTree::apply`, and
`apply_entry_patches` before loading. `Patch` supports `Insert`, `Update`,
`UpdateConfig`, `Remove`, and `Move`; it is applied to a clone and validated
before it becomes a candidate. `persist_entry_tree`/`EntryTree::persist` write
a same-directory temporary file, sync it, then rename it so a torn write does
not replace the previous document.

A host implements `PackageResolver::resolve(specifier, RuntimeKind)` and
registers one `LoaderRuntime` for each runtime kind with `Loader::try_new`.
A runtime turns a `ResolvedPackage`, `Entry`, and `ContextHandle` into a
`RuntimeHandle`; the handle supplies `activate` and `dispose`.

`Loader::{load,replace,update}` validates and imports a detached candidate,
activates it, then disposes the currently committed tree. It changes its tree
and context only after that process succeeds. Import, activation, and removal
failures produce `LoaderError`, preserve or restore the last committed tree
where possible, and append `config-rolled-back` to `trace()`. A successful
commit appends `config-committed`. `unload` disposes the active entries and
clears the tree only when disposal succeeds.

## Native plugins

Native plugins are trusted, in-process Rust code. Implement `NativePlugin`:

```rust
trait NativePlugin: Send {
    fn descriptor(&self) -> NativePluginDescriptor;
    fn start(&mut self, context: ContextHandle, config: &Value)
        -> NativePluginFuture<'_>;
    fn update(&mut self, context: ContextHandle, config: &Value)
        -> NativePluginFuture<'_>;
    fn stop(&mut self, context: ContextHandle) -> NativePluginFuture<'_>;
}
```

`NativePluginDescriptor` requires a nonblank `name` and `version`, unique
`Dependency` entries, and a `NativeConfigSchema`. The schema supports
`Any`, `Null`, `Boolean`, `Integer`, `Number`, `String`, `Array`, and
`Object { properties, required, allow_additional }`. Both descriptor and
configuration are validated before lifecycle effects. Failures are
`NativePluginError`; structured schema failures include stable paths such as
`$.enabled` and `$[0]`.

`NativePluginInstance::instantiate` creates a pending fiber after validation.
`start()` waits for required descriptor dependencies, then runs in the fiber's
child context. A failed start rolls back that child scope and calls `stop` once
for partial plugin state. `update(config)` validates the candidate, stops the
current instance, creates a fresh fiber, runs `update` and `start`, and commits
the new configuration only after both hooks succeed. `dispose()` stops the
plugin and disposes the fiber. `snapshot()` returns descriptor, committed
configuration, fiber state, scope, and outstanding effects.

For Loader use, register a fresh factory for each package in
`NativePluginRuntime` and pass it as `RuntimeKind::Native`. A duplicate or
blank package registration fails. See `examples/native-minimal` for provider,
consumer, typed event, replacement, reload, and root-disposal use.

## WASM and Extism

The wire contract is `cordis.plugin/v1`; see [PROTOCOLS.md](PROTOCOLS.md) for
the complete manifest and envelope. `tessivum-extism` exports:

- `WasmPackage::{from_bytes,from_manifest_file}` and `PluginManifest` for
  package validation;
- `WasmPluginInstance::{instantiate,init,call,event,update,stop,cancel_current}`
  for a single serialized guest instance;
- `ResourceLimits` for maximum memory pages, timeout, fuel, exactly one
  concurrent call, and input/output byte limits;
- `CapabilityRegistry`, `Capability`, `CapabilityRequest`, and
  `CapabilityHandler` for host-provided functions; and
- `WasmPluginRuntime` as the `RuntimeKind::Wasm` Loader adapter.

`WasmPackage::from_manifest_file` reads the manifest and entry through bounded single-open file descriptors: manifests are limited to 256 KiB, modules to 8 MiB, both must be regular files, and Unix final-component symlinks are rejected. `from_bytes` applies the same 8 MiB module admission bound.

A `CapabilityRegistry` is deny-by-default. A guest call reaches a handler only
when its manifest declares the capability, the registry grants it, and a
handler is registered. The v1 capability names are `cordis.log`,
`cordis.config.get`, `cordis.service.call`, `cordis.event.emit`,
`cordis.event.subscribe`, `cordis.registration.dispose`, `cordis.kv.get`, and
`cordis.kv.set`. The Core crate supplies neither domain capability handlers nor
persistent storage policy.

For Rust guests, implement `tessivum_pdk::Guest` and use
`export_guest!(GuestType)`. It exports all five required functions:
`cordis_init`, `cordis_call`, `cordis_event`, `cordis_update`, and
`cordis_stop`. The PDK helpers `log`, `config_get`, `service_call`,
`event_emit`, `event_subscribe`, `registration_dispose`, `kv_get`, and `kv_set`
marshal JSON through Extism host functions. They return
`tessivum_pdk::Result`; on a non-`wasm32` build, these helpers return
`unsupported_runtime` rather than silently using a host substitute. See
`examples/wasm-minimal`.

## Legacy Node bridge

`tessivum-node-bridge` is a Loader runtime for an externally spawned Bun
compat-host. `Frame`, `FrameCodec`, and `FrameKind` expose the bounded
`cordis.node/v1` transport; `BridgeClient` handles checked requests and
cancellation; `NodeSupervisor` owns one host process and its generation-wide
cleanups; `LegacyNodeRuntime` maps Loader activation to `plugin.load` and
disposal to `plugin.dispose`.

Create `HostCommand`, validate `ClientConfig`, then call
`NodeSupervisor::start()`. Start spawns the configured program with piped
stdin/stdout and completes the `hello`/`ready` handshake before returning a
client. `ClientConfig` controls max frame size, bounded queue capacity, and
handshake/request/shutdown deadlines. `BridgeClient::request` accepts only
request `FrameKind`s while ready. `BridgeRequest` cancels automatically if it
is dropped before settlement; explicit cancellation and timeout also send a
terminal `cancel` frame when possible.

`NodeSupervisor::register_cleanup(generation, cleanup)` binds local cleanup to
the active generation. It runs on disconnect, protocol failure, host exit,
crash, shutdown, or supervisor drop. `shutdown()` asks the host to exit and
waits only until its configured grace window before terminating the process.
The Node runtime is a compatibility boundary, not a Rust-type or memory
boundary; values on the bridge are JSON. See [PROTOCOLS.md](PROTOCOLS.md) and
[SECURITY.md](SECURITY.md).

## Errors

Branch on structured error fields, not display text. `PluginError` is the
cross-runtime payload with `code`, `message`, `phase`, and optional `details`.
`message` and `details` are diagnostic data and can change. Core-only APIs use
`CoreError`; Loader APIs use `LoaderError`; Native APIs use
`NativePluginError`; WASM APIs return `PluginError`; and bridge APIs return
`BridgeError` or `RemoteError`.
