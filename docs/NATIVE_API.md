# Native Plugin API

Native plugins run in-process and use the same `ContextHandle`, service registry,
event bus, Scope ownership, and Loader transaction rules as every other runtime.
They are for Rust code that needs native service and event types; they are not a
serialization boundary.

## Descriptor and configuration

Every plugin returns a `NativePluginDescriptor`:

```rust
NativePluginDescriptor {
    name: "acme.greeting".into(),
    version: "1.0.0".into(),
    dependencies: vec![Dependency::Required(greeting_service())],
    config_schema: NativeConfigSchema::Object {
        properties: BTreeMap::from([("enabled".into(), NativeConfigSchema::Boolean)]),
        required: BTreeSet::from(["enabled".into()]),
        allow_additional: false,
    },
}
```

`name` and `version` identify an implementation; a Loader entry ID identifies
one configured instance. `dependencies` declares required and optional
`Dependency` values. Required dependencies keep the instance pending; optional
dependencies are reported but never gate activation.

`NativeConfigSchema` is an explicit typed validator, not an executable schema:

- `Any`, `Null`, `Boolean`, `Integer`, `Number`, and `String` validate one
  value kind;
- `Array(Box<NativeConfigSchema>)` validates each item;
- `Object { properties, required, allow_additional }` validates declared
  properties, required keys, and closed/open object shape.

Call `descriptor.validate()` for an invalid descriptor (for example, a required
object property absent from `properties`) and `validate_config(&Value)` before
using configuration directly. The runtime always does both before lifecycle
effects. Failures are structured: `NativePluginError::Config` wraps
`NativeConfigError::{Type, Required, AdditionalProperty, InvalidSchema}` with
a stable path rooted at `$`, such as `$.enabled` or `$[0]`.

## Lifecycle

`NativePlugin` owns behavior; `NativePluginInstance` owns its lifecycle:

```rust
fn start<'a>(
    &'a mut self,
    context: ContextHandle,
    config: &'a Value,
) -> NativePluginFuture<'a>;

fn update<'a>(
    &'a mut self,
    context: ContextHandle,
    config: &'a Value,
) -> NativePluginFuture<'a>;

fn stop<'a>(&'a mut self, context: ContextHandle) -> NativePluginFuture<'a>;
```

`NativePluginFuture` is a boxed, sendable future returning `NativePluginResult`.
`NativePluginError` distinguishes descriptor, configuration, Core, plugin,
runtime, and cleanup failures.

Create a direct instance with:

```rust
let mut instance = NativePluginInstance::instantiate(
    Box::new(plugin),
    root_context,
    resolved_config,
)?;
instance.start().await?;
```

Start creates a child `ContextHandle`/`Scope` transaction. A failed start
disposes that transaction before returning, including any service, listener, or
effect already registered. `update(config)` validates the complete candidate,
then stops, calls `update`, and starts a fresh transaction; a rejected candidate
leaves the last validated configuration and its resources intact. `dispose()`
quiesces the instance and is idempotent.

Do not retain a `ServiceHandle` after its provider changes. Acquire it from the
current plugin context when needed; stale handles fail rather than calling an
old provider.

## Native data paths

Use `ContextHandle::provide`/`get` with concrete Rust types and use
`EventKey<T>` for typed events. Those service and event paths carry `T` directly
and do not serialize through JSON. `serde_json::Value` is used only for the
Loader configuration boundary passed to lifecycle hooks.

## Loader integration and diagnostics

Create `NativePluginRuntime`, register package factories, wrap it in `Arc`, and
pass it to `Loader` as `RuntimeKind::Native`:

```rust
let mut native = NativePluginRuntime::new();
native.register("acme/greeting", GreetingPlugin::new)?;
let native = Arc::new(native);
let runtime: Arc<dyn LoaderRuntime> = native.clone();
let mut loader = Loader::try_new(resolver, [runtime])?;
```

The runtime implements `LoaderRuntime`; the Loader imports and activates its
native candidate before replacing the committed tree. Descriptor dependency
gates, rather than entry order, drive pending-to-active transitions.

`NativePluginInstance::snapshot()` returns a descriptor, validated config, and
Fiber snapshot with the Scope state and owned resources. Use it after a failed
start, update, or disposal to confirm transactional cleanup. `NativePluginRuntime::snapshot()`
lists registered package factories without exposing mutable runtime internals.
At shutdown, dispose the root context and ensure both the instance Fiber
resources and root Scope effects are empty.

See `examples/native-minimal` for a complete provider, consumer, typed-event,
provider-replacement, consumer-reload, and root-disposal sequence.
