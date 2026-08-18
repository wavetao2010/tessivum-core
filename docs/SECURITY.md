# Security Review and Trust Boundaries

This review describes the implemented `0.1.0` boundaries. It is not a claim
that all plugin runtimes are equally trusted: Native, WASM, and legacy Node
have deliberately different security properties.

## Security model

| Runtime | Trust boundary | What Core enforces | What it does not enforce |
| --- | --- | --- | --- |
| Native | None. Plugin code runs in the host process. | Scoped cleanup, typed service handles, lifecycle and Loader transactions. | Filesystem, network, process, memory safety, or privilege isolation. |
| Extism/WASM | Guest ↔ host capability boundary. | Exact ABI, manifest/schema validation, capability allow-list, resource limits, single-call serialization, cancellation, invalidated bindings. | Correctness or least privilege of a host capability handler. |
| Legacy Node | Rust process ↔ supervised Bun process and framed transport. | Exact frame validation, handshake, generation checks, bounded queues and frames, request cancellation, process/generation cleanup. | A security sandbox for JavaScript or npm packages. |

The host must classify a plugin before loading it. Treat Native code and legacy
Node packages as fully trusted host code. Use WASM only with the smallest
capability set and handlers appropriate to the data and side effects exposed.

## Assets and intended protections

- **Host integrity and resources:** Scope/Fiber ownership, Loader candidates,
  WASM limits, bounded bridge queues, and process supervision prevent a failed
  plugin lifecycle or stale bridge generation from being retained as a live
  Core resource.
- **Service correctness:** Native `ServiceHandle<T>` carries a provider
  generation. `with` rejects a removed or replaced provider with
  `StaleServiceHandle`; service realm selection prevents accidental visibility
  between private realms.
- **Guest authority:** A WASM manifest permission is a request, not a grant. A
  capability call requires declaration by the guest, grant by
  `CapabilityRegistry`, and a registered handler.
- **Transport integrity:** Node frames carry exact protocol version,
  connection generation, nonzero request IDs where applicable, bounded length,
  UTF-8 JSON, and no unknown top-level fields. A bridge client sends no plugin
  request before `hello`/`ready` completes.

## Implemented controls

### Core lifecycle and Loader

- A scope entering `Unloading` rejects newly owned effects, child scopes,
  services, and event listeners. Disposal cancels its token, disposes children
  and effects in reverse order, continues past cleanup failures, and joins
  concurrent disposal callers.
- `Fiber::start` rolls back its setup scope on error. Native start failure calls
  `stop` once for partial plugin state before returning the error.
- The Loader validates, imports, and activates a detached candidate before it
  replaces a committed tree. Candidate failure is disposed; removal failure
  attempts to restore the prior tree and records a rollback trace event.
- Services are owned by scopes and removed at scope disposal. A replacement
  changes generation; old native handles cannot call the previous provider.
- Event listeners and dependency subscriptions are scope effects, so disposal
  unregisters them. These mechanisms prevent ownership leaks; they do not make
  arbitrary plugin-created OS resources safe unless the plugin registers a
  disposer for them.

### WASM package and guest controls

- `PluginManifest` uses `deny_unknown_fields`, rejects empty identifiers,
  requires a numeric three-component semver, exact `cordis.plugin/v1`, unique
  permissions/exports, all five lifecycle exports, and the supported config
  schema subset. Manifest files are bounded to 256 KiB and WASM modules to
  8 MiB; package files are read once as regular files with Unix final symlinks
  disabled before parser-only export validation and the single Extism compile.
- `WasmPluginInstance::instantiate` validates manifest, limits, and concrete
  configuration before creating a guest. The Extism engine configures the
  declared maximum linear-memory pages, timeout, fuel limit, no WASI, and
  `disallow_all_hosts`.
- V1 requires exactly one in-flight call. Calls have maximum input and output
  byte sizes and are serialized per instance. `stop` rejects new work,
  requests cancellation of the active call, waits for the current guest lock,
  drops the guest, and marks all host bindings unavailable.
- Each Extism capability call is permission-gated twice (manifest and host
  grant) and requires a handler. Capability responses are additionally bounded
  by `max_output_bytes`. There is no implicit file, HTTP, database, or product
  capability in Core.
- Guest responses must be valid JSON envelopes with the matching request ID.
  Malformed, oversized, mismatched, timed-out, cancelled, or stopped calls
  fail as `PluginError`; they do not become unchecked host calls.

### Node bridge controls

- `FrameCodec` verifies a positive u32 frame limit, verifies the prefix before
  allocation, and rejects malformed framing, oversized bodies, invalid JSON,
  and invalid frame semantics. Default maximum body size is 1 MiB.
- Both Rust and Bun endpoints require `cordis.node/v1` exactly. The host accepts
  `hello` first, binds one nonzero connection generation, and rejects a later
  frame from any other generation. The Rust client rejects work before ready.
- Client queues are bounded (default capacity 64). Queue pressure returns an
  error rather than allocating an unbounded backlog. Pending requests are
  settled on disconnect; timeout, explicit cancellation, and dropped request
  handles remove local pending state and send a cancel frame when possible.
- `NodeSupervisor` owns one child process per profile. On bridge disconnect,
  crash, protocol failure, shutdown, or drop, it closes the client, terminates
  the child if necessary, and runs all local cleanups registered for that
  generation. A stale client cannot use a restarted host generation.
- The compat-host serializes incoming operations, gives each one an abort
  signal, uses `Promise.allSettled` while shutting down plugins and
  registrations, and disposes its root Cordis fiber before exit.

## Required host practice

1. **Validate package provenance before `PackageResolver` returns a location.**
   The Loader and bridge validate package shape and protocol, not publisher,
   hash, signature, or filesystem provenance. Do not let untrusted users choose
   Native factories, WASM bytes, Node package specifiers, or bridge command
   lines.
2. **Use capability-specific authorization.** `CapabilityRegistry::grant` is
   global to that registry; a granted capability can be reached by any guest
   that also declares it. Create separate registries or handlers when plugins
   need different authority. A handler must validate its JSON payload, authorize
   `CapabilityRequest.plugin_id`, apply its own data/operation limits, and avoid
   returning sensitive data unnecessarily.
3. **Set explicit limits for untrusted or variable-size WASM workloads.** Review
   `memory_pages`, `timeout`, `fuel`, `max_input_bytes`, and
   `max_output_bytes`; defaults are implementation defaults, not a capacity
   promise. Cooperative cancellation cannot force an arbitrary Native task to
   stop.
4. **Run legacy Node with OS isolation.** Use a dedicated low-privilege user,
   container/sandbox, restricted filesystem and network policy, and an explicit
   environment. `HostCommand` deliberately accepts a program, arguments,
   environment, and working directory; do not inherit or expose secrets by
   accident.
5. **Dispose explicitly.** Call `Loader::unload`, `NativePluginInstance::dispose`,
   `WasmPluginInstance::stop`, root `Scope::dispose`, and
   `NodeSupervisor::shutdown` at the relevant boundary. Rust `Drop` is not an
   asynchronous cleanup guarantee.
6. **Treat diagnostics as sensitive.** Cross-runtime errors expose `message`
   and optional `details`; the Bun host's generic error serializer can include
   up to 16,384 characters of an error stack. It bounds strings, nesting,
   arrays, and object entries, but does not redact secrets. Do not put secrets
   in plugin errors, event payloads, service values, or logs; restrict and
   scrub captured `log` frames.

## Residual risks and non-goals

- **Native plugins are fully privileged.** They can retain references, spawn
  work, access host resources, or use unsafe code independent of Core. Scope
  cleanup covers only resources registered with it.
- **Legacy Node is not a sandbox.** The compat-host runs Bun and dynamically
  imports the package location/specifier. A valid bridge frame says nothing
  about package trust. Its controls are failure containment and cleanup, not
  protection against malicious JavaScript.
- **Capabilities are only as safe as their handlers.** The Core registry does
  not impose an authorization model, storage namespace, rate limit, or input
  schema on a handler. `kv.*` is merely a capability name, not built-in durable
  storage.
- **Frame bounds do not make payloads confidential.** The Node bridge is a
  local stdio protocol with no encryption, authentication, or signing. Secure
  the parent process, process tree, executable, working directory, and IPC
  visibility with the operating environment.
- **Limit enforcement cannot prove total resource safety.** WASM input/output,
  memory, timeout, fuel, and concurrency controls are implemented for the
  Extism instance, but host capability handlers and Native/Node work can still
  consume host resources unless the host applies their own deadlines and
  quotas.
- **Error details are diagnostic, not a stable API.** Control flow must use
  `code` and `phase` for `PluginError` or the typed bridge error; never parse a
  message or assume a stack is absent.

## Security response criteria

Treat the following as release-blocking defects: a capability call succeeding
without declaration and grant; a stopped WASM instance retaining a callable
binding; an ABI/version/generation mismatch executing plugin work; a bridge
frame causing unbounded allocation; a Node exit/crash leaving registered
local generation cleanup unrun; a scope allowing a newly owned resource after
unloading; or a stale native service handle invoking a replaced provider.
