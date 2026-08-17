//! The `cordis.plugin/v1` Extism host runtime.
//!
//! The runtime is deliberately synchronous at the guest boundary: one plugin
//! instance owns one Extism instance and its calls are serialized. The loader
//! still receives its usual asynchronous `LoaderRuntime` interface.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};

use extism::{Function, Manifest as ExtismManifest, Plugin, PluginBuilder, UserData, Wasm, PTR};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
pub use tessivum_core::PluginError;
use tessivum_core::{
    ContextHandle, Entry, LoaderError, LoaderFuture, LoaderRuntime, ResolvedPackage, RuntimeHandle,
    RuntimeKind,
};

/// The only ABI accepted by this runtime.
pub const ABI_VERSION: &str = "cordis.plugin/v1";
const EXTISM_USER_MODULE: &str = "extism:host/user";

/// Result used by all WASM-runtime public operations.
pub type WasmResult<T> = Result<T, PluginError>;

/// Stable host capabilities available to a guest only when both its manifest
/// and the host policy allow them.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum Capability {
    #[serde(rename = "cordis.log")]
    Log,
    #[serde(rename = "cordis.config.get")]
    ConfigGet,
    #[serde(rename = "cordis.service.call")]
    ServiceCall,
    #[serde(rename = "cordis.event.emit")]
    EventEmit,
    #[serde(rename = "cordis.event.subscribe")]
    EventSubscribe,
    #[serde(rename = "cordis.registration.dispose")]
    RegistrationDispose,
    #[serde(rename = "cordis.kv.get")]
    KvGet,
    #[serde(rename = "cordis.kv.set")]
    KvSet,
}

impl Capability {
    pub const ALL: [Self; 8] = [
        Self::Log,
        Self::ConfigGet,
        Self::ServiceCall,
        Self::EventEmit,
        Self::EventSubscribe,
        Self::RegistrationDispose,
        Self::KvGet,
        Self::KvSet,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Log => "cordis.log",
            Self::ConfigGet => "cordis.config.get",
            Self::ServiceCall => "cordis.service.call",
            Self::EventEmit => "cordis.event.emit",
            Self::EventSubscribe => "cordis.event.subscribe",
            Self::RegistrationDispose => "cordis.registration.dispose",
            Self::KvGet => "cordis.kv.get",
            Self::KvSet => "cordis.kv.set",
        }
    }
}

/// Guest lifecycle exports required by `cordis.plugin/v1`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum GuestExport {
    #[serde(rename = "cordis_init")]
    Init,
    #[serde(rename = "cordis_call")]
    Call,
    #[serde(rename = "cordis_event")]
    Event,
    #[serde(rename = "cordis_update")]
    Update,
    #[serde(rename = "cordis_stop")]
    Stop,
}

impl GuestExport {
    pub const ALL: [Self; 5] = [
        Self::Init,
        Self::Call,
        Self::Event,
        Self::Update,
        Self::Stop,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Init => "cordis_init",
            Self::Call => "cordis_call",
            Self::Event => "cordis_event",
            Self::Update => "cordis_update",
            Self::Stop => "cordis_stop",
        }
    }

    const fn phase(self) -> &'static str {
        match self {
            Self::Init => "init",
            Self::Call => "call",
            Self::Event => "event",
            Self::Update => "update",
            Self::Stop => "stop",
        }
    }
}

/// The package-side manifest for a `cordis.plugin/v1` guest.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginManifest {
    pub id: String,
    pub version: String,
    pub entry: String,
    pub abi: String,
    pub inject: Vec<String>,
    pub permissions: Vec<Capability>,
    pub config_schema: Value,
    pub exports: Vec<GuestExport>,
}

impl PluginManifest {
    pub fn from_json(document: &str) -> WasmResult<Self> {
        let manifest = serde_json::from_str::<Self>(document).map_err(|error| {
            PluginError::new(
                "MANIFEST_INVALID",
                format!("cannot parse manifest: {error}"),
                "manifest",
            )
        })?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Validates ABI and manifest shape before any guest module is instantiated.
    pub fn validate(&self) -> WasmResult<()> {
        for (field, value) in [
            ("id", self.id.as_str()),
            ("version", self.version.as_str()),
            ("entry", self.entry.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(manifest_error(format!("{field} must not be blank")));
            }
        }
        if !is_semver(&self.version) {
            return Err(manifest_error("version must be a semantic version"));
        }
        if self.abi != ABI_VERSION {
            return Err(PluginError::new(
                "ABI_UNSUPPORTED",
                format!("expected {ABI_VERSION}, got {}", self.abi),
                "manifest",
            ));
        }
        unique_strings("inject", &self.inject)?;
        unique_values("permissions", &self.permissions)?;
        unique_values("exports", &self.exports)?;
        for export in GuestExport::ALL {
            if !self.exports.contains(&export) {
                return Err(manifest_error(format!(
                    "exports must include {}",
                    export.as_str()
                )));
            }
        }
        validate_schema(&self.config_schema, "$schema")
    }

    /// Checks a concrete configuration against the supported JSON-schema subset.
    pub fn validate_config(&self, config: &Value) -> WasmResult<()> {
        self.validate()?;
        validate_config(&self.config_schema, config, "$")
    }

    fn permissions(&self) -> BTreeSet<Capability> {
        self.permissions.iter().copied().collect()
    }
}

/// Every request crossing the host/guest boundary has this shape.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RequestEnvelope {
    pub request_id: String,
    pub context: Value,
    pub payload: Value,
}

impl RequestEnvelope {
    pub fn new(request_id: impl Into<String>, context: Value, payload: Value) -> Self {
        Self {
            request_id: request_id.into(),
            context,
            payload,
        }
    }
}

/// Guest entrypoint response used by the PDK.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResponseEnvelope {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<PluginError>,
}

impl ResponseEnvelope {
    pub fn success(request: &RequestEnvelope, result: Value) -> Self {
        Self {
            request_id: request.request_id.clone(),
            result: Some(result),
            error: None,
        }
    }

    pub fn failure(request: &RequestEnvelope, error: PluginError) -> Self {
        Self {
            request_id: request.request_id.clone(),
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Serialize)]
struct HostResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<PluginError>,
}

impl HostResponse {
    fn success(result: Value) -> Self {
        Self {
            result: Some(result),
            error: None,
        }
    }

    fn failure(error: PluginError) -> Self {
        Self {
            result: None,
            error: Some(error),
        }
    }
}

/// Limits applied to one guest instance.
#[derive(Clone, Debug)]
pub struct ResourceLimits {
    /// Maximum WebAssembly linear-memory pages (64 KiB each).
    pub memory_pages: u32,
    pub timeout: Duration,
    pub fuel: u64,
    /// This ABI deliberately supports exactly one in-flight call per instance.
    pub max_concurrency: usize,
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            memory_pages: 1_024,
            timeout: Duration::from_secs(5),
            fuel: 10_000_000,
            max_concurrency: 1,
            max_input_bytes: 1_048_576,
            max_output_bytes: 1_048_576,
        }
    }
}

impl ResourceLimits {
    pub fn validate(&self) -> WasmResult<()> {
        if self.memory_pages == 0 {
            return Err(limit_error("memory_pages must be greater than zero"));
        }
        if self.timeout.is_zero() {
            return Err(limit_error("timeout must be greater than zero"));
        }
        if self.fuel == 0 {
            return Err(limit_error("fuel must be greater than zero"));
        }
        if self.max_concurrency != 1 {
            return Err(limit_error(
                "cordis.plugin/v1 requires max_concurrency to equal one",
            ));
        }
        if self.max_input_bytes == 0 || self.max_output_bytes == 0 {
            return Err(limit_error(
                "input and output limits must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// A registered host capability receives its capability-specific JSON payload.
#[derive(Clone, Debug)]
pub struct CapabilityRequest {
    pub capability: Capability,
    pub plugin_id: String,
    pub payload: Value,
}

/// Product hosts supply implementations for the small, universal capability set.
pub trait CapabilityHandler: Send + Sync {
    fn call(&self, request: CapabilityRequest) -> WasmResult<Value>;
}

impl<F> CapabilityHandler for F
where
    F: Fn(CapabilityRequest) -> WasmResult<Value> + Send + Sync,
{
    fn call(&self, request: CapabilityRequest) -> WasmResult<Value> {
        self(request)
    }
}

/// Host policy and implementations. Registration alone does not grant access.
#[derive(Default)]
pub struct CapabilityRegistry {
    handlers: Mutex<BTreeMap<Capability, Arc<dyn CapabilityHandler>>>,
    granted: Mutex<BTreeSet<Capability>>,
}

impl CapabilityRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &self,
        capability: Capability,
        handler: impl CapabilityHandler + 'static,
    ) -> WasmResult<()> {
        let mut handlers = lock(&self.handlers);
        if handlers.contains_key(&capability) {
            return Err(PluginError::new(
                "CAPABILITY_ALREADY_REGISTERED",
                format!("{} already has a host handler", capability.as_str()),
                "host",
            ));
        }
        handlers.insert(capability, Arc::new(handler));
        Ok(())
    }
    pub fn grant(&self, capability: Capability) {
        lock(&self.granted).insert(capability);
    }

    pub fn revoke(&self, capability: Capability) {
        lock(&self.granted).remove(&capability);
    }

    pub fn is_granted(&self, capability: Capability) -> bool {
        lock(&self.granted).contains(&capability)
    }

    fn invoke(
        &self,
        permissions: &BTreeSet<Capability>,
        request: CapabilityRequest,
    ) -> WasmResult<Value> {
        if !permissions.contains(&request.capability) {
            return Err(PluginError::new(
                "PERMISSION_DENIED",
                format!(
                    "{} is not declared by this plugin",
                    request.capability.as_str()
                ),
                "call",
            ));
        }
        if !self.is_granted(request.capability) {
            return Err(PluginError::new(
                "PERMISSION_DENIED",
                format!(
                    "{} is not granted by host policy",
                    request.capability.as_str()
                ),
                "call",
            ));
        }
        let handler = lock(&self.handlers)
            .get(&request.capability)
            .cloned()
            .ok_or_else(|| {
                PluginError::new(
                    "CAPABILITY_UNAVAILABLE",
                    format!("{} has no host handler", request.capability.as_str()),
                    "call",
                )
            })?;
        handler.call(request)
    }
}

/// The authorization context installed into all Extism host functions.
#[derive(Clone)]
pub struct HostBindings {
    registry: Arc<CapabilityRegistry>,
    permissions: Arc<BTreeSet<Capability>>,
    plugin_id: String,
    alive: Arc<AtomicBool>,
    max_output_bytes: usize,
}

impl HostBindings {
    fn new(
        registry: Arc<CapabilityRegistry>,
        manifest: &PluginManifest,
        alive: Arc<AtomicBool>,
        max_output_bytes: usize,
    ) -> Self {
        Self {
            registry,
            permissions: Arc::new(manifest.permissions()),
            plugin_id: manifest.id.clone(),
            alive,
            max_output_bytes,
        }
    }

    /// Invokes a host capability through the same permission gate used by Wasm.
    pub fn invoke(&self, capability: Capability, payload: Value) -> WasmResult<Value> {
        if !self.alive.load(Ordering::Acquire) {
            return Err(PluginError::new(
                "INSTANCE_STOPPED",
                "plugin instance has stopped",
                "host",
            ));
        }
        let value = self.registry.invoke(
            &self.permissions,
            CapabilityRequest {
                capability,
                plugin_id: self.plugin_id.clone(),
                payload,
            },
        )?;
        let actual = json_size(&value, "host")?;
        if actual > self.max_output_bytes {
            return Err(output_limit_error(self.max_output_bytes, actual));
        }
        Ok(value)
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    fn functions(&self) -> Vec<Function> {
        Capability::ALL
            .into_iter()
            .map(|capability| {
                Function::new(
                    capability.as_str(),
                    [PTR],
                    [PTR],
                    UserData::new(self.clone()),
                    move |plugin, inputs, outputs, user_data| {
                        let bindings = user_data.get()?;
                        let bindings = lock(bindings.as_ref()).clone();
                        let input: Vec<u8> = plugin.memory_get_val(&inputs[0])?;
                        let response = match serde_json::from_slice::<Value>(&input) {
                            Ok(payload) => match bindings.invoke(capability, payload) {
                                Ok(result) => HostResponse::success(result),
                                Err(error) => HostResponse::failure(error),
                            },
                            Err(error) => HostResponse::failure(
                                PluginError::new(
                                    "invalid_request",
                                    "invalid request envelope",
                                    "host",
                                )
                                .with_details(json!({"reason": error.to_string()})),
                            ),
                        };
                        let bytes = host_response_bytes(response, bindings.max_output_bytes);
                        plugin.memory_set_val(&mut outputs[0], bytes)?;
                        Ok(())
                    },
                )
                .with_namespace(EXTISM_USER_MODULE)
            })
            .collect()
    }
}

/// A module and its validated package manifest.
#[derive(Clone, Debug)]
pub struct WasmPackage {
    pub manifest: PluginManifest,
    /// `None` is allowed only for [`InMemoryGuestEngine`] test seams.
    pub wasm: Option<Vec<u8>>,
}

impl WasmPackage {
    pub fn from_bytes(manifest: PluginManifest, wasm: Vec<u8>) -> WasmResult<Self> {
        manifest.validate()?;
        validate_wasm_exports(&wasm)?;
        Ok(Self {
            manifest,
            wasm: Some(wasm),
        })
    }

    /// Makes a manifest-only package for deterministic engine tests.
    pub fn in_memory(manifest: PluginManifest) -> WasmResult<Self> {
        manifest.validate()?;
        Ok(Self {
            manifest,
            wasm: None,
        })
    }

    /// Loads a JSON manifest and its relative WebAssembly entry point.
    pub fn from_manifest_file(path: impl AsRef<Path>) -> WasmResult<Self> {
        let path = path.as_ref();
        let data = fs::read(path).map_err(|error| {
            PluginError::new(
                "PACKAGE_READ_FAILED",
                format!("cannot read {}: {error}", path.display()),
                "instantiate",
            )
        })?;
        let manifest = serde_json::from_slice::<PluginManifest>(&data).map_err(|error| {
            PluginError::new(
                "MANIFEST_INVALID",
                format!("cannot parse {}: {error}", path.display()),
                "manifest",
            )
        })?;
        manifest.validate()?;
        let entry = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(&manifest.entry);
        let wasm = fs::read(&entry).map_err(|error| {
            PluginError::new(
                "PACKAGE_READ_FAILED",
                format!("cannot read {}: {error}", entry.display()),
                "instantiate",
            )
        })?;
        Self::from_bytes(manifest, wasm)
    }
}

/// Interrupt path separate from the guest instance lock.
pub trait GuestCancellation: Send + Sync {
    fn cancel(&self) -> WasmResult<()>;
}

/// Synchronous guest API. The runtime serializes access to this object.
pub trait GuestInstance: Send {
    fn call(&mut self, export: GuestExport, input: &[u8]) -> WasmResult<Vec<u8>>;
    fn cancellation(&self) -> Arc<dyn GuestCancellation>;
}

/// Engine seam for deterministic tests and the official Extism implementation.
pub trait GuestEngine: Send + Sync {
    fn instantiate(
        &self,
        package: &WasmPackage,
        host: HostBindings,
        limits: ResourceLimits,
    ) -> WasmResult<Box<dyn GuestInstance>>;
}

/// The real guest engine backed by the official `extism` crate.
#[derive(Default)]
pub struct ExtismGuestEngine;

impl GuestEngine for ExtismGuestEngine {
    fn instantiate(
        &self,
        package: &WasmPackage,
        host: HostBindings,
        limits: ResourceLimits,
    ) -> WasmResult<Box<dyn GuestInstance>> {
        limits.validate()?;
        package.manifest.validate()?;
        let wasm = package.wasm.as_ref().ok_or_else(|| {
            PluginError::new(
                "MODULE_UNAVAILABLE",
                "the Extism engine requires WebAssembly bytes",
                "instantiate",
            )
        })?;
        validate_wasm_exports(wasm)?;
        let manifest = ExtismManifest::new([Wasm::data(wasm.clone())])
            .with_memory_max(limits.memory_pages)
            .with_timeout(limits.timeout)
            .disallow_all_hosts();
        let plugin = PluginBuilder::new(manifest)
            .with_functions(host.functions())
            .with_wasi(false)
            .with_fuel_limit(limits.fuel)
            .build()
            .map_err(|error| extism_error(error.to_string(), "instantiate"))?;
        let cancellation = Arc::new(ExtismCancellation(plugin.cancel_handle()));
        Ok(Box::new(ExtismGuestInstance {
            plugin,
            cancellation,
        }))
    }
}

struct ExtismGuestInstance {
    plugin: Plugin,
    cancellation: Arc<ExtismCancellation>,
}

impl GuestInstance for ExtismGuestInstance {
    fn call(&mut self, export: GuestExport, input: &[u8]) -> WasmResult<Vec<u8>> {
        self.plugin
            .call::<Vec<u8>, Vec<u8>>(export.as_str(), input.to_vec())
            .map_err(|error| extism_error(error.to_string(), export.phase()))
    }

    fn cancellation(&self) -> Arc<dyn GuestCancellation> {
        self.cancellation.clone()
    }
}

struct ExtismCancellation(extism::CancelHandle);

impl GuestCancellation for ExtismCancellation {
    fn cancel(&self) -> WasmResult<()> {
        self.0
            .cancel()
            .map_err(|error| extism_error(error.to_string(), "cancel"))
    }
}

/// Calls observed by the deterministic in-memory engine.
#[derive(Clone, Debug, PartialEq)]
pub struct GuestCall {
    pub export: GuestExport,
    pub envelope: RequestEnvelope,
}

/// A compact deterministic guest engine for host-runtime tests.
pub struct InMemoryGuestEngine {
    handler: Arc<
        dyn Fn(GuestExport, RequestEnvelope, HostBindings) -> WasmResult<ResponseEnvelope>
            + Send
            + Sync,
    >,
    calls: Arc<Mutex<Vec<GuestCall>>>,
}

impl InMemoryGuestEngine {
    pub fn new(
        handler: impl Fn(GuestExport, RequestEnvelope, HostBindings) -> WasmResult<ResponseEnvelope>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        Self {
            handler: Arc::new(handler),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn calls(&self) -> Vec<GuestCall> {
        lock(&self.calls).clone()
    }
}

impl GuestEngine for InMemoryGuestEngine {
    fn instantiate(
        &self,
        package: &WasmPackage,
        host: HostBindings,
        _limits: ResourceLimits,
    ) -> WasmResult<Box<dyn GuestInstance>> {
        package.manifest.validate()?;
        Ok(Box::new(InMemoryGuestInstance {
            handler: Arc::clone(&self.handler),
            calls: Arc::clone(&self.calls),
            host,
            cancellation: Arc::new(InMemoryCancellation::default()),
        }))
    }
}

struct InMemoryGuestInstance {
    handler: Arc<
        dyn Fn(GuestExport, RequestEnvelope, HostBindings) -> WasmResult<ResponseEnvelope>
            + Send
            + Sync,
    >,
    calls: Arc<Mutex<Vec<GuestCall>>>,
    host: HostBindings,
    cancellation: Arc<InMemoryCancellation>,
}

impl GuestInstance for InMemoryGuestInstance {
    fn call(&mut self, export: GuestExport, input: &[u8]) -> WasmResult<Vec<u8>> {
        if self.cancellation.cancelled.swap(false, Ordering::AcqRel) {
            return Err(cancelled_error());
        }
        let envelope = serde_json::from_slice::<RequestEnvelope>(input).map_err(|error| {
            PluginError::new(
                "PROTOCOL_INVALID",
                format!("invalid guest request envelope: {error}"),
                export.phase(),
            )
        })?;
        lock(&self.calls).push(GuestCall {
            export,
            envelope: envelope.clone(),
        });
        let response = (self.handler)(export, envelope, self.host.clone())?;
        if self.cancellation.cancelled.swap(false, Ordering::AcqRel) {
            return Err(cancelled_error());
        }
        serde_json::to_vec(&response).map_err(|error| {
            PluginError::new(
                "PROTOCOL_INVALID",
                format!("cannot encode in-memory guest response: {error}"),
                export.phase(),
            )
        })
    }

    fn cancellation(&self) -> Arc<dyn GuestCancellation> {
        self.cancellation.clone()
    }
}

#[derive(Default)]
struct InMemoryCancellation {
    cancelled: AtomicBool,
}

impl GuestCancellation for InMemoryCancellation {
    fn cancel(&self) -> WasmResult<()> {
        self.cancelled.store(true, Ordering::Release);
        Ok(())
    }
}

/// A running, serialized WASM plugin instance.
pub struct WasmPluginInstance {
    manifest: PluginManifest,
    state: Mutex<PluginState>,
    guest_available: Condvar,
    active: Mutex<Option<Arc<CallCancellation>>>,
    accepting: AtomicBool,
    host: HostBindings,
    limits: ResourceLimits,
    sequence: AtomicU64,
}

struct PluginState {
    guest: Option<Box<dyn GuestInstance>>,
    config: Value,
    stopped: bool,
}

impl WasmPluginInstance {
    pub fn instantiate(
        package: WasmPackage,
        engine: Arc<dyn GuestEngine>,
        registry: Arc<CapabilityRegistry>,
        limits: ResourceLimits,
        config: Value,
    ) -> WasmResult<Self> {
        package.manifest.validate()?;
        limits.validate()?;
        package.manifest.validate_config(&config)?;
        let alive = Arc::new(AtomicBool::new(true));
        let host = HostBindings::new(
            registry,
            &package.manifest,
            Arc::clone(&alive),
            limits.max_output_bytes,
        );
        let guest = engine.instantiate(&package, host.clone(), limits.clone())?;
        Ok(Self {
            manifest: package.manifest,
            state: Mutex::new(PluginState {
                guest: Some(guest),
                config,
                stopped: false,
            }),
            active: Mutex::new(None),
            guest_available: Condvar::new(),
            accepting: AtomicBool::new(true),
            host,
            limits,
            sequence: AtomicU64::new(0),
        })
    }

    pub fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    pub fn config(&self) -> Value {
        lock(&self.state).config.clone()
    }

    pub fn init(&self, context: Value) -> WasmResult<Value> {
        self.invoke(GuestExport::Init, context, self.config())
    }

    pub fn call(&self, context: Value, payload: Value) -> WasmResult<Value> {
        self.invoke(GuestExport::Call, context, payload)
    }

    pub fn event(&self, context: Value, payload: Value) -> WasmResult<Value> {
        self.invoke(GuestExport::Event, context, payload)
    }

    pub fn update(&self, context: Value, config: Value) -> WasmResult<Value> {
        self.manifest.validate_config(&config)?;
        let value = self.invoke(GuestExport::Update, context, config.clone())?;
        lock(&self.state).config = config;
        Ok(value)
    }

    /// Requests cancellation for the currently executing guest call. Exactly
    /// one caller wins, even if the underlying engine cancellation fails.
    pub fn cancel_current(&self) -> bool {
        lock(&self.active)
            .clone()
            .is_some_and(|active| active.cancel())
    }

    /// Stops the instance, rejects new calls, cancels current work, then drops
    /// the guest and invalidates all host bindings even when guest stop fails.
    pub fn stop(&self) -> WasmResult<()> {
        if self
            .accepting
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        self.cancel_current();
        let request = self.request(Value::Null, Value::Null);
        let mut state = lock(&self.state);
        while state.guest.is_none() {
            state = self
                .guest_available
                .wait(state)
                .unwrap_or_else(|poison| poison.into_inner());
        }
        let Some(mut guest) = state.guest.take() else {
            state.stopped = true;
            drop(state);
            self.host.alive.store(false, Ordering::Release);
            self.guest_available.notify_all();
            return Err(stopped_error(GuestExport::Stop.phase()));
        };
        state.stopped = true;
        drop(state);

        let result =
            call_guest(&mut guest, GuestExport::Stop, &request, &self.limits, None).map(|_| ());
        drop(guest);
        self.host.alive.store(false, Ordering::Release);
        self.guest_available.notify_all();
        result
    }

    pub fn is_stopped(&self) -> bool {
        !self.accepting.load(Ordering::Acquire)
    }

    fn invoke(&self, export: GuestExport, context: Value, payload: Value) -> WasmResult<Value> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(stopped_error(export.phase()));
        }
        let request = self.request(context, payload);
        let mut state = lock(&self.state);
        while self.accepting.load(Ordering::Acquire) && !state.stopped && state.guest.is_none() {
            state = self
                .guest_available
                .wait(state)
                .unwrap_or_else(|poison| poison.into_inner());
        }
        if !self.accepting.load(Ordering::Acquire) || state.stopped {
            return Err(stopped_error(export.phase()));
        }
        let Some(mut guest) = state.guest.take() else {
            return Err(stopped_error(export.phase()));
        };
        let active = Arc::new(CallCancellation::new(guest.cancellation()));
        *lock(&self.active) = Some(Arc::clone(&active));
        if !self.accepting.load(Ordering::Acquire) {
            *lock(&self.active) = None;
            state.guest = Some(guest);
            drop(state);
            self.guest_available.notify_all();
            return Err(stopped_error(export.phase()));
        }
        drop(state);

        let result = call_guest(&mut guest, export, &request, &self.limits, Some(&active));
        *lock(&self.active) = None;
        let mut state = lock(&self.state);
        state.guest = Some(guest);
        drop(state);
        self.guest_available.notify_all();
        result
    }

    fn request(&self, context: Value, payload: Value) -> RequestEnvelope {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        RequestEnvelope::new(format!("{}:{sequence}", self.manifest.id), context, payload)
    }
}

impl Drop for WasmPluginInstance {
    fn drop(&mut self) {
        self.accepting.store(false, Ordering::Release);
        self.cancel_current();
        self.host.alive.store(false, Ordering::Release);
    }
}

struct CallCancellation {
    cancelled: AtomicBool,
    guest: Arc<dyn GuestCancellation>,
}

impl CallCancellation {
    fn new(guest: Arc<dyn GuestCancellation>) -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            guest,
        }
    }

    fn cancel(&self) -> bool {
        if self
            .cancelled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        let _ = self.guest.cancel();
        true
    }
}

fn call_guest(
    guest: &mut Box<dyn GuestInstance>,
    export: GuestExport,
    request: &RequestEnvelope,
    limits: &ResourceLimits,
    cancellation: Option<&CallCancellation>,
) -> WasmResult<Value> {
    let input = serde_json::to_vec(request).map_err(|error| {
        PluginError::new(
            "PROTOCOL_INVALID",
            format!("cannot encode guest request: {error}"),
            export.phase(),
        )
    })?;
    if input.len() > limits.max_input_bytes {
        return Err(input_limit_error(limits.max_input_bytes, input.len()));
    }
    let started = Instant::now();
    let output = guest.call(export, &input);
    if cancellation.is_some_and(|active| active.cancelled.load(Ordering::Acquire)) {
        return Err(cancelled_error());
    }
    let elapsed = started.elapsed();
    if elapsed > limits.timeout {
        return Err(PluginError::new(
            "TIMEOUT",
            "guest call exceeded its configured timeout",
            export.phase(),
        )
        .with_details(json!({"timeoutMs": limits.timeout.as_millis()})));
    }
    let output = output?;
    if output.len() > limits.max_output_bytes {
        return Err(output_limit_error(limits.max_output_bytes, output.len()));
    }
    let response = serde_json::from_slice::<ResponseEnvelope>(&output).map_err(|error| {
        PluginError::new(
            "PROTOCOL_INVALID",
            format!("invalid guest response envelope: {error}"),
            export.phase(),
        )
    })?;
    if response.request_id != request.request_id {
        return Err(PluginError::new(
            "PROTOCOL_INVALID",
            "guest response requestId does not match request",
            export.phase(),
        ));
    }
    if let Some(error) = response.error {
        return Err(error);
    }
    response.result.ok_or_else(|| {
        PluginError::new(
            "PROTOCOL_INVALID",
            "guest response must contain result or error",
            export.phase(),
        )
    })
}

/// Loader adapter for registered packages and real Extism modules.
pub struct WasmPluginRuntime {
    engine: Arc<dyn GuestEngine>,
    registry: Arc<CapabilityRegistry>,
    limits: ResourceLimits,
    packages: Mutex<BTreeMap<String, WasmPackage>>,
}

impl WasmPluginRuntime {
    pub fn new(registry: Arc<CapabilityRegistry>, limits: ResourceLimits) -> Self {
        Self::with_engine(Arc::new(ExtismGuestEngine), registry, limits)
    }

    pub fn with_engine(
        engine: Arc<dyn GuestEngine>,
        registry: Arc<CapabilityRegistry>,
        limits: ResourceLimits,
    ) -> Self {
        Self {
            engine,
            registry,
            limits,
            packages: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn register(&self, specifier: impl Into<String>, package: WasmPackage) -> WasmResult<()> {
        package.manifest.validate()?;
        let specifier = specifier.into();
        if specifier.trim().is_empty() {
            return Err(PluginError::new(
                "PACKAGE_INVALID",
                "package specifier must not be blank",
                "instantiate",
            ));
        }
        let mut packages = lock(&self.packages);
        if packages.contains_key(&specifier) {
            return Err(PluginError::new(
                "PACKAGE_ALREADY_REGISTERED",
                format!("{specifier} is already registered"),
                "instantiate",
            ));
        }
        packages.insert(specifier, package);
        Ok(())
    }
}

impl LoaderRuntime for WasmPluginRuntime {
    fn kind(&self) -> RuntimeKind {
        RuntimeKind::Wasm
    }

    fn instantiate<'a>(
        &'a self,
        package: ResolvedPackage,
        entry: Entry,
        _context: ContextHandle,
    ) -> LoaderFuture<'a, Box<dyn RuntimeHandle>> {
        Box::pin(async move {
            let module = lock(&self.packages)
                .get(&package.specifier)
                .cloned()
                .map(Ok)
                .unwrap_or_else(|| WasmPackage::from_manifest_file(&package.location))
                .map_err(loader_error)?;
            let context = json!({
                "entryId": entry.options.id.to_string(),
                "entryName": entry.options.name,
                "inject": entry.options.inject,
            });
            let instance = WasmPluginInstance::instantiate(
                module,
                Arc::clone(&self.engine),
                Arc::clone(&self.registry),
                self.limits.clone(),
                entry.options.config,
            )
            .map_err(loader_error)?;
            Ok(Box::new(WasmRuntimeHandle {
                instance,
                context,
                activated: false,
            }) as Box<dyn RuntimeHandle>)
        })
    }
}

struct WasmRuntimeHandle {
    instance: WasmPluginInstance,
    context: Value,
    activated: bool,
}

impl RuntimeHandle for WasmRuntimeHandle {
    fn activate<'a>(&'a mut self) -> LoaderFuture<'a, ()> {
        Box::pin(async move {
            if !self.activated {
                self.instance
                    .init(self.context.clone())
                    .map_err(loader_error)?;
                self.activated = true;
            }
            Ok(())
        })
    }

    fn dispose<'a>(&'a mut self) -> LoaderFuture<'a, ()> {
        Box::pin(async move { self.instance.stop().map_err(loader_error) })
    }
}

fn loader_error(error: PluginError) -> LoaderError {
    LoaderError::Validation(error.to_string())
}

fn host_response_bytes(response: HostResponse, max_output_bytes: usize) -> Vec<u8> {
    let bytes = serde_json::to_vec(&response).expect("host response serializes");
    if bytes.len() <= max_output_bytes {
        return bytes;
    }
    serde_json::to_vec(&HostResponse::failure(output_limit_error(
        max_output_bytes,
        bytes.len(),
    )))
    .expect("limit error envelope serializes")
}

fn is_semver(version: &str) -> bool {
    let core = version.split_once('-').map_or(version, |(core, _)| core);
    let mut parts = core.split('.');
    let valid = (&mut parts).take(3).all(|part| {
        !part.is_empty()
            && part.bytes().all(|byte| byte.is_ascii_digit())
            && (part == "0" || !part.starts_with('0'))
    });
    valid && parts.next().is_none() && core.matches('.').count() == 2
}

fn manifest_error(message: impl Into<String>) -> PluginError {
    PluginError::new("MANIFEST_INVALID", message, "manifest")
}

fn limit_error(message: impl Into<String>) -> PluginError {
    PluginError::new("LIMIT_INVALID", message, "limit")
}

fn input_limit_error(limit: usize, actual: usize) -> PluginError {
    PluginError::new("INPUT_LIMIT_EXCEEDED", "guest input exceeds limit", "call")
        .with_details(json!({"limit": limit, "actual": actual}))
}

fn output_limit_error(limit: usize, actual: usize) -> PluginError {
    PluginError::new(
        "OUTPUT_LIMIT_EXCEEDED",
        "guest output exceeds limit",
        "call",
    )
    .with_details(json!({"limit": limit, "actual": actual}))
}

fn cancelled_error() -> PluginError {
    PluginError::new("CANCELLED", "guest call was cancelled", "call")
}

fn stopped_error(phase: &str) -> PluginError {
    PluginError::new("INSTANCE_STOPPED", "plugin instance has stopped", phase)
}

fn extism_error(message: String, phase: &str) -> PluginError {
    let code = if message.contains("timeout") {
        "TIMEOUT"
    } else if message.contains("out of fuel") {
        "FUEL_LIMIT_EXCEEDED"
    } else if message.contains("oom") {
        "MEMORY_LIMIT_EXCEEDED"
    } else {
        "GUEST_TRAP"
    };
    PluginError::new(code, message, phase)
}

fn unique_strings(field: &str, values: &[String]) -> WasmResult<()> {
    let mut seen = BTreeSet::new();
    for value in values {
        if value.trim().is_empty() || !seen.insert(value) {
            return Err(manifest_error(format!(
                "{field} contains a blank or duplicate value"
            )));
        }
    }
    Ok(())
}

fn unique_values<T: Ord>(field: &str, values: &[T]) -> WasmResult<()> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(manifest_error(format!(
                "{field} contains a duplicate value"
            )));
        }
    }
    Ok(())
}

fn validate_schema(schema: &Value, path: &str) -> WasmResult<()> {
    let object = schema
        .as_object()
        .ok_or_else(|| manifest_error(format!("{path} must be an object")))?;
    if let Some(kind) = object.get("type") {
        let kind = kind
            .as_str()
            .ok_or_else(|| manifest_error(format!("{path}.type must be a string")))?;
        if !matches!(
            kind,
            "null" | "boolean" | "integer" | "number" | "string" | "array" | "object"
        ) {
            return Err(manifest_error(format!("{path}.type is unsupported")));
        }
    }
    if let Some(properties) = object.get("properties") {
        let properties = properties
            .as_object()
            .ok_or_else(|| manifest_error(format!("{path}.properties must be an object")))?;
        for (name, nested) in properties {
            validate_schema(nested, &format!("{path}.properties.{name}"))?;
        }
    }
    if let Some(required) = object.get("required") {
        let required = required
            .as_array()
            .ok_or_else(|| manifest_error(format!("{path}.required must be an array")))?;
        let properties = object.get("properties").and_then(Value::as_object);
        let mut seen = BTreeSet::new();
        for name in required {
            let name = name
                .as_str()
                .ok_or_else(|| manifest_error(format!("{path}.required must contain strings")))?;
            if !seen.insert(name) {
                return Err(manifest_error(format!(
                    "{path}.required contains duplicate {name}"
                )));
            }
            if !properties.is_some_and(|properties| properties.contains_key(name)) {
                return Err(manifest_error(format!(
                    "{path}.required references missing property {name}"
                )));
            }
        }
    }
    if let Some(additional) = object.get("additionalProperties") {
        if !additional.is_boolean() {
            return Err(manifest_error(format!(
                "{path}.additionalProperties must be a boolean"
            )));
        }
    }
    if let Some(items) = object.get("items") {
        validate_schema(items, &format!("{path}.items"))?;
    }
    if let Some(values) = object.get("enum") {
        if !values.is_array() {
            return Err(manifest_error(format!("{path}.enum must be an array")));
        }
    }
    Ok(())
}

fn validate_config(schema: &Value, value: &Value, path: &str) -> WasmResult<()> {
    let object = schema
        .as_object()
        .ok_or_else(|| manifest_error("config schema must be an object"))?;
    if let Some(kind) = object.get("type").and_then(Value::as_str) {
        let valid = match kind {
            "null" => value.is_null(),
            "boolean" => value.is_boolean(),
            "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
            "number" => value.is_number(),
            "string" => value.is_string(),
            "array" => value.is_array(),
            "object" => value.is_object(),
            _ => false,
        };
        if !valid {
            return Err(PluginError::new(
                "CONFIG_INVALID",
                format!("{path} must be {kind}"),
                "config",
            ));
        }
    }
    if let Some(values) = object.get("enum").and_then(Value::as_array) {
        if !values.contains(value) {
            return Err(PluginError::new(
                "CONFIG_INVALID",
                format!("{path} is not an allowed value"),
                "config",
            ));
        }
    }
    if let Some(values) = value.as_object() {
        let properties = object.get("properties").and_then(Value::as_object);
        if let Some(required) = object.get("required").and_then(Value::as_array) {
            for property in required.iter().filter_map(Value::as_str) {
                if !values.contains_key(property) {
                    return Err(PluginError::new(
                        "CONFIG_INVALID",
                        format!("{path}.{property} is required"),
                        "config",
                    ));
                }
            }
        }
        if let Some(properties) = properties {
            for (property, nested) in properties {
                if let Some(value) = values.get(property) {
                    validate_config(nested, value, &format!("{path}.{property}"))?;
                }
            }
            if object
                .get("additionalProperties")
                .and_then(Value::as_bool)
                .is_some_and(|allowed| !allowed)
            {
                for property in values.keys() {
                    if !properties.contains_key(property) {
                        return Err(PluginError::new(
                            "CONFIG_INVALID",
                            format!("{path}.{property} is not allowed"),
                            "config",
                        ));
                    }
                }
            }
        }
    }
    if let (Some(items), Some(values)) = (object.get("items"), value.as_array()) {
        for (index, value) in values.iter().enumerate() {
            validate_config(items, value, &format!("{path}[{index}]"))?;
        }
    }
    Ok(())
}

fn validate_wasm_exports(wasm: &[u8]) -> WasmResult<()> {
    let exports = wasm_function_exports(wasm)?;
    for export in GuestExport::ALL {
        if !exports.contains(export.as_str()) {
            return Err(PluginError::new(
                "ABI_EXPORT_MISSING",
                format!("module does not export {}", export.as_str()),
                "manifest",
            ));
        }
    }
    Ok(())
}

fn wasm_function_exports(wasm: &[u8]) -> WasmResult<BTreeSet<String>> {
    if wasm.len() < 8 || &wasm[..4] != b"\0asm" || wasm[4..8] != [1, 0, 0, 0] {
        return Err(PluginError::new(
            "MODULE_INVALID",
            "module is not a supported WebAssembly binary",
            "manifest",
        ));
    }
    let mut offset = 8;
    let mut exports = BTreeSet::new();
    while offset < wasm.len() {
        let id = wasm_byte(wasm, &mut offset)?;
        let size = wasm_u32(wasm, &mut offset)? as usize;
        let end = offset
            .checked_add(size)
            .filter(|end| *end <= wasm.len())
            .ok_or_else(|| {
                PluginError::new(
                    "MODULE_INVALID",
                    "truncated WebAssembly section",
                    "manifest",
                )
            })?;
        if id == 7 {
            let count = wasm_u32(wasm, &mut offset)?;
            for _ in 0..count {
                let name = wasm_name(wasm, &mut offset)?;
                let kind = wasm_byte(wasm, &mut offset)?;
                let _index = wasm_u32(wasm, &mut offset)?;
                if kind == 0 {
                    exports.insert(name);
                }
            }
            if offset != end {
                return Err(PluginError::new(
                    "MODULE_INVALID",
                    "invalid WebAssembly export section",
                    "manifest",
                ));
            }
        }
        offset = end;
    }
    Ok(exports)
}

fn wasm_byte(wasm: &[u8], offset: &mut usize) -> WasmResult<u8> {
    let byte = wasm.get(*offset).copied().ok_or_else(|| {
        PluginError::new("MODULE_INVALID", "truncated WebAssembly binary", "manifest")
    })?;
    *offset += 1;
    Ok(byte)
}

fn wasm_u32(wasm: &[u8], offset: &mut usize) -> WasmResult<u32> {
    let mut value = 0u32;
    for shift in (0..35).step_by(7) {
        let byte = wasm_byte(wasm, offset)?;
        value |= u32::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(PluginError::new(
        "MODULE_INVALID",
        "invalid WebAssembly integer",
        "manifest",
    ))
}

fn wasm_name(wasm: &[u8], offset: &mut usize) -> WasmResult<String> {
    let length = wasm_u32(wasm, offset)? as usize;
    let end = offset
        .checked_add(length)
        .filter(|end| *end <= wasm.len())
        .ok_or_else(|| {
            PluginError::new("MODULE_INVALID", "truncated WebAssembly name", "manifest")
        })?;
    let name = std::str::from_utf8(&wasm[*offset..end]).map_err(|error| {
        PluginError::new(
            "MODULE_INVALID",
            format!("invalid WebAssembly export name: {error}"),
            "manifest",
        )
    })?;
    *offset = end;
    Ok(name.to_owned())
}

fn json_size(value: &Value, phase: &str) -> WasmResult<usize> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|error| PluginError::new("PROTOCOL_INVALID", error.to_string(), phase))
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}
