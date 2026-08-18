//! The `cordis.plugin/v1` Extism host runtime.
//!
//! The runtime is deliberately synchronous at the guest boundary: one plugin
//! instance owns one Extism instance and its calls are serialized. The loader
//! still receives its usual asynchronous `LoaderRuntime` interface.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path},
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
const MAX_WASM_BYTES: usize = 8 * 1024 * 1024;
static NEXT_INSTANCE_ID: AtomicU64 = AtomicU64::new(0);
static INSTANCE_IDS: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());

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
        if !is_confined_entry(Path::new(&self.entry)) {
            return Err(manifest_error(
                "entry must be a relative path without traversal",
            ));
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

/// The runtime calls `install` after manifest/config validation and before
/// engine construction, with the exact entry and instance identity it will use.
pub trait WasmLifecycleHook: Send + Sync {
    fn install(
        &self,
        manifest: &PluginManifest,
        entry: &Entry,
        instance_id: &str,
    ) -> WasmResult<Box<dyn WasmLifecycleGuard>>;
}

/// The active registration produced by a [`WasmLifecycleHook`].
///
/// `drain` may wait only until `timeout`. `revoke` must reject future work
/// synchronously and must not block; it is also called from runtime `Drop`, as
/// is the guard's own non-blocking `Drop` implementation.
pub trait WasmLifecycleGuard: Send {
    fn drain(&mut self, timeout: Duration) -> WasmResult<()>;

    fn revoke(&mut self);
}

/// A registered host capability receives its capability-specific JSON payload.
#[derive(Clone, Debug)]
pub struct CapabilityRequest {
    pub capability: Capability,
    pub plugin_id: String,
    pub instance_id: String,
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
    instance_id: String,
    alive: Arc<AtomicBool>,
    max_input_bytes: usize,
    max_output_bytes: usize,
}

impl HostBindings {
    fn new(
        registry: Arc<CapabilityRegistry>,
        manifest: &PluginManifest,
        instance_id: String,
        alive: Arc<AtomicBool>,
        max_input_bytes: usize,
        max_output_bytes: usize,
    ) -> Self {
        Self {
            registry,
            permissions: Arc::new(manifest.permissions()),
            plugin_id: manifest.id.clone(),
            instance_id,
            alive,
            max_input_bytes,
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
                instance_id: self.instance_id.clone(),
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

    /// Returns this host binding's opaque process-unique instance identifier.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
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
                        let response = match plugin.memory_from_val(&inputs[0]) {
                            Some(handle) => {
                                let actual = handle.len();
                                if actual > bindings.max_input_bytes {
                                    HostResponse::failure(input_limit_error(
                                        bindings.max_input_bytes,
                                        actual,
                                    ))
                                } else {
                                    match plugin.memory_bytes(handle) {
                                        Ok(input) => match serde_json::from_slice::<Value>(input) {
                                            Ok(payload) => {
                                                match bindings.invoke(capability, payload) {
                                                    Ok(result) => HostResponse::success(result),
                                                    Err(error) => HostResponse::failure(error),
                                                }
                                            }
                                            Err(error) => HostResponse::failure(
                                                PluginError::new(
                                                    "invalid_request",
                                                    "invalid request envelope",
                                                    "host",
                                                )
                                                .with_details(json!({"reason": error.to_string()})),
                                            ),
                                        },
                                        Err(error) => HostResponse::failure(
                                            PluginError::new(
                                                "invalid_request",
                                                "invalid guest memory handle",
                                                "host",
                                            )
                                            .with_details(json!({"reason": error.to_string()})),
                                        ),
                                    }
                                }
                            }
                            None => HostResponse::failure(PluginError::new(
                                "invalid_request",
                                "invalid guest memory handle",
                                "host",
                            )),
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
        let package_dir = fs::canonicalize(path.parent().unwrap_or_else(|| Path::new(".")))
            .map_err(|error| {
                PluginError::new(
                    "PACKAGE_READ_FAILED",
                    format!(
                        "cannot resolve package directory for {}: {error}",
                        path.display()
                    ),
                    "instantiate",
                )
            })?;
        let entry = fs::canonicalize(package_dir.join(&manifest.entry)).map_err(|error| {
            PluginError::new(
                "PACKAGE_READ_FAILED",
                format!("cannot resolve package entry {}: {error}", manifest.entry),
                "instantiate",
            )
        })?;
        if !entry.starts_with(&package_dir) {
            return Err(manifest_error(
                "entry resolves outside its package directory",
            ));
        }
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

/// Synchronous guest API. Engines enforce the output bound before copying
/// guest memory into host-owned storage.
pub trait GuestInstance: Send {
    fn call(
        &mut self,
        export: GuestExport,
        input: &[u8],
        max_output_bytes: usize,
    ) -> WasmResult<Vec<u8>>;
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
            .map_err(|error| extism_error(error, "instantiate"))?;
        let cancellation = Arc::new(ExtismCancellation(plugin.cancel_handle()));
        Ok(Box::new(ExtismGuestInstance {
            plugin,
            cancellation,
            fuel_limit: limits.fuel,
        }))
    }
}

struct ExtismGuestInstance {
    plugin: Plugin,
    cancellation: Arc<ExtismCancellation>,
    fuel_limit: u64,
}

impl GuestInstance for ExtismGuestInstance {
    fn call(
        &mut self,
        export: GuestExport,
        input: &[u8],
        max_output_bytes: usize,
    ) -> WasmResult<Vec<u8>> {
        let output: &[u8] = match self.plugin.call(export.as_str(), input) {
            Ok(output) => output,
            Err(error) => {
                if self.plugin.fuel_consumed() == Some(self.fuel_limit) {
                    return Err(PluginError::new(
                        "FUEL_LIMIT_EXCEEDED",
                        "guest exhausted its configured fuel",
                        export.phase(),
                    ));
                }
                return Err(extism_error(error, export.phase()));
            }
        };
        if output.len() > max_output_bytes {
            return Err(output_limit_error(max_output_bytes, output.len()));
        }
        Ok(output.to_vec())
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
            .map_err(|error| extism_error(error, "cancel"))
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
    fn call(
        &mut self,
        export: GuestExport,
        input: &[u8],
        _max_output_bytes: usize,
    ) -> WasmResult<Vec<u8>> {
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
    instance_id: String,
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
        let instance_id = allocate_instance_id(&package.manifest.id);
        Self::instantiate_reserved_instance_id(
            package,
            engine,
            registry,
            limits,
            config,
            instance_id.clone(),
        )
        .inspect_err(|_| release_instance_id(&instance_id))
    }

    /// Instantiates a guest with an identity preinstalled by the host policy.
    pub fn instantiate_with_instance_id(
        package: WasmPackage,
        engine: Arc<dyn GuestEngine>,
        registry: Arc<CapabilityRegistry>,
        limits: ResourceLimits,
        config: Value,
        instance_id: impl Into<String>,
    ) -> WasmResult<Self> {
        let instance_id = reserve_instance_id(instance_id.into(), &package.manifest.id)?;
        Self::instantiate_reserved_instance_id(
            package,
            engine,
            registry,
            limits,
            config,
            instance_id.clone(),
        )
        .inspect_err(|_| release_instance_id(&instance_id))
    }

    fn instantiate_reserved_instance_id(
        package: WasmPackage,
        engine: Arc<dyn GuestEngine>,
        registry: Arc<CapabilityRegistry>,
        limits: ResourceLimits,
        config: Value,
        instance_id: String,
    ) -> WasmResult<Self> {
        package.manifest.validate()?;
        if let Some(wasm) = package.wasm.as_deref() {
            validate_wasm_exports(wasm)?;
        }
        limits.validate()?;
        package.manifest.validate_config(&config)?;
        let alive = Arc::new(AtomicBool::new(true));
        let host = HostBindings::new(
            registry,
            &package.manifest,
            instance_id.clone(),
            Arc::clone(&alive),
            limits.max_input_bytes,
            limits.max_output_bytes,
        );
        let guest = engine.instantiate(&package, host.clone(), limits.clone())?;
        Ok(Self {
            manifest: package.manifest,
            instance_id,
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

    /// Returns this instance's opaque process-unique identifier.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
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
        RequestEnvelope::new(format!("{}:{sequence}", self.instance_id), context, payload)
    }

    fn revoke(&self) {
        if self.accepting.swap(false, Ordering::AcqRel) {
            self.cancel_current();
            self.host.alive.store(false, Ordering::Release);
            self.guest_available.notify_all();
        }
    }
}

impl Drop for WasmPluginInstance {
    fn drop(&mut self) {
        self.revoke();
        release_instance_id(&self.instance_id);
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
    let output = guest.call(export, &input, limits.max_output_bytes);
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
    let document = serde_json::from_slice::<Value>(&output).map_err(|_| {
        PluginError::new(
            "PROTOCOL_INVALID",
            "guest response is not a valid response envelope",
            export.phase(),
        )
    })?;
    let object = document.as_object().ok_or_else(|| {
        PluginError::new(
            "PROTOCOL_INVALID",
            "guest response is not a valid response envelope",
            export.phase(),
        )
    })?;
    let result = object.get("result").cloned();
    let has_error = object.contains_key("error");
    if result.is_some() == has_error {
        return Err(PluginError::new(
            "PROTOCOL_INVALID",
            "guest response must contain exactly one result or error",
            export.phase(),
        ));
    }
    let response = serde_json::from_value::<ResponseEnvelope>(document).map_err(|_| {
        PluginError::new(
            "PROTOCOL_INVALID",
            "guest response is not a valid response envelope",
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
    if let Some(result) = result {
        return Ok(result);
    }
    let error = response.error.ok_or_else(|| {
        PluginError::new(
            "PROTOCOL_INVALID",
            "guest error must not be null",
            export.phase(),
        )
    })?;
    if error.phase != export.phase() {
        return Err(PluginError::new(
            "PROTOCOL_INVALID",
            "guest error phase does not match invoked export",
            export.phase(),
        ));
    }
    Err(guest_rejected_error(export))
}

/// Loader adapter for registered packages and real Extism modules.
pub struct WasmPluginRuntime {
    engine: Arc<dyn GuestEngine>,
    registry: Arc<CapabilityRegistry>,
    limits: ResourceLimits,
    lifecycle_hook: Option<Arc<dyn WasmLifecycleHook>>,
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
            lifecycle_hook: None,
            packages: Mutex::new(BTreeMap::new()),
        }
    }

    /// Installs a product-defined lifecycle hook for every runtime instance.
    pub fn with_lifecycle_hook(mut self, hook: Arc<dyn WasmLifecycleHook>) -> Self {
        self.lifecycle_hook = Some(hook);
        self
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
            module.manifest.validate().map_err(loader_error)?;
            if let Some(wasm) = module.wasm.as_deref() {
                validate_wasm_exports(wasm).map_err(loader_error)?;
            }
            self.limits.validate().map_err(loader_error)?;
            module
                .manifest
                .validate_config(&entry.options.config)
                .map_err(loader_error)?;
            let instance_id = allocate_instance_id(&module.manifest.id);
            let context = json!({
                "entryId": entry.options.id.to_string(),
                "entryName": entry.options.name,
                "inject": entry.options.inject,
            });
            let mut guard = self
                .lifecycle_hook
                .as_ref()
                .map(|hook| hook.install(&module.manifest, &entry, &instance_id))
                .transpose()
                .map_err(loader_error)?;
            let instance = match WasmPluginInstance::instantiate_reserved_instance_id(
                module,
                Arc::clone(&self.engine),
                Arc::clone(&self.registry),
                self.limits.clone(),
                entry.options.config,
                instance_id,
            ) {
                Ok(instance) => instance,
                Err(error) => {
                    if let Some(guard) = guard.as_mut() {
                        guard.revoke();
                    }
                    return Err(loader_error(error));
                }
            };
            Ok(Box::new(WasmRuntimeHandle {
                instance,
                context,
                activated: false,
                guard,
                limits: self.limits.clone(),
            }) as Box<dyn RuntimeHandle>)
        })
    }
}

struct WasmRuntimeHandle {
    instance: WasmPluginInstance,
    context: Value,
    activated: bool,
    guard: Option<Box<dyn WasmLifecycleGuard>>,
    limits: ResourceLimits,
}

impl WasmRuntimeHandle {
    fn drain_and_revoke_guard(&mut self) -> WasmResult<()> {
        let Some(mut guard) = self.guard.take() else {
            return Ok(());
        };
        let result = guard.drain(self.limits.timeout);
        guard.revoke();
        result
    }

    fn revoke_guard(&mut self) {
        if let Some(guard) = self.guard.as_mut() {
            guard.revoke();
        }
    }
}

impl RuntimeHandle for WasmRuntimeHandle {
    fn activate<'a>(&'a mut self) -> LoaderFuture<'a, ()> {
        Box::pin(async move {
            if self.instance.is_stopped() {
                return Err(loader_error(stopped_error(GuestExport::Init.phase())));
            }
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
        Box::pin(async move {
            let stopped = self.instance.stop();
            let drained = self.drain_and_revoke_guard();
            stopped.and(drained).map_err(loader_error)
        })
    }
}

impl Drop for WasmRuntimeHandle {
    fn drop(&mut self) {
        self.instance.revoke();
        self.revoke_guard();
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

fn extism_error(_error: extism::Error, phase: &str) -> PluginError {
    PluginError::new("GUEST_TRAP", "guest execution failed", phase)
}

fn guest_rejected_error(export: GuestExport) -> PluginError {
    PluginError::new("GUEST_REJECTED", "guest rejected request", export.phase())
}

fn allocate_instance_id(plugin_id: &str) -> String {
    loop {
        let instance_id = format!(
            "wasm:{}:{}",
            std::process::id(),
            NEXT_INSTANCE_ID.fetch_add(1, Ordering::Relaxed)
        );
        if instance_id != plugin_id && lock(&INSTANCE_IDS).insert(instance_id.clone()) {
            return instance_id;
        }
    }
}

fn reserve_instance_id(instance_id: String, plugin_id: &str) -> WasmResult<String> {
    if instance_id.trim().is_empty() || instance_id == plugin_id {
        return Err(PluginError::new(
            "INSTANCE_ID_INVALID",
            "instance id must be non-empty and differ from plugin id",
            "instantiate",
        ));
    }
    if !lock(&INSTANCE_IDS).insert(instance_id.clone()) {
        return Err(PluginError::new(
            "INSTANCE_ID_DUPLICATE",
            "instance id already exists",
            "instantiate",
        ));
    }
    Ok(instance_id)
}

fn release_instance_id(instance_id: &str) {
    lock(&INSTANCE_IDS).remove(instance_id);
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
    if wasm.len() > MAX_WASM_BYTES {
        return Err(module_error("module exceeds the 8 MiB size limit"));
    }
    let exports = wasm_function_exports(wasm)?;
    for export in GuestExport::ALL {
        let signature = exports.get(export.as_str()).ok_or_else(|| {
            PluginError::new(
                "ABI_EXPORT_MISSING",
                format!("module does not export {}", export.as_str()),
                "manifest",
            )
        })?;
        if !signature.0.is_empty() || signature.1 != [0x7f] {
            return Err(PluginError::new(
                "ABI_EXPORT_INVALID",
                format!("module export {} must have type () -> i32", export.as_str()),
                "manifest",
            ));
        }
    }
    Ok(())
}

struct WasmReader<'a> {
    bytes: &'a [u8],
    offset: usize,
    end: usize,
}

impl<'a> WasmReader<'a> {
    fn byte(&mut self) -> WasmResult<u8> {
        if self.offset == self.end {
            return Err(module_error("truncated WebAssembly section"));
        }
        let byte = self.bytes[self.offset];
        self.offset += 1;
        Ok(byte)
    }

    fn bytes(&mut self, length: usize) -> WasmResult<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.end)
            .ok_or_else(|| module_error("truncated WebAssembly section"))?;
        let bytes = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn u32(&mut self) -> WasmResult<u32> {
        let mut value = 0u32;
        for index in 0..5 {
            let byte = self.byte()?;
            let payload = u32::from(byte & 0x7f);
            if index == 4 && payload > 0x0f {
                return Err(module_error("invalid WebAssembly integer"));
            }
            value |= payload << (index * 7);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(module_error("invalid WebAssembly integer"))
    }

    fn name(&mut self) -> WasmResult<&'a str> {
        let length = self.u32()? as usize;
        std::str::from_utf8(self.bytes(length)?)
            .map_err(|error| module_error(format!("invalid WebAssembly name: {error}")))
    }
}

type WasmSignature<'a> = (&'a [u8], &'a [u8]);

fn wasm_function_exports(wasm: &[u8]) -> WasmResult<BTreeMap<String, WasmSignature<'_>>> {
    let mut reader = WasmReader {
        bytes: wasm,
        offset: 0,
        end: wasm.len(),
    };
    if reader.bytes(4)? != b"\0asm" || reader.bytes(4)? != [1, 0, 0, 0] {
        return Err(module_error("module is not a supported WebAssembly binary"));
    }

    let mut types = Vec::new();
    let mut functions = Vec::new();
    let mut exports = BTreeMap::new();
    while reader.offset < reader.end {
        let id = reader.byte()?;
        let length = reader.u32()? as usize;
        let end = reader
            .offset
            .checked_add(length)
            .filter(|end| *end <= reader.end)
            .ok_or_else(|| module_error("truncated WebAssembly section"))?;
        let mut section = WasmReader {
            bytes: wasm,
            offset: reader.offset,
            end,
        };
        match id {
            1 => {
                for _ in 0..section.u32()? {
                    if section.byte()? != 0x60 {
                        return Err(module_error("unsupported WebAssembly type declaration"));
                    }
                    let parameter_count = section.u32()? as usize;
                    let parameters = section.bytes(parameter_count)?;
                    let result_count = section.u32()? as usize;
                    let results = section.bytes(result_count)?;
                    types.push((parameters, results));
                }
            }
            2 => {
                for _ in 0..section.u32()? {
                    section.name()?;
                    section.name()?;
                    if section.byte()? != 0 {
                        return Err(module_error("unsupported non-function WebAssembly import"));
                    }
                    functions.push(section.u32()?);
                }
            }
            3 => {
                for _ in 0..section.u32()? {
                    functions.push(section.u32()?);
                }
            }
            7 => {
                for _ in 0..section.u32()? {
                    let name = section.name()?.to_owned();
                    let kind = section.byte()?;
                    let index = section.u32()?;
                    if kind == 0 && exports.insert(name, index).is_some() {
                        return Err(module_error("duplicate WebAssembly export name"));
                    }
                }
            }
            _ => section.offset = end,
        }
        if section.offset != end {
            return Err(module_error("invalid WebAssembly section"));
        }
        reader.offset = end;
    }

    exports
        .into_iter()
        .map(|(name, index)| {
            let type_index = *functions
                .get(index as usize)
                .ok_or_else(|| module_error("WebAssembly export has an invalid function index"))?;
            let signature = types
                .get(type_index as usize)
                .ok_or_else(|| module_error("WebAssembly function has an invalid type index"))?;
            Ok((name, *signature))
        })
        .collect()
}

fn module_error(message: impl Into<String>) -> PluginError {
    PluginError::new("MODULE_INVALID", message, "manifest")
}

fn is_confined_entry(entry: &Path) -> bool {
    entry
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
}

fn json_size(value: &Value, phase: &str) -> WasmResult<usize> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|error| PluginError::new("PROTOCOL_INVALID", error.to_string(), phase))
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}
