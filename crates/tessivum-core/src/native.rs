use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    future::{poll_fn, Future},
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Poll, Waker},
    thread::{self, JoinHandle},
};

use serde_json::Value;

use crate::{
    ActivationState, CancellationToken, ContextHandle, CoreError, Dependency, DependencySnapshot,
    EffectMeta, Entry, Fiber, FiberId, FiberState, Generation, LoaderError, LoaderFuture,
    LoaderRuntime, ResolvedPackage, RuntimeHandle, RuntimeKind, Scope, ScopeId,
};

/// A boxed asynchronous native-plugin operation.
pub type NativePluginFuture<'a, T = ()> =
    Pin<Box<dyn Future<Output = NativePluginResult<T>> + Send + 'a>>;
/// Result returned by native-plugin lifecycle operations.
pub type NativePluginResult<T = ()> = Result<T, NativePluginError>;

/// A closed configuration contract for a native plugin.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum NativeConfigSchema {
    #[default]
    Any,
    Null,
    Boolean,
    Integer,
    Number,
    String,
    Array(Box<NativeConfigSchema>),
    Object {
        properties: BTreeMap<String, NativeConfigSchema>,
        required: BTreeSet<String>,
        allow_additional: bool,
    },
}

impl NativeConfigSchema {
    fn validate_schema(&self, path: &str) -> Result<(), NativeConfigError> {
        match self {
            Self::Array(items) => items.validate_schema(&format!("{path}[]")),
            Self::Object {
                properties,
                required,
                ..
            } => {
                for property in required {
                    if !properties.contains_key(property) {
                        return Err(NativeConfigError::InvalidSchema {
                            path: path.to_owned(),
                            message: format!("required property {property} has no schema"),
                        });
                    }
                }
                for (property, schema) in properties {
                    schema.validate_schema(&property_path(path, property))?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn validate_value(&self, value: &Value, path: &str) -> Result<(), NativeConfigError> {
        match self {
            Self::Any => Ok(()),
            Self::Null => validate_type(value.is_null(), path, self, value),
            Self::Boolean => validate_type(value.is_boolean(), path, self, value),
            Self::Integer => validate_type(
                value.as_i64().is_some() || value.as_u64().is_some(),
                path,
                self,
                value,
            ),
            Self::Number => validate_type(value.is_number(), path, self, value),
            Self::String => validate_type(value.is_string(), path, self, value),
            Self::Array(items) => {
                let values = value.as_array().ok_or_else(|| NativeConfigError::Type {
                    path: path.to_owned(),
                    expected: NativeConfigKind::Array,
                    actual: value.kind(),
                })?;
                for (index, value) in values.iter().enumerate() {
                    items.validate_value(value, &format!("{path}[{index}]"))?;
                }
                Ok(())
            }
            Self::Object {
                properties,
                required,
                allow_additional,
            } => {
                let values = value.as_object().ok_or_else(|| NativeConfigError::Type {
                    path: path.to_owned(),
                    expected: NativeConfigKind::Object,
                    actual: value.kind(),
                })?;
                for property in required {
                    if !values.contains_key(property) {
                        return Err(NativeConfigError::Required {
                            path: path.to_owned(),
                            property: property.clone(),
                        });
                    }
                }
                for (property, value) in values {
                    match properties.get(property) {
                        Some(schema) => {
                            schema.validate_value(value, &property_path(path, property))?
                        }
                        None if !allow_additional => {
                            return Err(NativeConfigError::AdditionalProperty {
                                path: path.to_owned(),
                                property: property.clone(),
                            });
                        }
                        None => {}
                    }
                }
                Ok(())
            }
        }
    }

    fn kind(&self) -> NativeConfigKind {
        match self {
            Self::Any => NativeConfigKind::Any,
            Self::Null => NativeConfigKind::Null,
            Self::Boolean => NativeConfigKind::Boolean,
            Self::Integer => NativeConfigKind::Integer,
            Self::Number => NativeConfigKind::Number,
            Self::String => NativeConfigKind::String,
            Self::Array(_) => NativeConfigKind::Array,
            Self::Object { .. } => NativeConfigKind::Object,
        }
    }
}

/// One native configuration value kind, used in structured validation errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeConfigKind {
    Any,
    Null,
    Boolean,
    Integer,
    Number,
    String,
    Array,
    Object,
}

impl fmt::Display for NativeConfigKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Any => "any value",
            Self::Null => "null",
            Self::Boolean => "boolean",
            Self::Integer => "integer",
            Self::Number => "number",
            Self::String => "string",
            Self::Array => "array",
            Self::Object => "object",
        })
    }
}

trait JsonKind {
    fn kind(&self) -> NativeConfigKind;
}

impl JsonKind for Value {
    fn kind(&self) -> NativeConfigKind {
        match self {
            Value::Null => NativeConfigKind::Null,
            Value::Bool(_) => NativeConfigKind::Boolean,
            Value::Number(value) if value.as_i64().is_some() || value.as_u64().is_some() => {
                NativeConfigKind::Integer
            }
            Value::Number(_) => NativeConfigKind::Number,
            Value::String(_) => NativeConfigKind::String,
            Value::Array(_) => NativeConfigKind::Array,
            Value::Object(_) => NativeConfigKind::Object,
        }
    }
}

/// A structured native configuration validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeConfigError {
    InvalidSchema {
        path: String,
        message: String,
    },
    Type {
        path: String,
        expected: NativeConfigKind,
        actual: NativeConfigKind,
    },
    Required {
        path: String,
        property: String,
    },
    AdditionalProperty {
        path: String,
        property: String,
    },
}

impl fmt::Display for NativeConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSchema { path, message } => {
                write!(
                    formatter,
                    "invalid native config schema at {path}: {message}"
                )
            }
            Self::Type {
                path,
                expected,
                actual,
            } => write!(
                formatter,
                "native config at {path} must be {expected}, found {actual}"
            ),
            Self::Required { path, property } => {
                write!(
                    formatter,
                    "native config at {path} is missing required property {property}"
                )
            }
            Self::AdditionalProperty { path, property } => {
                write!(
                    formatter,
                    "native config at {path} has unexpected property {property}"
                )
            }
        }
    }
}

impl Error for NativeConfigError {}

/// Declarative metadata for a native plugin.
#[derive(Clone, Debug, PartialEq)]
pub struct NativePluginDescriptor {
    pub name: String,
    pub version: String,
    pub dependencies: Vec<Dependency>,
    pub config_schema: NativeConfigSchema,
}

impl NativePluginDescriptor {
    /// Validates metadata before a plugin is allowed to run.
    pub fn validate(&self) -> NativePluginResult<()> {
        if self.name.trim().is_empty() {
            return Err(NativePluginError::Descriptor {
                field: "name",
                message: "must not be blank".into(),
            });
        }
        if self.version.trim().is_empty() {
            return Err(NativePluginError::Descriptor {
                field: "version",
                message: "must not be blank".into(),
            });
        }

        let mut dependencies = BTreeSet::new();
        for dependency in &self.dependencies {
            let key = dependency.key().diagnostic_key();
            if !dependencies.insert(key.clone()) {
                return Err(NativePluginError::Descriptor {
                    field: "dependencies",
                    message: format!("contains duplicate dependency {key}"),
                });
            }
        }
        self.config_schema
            .validate_schema("$schema")
            .map_err(NativePluginError::Config)
    }

    /// Validates a resolved configuration value against this descriptor's schema.
    pub fn validate_config(&self, config: &Value) -> NativePluginResult<()> {
        self.validate()?;
        self.config_schema
            .validate_value(config, "$")
            .map_err(NativePluginError::Config)
    }
}

/// The lifecycle phases in which a native plugin can fail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativePluginPhase {
    Start,
    Update,
    Stop,
}

impl NativePluginPhase {
    fn operation(self) -> &'static str {
        match self {
            Self::Start => "start native plugin",
            Self::Update => "update native plugin",
            Self::Stop => "stop native plugin",
        }
    }
}

/// A native-plugin lifecycle, descriptor, or configuration failure.
#[derive(Clone, Debug, PartialEq)]
pub enum NativePluginError {
    Descriptor {
        field: &'static str,
        message: String,
    },
    Config(NativeConfigError),
    Core(CoreError),
    Plugin {
        phase: NativePluginPhase,
        message: String,
    },
    Runtime {
        package: String,
        message: String,
    },
    Cleanup(Vec<NativePluginError>),
}

impl NativePluginError {
    pub fn plugin(phase: NativePluginPhase, message: impl Into<String>) -> Self {
        Self::Plugin {
            phase,
            message: message.into(),
        }
    }

    fn cleanup(errors: Vec<Self>) -> Self {
        match errors.len() {
            0 => unreachable!("a cleanup error needs at least one cause"),
            1 => errors.into_iter().next().expect("one error"),
            _ => Self::Cleanup(errors),
        }
    }
}

impl From<CoreError> for NativePluginError {
    fn from(error: CoreError) -> Self {
        Self::Core(error)
    }
}

impl fmt::Display for NativePluginError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Descriptor { field, message } => {
                write!(
                    formatter,
                    "invalid native plugin descriptor {field}: {message}"
                )
            }
            Self::Config(error) => error.fmt(formatter),
            Self::Core(error) => error.fmt(formatter),
            Self::Plugin { phase, message } => {
                write!(formatter, "native plugin {phase:?} failed: {message}")
            }
            Self::Runtime { package, message } => {
                write!(
                    formatter,
                    "native plugin package {package} failed: {message}"
                )
            }
            Self::Cleanup(errors) => {
                write!(formatter, "{} native plugin cleanup error(s)", errors.len())
            }
        }
    }
}

impl Error for NativePluginError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Core(error) => Some(error),
            Self::Cleanup(errors) => errors.first().map(|error| error as &(dyn Error + 'static)),
            _ => None,
        }
    }
}

/// A same-process plugin. `ContextHandle` exposes typed service and event APIs;
/// JSON is restricted to configuration validation and never crosses those hot paths.
pub trait NativePlugin: Send {
    fn descriptor(&self) -> NativePluginDescriptor;

    fn start<'a>(&'a mut self, context: ContextHandle, config: &'a Value)
        -> NativePluginFuture<'a>;

    fn update<'a>(
        &'a mut self,
        context: ContextHandle,
        config: &'a Value,
    ) -> NativePluginFuture<'a>;

    fn stop<'a>(&'a mut self, context: ContextHandle) -> NativePluginFuture<'a>;
}

/// The durable diagnostic state of one native plugin fiber.
#[derive(Clone, Debug, PartialEq)]
pub struct NativeFiberSnapshot {
    pub id: FiberId,
    pub name: String,
    pub state: FiberState,
    pub scope: ScopeId,
    pub resources: Vec<EffectMeta>,
}

/// A copyable inspection view of one native plugin instance.
#[derive(Clone, Debug, PartialEq)]
pub struct NativePluginSnapshot {
    pub descriptor: NativePluginDescriptor,
    pub config: Value,
    pub fiber: NativeFiberSnapshot,
}

/// A wakeable readiness signal driven by a scope-owned dependency subscription.
#[derive(Default)]
struct DependencySignal {
    ready: bool,
    waker: Option<Waker>,
}

struct DependencyGate {
    subscription: Option<crate::DependencySubscription>,
    signal: Arc<Mutex<DependencySignal>>,
}

impl DependencyGate {
    fn new(context: ContextHandle, dependencies: Vec<Dependency>) -> Result<Self, CoreError> {
        let signal = Arc::new(Mutex::new(DependencySignal {
            ready: dependencies.is_empty(),
            waker: None,
        }));
        if dependencies.is_empty() {
            return Ok(Self {
                subscription: None,
                signal,
            });
        }

        let listener_signal = Arc::clone(&signal);
        let subscription = context.subscribe(dependencies, move |snapshot| {
            let waker = {
                let mut signal = listener_signal
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                signal.ready = snapshot.ready;
                signal.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        })?;
        Ok(Self {
            subscription: Some(subscription),
            signal,
        })
    }

    async fn wait(&self, cancellation: crate::CancellationToken) -> Result<(), CoreError> {
        if cancellation.is_cancelled() {
            return Err(CoreError::Cancelled);
        }
        if self
            .subscription
            .as_ref()
            .is_none_or(crate::DependencySubscription::ready)
        {
            return if cancellation.is_cancelled() {
                Err(CoreError::Cancelled)
            } else {
                Ok(())
            };
        }

        let signal = Arc::clone(&self.signal);
        let mut cancelled = Box::pin(cancellation.cancelled());
        poll_fn(move |context| {
            if cancelled.as_mut().poll(context).is_ready() {
                return Poll::Ready(Err(CoreError::Cancelled));
            }

            {
                let mut signal = signal.lock().unwrap_or_else(|poison| poison.into_inner());
                if signal.ready {
                    return Poll::Ready(Ok(()));
                }
                signal.waker = Some(context.waker().clone());
            }

            if cancelled.as_mut().poll(context).is_ready() {
                Poll::Ready(Err(CoreError::Cancelled))
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

/// One plugin and the fiber which owns every lifecycle-created resource.
pub struct NativePluginInstance {
    plugin: Box<dyn NativePlugin>,
    factory: Option<NativePluginFactory>,
    descriptor: NativePluginDescriptor,
    context: ContextHandle,
    config: Value,
    fiber: Fiber,
    dependency_gate: Option<DependencyGate>,
    start_attempted: bool,
    stopped: bool,
}

impl fmt::Debug for NativePluginInstance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativePluginInstance")
            .field("descriptor", &self.descriptor)
            .field("fiber", &self.snapshot().fiber)
            .finish()
    }
}

impl NativePluginInstance {
    /// Creates a pending instance after validating the descriptor and initial config.
    pub fn instantiate(
        plugin: Box<dyn NativePlugin>,
        context: ContextHandle,
        config: Value,
    ) -> NativePluginResult<Self> {
        Self::instantiate_inner(plugin, None, context, config)
    }

    /// Creates an instance which can stage a fresh plugin during transactional updates.
    pub fn instantiate_with_factory(
        factory: NativePluginFactory,
        context: ContextHandle,
        config: Value,
    ) -> NativePluginResult<Self> {
        Self::instantiate_inner(factory(), Some(factory), context, config)
    }

    fn instantiate_inner(
        plugin: Box<dyn NativePlugin>,
        factory: Option<NativePluginFactory>,
        context: ContextHandle,
        config: Value,
    ) -> NativePluginResult<Self> {
        let descriptor = plugin.descriptor();
        descriptor.validate_config(&config)?;
        let fiber = Fiber::new(&context.scope(), descriptor.name.clone())?;
        Ok(Self {
            plugin,
            factory,
            descriptor,
            context,
            config,
            fiber,
            start_attempted: false,
            dependency_gate: None,
            stopped: false,
        })
    }

    pub fn descriptor(&self) -> &NativePluginDescriptor {
        &self.descriptor
    }

    pub fn config(&self) -> &Value {
        &self.config
    }

    pub fn context(&self) -> ContextHandle {
        self.context.clone()
    }

    pub fn fiber(&self) -> &Fiber {
        &self.fiber
    }

    pub fn snapshot(&self) -> NativePluginSnapshot {
        let scope = self.fiber.scope();
        NativePluginSnapshot {
            descriptor: self.descriptor.clone(),
            config: self.config.clone(),
            fiber: NativeFiberSnapshot {
                id: self.fiber.id(),
                name: self.fiber.name().to_owned(),
                state: self.fiber.state(),
                scope: scope.id(),
                resources: scope.effects(),
            },
        }
    }

    /// Starts exactly once after every required descriptor dependency is available.
    /// A failed plugin start is rolled back by Fiber before this future resolves,
    /// then `stop` is called once for partial plugin state.
    pub async fn start(&mut self) -> NativePluginResult<()> {
        self.descriptor.validate_config(&self.config)?;
        if self.fiber.state() != FiberState::Pending {
            return Err(invalid_state("start native plugin", self.fiber.state()));
        }
        let cancellation = self.cancellation();
        self.wait_for_dependencies(cancellation).await?;
        self.start_attempted = true;

        let base = self.context.clone();
        let config = &self.config;
        let (plugin, fiber) = (&mut self.plugin, &self.fiber);
        let result = fiber
            .start(|scope| {
                let context = base.with_scope(scope);
                async move {
                    plugin
                        .start(context, config)
                        .await
                        .map_err(|error| lifecycle_error(error, NativePluginPhase::Start))
                }
            })
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                self.finish_failed_start(NativePluginError::Core(error))
                    .await
            }
        }
    }

    /// Stages a fresh plugin and Fiber, preserving the active instance until
    /// the candidate's update and start hooks have both succeeded.
    pub async fn update(&mut self, config: Value) -> NativePluginResult<()> {
        self.descriptor.validate_config(&config)?;
        if self.fiber.state() != FiberState::Active {
            return Err(invalid_state("update native plugin", self.fiber.state()));
        }
        let factory = self
            .factory
            .clone()
            .ok_or_else(|| NativePluginError::Runtime {
                package: self.descriptor.name.clone(),
                message: "transactional updates require an instance factory".into(),
            })?;
        let cancellation = self.cancellation();
        let mut candidate = Self::instantiate_with_factory(factory, self.context.staged(), config)?;

        if let Err(error) = candidate.start_after_update(cancellation).await {
            return match candidate.dispose().await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(NativePluginError::cleanup(vec![error, cleanup])),
            };
        }
        candidate.context.commit_staged();
        let result = self.stop_current().await;
        *self = candidate;
        result
    }

    async fn start_after_update(
        &mut self,
        cancellation: CancellationToken,
    ) -> NativePluginResult<()> {
        self.wait_for_dependencies(cancellation).await?;
        self.start_attempted = true;
        let base = self.context.clone();
        let config = &self.config;
        let (plugin, fiber) = (&mut self.plugin, &self.fiber);
        let result = fiber
            .start(|scope| {
                let context = base.with_scope(scope);
                async move {
                    plugin
                        .update(context.clone(), config)
                        .await
                        .map_err(|error| lifecycle_error(error, NativePluginPhase::Update))?;
                    plugin
                        .start(context, config)
                        .await
                        .map_err(|error| lifecycle_error(error, NativePluginPhase::Start))
                }
            })
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                self.finish_failed_start(NativePluginError::Core(error))
                    .await
            }
        }
    }

    async fn wait_for_dependencies(
        &mut self,
        cancellation: CancellationToken,
    ) -> NativePluginResult<()> {
        if self.dependency_gate.is_none() {
            let context = self.context.with_scope(self.fiber.scope());
            self.dependency_gate = Some(DependencyGate::new(
                context,
                self.descriptor.dependencies.clone(),
            )?);
        }
        self.dependency_gate
            .as_ref()
            .expect("dependency gate is initialized")
            .wait(cancellation)
            .await
            .map_err(Into::into)
    }

    fn cancellation(&self) -> crate::CancellationToken {
        self.fiber.scope().cancellation()
    }

    async fn suspend(&mut self) -> NativePluginResult<()> {
        if self.fiber.state() == FiberState::Pending {
            return Ok(());
        }
        self.stop_current().await?;
        self.dependency_gate = None;
        self.fiber = Fiber::new(&self.context.scope(), self.descriptor.name.clone())?;
        self.start_attempted = false;
        self.stopped = false;
        Ok(())
    }

    /// Stops the plugin once and waits for all owned child scopes and effects.
    pub async fn dispose(&mut self) -> NativePluginResult<()> {
        self.stop_current().await
    }

    async fn finish_failed_start(&mut self, failure: NativePluginError) -> NativePluginResult<()> {
        match self.stop_plugin().await {
            Ok(()) => Err(failure),
            Err(cleanup) => Err(NativePluginError::cleanup(vec![failure, cleanup])),
        }
    }

    async fn stop_current(&mut self) -> NativePluginResult<()> {
        let mut errors = Vec::new();
        if let Err(error) = self.stop_plugin().await {
            errors.push(error);
        }
        if let Err(error) = self.fiber.dispose().await {
            errors.push(NativePluginError::Core(error));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(NativePluginError::cleanup(errors))
        }
    }

    async fn stop_plugin(&mut self) -> NativePluginResult<()> {
        if !self.start_attempted || self.stopped {
            return Ok(());
        }
        self.stopped = true;
        let context = self.context.with_scope(self.fiber.scope());
        self.plugin.stop(context).await
    }
}

fn validate_type(
    valid: bool,
    path: &str,
    schema: &NativeConfigSchema,
    value: &Value,
) -> Result<(), NativeConfigError> {
    if valid {
        Ok(())
    } else {
        Err(NativeConfigError::Type {
            path: path.to_owned(),
            expected: schema.kind(),
            actual: value.kind(),
        })
    }
}

fn invalid_state(operation: &'static str, state: FiberState) -> NativePluginError {
    NativePluginError::Core(CoreError::InvalidState { operation, state })
}

fn lifecycle_error(error: NativePluginError, phase: NativePluginPhase) -> CoreError {
    match error {
        NativePluginError::Core(error) => error,
        error => CoreError::Plugin {
            phase: phase.operation(),
            message: error.to_string(),
        },
    }
}

fn property_path(path: &str, property: &str) -> String {
    format!("{path}.{property}")
}

/// A construct-on-demand factory for one native plugin implementation.
pub type NativePluginFactory = Arc<dyn Fn() -> Box<dyn NativePlugin> + Send + Sync>;

/// A stable inspection view of the native package registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativePluginRuntimeSnapshot {
    pub packages: Vec<String>,
}

/// The native Loader runtime. Factories are closures rather than a second plugin
/// abstraction, so every loader candidate gets a fresh native instance.
#[derive(Default)]
pub struct NativePluginRuntime {
    factories: BTreeMap<String, NativePluginFactory>,
}

impl NativePluginRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<P, F>(
        &mut self,
        package: impl Into<String>,
        factory: F,
    ) -> NativePluginResult<()>
    where
        P: NativePlugin + 'static,
        F: Fn() -> P + Send + Sync + 'static,
    {
        self.register_boxed(package, Arc::new(move || Box::new(factory())))
    }

    pub fn register_boxed(
        &mut self,
        package: impl Into<String>,
        factory: NativePluginFactory,
    ) -> NativePluginResult<()> {
        let package = package.into();
        if package.trim().is_empty() {
            return Err(NativePluginError::Runtime {
                package,
                message: "package specifier must not be blank".into(),
            });
        }
        if self.factories.contains_key(&package) {
            return Err(NativePluginError::Runtime {
                package,
                message: "a factory is already registered".into(),
            });
        }
        self.factories.insert(package, factory);
        Ok(())
    }

    pub fn snapshot(&self) -> NativePluginRuntimeSnapshot {
        NativePluginRuntimeSnapshot {
            packages: self.factories.keys().cloned().collect(),
        }
    }
}

impl LoaderRuntime for NativePluginRuntime {
    fn kind(&self) -> RuntimeKind {
        RuntimeKind::Native
    }

    fn instantiate<'a>(
        &'a self,
        package: ResolvedPackage,
        entry: Entry,
        context: ContextHandle,
    ) -> LoaderFuture<'a, Box<dyn RuntimeHandle>> {
        Box::pin(async move {
            let factory = self.factories.get(&package.specifier).ok_or_else(|| {
                LoaderError::Validation(format!(
                    "no native plugin factory is registered for {}",
                    package.specifier
                ))
            })?;
            let instance = NativePluginInstance::instantiate_with_factory(
                Arc::clone(factory),
                context,
                entry.options.config,
            )
            .map_err(|error| LoaderError::Validation(error.to_string()))?;
            let handle = NativeRuntimeHandle::new(instance)
                .map_err(|error| LoaderError::Validation(error.to_string()))?;
            Ok(Box::new(handle) as Box<dyn RuntimeHandle>)
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ManagedPhase {
    Pending,
    Starting,
    Active,
    Failed,
}

struct ManagedState {
    activated: bool,
    stopping: bool,
    dirty: bool,
    snapshot: DependencySnapshot,
    phase: ManagedPhase,
    error: Option<NativePluginError>,
    cancellation: Option<CancellationToken>,
    waker: Option<Waker>,
}

struct ManagedControl {
    state: Mutex<ManagedState>,
    changed: Condvar,
}

impl ManagedControl {
    fn new(dependencies: &[Dependency]) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ManagedState {
                activated: false,
                stopping: false,
                dirty: false,
                snapshot: DependencySnapshot {
                    ready: dependencies
                        .iter()
                        .all(|dependency| !dependency.is_required()),
                    services: Vec::new(),
                },
                phase: ManagedPhase::Pending,
                error: None,
                cancellation: None,
                waker: None,
            }),
            changed: Condvar::new(),
        })
    }

    fn set_snapshot(&self, snapshot: DependencySnapshot) {
        let waker = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.snapshot = snapshot;
            state.dirty = true;
            state.waker.take()
        };
        self.changed.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn set_cancellation(&self, cancellation: CancellationToken) {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .cancellation = Some(cancellation);
    }

    fn request_activation(&self) {
        let waker = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.activated = true;
            state.dirty = true;
            state.waker.take()
        };
        self.changed.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn set_phase(&self, phase: ManagedPhase, error: Option<NativePluginError>) {
        let waker = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.phase = phase;
            state.error = error;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn next_snapshot(&self) -> (bool, DependencySnapshot) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        while !state.stopping && (!state.activated || !state.dirty) {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|poison| poison.into_inner());
        }
        let stopping = state.stopping;
        let snapshot = state.snapshot.clone();
        state.dirty = false;
        (stopping, snapshot)
    }

    fn activation(self: &Arc<Self>) -> LoaderFuture<'static, ActivationState> {
        self.request_activation();
        let control = Arc::clone(self);
        Box::pin(poll_fn(move |context| {
            let mut state = control
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            match state.phase {
                ManagedPhase::Active if state.snapshot.ready => {
                    Poll::Ready(Ok(ActivationState::Active))
                }
                ManagedPhase::Failed => Poll::Ready(Err(LoaderError::Validation(
                    state
                        .error
                        .as_ref()
                        .expect("failed native runtime stores its error")
                        .to_string(),
                ))),
                ManagedPhase::Pending if !state.snapshot.ready => {
                    Poll::Ready(Ok(ActivationState::Pending))
                }
                _ => {
                    state.waker = Some(context.waker().clone());
                    Poll::Pending
                }
            }
        }))
    }

    fn stop(&self) -> Option<CancellationToken> {
        let (cancellation, waker) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.stopping = true;
            state.dirty = true;
            (state.cancellation.clone(), state.waker.take())
        };
        self.changed.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
        cancellation
    }
}

struct ManagedWatcher {
    scope: Scope,
    subscription: crate::DependencySubscription,
}

impl ManagedWatcher {
    fn new(
        context: ContextHandle,
        dependencies: Vec<Dependency>,
        control: Arc<ManagedControl>,
    ) -> Result<Self, CoreError> {
        let scope = context.scope().child()?;
        let subscription_context = context.with_scope(scope.clone());
        let listener_control = Arc::clone(&control);
        let subscription = subscription_context.subscribe(dependencies, move |snapshot| {
            listener_control.set_snapshot(snapshot);
        })?;
        let disposer_control = Arc::clone(&control);
        let disposer: crate::BoxDisposer = Box::new(move || {
            if let Some(cancellation) = disposer_control.stop() {
                cancellation.cancel();
            }
            Box::pin(async { Ok(()) })
        });
        if let Err(error) = scope.add_effect("native-dependency-manager", disposer) {
            subscription.unsubscribe();
            return Err(error);
        }
        Ok(Self {
            scope,
            subscription,
        })
    }

    async fn dispose(self) -> Result<(), CoreError> {
        let _ = self.subscription.snapshot();
        self.scope.dispose().await
    }
}

fn required_generations(
    snapshot: &DependencySnapshot,
    dependencies: &[Dependency],
) -> Vec<Option<Generation>> {
    dependencies
        .iter()
        .zip(&snapshot.services)
        .filter_map(|(dependency, service)| dependency.is_required().then_some(service.generation))
        .collect()
}

fn run_managed_native(
    mut instance: NativePluginInstance,
    dependencies: Vec<Dependency>,
    control: Arc<ManagedControl>,
    watcher: ManagedWatcher,
) -> NativePluginResult<()> {
    let package = instance.descriptor().name.clone();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| NativePluginError::Runtime {
            package,
            message: format!("failed to create Tokio runtime: {error}"),
        })?;
    let mut active_generations = None;
    loop {
        let (stopping, snapshot) = control.next_snapshot();
        if stopping {
            let mut errors = Vec::new();
            if let Err(error) = native_block_on(&runtime, instance.dispose()) {
                errors.push(error);
            }
            if let Err(error) = native_block_on(&runtime, watcher.dispose()) {
                errors.push(NativePluginError::Core(error));
            }
            return if errors.is_empty() {
                Ok(())
            } else {
                Err(NativePluginError::cleanup(errors))
            };
        }

        let generations = required_generations(&snapshot, &dependencies);
        if !snapshot.ready {
            if instance.fiber().state() != FiberState::Pending {
                if let Err(error) = native_block_on(&runtime, instance.suspend()) {
                    control.set_phase(ManagedPhase::Failed, Some(error));
                    continue;
                }
                control.set_cancellation(instance.cancellation());
            }
            active_generations = None;
            control.set_phase(ManagedPhase::Pending, None);
            continue;
        }

        match instance.fiber().state() {
            FiberState::Pending => {
                control.set_phase(ManagedPhase::Starting, None);
                match native_block_on(&runtime, instance.start()) {
                    Ok(()) => {
                        active_generations = Some(generations);
                        control.set_phase(ManagedPhase::Active, None);
                    }
                    Err(error) => control.set_phase(ManagedPhase::Failed, Some(error)),
                }
            }
            FiberState::Active if active_generations.as_ref() != Some(&generations) => {
                control.set_phase(ManagedPhase::Starting, None);
                match native_block_on(&runtime, instance.update(instance.config().clone())) {
                    Ok(()) => {
                        active_generations = Some(generations);
                        control.set_cancellation(instance.cancellation());
                        control.set_phase(ManagedPhase::Active, None);
                    }
                    Err(error) => control.set_phase(ManagedPhase::Failed, Some(error)),
                }
            }
            FiberState::Active => control.set_phase(ManagedPhase::Active, None),
            _ => match native_block_on(&runtime, instance.suspend()) {
                Ok(()) => {
                    control.set_cancellation(instance.cancellation());
                    control.set_phase(ManagedPhase::Pending, None);
                }
                Err(error) => control.set_phase(ManagedPhase::Failed, Some(error)),
            },
        }
    }
}

fn native_block_on<F: Future>(runtime: &tokio::runtime::Runtime, future: F) -> F::Output {
    runtime.block_on(future)
}

struct NativeRuntimeHandle {
    control: Arc<ManagedControl>,
    worker: Option<JoinHandle<NativePluginResult<()>>>,
}

impl NativeRuntimeHandle {
    fn new(instance: NativePluginInstance) -> NativePluginResult<Self> {
        let dependencies = instance.descriptor().dependencies.clone();
        let control = ManagedControl::new(&dependencies);
        let watcher = ManagedWatcher::new(
            instance.context(),
            dependencies.clone(),
            Arc::clone(&control),
        )?;
        control.set_cancellation(instance.cancellation());
        let worker_control = Arc::clone(&control);
        let worker = thread::spawn(move || {
            run_managed_native(instance, dependencies, worker_control, watcher)
        });
        Ok(Self {
            control,
            worker: Some(worker),
        })
    }
}

impl RuntimeHandle for NativeRuntimeHandle {
    fn activate<'a>(&'a mut self) -> LoaderFuture<'a, ()> {
        let control = Arc::clone(&self.control);
        Box::pin(async move { control.activation().await.map(|_| ()) })
    }

    fn activation<'a>(&'a mut self) -> LoaderFuture<'a, ActivationState> {
        self.control.activation()
    }

    fn dispose<'a>(&'a mut self) -> LoaderFuture<'a, ()> {
        if let Some(cancellation) = self.control.stop() {
            cancellation.cancel();
        }
        let worker = self.worker.take();
        Box::pin(async move {
            match worker {
                Some(worker) => match worker.join() {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(LoaderError::Validation(error.to_string())),
                    Err(_) => Err(LoaderError::Validation(
                        "native runtime worker panicked".into(),
                    )),
                },
                None => Ok(()),
            }
        })
    }
}
