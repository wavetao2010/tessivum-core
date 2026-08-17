use std::{
    collections::{BTreeMap, BTreeSet},
    future::{poll_fn, Future},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::Poll,
    time::Duration,
};

use serde_json::{json, Value};
use tessivum_core::{
    ContextHandle, CoreError, Dependency, Entry, EntryId, EntryOptions, EntryTree, EventKey,
    EventOptions, Loader, LoaderFuture, LoaderRuntime, NativeConfigError, NativeConfigSchema,
    NativePlugin, NativePluginDescriptor, NativePluginError, NativePluginFuture,
    NativePluginInstance, NativePluginRuntime, PackageResolver, ResolvedPackage, RuntimeKind,
    ServiceKey,
};

#[derive(Default, Debug, PartialEq)]
struct Log {
    starts: Vec<String>,
    updates: Vec<String>,
    stops: Vec<String>,
    events: Vec<String>,
}

type SharedLog = Arc<Mutex<Log>>;

fn provider_service() -> ServiceKey {
    ServiceKey::new("minimal.provider", "native/v1")
}

fn managed_dependency_service() -> ServiceKey {
    ServiceKey::new("managed.dependency", "native/v1")
}

fn managed_service() -> ServiceKey {
    ServiceKey::new("managed.service", "native/v1")
}

fn notice_event() -> EventKey<Notice> {
    EventKey::new("minimal.notice")
}

fn object_schema(
    properties: BTreeMap<String, NativeConfigSchema>,
    required: BTreeSet<String>,
) -> NativeConfigSchema {
    NativeConfigSchema::Object {
        properties,
        required,
        allow_additional: false,
    }
}

fn provider_schema() -> NativeConfigSchema {
    object_schema(
        BTreeMap::from([("label".into(), NativeConfigSchema::String)]),
        BTreeSet::from(["label".into()]),
    )
}

fn probe_schema() -> NativeConfigSchema {
    object_schema(
        BTreeMap::from([("enabled".into(), NativeConfigSchema::Boolean)]),
        BTreeSet::from(["enabled".into()]),
    )
}

#[derive(Clone)]
struct ProviderService(&'static str);

struct Notice(&'static str);

struct Provider {
    label: &'static str,
    log: SharedLog,
}

impl NativePlugin for Provider {
    fn descriptor(&self) -> NativePluginDescriptor {
        NativePluginDescriptor {
            name: "minimal-provider".into(),
            version: self.label.into(),
            dependencies: Vec::new(),
            config_schema: provider_schema(),
        }
    }

    fn start<'a>(
        &'a mut self,
        context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        let label = self.label;
        let log = Arc::clone(&self.log);
        Box::pin(async move {
            context
                .provide(provider_service(), ProviderService(label))
                .expect("provider service registers in its scoped transaction");
            log.lock()
                .expect("provider log is available")
                .starts
                .push(format!("provider:{label}"));
            Ok(())
        })
    }

    fn update<'a>(
        &'a mut self,
        _context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn stop<'a>(&'a mut self, _context: ContextHandle) -> NativePluginFuture<'a> {
        let label = self.label;
        let log = Arc::clone(&self.log);
        Box::pin(async move {
            log.lock()
                .expect("provider log is available")
                .stops
                .push(format!("provider:{label}"));
            Ok(())
        })
    }
}

struct Consumer {
    log: SharedLog,
    dependencies: Vec<Dependency>,
    on_start: Option<tokio::sync::mpsc::UnboundedSender<&'static str>>,
}

impl NativePlugin for Consumer {
    fn descriptor(&self) -> NativePluginDescriptor {
        NativePluginDescriptor {
            name: "minimal-consumer".into(),
            version: "1.0.0".into(),
            dependencies: self.dependencies.clone(),
            config_schema: NativeConfigSchema::Any,
        }
    }

    fn start<'a>(
        &'a mut self,
        context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        let log = Arc::clone(&self.log);
        Box::pin(async move {
            let provider = if self.dependencies.iter().any(Dependency::is_required) {
                let provider = context
                    .get::<ProviderService>(&provider_service())
                    .expect("provider lookup is valid")
                    .expect("required provider is present before consumer start");
                provider
                    .with(|service| service.0)
                    .expect("provider stays current while consumer starts")
            } else {
                "optional"
            };
            log.lock()
                .expect("consumer log is available")
                .starts
                .push(format!("consumer:{provider}"));

            let event_log = Arc::clone(&log);
            context
                .events()
                .on(
                    &context.scope(),
                    notice_event(),
                    EventOptions {
                        global: true,
                        ..EventOptions::default()
                    },
                    move |notice| {
                        event_log
                            .lock()
                            .expect("consumer event log is available")
                            .events
                            .push(notice.0.into());
                        Ok(Value::Null)
                    },
                )
                .expect("typed listener belongs to the consumer transaction");
            if let Some(on_start) = self.on_start.take() {
                let _ = on_start.send(provider);
            }
            Ok(())
        })
    }

    fn update<'a>(
        &'a mut self,
        _context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn stop<'a>(&'a mut self, _context: ContextHandle) -> NativePluginFuture<'a> {
        let log = Arc::clone(&self.log);
        Box::pin(async move {
            log.lock()
                .expect("consumer log is available")
                .stops
                .push("consumer".into());
            Ok(())
        })
    }
}

struct Probe {
    log: SharedLog,
    cleanups: Arc<AtomicUsize>,
}

impl NativePlugin for Probe {
    fn descriptor(&self) -> NativePluginDescriptor {
        NativePluginDescriptor {
            name: "probe".into(),
            version: "1.0.0".into(),
            dependencies: Vec::new(),
            config_schema: probe_schema(),
        }
    }

    fn start<'a>(
        &'a mut self,
        context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        let log = Arc::clone(&self.log);
        let cleanups = Arc::clone(&self.cleanups);
        Box::pin(async move {
            context
                .scope()
                .add_effect(
                    "probe-resource",
                    Box::new(move || {
                        Box::pin(async move {
                            cleanups.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        })
                    }),
                )
                .expect("probe effect belongs to its transaction");
            log.lock()
                .expect("probe log is available")
                .starts
                .push("probe".into());
            Ok(())
        })
    }

    fn update<'a>(
        &'a mut self,
        _context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        let log = Arc::clone(&self.log);
        Box::pin(async move {
            log.lock()
                .expect("probe log is available")
                .updates
                .push("probe".into());
            Ok(())
        })
    }

    fn stop<'a>(&'a mut self, _context: ContextHandle) -> NativePluginFuture<'a> {
        let log = Arc::clone(&self.log);
        Box::pin(async move {
            log.lock()
                .expect("probe log is available")
                .stops
                .push("probe".into());
            Ok(())
        })
    }
}

struct TimedPlugin {
    completed: Arc<AtomicUsize>,
}

impl NativePlugin for TimedPlugin {
    fn descriptor(&self) -> NativePluginDescriptor {
        NativePluginDescriptor {
            name: "timed".into(),
            version: "1.0.0".into(),
            dependencies: Vec::new(),
            config_schema: NativeConfigSchema::Any,
        }
    }

    fn start<'a>(
        &'a mut self,
        _context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        let completed = Arc::clone(&self.completed);
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            completed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }

    fn update<'a>(
        &'a mut self,
        _context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn stop<'a>(&'a mut self, _context: ContextHandle) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

struct ManagedService(&'static str);

struct FailingUpdatePlugin {
    started: tokio::sync::mpsc::UnboundedSender<()>,
    failed_updates: tokio::sync::mpsc::UnboundedSender<()>,
    resource_disposals: Arc<AtomicUsize>,
}

impl NativePlugin for FailingUpdatePlugin {
    fn descriptor(&self) -> NativePluginDescriptor {
        NativePluginDescriptor {
            name: "failing-update".into(),
            version: "1.0.0".into(),
            dependencies: vec![Dependency::Required(managed_dependency_service())],
            config_schema: NativeConfigSchema::Any,
        }
    }

    fn start<'a>(
        &'a mut self,
        context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        let started = self.started.clone();
        let resource_disposals = Arc::clone(&self.resource_disposals);
        Box::pin(async move {
            context.provide(managed_service(), ManagedService("old"))?;
            context.scope().add_effect(
                "managed-resource",
                Box::new(move || {
                    Box::pin(async move {
                        resource_disposals.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                }),
            )?;
            let _ = started.send(());
            Ok(())
        })
    }

    fn update<'a>(
        &'a mut self,
        context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        let failed_updates = self.failed_updates.clone();
        Box::pin(async move {
            context.provide(managed_service(), ManagedService("candidate"))?;
            let _ = failed_updates.send(());
            Err(NativePluginError::Runtime {
                package: "failing-update".into(),
                message: "candidate update fails".into(),
            })
        })
    }

    fn stop<'a>(&'a mut self, _context: ContextHandle) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

struct StopFailingUpdatePlugin {
    label: &'static str,
    resource_disposals: Arc<AtomicUsize>,
}

impl NativePlugin for StopFailingUpdatePlugin {
    fn descriptor(&self) -> NativePluginDescriptor {
        NativePluginDescriptor {
            name: "stop-failing-update".into(),
            version: "1.0.0".into(),
            dependencies: Vec::new(),
            config_schema: NativeConfigSchema::Any,
        }
    }

    fn start<'a>(
        &'a mut self,
        context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        let label = self.label;
        let resource_disposals = Arc::clone(&self.resource_disposals);
        Box::pin(async move {
            context.provide(managed_service(), ManagedService(label))?;
            context.scope().add_effect(
                "stop-failure-resource",
                Box::new(move || {
                    Box::pin(async move {
                        resource_disposals.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                }),
            )?;
            Ok(())
        })
    }

    fn update<'a>(
        &'a mut self,
        _context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn stop<'a>(&'a mut self, _context: ContextHandle) -> NativePluginFuture<'a> {
        let label = self.label;
        Box::pin(async move {
            if label == "old" {
                Err(NativePluginError::Runtime {
                    package: "stop-failing-update".into(),
                    message: "old stop fails".into(),
                })
            } else {
                Ok(())
            }
        })
    }
}

struct FailingPlugin;

impl NativePlugin for FailingPlugin {
    fn descriptor(&self) -> NativePluginDescriptor {
        NativePluginDescriptor {
            name: "failing".into(),
            version: "1.0.0".into(),
            dependencies: Vec::new(),
            config_schema: NativeConfigSchema::Any,
        }
    }

    fn start<'a>(
        &'a mut self,
        context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        Box::pin(async move {
            let key = ServiceKey::new("failing.service", "native/v1");
            context
                .provide(key.clone(), 1_u8)
                .expect("failing transaction registers its provisional service");
            match context.get::<String>(&key) {
                Err(error) => Err(NativePluginError::Core(error)),
                Ok(_) => panic!("the provisional service intentionally has the wrong type"),
            }
        })
    }

    fn update<'a>(
        &'a mut self,
        _context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn stop<'a>(&'a mut self, _context: ContextHandle) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

struct Resolver;

impl PackageResolver for Resolver {
    fn resolve<'a>(
        &'a self,
        specifier: &'a str,
        _runtime: RuntimeKind,
    ) -> LoaderFuture<'a, ResolvedPackage> {
        let specifier = specifier.to_owned();
        Box::pin(async move {
            Ok(ResolvedPackage {
                location: specifier.clone(),
                specifier,
            })
        })
    }
}

fn entry(id: &str, package: &str, config: Value) -> Entry {
    Entry {
        package: package.into(),
        options: EntryOptions {
            id: EntryId::new(id).expect("test entry id is valid"),
            name: Some(id.into()),
            runtime: RuntimeKind::Native,
            config,
            inject: Vec::new(),
            isolate: Vec::new(),
            intercept: json!({}),
            disabled: false,
            group: None,
        },
    }
}

fn tree(provider: &str) -> EntryTree {
    EntryTree {
        // Consumer intentionally comes first: descriptor dependencies, not row order,
        // determine activation.
        entries: vec![
            entry("consumer", "native/consumer", Value::Null),
            entry("provider", provider, json!({"label": provider})),
        ],
        groups: Vec::new(),
    }
}

fn consumer_tree() -> EntryTree {
    EntryTree {
        entries: vec![entry("consumer", "native/consumer", Value::Null)],
        groups: Vec::new(),
    }
}

#[tokio::test]
async fn native_instance_waits_for_a_required_provider_before_starting() {
    let log = Arc::new(Mutex::new(Log::default()));
    let root = ContextHandle::root();
    let mut consumer = NativePluginInstance::instantiate(
        Box::new(Consumer {
            log: Arc::clone(&log),
            dependencies: vec![Dependency::Required(provider_service())],
            on_start: None,
        }),
        root.clone(),
        Value::Null,
    )
    .expect("required consumer instantiates");
    let mut consumer_start = Box::pin(consumer.start());
    poll_fn(|context| match consumer_start.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(result) => {
            panic!("consumer started before its provider was available: {result:?}")
        }
    })
    .await;
    assert!(
        log.lock()
            .expect("shared log is available")
            .starts
            .is_empty(),
        "a required consumer remains pending before its provider starts"
    );

    let mut provider = NativePluginInstance::instantiate(
        Box::new(Provider {
            label: "direct",
            log: Arc::clone(&log),
        }),
        root.clone(),
        json!({"label": "direct"}),
    )
    .expect("provider instantiates");
    provider.start().await.expect("provider starts");
    consumer_start
        .await
        .expect("provider availability wakes the pending consumer");

    assert_eq!(
        log.lock().expect("shared log is available").starts,
        ["provider:direct", "consumer:direct"]
    );
    consumer.dispose().await.expect("consumer disposes");
    provider.dispose().await.expect("provider disposes");
    root.scope()
        .dispose()
        .await
        .expect("direct instance root disposes");
}

#[tokio::test]
async fn native_pending_consumer_cancellation_settles() {
    let log = Arc::new(Mutex::new(Log::default()));
    let root = ContextHandle::root();
    let mut consumer = NativePluginInstance::instantiate(
        Box::new(Consumer {
            log: Arc::clone(&log),
            dependencies: vec![Dependency::Required(provider_service())],
            on_start: None,
        }),
        root.clone(),
        Value::Null,
    )
    .expect("pending consumer instantiates");
    let mut consumer_start = Box::pin(consumer.start());
    poll_fn(|context| match consumer_start.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(result) => panic!("consumer started without its required provider: {result:?}"),
    })
    .await;

    root.scope()
        .dispose()
        .await
        .expect("disposing the owner cancels its pending consumer");
    assert!(matches!(
        consumer_start.await,
        Err(NativePluginError::Core(CoreError::Cancelled))
    ));
    assert!(
        consumer.snapshot().fiber.resources.is_empty(),
        "cancelling a pending consumer disposes its scope-owned subscription"
    );
    assert!(
        log.lock()
            .expect("shared log is available")
            .starts
            .is_empty(),
        "cancellation never enters the pending plugin's start hook"
    );
}

#[tokio::test]
async fn native_loader_starts_an_optional_consumer_without_a_provider() {
    let log = Arc::new(Mutex::new(Log::default()));
    let (on_start, mut started) = tokio::sync::mpsc::unbounded_channel();
    let mut runtime = NativePluginRuntime::new();
    runtime
        .register("native/optional-consumer", {
            let log = Arc::clone(&log);
            let on_start = on_start.clone();
            move || Consumer {
                log: Arc::clone(&log),
                dependencies: vec![Dependency::Optional(provider_service())],
                on_start: Some(on_start.clone()),
            }
        })
        .expect("optional consumer factory registers");
    let runtime = Arc::new(runtime);
    let runtime_for_loader: Arc<dyn LoaderRuntime> = runtime.clone();
    let mut loader = Loader::try_new(Arc::new(Resolver), [runtime_for_loader])
        .expect("native runtime registers with the loader");

    loader
        .load(EntryTree {
            entries: vec![entry("optional", "native/optional-consumer", Value::Null)],
            groups: Vec::new(),
        })
        .await
        .expect("a missing optional provider does not gate loader activation");
    assert_eq!(
        started.recv().await,
        Some("optional"),
        "the optional consumer starts with an absent provider"
    );

    let root = loader.context();
    loader.unload().await.expect("optional consumer unloads");
    root.scope()
        .dispose()
        .await
        .expect("optional consumer root disposes");
    assert_eq!(
        log.lock().expect("shared log is available").starts,
        ["consumer:optional"]
    );
}

#[tokio::test]
async fn native_loader_runs_provider_consumer_removal_replacement_and_root_disposal() {
    let log = Arc::new(Mutex::new(Log::default()));
    let (on_start, mut started) = tokio::sync::mpsc::unbounded_channel();
    let mut runtime = NativePluginRuntime::new();
    runtime
        .register("native/provider-v1", {
            let log = Arc::clone(&log);
            move || Provider {
                label: "v1",
                log: Arc::clone(&log),
            }
        })
        .expect("provider v1 factory registers");
    runtime
        .register("native/provider-v2", {
            let log = Arc::clone(&log);
            move || Provider {
                label: "v2",
                log: Arc::clone(&log),
            }
        })
        .expect("provider v2 factory registers");
    runtime
        .register("native/consumer", {
            let log = Arc::clone(&log);
            let on_start = on_start.clone();
            move || Consumer {
                log: Arc::clone(&log),
                dependencies: vec![Dependency::Required(provider_service())],
                on_start: Some(on_start.clone()),
            }
        })
        .expect("consumer factory registers");
    let runtime = Arc::new(runtime);
    let runtime_for_loader: Arc<dyn LoaderRuntime> = runtime.clone();
    let mut loader = Loader::try_new(Arc::new(Resolver), [runtime_for_loader])
        .expect("native runtime registers with the loader");

    loader
        .load(tree("native/provider-v1"))
        .await
        .expect("provider and dependency-gated consumer activate");
    assert_eq!(
        started.recv().await,
        Some("v1"),
        "the consumer starts only after its first required provider is active"
    );
    loader
        .context()
        .events()
        .emit(&notice_event(), &Notice("v1"))
        .expect("typed event dispatches without JSON");

    loader
        .replace(consumer_tree())
        .await
        .expect("removing a required provider leaves its consumer managed and pending");
    assert_eq!(
        log.lock().expect("shared log is available").starts,
        ["provider:v1", "consumer:v1"],
        "removal unloads the active consumer instead of restarting it without a provider"
    );
    assert_eq!(
        log.lock().expect("shared log is available").stops.len(),
        2,
        "provider removal unloads the former provider and consumer"
    );
    assert!(matches!(
        started.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));

    loader
        .replace(tree("native/provider-v2"))
        .await
        .expect("provider replacement reloads the managed consumer");
    assert_eq!(
        started.recv().await,
        Some("v2"),
        "the replacement provider wakes and reloads the pending consumer"
    );
    loader
        .context()
        .events()
        .emit(&notice_event(), &Notice("v2"))
        .expect("replacement tree dispatches its typed event");

    let root = loader.context();
    loader
        .unload()
        .await
        .expect("native instances quiesce before root disposal");
    root.scope()
        .dispose()
        .await
        .expect("root disposal is successful and idempotent");
    root.scope()
        .dispose()
        .await
        .expect("a second root disposal joins the completed cleanup");

    let log = log.lock().expect("shared log is available");
    assert_eq!(
        log.starts,
        ["provider:v1", "consumer:v1", "provider:v2", "consumer:v2"],
        "the required service gates each consumer activation independently of entry order"
    );
    assert_eq!(log.events, ["v1", "v2"]);
    assert_eq!(log.stops.len(), 4, "each live plugin stops exactly once");
    assert!(
        root.scope().effects().is_empty(),
        "root owns no residual effects"
    );
    assert_eq!(
        runtime.snapshot().packages,
        vec![
            "native/consumer".to_owned(),
            "native/provider-v1".to_owned(),
            "native/provider-v2".to_owned(),
        ],
        "runtime diagnostics retain only registered factories, not live resources"
    );
}

#[tokio::test]
async fn native_loader_hooks_run_inside_a_tokio_runtime() {
    let completed = Arc::new(AtomicUsize::new(0));
    let mut runtime = NativePluginRuntime::new();
    runtime
        .register("native/timed", {
            let completed = Arc::clone(&completed);
            move || TimedPlugin {
                completed: Arc::clone(&completed),
            }
        })
        .expect("timed factory registers");
    let runtime_for_loader: Arc<dyn LoaderRuntime> = Arc::new(runtime);
    let mut loader = Loader::try_new(Arc::new(Resolver), [runtime_for_loader])
        .expect("native runtime registers with the loader");

    tokio::time::timeout(
        Duration::from_secs(1),
        loader.load(EntryTree {
            entries: vec![entry("timed", "native/timed", Value::Null)],
            groups: Vec::new(),
        }),
    )
    .await
    .expect("loader-managed hook settles on its Tokio executor")
    .expect("Tokio timer completes in the managed native hook");
    assert_eq!(completed.load(Ordering::SeqCst), 1);

    let root = loader.context();
    loader.unload().await.expect("timed native hook unloads");
    root.scope().dispose().await.expect("timed root disposes");
}

#[tokio::test]
async fn managed_native_rejected_update_retains_old_service_and_resource() {
    let (started, mut started_events) = tokio::sync::mpsc::unbounded_channel();
    let (failed_updates, mut failed_update_events) = tokio::sync::mpsc::unbounded_channel();
    let resource_disposals = Arc::new(AtomicUsize::new(0));
    let mut runtime = NativePluginRuntime::new();
    runtime
        .register("native/failing-update", {
            let started = started.clone();
            let failed_updates = failed_updates.clone();
            let resource_disposals = Arc::clone(&resource_disposals);
            move || FailingUpdatePlugin {
                started: started.clone(),
                failed_updates: failed_updates.clone(),
                resource_disposals: Arc::clone(&resource_disposals),
            }
        })
        .expect("failing-update factory registers");
    let runtime_for_loader: Arc<dyn LoaderRuntime> = Arc::new(runtime);
    let mut loader = Loader::try_new(Arc::new(Resolver), [runtime_for_loader])
        .expect("native runtime registers with the loader");

    loader
        .load(EntryTree {
            entries: vec![entry("managed", "native/failing-update", Value::Null)],
            groups: Vec::new(),
        })
        .await
        .expect("missing dependency leaves the managed plugin pending");
    let root = loader.context();
    root.provide(managed_dependency_service(), 1_u8)
        .expect("initial dependency registers");
    tokio::time::timeout(Duration::from_secs(1), started_events.recv())
        .await
        .expect("initial managed plugin starts")
        .expect("initial start event arrives");

    root.provide(managed_dependency_service(), 2_u8)
        .expect("dependency generation changes");
    tokio::time::timeout(Duration::from_secs(1), failed_update_events.recv())
        .await
        .expect("candidate update is attempted")
        .expect("candidate update reports its failure");
    let service = root
        .get::<ManagedService>(&managed_service())
        .expect("managed service lookup succeeds")
        .expect("failed candidate leaves the old managed service live");
    assert_eq!(
        service
            .with(|service| service.0)
            .expect("old managed service remains callable"),
        "old"
    );
    assert_eq!(
        resource_disposals.load(Ordering::SeqCst),
        0,
        "failed candidate does not dispose the old resource"
    );

    loader
        .unload()
        .await
        .expect("old managed instance unloads after the rejected update");
    assert_eq!(resource_disposals.load(Ordering::SeqCst), 1);
    root.scope()
        .dispose()
        .await
        .expect("managed test root disposes");
}

#[tokio::test]
async fn native_update_waits_for_missing_required_dependency_and_cancels() {
    let log = Arc::new(Mutex::new(Log::default()));
    let root = ContextHandle::root();
    let mut provider = NativePluginInstance::instantiate(
        Box::new(Provider {
            label: "gate",
            log: Arc::clone(&log),
        }),
        root.clone(),
        json!({"label": "gate"}),
    )
    .expect("provider instantiates");
    provider.start().await.expect("provider starts");

    let factory = Arc::new({
        let log = Arc::clone(&log);
        move || {
            Box::new(Consumer {
                log: Arc::clone(&log),
                dependencies: vec![Dependency::Required(provider_service())],
                on_start: None,
            }) as Box<dyn NativePlugin>
        }
    });
    let mut consumer =
        NativePluginInstance::instantiate_with_factory(factory, root.clone(), Value::Null)
            .expect("consumer instantiates");
    consumer.start().await.expect("consumer starts");
    provider.dispose().await.expect("provider stops");

    let mut update = Box::pin(consumer.update(Value::Null));
    poll_fn(|context| match update.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(result) => {
            panic!("candidate started without its required provider: {result:?}")
        }
    })
    .await;
    root.scope()
        .dispose()
        .await
        .expect("disposing the host cancels the pending candidate");
    assert!(matches!(
        update.await,
        Err(NativePluginError::Core(CoreError::Cancelled))
    ));
    assert_eq!(
        log.lock().expect("shared log is available").starts,
        ["provider:gate", "consumer:gate"],
        "pending candidate never enters update or start"
    );
}

#[tokio::test]
async fn native_update_keeps_candidate_live_when_old_stop_fails() {
    let resource_disposals = Arc::new(AtomicUsize::new(0));
    let sequence = Arc::new(AtomicUsize::new(0));
    let factory = Arc::new({
        let resource_disposals = Arc::clone(&resource_disposals);
        let sequence = Arc::clone(&sequence);
        move || {
            let label = if sequence.fetch_add(1, Ordering::SeqCst) == 0 {
                "old"
            } else {
                "candidate"
            };
            Box::new(StopFailingUpdatePlugin {
                label,
                resource_disposals: Arc::clone(&resource_disposals),
            }) as Box<dyn NativePlugin>
        }
    });
    let root = ContextHandle::root();
    let mut instance =
        NativePluginInstance::instantiate_with_factory(factory, root.clone(), Value::Null)
            .expect("stop-failing plugin instantiates");
    instance.start().await.expect("old plugin starts");
    let old_service = root
        .get::<ManagedService>(&managed_service())
        .expect("old service lookup succeeds")
        .expect("old service is present");

    assert!(matches!(
        instance.update(Value::Null).await,
        Err(NativePluginError::Runtime { message, .. }) if message == "old stop fails"
    ));
    assert!(
        !old_service.is_current(),
        "committing the candidate invalidates only the retired provider"
    );
    let service = root
        .get::<ManagedService>(&managed_service())
        .expect("candidate service lookup succeeds")
        .expect("candidate service remains current after old cleanup fails");
    assert_eq!(
        service
            .with(|service| service.0)
            .expect("candidate service remains callable"),
        "candidate"
    );
    assert_eq!(
        resource_disposals.load(Ordering::SeqCst),
        1,
        "only the retired resource is cleaned up"
    );
    assert!(
        !instance.snapshot().fiber.resources.is_empty(),
        "the ready candidate still owns its resources"
    );

    instance.dispose().await.expect("candidate disposes");
    assert_eq!(resource_disposals.load(Ordering::SeqCst), 2);
    root.scope().dispose().await.expect("root disposes");
}

#[tokio::test]
async fn native_instances_validate_rollback_update_and_dispose_without_resources() {
    let descriptor = NativePluginDescriptor {
        name: "schema-probe".into(),
        version: "1.0.0".into(),
        dependencies: Vec::new(),
        config_schema: probe_schema(),
    };
    assert!(matches!(
        descriptor.validate_config(&json!({"enabled": "not-a-bool"})),
        Err(NativePluginError::Config(NativeConfigError::Type { path, .. })) if path == "$.enabled"
    ));
    assert!(matches!(
        descriptor.validate_config(&json!({"enabled": true, "surprise": 1})),
        Err(NativePluginError::Config(NativeConfigError::AdditionalProperty { path, property }))
            if path == "$" && property == "surprise"
    ));

    let failed_root = ContextHandle::root();
    let mut failed = NativePluginInstance::instantiate(
        Box::new(FailingPlugin),
        failed_root.clone(),
        Value::Null,
    )
    .expect("failing plugin descriptor instantiates");
    assert!(matches!(
        failed.start().await,
        Err(NativePluginError::Core(
            CoreError::ServiceTypeMismatch { .. }
        ))
    ));
    assert!(
        failed.snapshot().fiber.resources.is_empty(),
        "a failed start rolls back the service it registered"
    );
    assert!(
        failed_root
            .get::<u8>(&ServiceKey::new("failing.service", "native/v1"))
            .expect("root lookup succeeds")
            .is_none(),
        "the failed child transaction never leaks its provisional service"
    );

    let log = Arc::new(Mutex::new(Log::default()));
    let cleanups = Arc::new(AtomicUsize::new(0));
    let root = ContextHandle::root();
    let factory = Arc::new({
        let log = Arc::clone(&log);
        let cleanups = Arc::clone(&cleanups);
        move || {
            Box::new(Probe {
                log: Arc::clone(&log),
                cleanups: Arc::clone(&cleanups),
            }) as Box<dyn NativePlugin>
        }
    });
    let mut instance = NativePluginInstance::instantiate_with_factory(
        factory,
        root.clone(),
        json!({"enabled": true}),
    )
    .expect("valid probe config instantiates");
    instance.start().await.expect("probe starts");
    assert!(matches!(
        instance.update(json!({"enabled": "no"})).await,
        Err(NativePluginError::Config(NativeConfigError::Type { path, .. })) if path == "$.enabled"
    ));
    assert_eq!(
        instance.snapshot().config,
        json!({"enabled": true}),
        "a rejected update keeps the last validated configuration"
    );
    instance
        .update(json!({"enabled": false}))
        .await
        .expect("validated update restarts the transaction");
    instance.dispose().await.expect("first disposal succeeds");
    instance
        .dispose()
        .await
        .expect("second disposal is idempotent");
    root.scope()
        .dispose()
        .await
        .expect("root disposal has no remaining child work");

    let log = log.lock().expect("probe log is available");
    assert_eq!(log.starts, ["probe", "probe"]);
    assert_eq!(log.updates, ["probe"]);
    assert_eq!(log.stops, ["probe", "probe"]);
    assert_eq!(cleanups.load(Ordering::SeqCst), 2);
    assert!(
        instance.snapshot().fiber.resources.is_empty(),
        "disposed instance snapshots expose zero owned resources"
    );
    assert!(root.scope().effects().is_empty());
}
