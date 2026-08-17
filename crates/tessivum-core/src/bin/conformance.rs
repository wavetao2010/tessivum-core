use std::{
    collections::BTreeSet,
    env, fs,
    future::Future,
    process::ExitCode,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use serde_json::{json, Map, Value};
use tessivum_core::{
    parse_entry_tree, ContextHandle, CoreError, Entry, EntryGroup, EntryId, EntryOptions,
    EntryTree, EventBus, EventOptions, Fiber, FiberState, Fixture, Loader, LoaderError,
    LoaderFuture, LoaderRuntime, PackageResolver, Patch, ResolvedPackage, RuntimeHandle,
    RuntimeKind, Scope, ServiceKey, TraceEvent, TraceEventKind,
};

#[derive(Clone, Default)]
struct Trace {
    events: Arc<Mutex<Vec<TraceEvent>>>,
}

impl Trace {
    fn add(&self, event: TraceEvent) {
        self.events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(event);
    }

    fn finish(&self) -> Vec<TraceEvent> {
        self.events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }
}

fn record(
    trace: &Trace,
    kind: TraceEventKind,
    subject: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
    label: Option<&str>,
    phase: Option<&str>,
) {
    trace.add(TraceEvent {
        kind,
        subject: subject.map(str::to_owned),
        from: from.map(str::to_owned),
        to: to.map(str::to_owned),
        label: label.map(str::to_owned),
        phase: phase.map(str::to_owned),
        value: None,
        error: None,
    });
}

fn state(trace: &Trace, subject: &str, from: &str, to: &str) {
    record(
        trace,
        TraceEventKind::FiberStateChanged,
        Some(subject),
        Some(from),
        Some(to),
        None,
        None,
    );
}

fn setup_error(message: impl Into<String>) -> CoreError {
    CoreError::SetupFailed(message.into())
}

fn fixture_input(fixture: &Fixture) -> Option<&Map<String, Value>> {
    fixture.input.as_ref().and_then(Value::as_object)
}

fn input_text(fixture: &Fixture, key: &str, fallback: &str) -> String {
    fixture_input(fixture)
        .and_then(|input| input.get(key))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback)
        .to_owned()
}

fn lifecycle_effect_label(fixture: &Fixture) -> String {
    fixture_input(fixture)
        .and_then(|input| input.get("effects"))
        .and_then(Value::as_array)
        .and_then(|effects| effects.first())
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or("effect-dispose")
        .to_owned()
}

fn event_labels(fixture: &Fixture) -> Vec<String> {
    let Some(labels) = fixture_input(fixture)
        .and_then(|input| input.get("listeners"))
        .and_then(Value::as_array)
    else {
        return vec!["first".into(), "second".into()];
    };
    let labels = labels
        .iter()
        .map(|listener| listener.get("label").and_then(Value::as_str))
        .collect::<Option<Vec<_>>>();
    labels
        .map(|labels| labels.into_iter().map(str::to_owned).collect())
        .unwrap_or_else(|| vec!["first".into(), "second".into()])
}

async fn lifecycle(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
    let trace = Trace::default();
    let root = Scope::root();
    let fiber = Fiber::new(&root, "effect-dispose").map_err(|error| error.to_string())?;
    let label = lifecycle_effect_label(fixture);

    record(
        &trace,
        TraceEventKind::FiberCreated,
        Some(fiber.name()),
        None,
        None,
        None,
        None,
    );
    let state_probe = fiber.clone();
    let setup_trace = trace.clone();
    fiber
        .start(move |scope| {
            let trace = setup_trace.clone();
            let state_probe = state_probe.clone();
            let effect_label = label.clone();
            async move {
                if state_probe.state() != FiberState::Loading {
                    return Err(setup_error("lifecycle fiber did not enter loading"));
                }
                state(&trace, state_probe.name(), "PENDING", "LOADING");

                let disposal_trace = trace.clone();
                let disposal_fiber = state_probe.clone();
                let cleanup_label = effect_label.clone();
                scope.add_effect(
                    effect_label.clone(),
                    Box::new(move || {
                        Box::pin(async move {
                            if disposal_fiber.state() != FiberState::Unloading {
                                return Err(setup_error("lifecycle fiber did not enter unloading"));
                            }
                            state(
                                &disposal_trace,
                                disposal_fiber.name(),
                                "ACTIVE",
                                "UNLOADING",
                            );
                            record(
                                &disposal_trace,
                                TraceEventKind::EffectDisposed,
                                Some("effect-dispose.effect"),
                                None,
                                None,
                                Some(&cleanup_label),
                                None,
                            );
                            Ok(())
                        })
                    }),
                )?;
                record(
                    &trace,
                    TraceEventKind::EffectCreated,
                    Some("effect-dispose.effect"),
                    None,
                    None,
                    Some(&effect_label),
                    None,
                );
                Ok(())
            }
        })
        .await
        .map_err(|error| error.to_string())?;
    if fiber.state() != FiberState::Active {
        return Err("lifecycle fiber did not become active".into());
    }
    state(&trace, fiber.name(), "LOADING", "ACTIVE");

    fiber.dispose().await.map_err(|error| error.to_string())?;
    if fiber.state() != FiberState::Disposed {
        return Err("lifecycle fiber did not become disposed".into());
    }
    state(&trace, fiber.name(), "UNLOADING", "DISPOSED");
    Ok(trace.finish())
}

#[derive(Debug)]
struct CatalogService(String);

async fn provider(
    parent: &Scope,
    context: ContextHandle,
    service: ServiceKey,
    label: String,
    trace: Trace,
) -> Result<Fiber, String> {
    let name = format!("isolate-realm.{label}");
    let fiber = Fiber::new(parent, name).map_err(|error| error.to_string())?;
    record(
        &trace,
        TraceEventKind::FiberCreated,
        Some(fiber.name()),
        None,
        None,
        None,
        None,
    );

    let state_probe = fiber.clone();
    let setup_context = context.clone();
    let setup_service = service.clone();
    let setup_label = label.clone();
    let setup_trace = trace.clone();
    fiber
        .start(move |scope| {
            let trace = setup_trace.clone();
            let context = setup_context.with_scope(scope.clone());
            let service = setup_service.clone();
            let label = setup_label.clone();
            let state_probe = state_probe.clone();
            async move {
                if state_probe.state() != FiberState::Loading {
                    return Err(setup_error("service fiber did not enter loading"));
                }
                state(&trace, state_probe.name(), "PENDING", "LOADING");

                let removed_context = context.clone();
                let removed_service = service.clone();
                let removed_label = label.clone();
                let removed_trace = trace.clone();
                scope.add_effect(
                    "trace-service-removed",
                    Box::new(move || {
                        Box::pin(async move {
                            if removed_context
                                .get::<CatalogService>(&removed_service)?
                                .is_some()
                            {
                                return Err(setup_error("service survived provider disposal"));
                            }
                            record(
                                &removed_trace,
                                TraceEventKind::ServiceRemoved,
                                Some(&removed_service.name),
                                None,
                                None,
                                Some(&removed_label),
                                None,
                            );
                            Ok(())
                        })
                    }),
                )?;

                let handle = context.provide(service.clone(), CatalogService(label.clone()))?;
                if !handle.is_current() {
                    return Err(setup_error("new service provider was not current"));
                }
                record(
                    &trace,
                    TraceEventKind::ServiceProvided,
                    Some(&service.name),
                    None,
                    None,
                    Some(&label),
                    None,
                );

                let unloading_trace = trace.clone();
                let unloading_fiber = state_probe.clone();
                scope.add_effect(
                    "trace-service-unloading",
                    Box::new(move || {
                        Box::pin(async move {
                            if unloading_fiber.state() != FiberState::Unloading {
                                return Err(setup_error("service fiber did not enter unloading"));
                            }
                            state(
                                &unloading_trace,
                                unloading_fiber.name(),
                                "ACTIVE",
                                "UNLOADING",
                            );
                            Ok(())
                        })
                    }),
                )?;
                Ok(())
            }
        })
        .await
        .map_err(|error| error.to_string())?;
    if fiber.state() != FiberState::Active {
        return Err("service fiber did not become active".into());
    }
    state(&trace, fiber.name(), "LOADING", "ACTIVE");
    Ok(fiber)
}

async fn services(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
    let trace = Trace::default();
    let root = ContextHandle::root();
    let root_scope = root.scope();
    let service = ServiceKey::new(input_text(fixture, "service", "storage"), "conformance/v1");
    let first = input_text(fixture, "first", "root");
    let second = input_text(fixture, "second", "isolated");

    let root_provider = provider(
        &root_scope,
        root.clone(),
        service.clone(),
        first.clone(),
        trace.clone(),
    )
    .await?;
    let isolated = root.isolate(service.clone(), None);
    let isolated_provider = provider(
        &root_scope,
        isolated.clone(),
        service.clone(),
        second.clone(),
        trace.clone(),
    )
    .await?;

    let root_label = root
        .get::<CatalogService>(&service)
        .map_err(|error| error.to_string())?
        .ok_or("root service is missing")?
        .with(|value| value.0.clone())
        .map_err(|error| error.to_string())?;
    let isolated_label = isolated
        .get::<CatalogService>(&service)
        .map_err(|error| error.to_string())?
        .ok_or("isolated service is missing")?
        .with(|value| value.0.clone())
        .map_err(|error| error.to_string())?;
    if root_label != first || isolated_label != second {
        return Err("service realm visibility failed".into());
    }

    isolated_provider
        .dispose()
        .await
        .map_err(|error| error.to_string())?;
    if isolated_provider.state() != FiberState::Disposed {
        return Err("isolated provider did not become disposed".into());
    }
    state(&trace, isolated_provider.name(), "UNLOADING", "DISPOSED");

    root_provider
        .dispose()
        .await
        .map_err(|error| error.to_string())?;
    if root_provider.state() != FiberState::Disposed {
        return Err("root provider did not become disposed".into());
    }
    state(&trace, root_provider.name(), "UNLOADING", "DISPOSED");
    Ok(trace.finish())
}

async fn events(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
    let trace = Trace::default();
    let scope = Scope::root();
    let bus = EventBus::new();
    let event = input_text(fixture, "event", "emit");
    let mode = input_text(fixture, "mode", "emit");
    if mode != "emit" {
        return Err("emit scenario requires emit mode".into());
    }

    let labels = event_labels(fixture);
    let mut handles = Vec::with_capacity(labels.len());
    for label in &labels {
        let dispatched_trace = trace.clone();
        let dispatched_event = event.clone();
        let dispatched_label = label.clone();
        let handle = bus
            .on_dynamic(&scope, event.clone(), EventOptions::default(), move |_| {
                record(
                    &dispatched_trace,
                    TraceEventKind::EventDispatched,
                    Some(&dispatched_event),
                    None,
                    None,
                    Some(&dispatched_label),
                    Some("emit"),
                );
                Ok(Value::Null)
            })
            .map_err(|error| error.to_string())?;
        record(
            &trace,
            TraceEventKind::ListenerAdded,
            Some(&event),
            None,
            None,
            Some(label),
            None,
        );
        handles.push(handle);
    }
    bus.emit_dynamic(&event, &Value::Null)
        .map_err(|error| error.to_string())?;
    for (label, handle) in labels.iter().zip(handles.iter()).rev() {
        if !handle.remove() {
            return Err(format!("listener {label:?} was already removed"));
        }
        record(
            &trace,
            TraceEventKind::ListenerRemoved,
            Some(&event),
            None,
            None,
            Some(label),
            None,
        );
    }
    Ok(trace.finish())
}

#[derive(Default)]
struct LoaderCatalogState {
    active: Mutex<BTreeSet<String>>,
    fail_activation: Mutex<BTreeSet<String>>,
    fail_disposal: Mutex<BTreeSet<String>>,
}

struct CatalogResolver;

impl PackageResolver for CatalogResolver {
    fn resolve<'a>(
        &'a self,
        specifier: &'a str,
        _runtime: RuntimeKind,
    ) -> LoaderFuture<'a, ResolvedPackage> {
        Box::pin(async move {
            Ok(ResolvedPackage {
                specifier: specifier.to_owned(),
                location: format!("catalog:{specifier}"),
            })
        })
    }
}

#[derive(Clone)]
struct CatalogRuntime {
    state: Arc<LoaderCatalogState>,
}

impl LoaderRuntime for CatalogRuntime {
    fn kind(&self) -> RuntimeKind {
        RuntimeKind::Native
    }

    fn instantiate<'a>(
        &'a self,
        _package: ResolvedPackage,
        entry: Entry,
        _context: ContextHandle,
    ) -> LoaderFuture<'a, Box<dyn RuntimeHandle>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let handle: Box<dyn RuntimeHandle> = Box::new(CatalogHandle {
                id: entry.options.id,
                state,
            });
            Ok(handle)
        })
    }
}

struct CatalogHandle {
    id: EntryId,
    state: Arc<LoaderCatalogState>,
}

impl RuntimeHandle for CatalogHandle {
    fn activate<'a>(&'a mut self) -> LoaderFuture<'a, ()> {
        let id = self.id.0.clone();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            if state
                .fail_activation
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .contains(&id)
            {
                return Err(LoaderError::Validation(format!(
                    "activation failed for {id}"
                )));
            }
            state
                .active
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .insert(id);
            Ok(())
        })
    }

    fn activation<'a>(&'a mut self) -> LoaderFuture<'a, tessivum_core::ActivationState> {
        let id = self.id.0.clone();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            if state
                .fail_activation
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .contains(&id)
            {
                return Err(LoaderError::Validation(format!(
                    "activation failed for {id}"
                )));
            }
            if id == "consumer"
                && !state
                    .active
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .contains("provider")
            {
                return Ok(tessivum_core::ActivationState::Pending);
            }
            state
                .active
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .insert(id);
            Ok(tessivum_core::ActivationState::Active)
        })
    }

    fn dispose<'a>(&'a mut self) -> LoaderFuture<'a, ()> {
        let id = self.id.0.clone();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            state
                .active
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .remove(&id);
            if state
                .fail_disposal
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .contains(&id)
            {
                return Err(LoaderError::Validation(format!("disposal failed for {id}")));
            }
            Ok(())
        })
    }
}

fn catalog_entry(id: &str) -> Result<Entry, String> {
    Ok(Entry {
        package: format!("catalog/{id}"),
        options: EntryOptions {
            id: EntryId::new(id).map_err(|error| error.to_string())?,
            name: Some(format!("catalog.{id}")),
            runtime: RuntimeKind::Native,
            config: json!({"catalog": id}),
            inject: Vec::new(),
            isolate: Vec::new(),
            intercept: json!({}),
            disabled: false,
            group: None,
        },
    })
}

fn catalog_tree(ids: &[&str]) -> Result<EntryTree, String> {
    ids.iter()
        .map(|id| catalog_entry(id))
        .collect::<Result<Vec<_>, _>>()
        .map(|entries| EntryTree {
            entries,
            groups: Vec::new(),
        })
}

fn catalog_loader(state: Arc<LoaderCatalogState>) -> Result<Loader, String> {
    let resolver: Arc<dyn PackageResolver> = Arc::new(CatalogResolver);
    let runtime: Arc<dyn LoaderRuntime> = Arc::new(CatalogRuntime { state });
    Loader::try_new(resolver, [runtime]).map_err(|error| error.to_string())
}

fn append_loader_trace(loader: &Loader, trace: &Trace) {
    for event in loader.trace() {
        trace.add(event.clone());
    }
}

fn expect_loader_failure<T>(
    result: Result<T, LoaderError>,
    case: &str,
) -> Result<LoaderError, String> {
    match result {
        Err(error) => Ok(error),
        Ok(_) => Err(format!("loader catalog case {case} unexpectedly succeeded")),
    }
}

fn loader_cases(fixture: &Fixture) -> Result<Vec<&str>, String> {
    fixture_input(fixture)
        .and_then(|input| input.get("cases"))
        .and_then(Value::as_array)
        .ok_or("loader catalog requires a cases array")?
        .iter()
        .map(|case| {
            case.get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| "loader catalog case requires a non-empty id".to_owned())
        })
        .collect()
}

async fn loader_case(id: &str, trace: &Trace) -> Result<(), String> {
    let state = Arc::new(LoaderCatalogState::default());
    let mut loader = catalog_loader(Arc::clone(&state))?;
    match id {
        "unordered-dependencies" => {
            loader
                .load(catalog_tree(&["consumer", "provider"])?)
                .await
                .map_err(|error| error.to_string())?;
            if !state
                .active
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .contains("consumer")
            {
                return Err("consumer did not activate after provider".into());
            }
        }
        "duplicate-id" => {
            let duplicate = catalog_entry("duplicate")?;
            expect_loader_failure(
                loader
                    .load(EntryTree {
                        entries: vec![duplicate.clone(), duplicate],
                        groups: Vec::new(),
                    })
                    .await,
                id,
            )?;
        }
        "add-update-remove-move" => {
            loader
                .load(catalog_tree(&["first", "second"])?)
                .await
                .map_err(|error| error.to_string())?;
            loader
                .update(&[
                    Patch::Insert {
                        entry: catalog_entry("third")?,
                        before: Some(EntryId::new("second").map_err(|error| error.to_string())?),
                    },
                    Patch::Update {
                        id: EntryId::new("second").map_err(|error| error.to_string())?,
                        entry: Entry {
                            package: "catalog/replaced-second".into(),
                            options: catalog_entry("second")?.options,
                        },
                    },
                    Patch::Move {
                        id: EntryId::new("third").map_err(|error| error.to_string())?,
                        before: Some(EntryId::new("first").map_err(|error| error.to_string())?),
                    },
                    Patch::Remove {
                        id: EntryId::new("first").map_err(|error| error.to_string())?,
                    },
                ])
                .await
                .map_err(|error| error.to_string())?;
            let ids = loader
                .tree()
                .entries
                .iter()
                .map(|entry| entry.options.id.as_str())
                .collect::<Vec<_>>();
            if ids != ["third", "second"] {
                return Err("ordered loader patches produced the wrong tree".into());
            }
        }
        "config-only-update" => {
            loader
                .load(catalog_tree(&["config"])?)
                .await
                .map_err(|error| error.to_string())?;
            loader
                .update(&[Patch::UpdateConfig {
                    id: EntryId::new("config").map_err(|error| error.to_string())?,
                    config: json!({"revision": 2}),
                }])
                .await
                .map_err(|error| error.to_string())?;
            if loader.tree().entries[0].options.config != json!({"revision": 2}) {
                return Err("config-only update was not committed".into());
            }
        }
        "plugin-replacement" => {
            loader
                .load(catalog_tree(&["old"])?)
                .await
                .map_err(|error| error.to_string())?;
            loader
                .replace(catalog_tree(&["replacement"])?)
                .await
                .map_err(|error| error.to_string())?;
            if loader.tree().entries[0].options.id.as_str() != "replacement" {
                return Err("plugin replacement did not commit".into());
            }
        }
        "candidate-failure" => {
            loader
                .load(catalog_tree(&["old"])?)
                .await
                .map_err(|error| error.to_string())?;
            state
                .fail_activation
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .insert("broken".into());
            expect_loader_failure(loader.replace(catalog_tree(&["broken"])?).await, id)?;
            if loader.tree().entries[0].options.id.as_str() != "old" {
                return Err("failed candidate replaced the last known-good tree".into());
            }
        }
        "rollback-failure" => {
            loader
                .load(catalog_tree(&["old"])?)
                .await
                .map_err(|error| error.to_string())?;
            state
                .fail_activation
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .insert("broken".into());
            state
                .fail_disposal
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .insert("broken".into());
            let error =
                expect_loader_failure(loader.replace(catalog_tree(&["broken"])?).await, id)?;
            if error.rollback_errors().is_empty() {
                return Err("rollback failure was not retained".into());
            }
            if loader.tree().entries[0].options.id.as_str() != "old" {
                return Err("rollback failure changed the last known-good tree".into());
            }
        }
        "nested-group" => {
            loader
                .load(EntryTree {
                    entries: Vec::new(),
                    groups: vec![EntryGroup {
                        id: "root".into(),
                        entries: Vec::new(),
                        groups: vec![EntryGroup {
                            id: "root.child".into(),
                            entries: vec![catalog_entry("nested")?],
                            groups: Vec::new(),
                            disabled: false,
                        }],
                        disabled: false,
                    }],
                })
                .await
                .map_err(|error| error.to_string())?;
            if loader.tree().active_entries().len() != 1 {
                return Err("nested group did not retain its entry".into());
            }
        }
        "patch-insertion-later-targeting" => {
            loader
                .load(catalog_tree(&["target"])?)
                .await
                .map_err(|error| error.to_string())?;
            loader
                .update(&[
                    Patch::Insert {
                        entry: catalog_entry("inserted")?,
                        before: Some(EntryId::new("target").map_err(|error| error.to_string())?),
                    },
                    Patch::UpdateConfig {
                        id: EntryId::new("inserted").map_err(|error| error.to_string())?,
                        config: json!({"patched": true}),
                    },
                ])
                .await
                .map_err(|error| error.to_string())?;
            if loader.tree().entries[0].options.config != json!({"patched": true}) {
                return Err("inserted entry was not targetable by a later patch".into());
            }
        }
        "atomic-write" => {
            loader
                .load(catalog_tree(&["persisted"])?)
                .await
                .map_err(|error| error.to_string())?;
            let path =
                env::temp_dir().join(format!("tessivum-conformance-{}.json", std::process::id()));
            loader.persist(&path).map_err(|error| error.to_string())?;
            let document = fs::read_to_string(&path).map_err(|error| error.to_string())?;
            let _ = fs::remove_file(&path);
            if parse_entry_tree(&document).map_err(|error| error.to_string())? != *loader.tree() {
                return Err("atomic write did not persist the committed tree".into());
            }
        }
        other => return Err(format!("unsupported loader catalog case {other:?}")),
    }
    append_loader_trace(&loader, trace);
    loader.unload().await.map_err(|error| error.to_string())?;
    Ok(())
}

async fn loader(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
    let trace = Trace::default();
    for case in loader_cases(fixture)? {
        loader_case(case, &trace).await?;
    }
    Ok(trace.finish())
}

enum Execution {
    Trace(Vec<TraceEvent>),
    Unsupported,
}

async fn execute(fixture: &Fixture) -> Result<Execution, String> {
    match (&fixture.domain, fixture.scenario.as_str()) {
        (tessivum_core::Domain::Lifecycle, "effect-dispose") => {
            lifecycle(fixture).await.map(Execution::Trace)
        }
        (tessivum_core::Domain::Service, "isolate-realm") => {
            services(fixture).await.map(Execution::Trace)
        }
        (tessivum_core::Domain::Event, "emit") => events(fixture).await.map(Execution::Trace),
        (tessivum_core::Domain::Loader, "loader-catalog") => {
            loader(fixture).await.map(Execution::Trace)
        }
        _ => Ok(Execution::Unsupported),
    }
}

fn normalize_value(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(normalize_value).collect()),
        Value::Object(values) => {
            let values = values
                .into_iter()
                .map(|(key, value)| (key, normalize_value(value)))
                .collect::<std::collections::BTreeMap<_, _>>();
            Value::Object(values.into_iter().collect())
        }
        value => value,
    }
}

fn normalize_trace(trace: Vec<TraceEvent>) -> Vec<TraceEvent> {
    trace
        .into_iter()
        .map(|mut event| {
            event.value = event.value.map(normalize_value);
            event
        })
        .collect()
}

fn trace_value(event: &TraceEvent) -> Value {
    let mut value = Map::new();
    value.insert(
        "event".into(),
        serde_json::to_value(&event.kind).expect("trace event kind is serializable"),
    );
    for (field, text) in [
        ("subject", event.subject.as_ref()),
        ("from", event.from.as_ref()),
        ("to", event.to.as_ref()),
        ("label", event.label.as_ref()),
        ("phase", event.phase.as_ref()),
        ("error", event.error.as_ref()),
    ] {
        if let Some(text) = text {
            value.insert(field.into(), Value::String(text.clone()));
        }
    }
    if let Some(event_value) = &event.value {
        value.insert("value".into(), event_value.clone());
    }
    Value::Object(value)
}

fn trace_values(trace: &[TraceEvent]) -> Value {
    Value::Array(trace.iter().map(trace_value).collect())
}

fn fail(status: &str, fixture: &str, error: impl std::fmt::Display) -> ExitCode {
    println!(
        "{}",
        json!({
            "fixture": fixture,
            "status": status,
            "error": {
                "code": status,
                "fixture": fixture,
                "message": error.to_string(),
            },
        })
    );
    ExitCode::from(1)
}

fn unsupported(fixture: &Fixture) -> ExitCode {
    println!(
        "{}",
        json!({
            "fixture": fixture.name,
            "status": "UNSUPPORTED_SCENARIO",
            "error": {
                "code": "UNSUPPORTED_SCENARIO",
                "fixture": fixture.name,
                "message": format!("{}/{} is not supported by the Rust conformance runner", serde_json::to_value(&fixture.domain).expect("domain is serializable").as_str().expect("domain serializes to string"), fixture.scenario),
            },
        })
    );
    ExitCode::from(1)
}

fn mismatch(fixture: &Fixture, expected: &[TraceEvent], actual: &[TraceEvent]) -> ExitCode {
    let index = expected
        .iter()
        .zip(actual)
        .position(|(expected, actual)| expected != actual)
        .unwrap_or_else(|| expected.len().min(actual.len()));
    let mut error = Map::new();
    error.insert("code".into(), Value::String("TRACE_MISMATCH".into()));
    error.insert("fixture".into(), Value::String(fixture.name.clone()));
    error.insert("event".into(), json!(index));
    error.insert(
        "message".into(),
        Value::String(format!("trace mismatch at event {index}")),
    );
    if let Some(event) = expected.get(index) {
        error.insert("expected".into(), trace_value(event));
    }
    if let Some(event) = actual.get(index) {
        error.insert("actual".into(), trace_value(event));
    }
    println!(
        "{}",
        Value::Object(Map::from_iter([
            ("fixture".into(), Value::String(fixture.name.clone())),
            ("status".into(), Value::String("MISMATCH".into())),
            ("trace".into(), trace_values(actual)),
            ("error".into(), Value::Object(error)),
        ]))
    );
    ExitCode::from(1)
}

fn passed(fixture: &Fixture, trace: &[TraceEvent]) -> ExitCode {
    println!(
        "{}",
        json!({
            "fixture": fixture.name,
            "status": "PASS",
            "trace": trace_values(trace),
        })
    );
    ExitCode::SUCCESS
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(Waker::noop());
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn main() -> ExitCode {
    let mut paths = env::args_os().skip(1);
    let Some(path) = paths.next() else {
        return fail("USAGE", "<input>", "expected exactly one fixture path");
    };
    if paths.next().is_some() {
        return fail("USAGE", "<input>", "expected exactly one fixture path");
    }

    let display = path.to_string_lossy();
    let document = match fs::read_to_string(&path) {
        Ok(document) => document,
        Err(error) => return fail("INPUT_ERROR", &display, error),
    };
    let fixture = match tessivum_core::parse_fixture(&document) {
        Ok(fixture) => fixture,
        Err(error) => return fail("INVALID_FIXTURE", &display, error),
    };

    match block_on(execute(&fixture)) {
        Ok(Execution::Unsupported) => unsupported(&fixture),
        Err(error) => fail("EXECUTION_ERROR", &fixture.name, error),
        Ok(Execution::Trace(trace)) => {
            let expected = normalize_trace(fixture.expected_trace.clone());
            let actual = normalize_trace(trace);
            if expected.is_empty() || expected == actual {
                passed(&fixture, &actual)
            } else {
                mismatch(&fixture, &expected, &actual)
            }
        }
    }
}
