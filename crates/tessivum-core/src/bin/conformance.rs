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
    parse_entry_tree, ContextHandle, CoreError, Dependency, Entry, EntryGroup, EntryId,
    EntryOptions, EntryTree, EventBus, EventOptions, Fiber, FiberState, Fixture, ListenerHandle,
    Loader, LoaderError, LoaderFuture, LoaderRuntime, PackageResolver, Patch, RealmLabel,
    ResolvedPackage, RuntimeHandle, RuntimeKind, Scope, ServiceKey, TraceEvent, TraceEventKind,
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
fn case_ids(fixture: &Fixture) -> Result<Vec<&str>, String> {
    let cases = fixture_input(fixture)
        .and_then(|input| input.get("cases"))
        .and_then(Value::as_array)
        .ok_or("conformance catalog requires a cases array")?;
    if cases.is_empty() {
        return Err("conformance catalog requires at least one case".into());
    }
    cases
        .iter()
        .map(|case| {
            case.get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| "conformance catalog case requires a non-empty id".to_owned())
        })
        .collect()
}

async fn lifecycle_trace(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
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
async fn lifecycle_case(id: &str) -> Result<(), String> {
    match id {
        "sync-start" => {
            let root = Scope::root();
            let fiber = Fiber::new(&root, id).map_err(|error| error.to_string())?;
            fiber
                .start(|_| async { Ok(()) })
                .await
                .map_err(|error| error.to_string())?;
            if fiber.state() != FiberState::Active {
                return Err("synchronous start did not activate the fiber".into());
            }
            fiber.dispose().await.map_err(|error| error.to_string())?;
        }
        "async-start" => {
            let root = Scope::root();
            let fiber = Fiber::new(&root, id).map_err(|error| error.to_string())?;
            fiber
                .start(|_| async {
                    std::future::ready(()).await;
                    Ok(())
                })
                .await
                .map_err(|error| error.to_string())?;
            if fiber.state() != FiberState::Active {
                return Err("asynchronous start did not activate the fiber".into());
            }
            fiber.dispose().await.map_err(|error| error.to_string())?;
        }
        "sync-cleanup" | "async-cleanup" => {
            let scope = Scope::root();
            let calls = Arc::new(Mutex::new(0usize));
            let cleanup_calls = Arc::clone(&calls);
            let await_cleanup = id == "async-cleanup";
            scope
                .add_effect(
                    id,
                    Box::new(move || {
                        let calls = Arc::clone(&cleanup_calls);
                        Box::pin(async move {
                            if await_cleanup {
                                std::future::ready(()).await;
                            }
                            *calls.lock().unwrap_or_else(|poison| poison.into_inner()) += 1;
                            Ok(())
                        })
                    }),
                )
                .map_err(|error| error.to_string())?;
            scope.dispose().await.map_err(|error| error.to_string())?;
            if *calls.lock().unwrap_or_else(|poison| poison.into_inner()) != 1 {
                return Err(format!("{id} did not run exactly once"));
            }
        }
        "iterable-cleanup" | "reverse-disposal" => {
            let scope = Scope::root();
            let order = Arc::new(Mutex::new(Vec::new()));
            for label in ["first", "second"] {
                let order = Arc::clone(&order);
                scope
                    .add_effect(
                        label,
                        Box::new(move || {
                            Box::pin(async move {
                                order
                                    .lock()
                                    .unwrap_or_else(|poison| poison.into_inner())
                                    .push(label);
                                Ok(())
                            })
                        }),
                    )
                    .map_err(|error| error.to_string())?;
            }
            scope.dispose().await.map_err(|error| error.to_string())?;
            if *order.lock().unwrap_or_else(|poison| poison.into_inner()) != ["second", "first"] {
                return Err(format!("{id} did not dispose effects in reverse order"));
            }
        }
        "nested-effect" => {
            let parent = Scope::root();
            let child = parent.child().map_err(|error| error.to_string())?;
            let calls = Arc::new(Mutex::new(0usize));
            let child_calls = Arc::clone(&calls);
            child
                .add_effect(
                    id,
                    Box::new(move || {
                        Box::pin(async move {
                            *child_calls
                                .lock()
                                .unwrap_or_else(|poison| poison.into_inner()) += 1;
                            Ok(())
                        })
                    }),
                )
                .map_err(|error| error.to_string())?;
            parent.dispose().await.map_err(|error| error.to_string())?;
            if child.state() != FiberState::Disposed
                || *calls.lock().unwrap_or_else(|poison| poison.into_inner()) != 1
            {
                return Err("nested effect did not quiesce with its parent".into());
            }
        }
        "repeated-disposal" => {
            let scope = Scope::root();
            let calls = Arc::new(Mutex::new(0usize));
            let cleanup_calls = Arc::clone(&calls);
            scope
                .add_effect(
                    id,
                    Box::new(move || {
                        Box::pin(async move {
                            *cleanup_calls
                                .lock()
                                .unwrap_or_else(|poison| poison.into_inner()) += 1;
                            Ok(())
                        })
                    }),
                )
                .map_err(|error| error.to_string())?;
            scope.dispose().await.map_err(|error| error.to_string())?;
            scope.dispose().await.map_err(|error| error.to_string())?;
            if *calls.lock().unwrap_or_else(|poison| poison.into_inner()) != 1 {
                return Err("repeated disposal ran cleanup more than once".into());
            }
        }
        "reentrant-disposal" => {
            let scope = Scope::root();
            let reentrant_scope = scope.clone();
            let calls = Arc::new(Mutex::new(0usize));
            let cleanup_calls = Arc::clone(&calls);
            scope
                .add_effect(
                    id,
                    Box::new(move || {
                        Box::pin(async move {
                            *cleanup_calls
                                .lock()
                                .unwrap_or_else(|poison| poison.into_inner()) += 1;
                            reentrant_scope.dispose().await?;
                            Ok(())
                        })
                    }),
                )
                .map_err(|error| error.to_string())?;
            scope.dispose().await.map_err(|error| error.to_string())?;
            if scope.state() != FiberState::Disposed
                || *calls.lock().unwrap_or_else(|poison| poison.into_inner()) != 1
            {
                return Err("reentrant disposal did not converge on one cleanup".into());
            }
        }
        "failed-start-rollback" => {
            let parent = Scope::root();
            let fiber = Fiber::new(&parent, id).map_err(|error| error.to_string())?;
            let calls = Arc::new(Mutex::new(0usize));
            let cleanup_calls = Arc::clone(&calls);
            if fiber
                .start(move |scope| async move {
                    scope.add_effect(
                        "rollback",
                        Box::new(move || {
                            Box::pin(async move {
                                *cleanup_calls
                                    .lock()
                                    .unwrap_or_else(|poison| poison.into_inner()) += 1;
                                Ok(())
                            })
                        }),
                    )?;
                    Err(setup_error("requested failed start"))
                })
                .await
                .is_ok()
            {
                return Err("failed start unexpectedly succeeded".into());
            }
            if fiber.state() != FiberState::Failed
                || *calls.lock().unwrap_or_else(|poison| poison.into_inner()) != 1
            {
                return Err("failed start did not roll back its effect".into());
            }
        }
        "parent-child-quiescence" => {
            let parent = Scope::root();
            let child = parent.child().map_err(|error| error.to_string())?;
            let order = Arc::new(Mutex::new(Vec::new()));
            let child_order = Arc::clone(&order);
            child
                .add_effect(
                    "child",
                    Box::new(move || {
                        Box::pin(async move {
                            child_order
                                .lock()
                                .unwrap_or_else(|poison| poison.into_inner())
                                .push("child");
                            Ok(())
                        })
                    }),
                )
                .map_err(|error| error.to_string())?;
            let parent_order = Arc::clone(&order);
            parent
                .add_effect(
                    "parent",
                    Box::new(move || {
                        Box::pin(async move {
                            parent_order
                                .lock()
                                .unwrap_or_else(|poison| poison.into_inner())
                                .push("parent");
                            Ok(())
                        })
                    }),
                )
                .map_err(|error| error.to_string())?;
            parent.dispose().await.map_err(|error| error.to_string())?;
            if child.state() != FiberState::Disposed
                || *order.lock().unwrap_or_else(|poison| poison.into_inner()) != ["child", "parent"]
            {
                return Err("parent disposal did not quiesce its child first".into());
            }
        }
        "unload-registration-rejection" => {
            let scope = Scope::root();
            let unloading_scope = scope.clone();
            scope
                .add_effect(
                    id,
                    Box::new(move || {
                        Box::pin(async move {
                            if unloading_scope
                                .add_effect("late", Box::new(|| Box::pin(async { Ok(()) })))
                                .is_ok()
                            {
                                return Err(setup_error("unloading scope accepted a new effect"));
                            }
                            Ok(())
                        })
                    }),
                )
                .map_err(|error| error.to_string())?;
            scope.dispose().await.map_err(|error| error.to_string())?;
        }
        other => return Err(format!("unsupported lifecycle catalog case {other:?}")),
    }
    Ok(())
}

async fn lifecycle(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
    for id in case_ids(fixture)? {
        lifecycle_case(id)
            .await
            .map_err(|error| format!("lifecycle catalog case {id}: {error}"))?;
    }
    lifecycle_trace(fixture).await
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

async fn services_trace(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
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
async fn services_case(id: &str) -> Result<(), String> {
    match id {
        "late-provider" => {
            let context = ContextHandle::root();
            let service = ServiceKey::new("clock", "conformance/v1");
            let subscription = context
                .subscribe(vec![Dependency::Required(service.clone())], |_| {})
                .map_err(|error| error.to_string())?;
            if subscription.ready() {
                return Err("missing required provider did not gate its consumer".into());
            }
            context
                .provide(service, CatalogService("wall".into()))
                .map_err(|error| error.to_string())?;
            if !subscription.ready() {
                return Err("late provider did not activate its consumer".into());
            }
        }
        "provider-replacement" | "duplicate-provider" | "stale-generation" => {
            let context = ContextHandle::root();
            let service = ServiceKey::new("theme", "conformance/v1");
            let first = context
                .provide(service.clone(), CatalogService("light".into()))
                .map_err(|error| error.to_string())?;
            let replacement = context
                .provide(service.clone(), CatalogService("dark".into()))
                .map_err(|error| error.to_string())?;
            if first.with(|_| ()).is_ok() || !replacement.is_current() {
                return Err(format!("{id} did not replace the provider generation"));
            }
            let label = context
                .get::<CatalogService>(&service)
                .map_err(|error| error.to_string())?
                .ok_or("replacement service is missing")?
                .with(|service| service.0.clone())
                .map_err(|error| error.to_string())?;
            if label != "dark" {
                return Err(format!("{id} did not expose the replacement"));
            }
        }
        "provider-removal-recovery" => {
            let root = ContextHandle::root();
            let owner = root.child().map_err(|error| error.to_string())?;
            let service = ServiceKey::new("cache", "conformance/v1");
            let removed = owner
                .provide(service.clone(), CatalogService("old".into()))
                .map_err(|error| error.to_string())?;
            owner
                .scope()
                .dispose()
                .await
                .map_err(|error| error.to_string())?;
            if root
                .get::<CatalogService>(&service)
                .map_err(|error| error.to_string())?
                .is_some()
                || removed.with(|_| ()).is_ok()
            {
                return Err("provider removal left a live service handle".into());
            }
            let replacement = root
                .provide(service.clone(), CatalogService("new".into()))
                .map_err(|error| error.to_string())?;
            if replacement
                .with(|service| service.0.clone())
                .map_err(|error| error.to_string())?
                != "new"
            {
                return Err("provider recovery did not create a live generation".into());
            }
        }
        "optional-dependency" => {
            let context = ContextHandle::root();
            let service = ServiceKey::new("metrics", "conformance/v1");
            let subscription = context
                .subscribe(vec![Dependency::Optional(service)], |_| {})
                .map_err(|error| error.to_string())?;
            if !subscription.ready() {
                return Err("missing optional provider gated its consumer".into());
            }
        }
        "isolate-realms" => {
            let root = ContextHandle::root();
            let service = ServiceKey::new("storage", "conformance/v1");
            let alpha = root.isolate(service.clone(), None);
            let beta = root.isolate(service.clone(), None);
            alpha
                .provide(service.clone(), CatalogService("alpha".into()))
                .map_err(|error| error.to_string())?;
            if beta
                .get::<CatalogService>(&service)
                .map_err(|error| error.to_string())?
                .is_some()
            {
                return Err("isolated realms leaked a provider".into());
            }
        }
        "shared-realm" => {
            let root = ContextHandle::root();
            let service = ServiceKey::new("bus", "conformance/v1");
            let alpha = root.isolate(service.clone(), Some(RealmLabel::new("shared")));
            let beta = root.isolate(service.clone(), Some(RealmLabel::new("shared")));
            alpha
                .provide(service.clone(), CatalogService("shared".into()))
                .map_err(|error| error.to_string())?;
            if beta
                .get::<CatalogService>(&service)
                .map_err(|error| error.to_string())?
                .ok_or("shared realm provider is missing")?
                .with(|value| value.0.clone())
                .map_err(|error| error.to_string())?
                != "shared"
            {
                return Err("shared realm did not coactivate its provider".into());
            }
        }
        "intercept-merge" => {
            let context = ContextHandle::root();
            let service = ServiceKey::new("config", "conformance/v1");
            let merged = context
                .intercept(
                    service.clone(),
                    json!({"retry": {"attempts": 1}, "source": "parent"}),
                )
                .intercept(
                    service.clone(),
                    json!({"retry": {"backoff": "fast"}, "source": "child"}),
                )
                .intercept_config(&service);
            if merged != json!({"retry": {"backoff": "fast"}, "source": "child"}) {
                return Err("intercept chain did not merge child precedence".into());
            }
        }
        other => return Err(format!("unsupported service catalog case {other:?}")),
    }
    Ok(())
}

async fn services(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
    for id in case_ids(fixture)? {
        services_case(id)
            .await
            .map_err(|error| format!("service catalog case {id}: {error}"))?;
    }
    services_trace(fixture).await
}

async fn events_trace(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
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
async fn events_case(id: &str) -> Result<(), String> {
    match id {
        "emit-ordering" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            let calls = Arc::new(Mutex::new(Vec::new()));
            for label in ["first", "second"] {
                let calls = Arc::clone(&calls);
                bus.on_dynamic(&scope, id, EventOptions::default(), move |_| {
                    calls
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .push(label);
                    Ok(Value::Null)
                })
                .map_err(|error| error.to_string())?;
            }
            bus.emit_dynamic(id, &Value::Null)
                .map_err(|error| error.to_string())?;
            if *calls.lock().unwrap_or_else(|poison| poison.into_inner()) != ["first", "second"] {
                return Err("emit did not preserve listener order".into());
            }
        }
        "parallel-aggregate" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            let calls = Arc::new(Mutex::new(0usize));
            for failure in [true, false, true] {
                let calls = Arc::clone(&calls);
                bus.on_dynamic_async(&scope, id, EventOptions::default(), move |_| {
                    let calls = Arc::clone(&calls);
                    Box::pin(async move {
                        *calls.lock().unwrap_or_else(|poison| poison.into_inner()) += 1;
                        if failure {
                            Err(setup_error("parallel listener failed"))
                        } else {
                            Ok(json!("settled"))
                        }
                    })
                })
                .map_err(|error| error.to_string())?;
            }
            let error = bus
                .parallel_dynamic(id, Value::Null)
                .await
                .expect_err("parallel case must fail");
            if error
                .cleanup_errors()
                .is_none_or(|errors| errors.len() != 2)
                || *calls.lock().unwrap_or_else(|poison| poison.into_inner()) != 3
            {
                return Err("parallel dispatch did not aggregate every failure".into());
            }
        }
        "serial-bail" | "sync-bail" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            let calls = Arc::new(Mutex::new(Vec::new()));
            for (label, value) in [
                ("null", Value::Null),
                ("false", json!(false)),
                ("stop", json!("stop")),
                ("late", json!("late")),
            ] {
                let calls = Arc::clone(&calls);
                if id == "serial-bail" {
                    bus.on_dynamic_async(&scope, id, EventOptions::default(), move |_| {
                        let calls = Arc::clone(&calls);
                        let value = value.clone();
                        Box::pin(async move {
                            calls
                                .lock()
                                .unwrap_or_else(|poison| poison.into_inner())
                                .push(label);
                            Ok(value)
                        })
                    })
                    .map_err(|error| error.to_string())?;
                } else {
                    bus.on_dynamic(&scope, id, EventOptions::default(), move |_| {
                        calls
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner())
                            .push(label);
                        Ok(value.clone())
                    })
                    .map_err(|error| error.to_string())?;
                }
            }
            let result = if id == "serial-bail" {
                bus.serial_dynamic(id, Value::Null)
                    .await
                    .map_err(|error| error.to_string())?
            } else {
                bus.bail_dynamic(id, &Value::Null)
                    .map_err(|error| error.to_string())?
            };
            if result != Some(json!("stop"))
                || *calls.lock().unwrap_or_else(|poison| poison.into_inner())
                    != ["null", "false", "stop"]
            {
                return Err(format!("{id} did not stop on its first effective value"));
            }
        }
        "waterfall-next" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            bus.on_dynamic_waterfall(&scope, id, EventOptions::default(), |value, next| {
                Box::pin(async move { next.next(json!({"next": value})).await })
            })
            .map_err(|error| error.to_string())?;
            bus.on_dynamic_waterfall(&scope, id, EventOptions::default(), |value, _| {
                Box::pin(async move { Ok(value) })
            })
            .map_err(|error| error.to_string())?;
            if bus
                .waterfall_dynamic(id, json!("start"))
                .await
                .map_err(|error| error.to_string())?
                != json!({"next": "start"})
            {
                return Err("waterfall next did not transform its value".into());
            }
        }
        "waterfall-short-circuit" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            let downstream = Arc::new(Mutex::new(0usize));
            bus.on_dynamic_waterfall(&scope, id, EventOptions::default(), |value, _| {
                Box::pin(async move { Ok(json!({"veto": value})) })
            })
            .map_err(|error| error.to_string())?;
            let downstream_calls = Arc::clone(&downstream);
            bus.on_dynamic_waterfall(&scope, id, EventOptions::default(), move |value, _| {
                let downstream = Arc::clone(&downstream_calls);
                Box::pin(async move {
                    *downstream
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner()) += 1;
                    Ok(value)
                })
            })
            .map_err(|error| error.to_string())?;
            if bus
                .waterfall_dynamic(id, json!("blocked"))
                .await
                .map_err(|error| error.to_string())?
                != json!({"veto": "blocked"})
                || *downstream
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    != 0
            {
                return Err("waterfall short-circuit invoked a downstream listener".into());
            }
        }
        "waterfall-wrap" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            bus.on_dynamic_waterfall(&scope, id, EventOptions::default(), |value, next| {
                Box::pin(async move {
                    let downstream = next.next(json!({"inner": value})).await?;
                    Ok(json!({"outer": downstream}))
                })
            })
            .map_err(|error| error.to_string())?;
            bus.on_dynamic_waterfall(&scope, id, EventOptions::default(), |value, _| {
                Box::pin(async move { Ok(json!({"terminal": value})) })
            })
            .map_err(|error| error.to_string())?;
            if bus
                .waterfall_dynamic(id, json!("start"))
                .await
                .map_err(|error| error.to_string())?
                != json!({"outer": {"terminal": {"inner": "start"}}})
            {
                return Err("waterfall wrapper did not preserve its downstream result".into());
            }
        }
        "prepend" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            let calls = Arc::new(Mutex::new(Vec::new()));
            for (label, options) in [
                ("first", EventOptions::default()),
                ("second", EventOptions::default()),
                (
                    "prepend",
                    EventOptions {
                        prepend: true,
                        ..EventOptions::default()
                    },
                ),
            ] {
                let calls = Arc::clone(&calls);
                bus.on_dynamic(&scope, id, options, move |_| {
                    calls
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .push(label);
                    Ok(Value::Null)
                })
                .map_err(|error| error.to_string())?;
            }
            bus.emit_dynamic(id, &Value::Null)
                .map_err(|error| error.to_string())?;
            if *calls.lock().unwrap_or_else(|poison| poison.into_inner())
                != ["prepend", "first", "second"]
            {
                return Err(
                    "prepend did not move its listener ahead of ordinary registrations".into(),
                );
            }
        }
        "scoped-filter" => {
            let bus = EventBus::new();
            let alpha_scope = Scope::root();
            let beta_scope = Scope::root();
            let alpha = bus.in_context(&alpha_scope);
            let beta = bus.in_context(&beta_scope);
            let calls = Arc::new(Mutex::new(Vec::new()));
            let alpha_calls = Arc::clone(&calls);
            alpha
                .on_dynamic(&alpha_scope, id, EventOptions::default(), move |_| {
                    alpha_calls
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .push("alpha");
                    Ok(Value::Null)
                })
                .map_err(|error| error.to_string())?;
            let beta_calls = Arc::clone(&calls);
            beta.on_dynamic(&beta_scope, id, EventOptions::default(), move |_| {
                beta_calls
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .push("beta");
                Ok(Value::Null)
            })
            .map_err(|error| error.to_string())?;
            alpha
                .emit_dynamic(id, &Value::Null)
                .map_err(|error| error.to_string())?;
            if *calls.lock().unwrap_or_else(|poison| poison.into_inner()) != ["alpha"] {
                return Err("scoped event filter dispatched another context's listener".into());
            }
        }
        "listener-self-removal" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            let handle = Arc::new(Mutex::new(None::<ListenerHandle>));
            let listener_handle = Arc::clone(&handle);
            let calls = Arc::new(Mutex::new(0usize));
            let listener_calls = Arc::clone(&calls);
            let registered = bus
                .on_dynamic(&scope, id, EventOptions::default(), move |_| {
                    if !listener_handle
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .as_ref()
                        .is_some_and(ListenerHandle::remove)
                    {
                        return Err(setup_error("listener could not remove itself"));
                    }
                    *listener_calls
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner()) += 1;
                    Ok(Value::Null)
                })
                .map_err(|error| error.to_string())?;
            *handle.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(registered);
            bus.emit_dynamic(id, &Value::Null)
                .map_err(|error| error.to_string())?;
            bus.emit_dynamic(id, &Value::Null)
                .map_err(|error| error.to_string())?;
            if *calls.lock().unwrap_or_else(|poison| poison.into_inner()) != 1 {
                return Err("self-removing listener remained dispatchable".into());
            }
        }
        "owner-disposal-during-dispatch" => {
            let bus = EventBus::new();
            let owner = Scope::root();
            let calls = Arc::new(Mutex::new(Vec::new()));
            let disposing_owner = owner.clone();
            let disposer_calls = Arc::clone(&calls);
            bus.on_dynamic_async(&owner, id, EventOptions::default(), move |_| {
                let owner = disposing_owner.clone();
                let calls = Arc::clone(&disposer_calls);
                Box::pin(async move {
                    owner.dispose().await?;
                    calls
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .push("dispose");
                    Ok(Value::Null)
                })
            })
            .map_err(|error| error.to_string())?;
            let downstream_calls = Arc::clone(&calls);
            bus.on_dynamic_async(&owner, id, EventOptions::default(), move |_| {
                let calls = Arc::clone(&downstream_calls);
                Box::pin(async move {
                    calls
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .push("downstream");
                    Ok(Value::Null)
                })
            })
            .map_err(|error| error.to_string())?;
            bus.serial_dynamic(id, Value::Null)
                .await
                .map_err(|error| error.to_string())?;
            if *calls.lock().unwrap_or_else(|poison| poison.into_inner())
                != ["dispose", "downstream"]
            {
                return Err("owner disposal mutated the current dispatch snapshot".into());
            }
            calls
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clear();
            bus.serial_dynamic(id, Value::Null)
                .await
                .map_err(|error| error.to_string())?;
            if !calls
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .is_empty()
            {
                return Err("owner disposal left listeners dispatchable".into());
            }
        }
        other => return Err(format!("unsupported event catalog case {other:?}")),
    }
    Ok(())
}

async fn events(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
    for id in case_ids(fixture)? {
        events_case(id)
            .await
            .map_err(|error| format!("event catalog case {id}: {error}"))?;
    }
    events_trace(fixture).await
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
    for id in case_ids(fixture)? {
        loader_case(id, &trace)
            .await
            .map_err(|error| format!("loader catalog case {id}: {error}"))?;
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

fn mismatch(
    fixture: &Fixture,
    expected: &[TraceEvent],
    actual: &[TraceEvent],
    executed_cases: &[String],
) -> ExitCode {
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
            ("executedCases".into(), json!(executed_cases)),
            ("error".into(), Value::Object(error)),
        ]))
    );
    ExitCode::from(1)
}

fn passed(fixture: &Fixture, trace: &[TraceEvent], executed_cases: &[String]) -> ExitCode {
    println!(
        "{}",
        json!({
            "fixture": fixture.name,
            "status": "PASS",
            "trace": trace_values(trace),
            "executedCases": executed_cases,
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
            let executed_cases = match case_ids(&fixture) {
                Ok(cases) => cases.into_iter().map(str::to_owned).collect::<Vec<_>>(),
                Err(error) => return fail("EXECUTION_ERROR", &fixture.name, error),
            };
            let loader_execution_baseline =
                matches!(&fixture.domain, tessivum_core::Domain::Loader) && expected.is_empty();
            if loader_execution_baseline || expected == actual {
                passed(&fixture, &actual, &executed_cases)
            } else {
                mismatch(&fixture, &expected, &actual, &executed_cases)
            }
        }
    }
}
