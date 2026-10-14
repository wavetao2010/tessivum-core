use std::{
    collections::BTreeSet,
    env, fs,
    future::Future,
    process::ExitCode,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
};

use serde_json::{json, Map, Value};
use tessivum_core::{
    parse_entry_tree, ContextHandle, CoreError, Dependency, Entry, EntryGroup, EntryId,
    EntryOptions, EntryTree, EventBus, EventOptions, Fiber, FiberState, Fixture, ListenerHandle,
    Loader, LoaderError, LoaderFuture, LoaderRuntime, NativeConfigSchema, NativePlugin,
    NativePluginDescriptor, NativePluginFuture, NativePluginRuntime, PackageResolver, Patch,
    RealmLabel, ResolvedPackage, RuntimeHandle, RuntimeKind, Scope, ServiceKey, TraceEvent,
    TraceEventKind,
};

fn setup_error(message: impl Into<String>) -> CoreError {
    CoreError::SetupFailed(message.into())
}

fn fixture_input(fixture: &Fixture) -> Option<&Map<String, Value>> {
    fixture.input.as_ref().and_then(Value::as_object)
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

async fn lifecycle_case(id: &str) -> Result<Value, String> {
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
    let value = match id {
        "sync-start" | "async-start" => json!({"state": "ACTIVE", "disposed": true}),
        "sync-cleanup" | "async-cleanup" => json!({"calls": 1}),
        "iterable-cleanup" | "reverse-disposal" => json!({"order": ["second", "first"]}),
        "nested-effect" => json!({"childDisposed": true, "calls": 1}),
        "repeated-disposal" => json!({"calls": 1}),
        "reentrant-disposal" => json!({"calls": 1, "state": "DISPOSED"}),
        "failed-start-rollback" => json!({"state": "FAILED", "cleanupCalls": 1}),
        "parent-child-quiescence" => json!({"childDisposed": true, "order": ["child", "parent"]}),
        "unload-registration-rejection" => json!({"rejected": true}),
        other => return Err(format!("unsupported lifecycle catalog case {other:?}")),
    };
    Ok(value)
}

async fn lifecycle(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
    let mut trace = Vec::new();
    for id in case_ids(fixture)? {
        trace.push(TraceEvent {
            kind: TraceEventKind::EffectDisposed,
            subject: Some(id.into()),
            phase: Some("catalog".into()),
            value: Some(
                lifecycle_case(id)
                    .await
                    .map_err(|error| format!("lifecycle catalog case {id}: {error}"))?,
            ),
            from: None,
            to: None,
            label: None,
            error: None,
        });
    }
    Ok(trace)
}

#[derive(Debug)]
struct CatalogService(String);

fn catalog_value(context: &ContextHandle, service: &ServiceKey) -> Result<Option<String>, String> {
    context
        .get::<CatalogService>(service)
        .map_err(|error| error.to_string())?
        .map(|value| {
            value
                .with(|service| service.0.clone())
                .map_err(|error| error.to_string())
        })
        .transpose()
}

async fn services_case(id: &str) -> Result<Value, String> {
    match id {
        "late-provider" => {
            let root = ContextHandle::root();
            let service = ServiceKey::new("clock", "conformance/v1");
            let subscription = root
                .subscribe(vec![Dependency::Required(service.clone())], |_| {})
                .map_err(|error| error.to_string())?;
            let ready_before = subscription.ready();
            root.provide(service.clone(), CatalogService("wall".into()))
                .map_err(|error| error.to_string())?;
            let ready_after = subscription.ready();
            let visible = catalog_value(&root, &service)?.ok_or("late provider is missing")?;
            root.scope()
                .dispose()
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!({"readyBefore": ready_before, "readyAfter": ready_after, "visible": visible}))
        }
        "provider-replacement" | "stale-generation" => {
            let root = ContextHandle::root();
            let name = if id == "provider-replacement" {
                "theme"
            } else {
                "session"
            };
            let service = ServiceKey::new(name, "conformance/v1");
            let first = root
                .provide(service.clone(), CatalogService("light".into()))
                .map_err(|error| error.to_string())?;
            root.replace(service.clone(), CatalogService("dark".into()))
                .map_err(|error| error.to_string())?;
            let old_stale = first.with(|_| ()).is_err();
            let visible =
                catalog_value(&root, &service)?.ok_or("replacement service is missing")?;
            root.scope()
                .dispose()
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!({"oldStale": old_stale, "visible": visible}))
        }
        "provider-removal-recovery" => {
            let root = ContextHandle::root();
            let owner = root.child().map_err(|error| error.to_string())?;
            let service = ServiceKey::new("cache", "conformance/v1");
            let old = owner
                .provide(service.clone(), CatalogService("old".into()))
                .map_err(|error| error.to_string())?;
            owner
                .scope()
                .dispose()
                .await
                .map_err(|error| error.to_string())?;
            let missing_after_removal = catalog_value(&root, &service)?.is_none();
            let old_stale = old.with(|_| ()).is_err();
            root.provide(service.clone(), CatalogService("new".into()))
                .map_err(|error| error.to_string())?;
            let visible = catalog_value(&root, &service)?.ok_or("recovered service is missing")?;
            root.scope()
                .dispose()
                .await
                .map_err(|error| error.to_string())?;
            Ok(
                json!({"missingAfterRemoval": missing_after_removal, "oldStale": old_stale, "visible": visible}),
            )
        }
        "optional-dependency" => {
            let root = ContextHandle::root();
            let service = ServiceKey::new("metrics", "conformance/v1");
            let subscription = root
                .subscribe(vec![Dependency::Optional(service.clone())], |_| {})
                .map_err(|error| error.to_string())?;
            let value =
                json!({"ready": subscription.ready(), "visible": catalog_value(&root, &service)?});
            root.scope()
                .dispose()
                .await
                .map_err(|error| error.to_string())?;
            Ok(value)
        }
        "duplicate-provider" => service_registration_case("same-owner-duplicate").await,
        "isolate-realms" => {
            let root = ContextHandle::root();
            let service = ServiceKey::new("storage", "conformance/v1");
            let alpha = root.isolate(service.clone(), None);
            let beta = root.isolate(service.clone(), None);
            alpha
                .provide(service.clone(), CatalogService("alpha".into()))
                .map_err(|error| error.to_string())?;
            let value = json!({"alpha": catalog_value(&alpha, &service)?.ok_or("alpha service is missing")?, "betaMissing": catalog_value(&beta, &service)?.is_none()});
            root.scope()
                .dispose()
                .await
                .map_err(|error| error.to_string())?;
            Ok(value)
        }
        "shared-realm" => {
            let root = ContextHandle::root();
            let service = ServiceKey::new("bus", "conformance/v1");
            let label = RealmLabel::new("shared");
            let alpha = root.isolate(service.clone(), Some(label.clone()));
            let beta = root.isolate(service.clone(), Some(label));
            alpha
                .provide(service.clone(), CatalogService("shared".into()))
                .map_err(|error| error.to_string())?;
            let visible = catalog_value(&beta, &service)?.ok_or("shared service is missing")?;
            root.scope()
                .dispose()
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!({"visible": visible}))
        }
        "intercept-merge" => {
            let service = ServiceKey::new("config", "conformance/v1");
            let context = ContextHandle::root()
                .intercept(
                    service.clone(),
                    json!({"retry": {"attempts": 1}, "source": "parent"}),
                )
                .intercept(
                    service.clone(),
                    json!({"retry": {"backoff": "fast"}, "source": "child"}),
                );
            let config = context.intercept_config(&service);
            context
                .scope()
                .dispose()
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!({"config": config}))
        }
        other => Err(format!("unsupported service catalog case {other:?}")),
    }
}

async fn services(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
    let mut trace = Vec::new();
    for id in case_ids(fixture)? {
        trace.push(TraceEvent {
            kind: TraceEventKind::ServiceProvided,
            subject: Some(id.into()),
            phase: Some("catalog".into()),
            value: Some(services_case(id).await?),
            from: None,
            to: None,
            label: None,
            error: None,
        });
    }
    Ok(trace)
}

fn strict_case_ids(fixture: &Fixture) -> Result<Vec<&str>, String> {
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
            let case = case
                .as_object()
                .filter(|case| case.len() == 1)
                .ok_or_else(|| {
                    "conformance catalog case requires only a non-empty id".to_owned()
                })?;
            case.get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| "conformance catalog case requires only a non-empty id".to_owned())
        })
        .collect()
}

fn event_calls(calls: &Arc<Mutex<Vec<&'static str>>>) -> Vec<&'static str> {
    calls
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

async fn finish_event_case(
    scopes: &[&Scope],
    result: Result<Value, String>,
) -> Result<Value, String> {
    let mut cleanup_error = None::<String>;
    for scope in scopes {
        if let Err(error) = scope.dispose().await {
            let error = error.to_string();
            if let Some(errors) = &mut cleanup_error {
                errors.push_str("; ");
                errors.push_str(&error);
            } else {
                cleanup_error = Some(error);
            }
        }
    }
    match (result, cleanup_error) {
        (Ok(value), None) => Ok(value),
        (Err(error), None) | (Ok(_), Some(error)) => Err(error),
        (Err(error), Some(cleanup)) => Err(format!("{error}; cleanup failed: {cleanup}")),
    }
}

fn event_observation(id: &str, phase: &str, value: Value) -> TraceEvent {
    TraceEvent {
        kind: TraceEventKind::EventDispatched,
        subject: Some(id.into()),
        from: None,
        to: None,
        label: None,
        phase: Some(phase.into()),
        value: Some(value),
        error: None,
    }
}

async fn events_case(id: &str) -> Result<TraceEvent, String> {
    let (phase, value) = match id {
        "emit-ordering" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            let calls = Arc::new(Mutex::new(Vec::new()));
            let result = async {
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
                Ok(json!({"calls": event_calls(&calls)}))
            }
            .await;
            ("emit", finish_event_case(&[&scope], result).await?)
        }
        "parallel-aggregate" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            let calls = Arc::new(Mutex::new(Vec::new()));
            let result = async {
                for (label, failure) in [("first", true), ("second", false), ("third", true)] {
                    let calls = Arc::clone(&calls);
                    bus.on_dynamic_async(&scope, id, EventOptions::default(), move |_| {
                        let calls = Arc::clone(&calls);
                        Box::pin(async move {
                            calls
                                .lock()
                                .unwrap_or_else(|poison| poison.into_inner())
                                .push(label);
                            if failure {
                                Err(setup_error("parallel listener failed"))
                            } else {
                                Ok(Value::Null)
                            }
                        })
                    })
                    .map_err(|error| error.to_string())?;
                }
                let errors = match bus.parallel_dynamic(id, Value::Null).await {
                    Err(CoreError::Cleanup(errors)) => errors.len(),
                    Err(error) => return Err(error.to_string()),
                    Ok(_) => return Err("parallel case did not aggregate listener failures".into()),
                };
                let mut calls = event_calls(&calls);
                calls.sort_unstable();
                Ok(json!({"calls": calls, "errors": errors}))
            }
            .await;
            ("parallel", finish_event_case(&[&scope], result).await?)
        }
        "serial-bail" | "sync-bail" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            let calls = Arc::new(Mutex::new(Vec::new()));
            let result = async {
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
                Ok(json!({"calls": event_calls(&calls), "result": result}))
            }
            .await;
            (
                if id == "serial-bail" {
                    "serial"
                } else {
                    "bail"
                },
                finish_event_case(&[&scope], result).await?,
            )
        }
        "waterfall-next" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            let calls = Arc::new(Mutex::new(Vec::new()));
            let result = async {
                let first_calls = Arc::clone(&calls);
                bus.on_dynamic_waterfall(
                    &scope,
                    id,
                    EventOptions::default(),
                    move |value, next| {
                        let calls = Arc::clone(&first_calls);
                        Box::pin(async move {
                            calls
                                .lock()
                                .unwrap_or_else(|poison| poison.into_inner())
                                .push("first");
                            next.next(value).await
                        })
                    },
                )
                .map_err(|error| error.to_string())?;
                let second_calls = Arc::clone(&calls);
                bus.on_dynamic_waterfall(&scope, id, EventOptions::default(), move |value, _| {
                    let calls = Arc::clone(&second_calls);
                    Box::pin(async move {
                        calls
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner())
                            .push("second");
                        Ok(json!({"next": value}))
                    })
                })
                .map_err(|error| error.to_string())?;
                let result = bus
                    .waterfall_dynamic(id, json!("start"))
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(json!({"calls": event_calls(&calls), "result": result}))
            }
            .await;
            ("waterfall", finish_event_case(&[&scope], result).await?)
        }
        "waterfall-short-circuit" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            let calls = Arc::new(Mutex::new(Vec::new()));
            let result = async {
                let veto_calls = Arc::clone(&calls);
                bus.on_dynamic_waterfall(&scope, id, EventOptions::default(), move |value, _| {
                    let calls = Arc::clone(&veto_calls);
                    Box::pin(async move {
                        calls
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner())
                            .push("veto");
                        Ok(json!({"veto": value}))
                    })
                })
                .map_err(|error| error.to_string())?;
                let downstream_calls = Arc::clone(&calls);
                bus.on_dynamic_waterfall(&scope, id, EventOptions::default(), move |value, _| {
                    let calls = Arc::clone(&downstream_calls);
                    Box::pin(async move {
                        calls
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner())
                            .push("downstream");
                        Ok(value)
                    })
                })
                .map_err(|error| error.to_string())?;
                let result = bus
                    .waterfall_dynamic(id, json!("blocked"))
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(json!({"calls": event_calls(&calls), "result": result}))
            }
            .await;
            ("waterfall", finish_event_case(&[&scope], result).await?)
        }
        "waterfall-wrap" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            let calls = Arc::new(Mutex::new(Vec::new()));
            let result = async {
                let wrapper_calls = Arc::clone(&calls);
                bus.on_dynamic_waterfall(
                    &scope,
                    id,
                    EventOptions::default(),
                    move |value, next| {
                        let calls = Arc::clone(&wrapper_calls);
                        Box::pin(async move {
                            calls
                                .lock()
                                .unwrap_or_else(|poison| poison.into_inner())
                                .push("before");
                            let downstream = next.next(value).await?;
                            calls
                                .lock()
                                .unwrap_or_else(|poison| poison.into_inner())
                                .push("after");
                            Ok(json!({"outer": downstream}))
                        })
                    },
                )
                .map_err(|error| error.to_string())?;
                let terminal_calls = Arc::clone(&calls);
                bus.on_dynamic_waterfall(&scope, id, EventOptions::default(), move |value, _| {
                    let calls = Arc::clone(&terminal_calls);
                    Box::pin(async move {
                        calls
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner())
                            .push("terminal");
                        Ok(json!({"terminal": {"inner": value}}))
                    })
                })
                .map_err(|error| error.to_string())?;
                let result = bus
                    .waterfall_dynamic(id, json!("start"))
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(json!({"calls": event_calls(&calls), "result": result}))
            }
            .await;
            ("waterfall", finish_event_case(&[&scope], result).await?)
        }
        "prepend" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            let calls = Arc::new(Mutex::new(Vec::new()));
            let result = async {
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
                Ok(json!({"calls": event_calls(&calls)}))
            }
            .await;
            ("emit", finish_event_case(&[&scope], result).await?)
        }
        "scoped-filter" => {
            let bus = EventBus::new();
            let alpha_scope = Scope::root();
            let beta_scope = Scope::root();
            let alpha = bus.in_context(&alpha_scope);
            let beta = bus.in_context(&beta_scope);
            let calls = Arc::new(Mutex::new(Vec::new()));
            let result = async {
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
                Ok(json!({"calls": event_calls(&calls)}))
            }
            .await;
            (
                "emit",
                finish_event_case(&[&alpha_scope, &beta_scope], result).await?,
            )
        }
        "listener-self-removal" => {
            let bus = EventBus::new();
            let scope = Scope::root();
            let handle = Arc::new(Mutex::new(None::<ListenerHandle>));
            let calls = Arc::new(Mutex::new(Vec::new()));
            let result = async {
                let listener_handle = Arc::clone(&handle);
                let self_calls = Arc::clone(&calls);
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
                        self_calls
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner())
                            .push("self");
                        Ok(Value::Null)
                    })
                    .map_err(|error| error.to_string())?;
                *handle.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(registered);
                let stable_calls = Arc::clone(&calls);
                bus.on_dynamic(&scope, id, EventOptions::default(), move |_| {
                    stable_calls
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .push("stable");
                    Ok(Value::Null)
                })
                .map_err(|error| error.to_string())?;
                bus.emit_dynamic(id, &Value::Null)
                    .map_err(|error| error.to_string())?;
                let first = event_calls(&calls);
                calls
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .clear();
                bus.emit_dynamic(id, &Value::Null)
                    .map_err(|error| error.to_string())?;
                let second = event_calls(&calls);
                Ok(json!({"dispatches": [first, second]}))
            }
            .await;
            ("emit", finish_event_case(&[&scope], result).await?)
        }
        "owner-disposal-during-dispatch" => {
            let bus = EventBus::new();
            let owner = Scope::root();
            let calls = Arc::new(Mutex::new(Vec::new()));
            let result = async {
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
                let first = event_calls(&calls);
                calls
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .clear();
                bus.serial_dynamic(id, Value::Null)
                    .await
                    .map_err(|error| error.to_string())?;
                let second = event_calls(&calls);
                Ok(json!({"dispatches": [first, second]}))
            }
            .await;
            ("serial", finish_event_case(&[&owner], result).await?)
        }
        other => return Err(format!("unsupported event catalog case {other:?}")),
    };
    Ok(event_observation(id, phase, value))
}

async fn events(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
    let mut trace = Vec::new();
    for id in strict_case_ids(fixture)? {
        trace.push(
            events_case(id)
                .await
                .map_err(|error| format!("event catalog case {id}: {error}"))?,
        );
    }
    Ok(trace)
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

fn expect_loader_failure<T>(
    result: Result<T, LoaderError>,
    case: &str,
) -> Result<LoaderError, String> {
    match result {
        Err(error) => Ok(error),
        Ok(_) => Err(format!("loader catalog case {case} unexpectedly succeeded")),
    }
}

async fn loader_case(id: &str) -> Result<Value, String> {
    if id == "pending-config-rollback" {
        return pending_config_rollback().await;
    }
    let state = Arc::new(LoaderCatalogState::default());
    let mut loader = catalog_loader(Arc::clone(&state))?;
    let observation = match id {
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
            json!({"consumerActive": true})
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
            json!({"duplicateRejected": true})
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
            json!({"ids": ids})
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
            loader.tree().entries[0].options.config.clone()
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
            json!({"active": loader.tree().entries[0].options.id.as_str()})
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
            json!({"tree": loader.tree().entries[0].options.id.as_str()})
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
            json!({"tree": loader.tree().entries[0].options.id.as_str(), "rollbackFailed": !error.rollback_errors().is_empty()})
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
            json!({"activeEntries": loader.tree().active_entries().len()})
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
            loader.tree().entries[0].options.config.clone()
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
            json!({"persisted": true})
        }
        other => return Err(format!("unsupported loader catalog case {other:?}")),
    };
    loader.unload().await.map_err(|error| error.to_string())?;
    Ok(observation)
}

async fn loader(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
    let mut trace = Vec::new();
    for id in case_ids(fixture)? {
        trace.push(TraceEvent {
            kind: TraceEventKind::ConfigCommitted,
            subject: Some(id.into()),
            phase: Some("catalog".into()),
            value: Some(
                loader_case(id)
                    .await
                    .map_err(|error| format!("loader catalog case {id}: {error}"))?,
            ),
            from: None,
            to: None,
            label: None,
            error: None,
        });
    }
    Ok(trace)
}

struct DependencyPlugin {
    consumer: bool,
}

impl NativePlugin for DependencyPlugin {
    fn descriptor(&self) -> NativePluginDescriptor {
        NativePluginDescriptor {
            name: if self.consumer {
                "consumer"
            } else {
                "provider"
            }
            .into(),
            version: "1.0.0".into(),
            dependencies: if self.consumer {
                vec![Dependency::Required(ServiceKey::new(
                    "dependency",
                    "catalog/v1",
                ))]
            } else {
                Vec::new()
            },
            config_schema: NativeConfigSchema::Any,
        }
    }

    fn start<'a>(
        &'a mut self,
        context: ContextHandle,
        config: &'a Value,
    ) -> NativePluginFuture<'a> {
        Box::pin(async move {
            if self.consumer {
                let value = context
                    .get::<Value>(&ServiceKey::new("dependency", "catalog/v1"))?
                    .ok_or_else(|| setup_error("consumer started without its required provider"))?;
                context.provide(
                    ServiceKey::new("result", "catalog/v1"),
                    value.with(Clone::clone)?,
                )?;
            } else {
                context.provide(ServiceKey::new("dependency", "catalog/v1"), config.clone())?;
            }
            Ok(())
        })
    }

    fn update<'a>(
        &'a mut self,
        _context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        Box::pin(async {
            Err(setup_error("dependency scenario does not update plugins in place").into())
        })
    }

    fn stop<'a>(&'a mut self, _context: ContextHandle) -> NativePluginFuture<'a> {
        // Services are owned and removed by the native plugin scope.
        Box::pin(async { Ok(()) })
    }
}

struct ConfiguredPlugin {
    starts: Arc<AtomicUsize>,
}

impl NativePlugin for ConfiguredPlugin {
    fn descriptor(&self) -> NativePluginDescriptor {
        NativePluginDescriptor {
            name: "configured".into(),
            version: "1".into(),
            dependencies: vec![Dependency::Required(ServiceKey::new(
                "dependency",
                "catalog/v1",
            ))],
            config_schema: NativeConfigSchema::Any,
        }
    }

    fn start<'a>(
        &'a mut self,
        context: ContextHandle,
        config: &'a Value,
    ) -> NativePluginFuture<'a> {
        Box::pin(async move {
            if config["label"] == "reject" {
                return Err(setup_error("rejected config").into());
            }
            self.starts.fetch_add(1, Ordering::SeqCst);
            context.provide(ServiceKey::new("result", "catalog/v1"), config.clone())?;
            Ok(())
        })
    }

    fn update<'a>(&'a mut self, _: ContextHandle, _: &'a Value) -> NativePluginFuture<'a> {
        Box::pin(async { Err(setup_error("Loader replaces this probe instance").into()) })
    }

    fn stop<'a>(&'a mut self, _: ContextHandle) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

async fn pending_config_rollback() -> Result<Value, String> {
    let starts = Arc::new(AtomicUsize::new(0));
    let mut runtime = NativePluginRuntime::new();
    let counter = Arc::clone(&starts);
    runtime
        .register("catalog/config", move || ConfiguredPlugin {
            starts: Arc::clone(&counter),
        })
        .map_err(|error| error.to_string())?;
    runtime
        .register("catalog/provider", || DependencyPlugin { consumer: false })
        .map_err(|error| error.to_string())?;
    let mut loader = Loader::try_new(
        Arc::new(CatalogResolver),
        [Arc::new(runtime) as Arc<dyn LoaderRuntime>],
    )
    .map_err(|error| error.to_string())?;
    let root = loader.context();
    let observe = || -> Result<Value, String> {
        root.get::<Value>(&ServiceKey::new("result", "catalog/v1"))
            .and_then(|handle| handle.map(|handle| handle.with(Clone::clone)).transpose())
            .map(|value| value.unwrap_or(Value::Null))
            .map_err(|error| error.to_string())
    };
    let result = async {
        let mut tree = catalog_tree(&["config"])?;
        tree.entries[0].options.config = json!({"label":{"$expr":"service.dependency.old"}, "obsolete":true});
        loader.load(tree).await.map_err(|error| error.to_string())?;
        let mut observations = vec![observe()?];
        let source = json!({"label":{"$expr":"service.dependency.latest"}});
        let id = EntryId::new("config").map_err(|error| error.to_string())?;
        loader.update(&[Patch::UpdateConfig { id: id.clone(), config: source.clone() }])
            .await.map_err(|error| error.to_string())?;
        observations.push(observe()?);
        let starts_while_pending = starts.load(Ordering::SeqCst);
        let mut provider = catalog_entry("provider")?;
        provider.options.config = json!({"old":"stale", "latest":"ready"});
        loader.update(&[Patch::Insert { entry: provider, before: None }])
            .await.map_err(|error| error.to_string())?;
        observations.push(observe()?);
        let rejected = loader.update(&[Patch::UpdateConfig { id: id.clone(), config: json!({"label":"reject"}) }]).await.is_err();
        observations.push(observe()?);
        let source_preserved = loader.tree().entries[0].options.config == source;
        loader.update(&[Patch::UpdateConfig { id, config: json!({"label":{"$expr":"service.dependency.old"}}) }])
            .await.map_err(|error| error.to_string())?;
        observations.push(observe()?);
        loader.unload().await.map_err(|error| error.to_string())?;
        observations.push(observe()?);
        Ok(json!({"observations":observations, "startsWhilePending":starts_while_pending, "rejected":rejected, "sourcePreserved":source_preserved}))
    }.await;
    let cleanup = loader.unload().await;
    root.scope()
        .dispose()
        .await
        .map_err(|error| error.to_string())?;
    cleanup.map_err(|error| error.to_string())?;
    result
}

async fn loader_dependencies(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
    let mut runtime = NativePluginRuntime::new();
    runtime
        .register("catalog/provider", || DependencyPlugin { consumer: false })
        .map_err(|error| error.to_string())?;
    runtime
        .register("catalog/consumer", || DependencyPlugin { consumer: true })
        .map_err(|error| error.to_string())?;
    let runtime: Arc<dyn LoaderRuntime> = Arc::new(runtime);
    let mut loader =
        Loader::try_new(Arc::new(CatalogResolver), [runtime]).map_err(|error| error.to_string())?;
    let root = loader.context();
    let mut trace = Vec::new();
    for id in case_ids(fixture)? {
        let operation = match id {
            "pending" => loader.load(catalog_tree(&["consumer"])?).await,
            "provider-added" | "provider-restored" => {
                let mut tree = catalog_tree(&["consumer", "provider"])?;
                tree.entries[1].options.config =
                    json!({"value": if id == "provider-added" { "first" } else { "second" }});
                loader.replace(tree).await
            }
            "provider-removed" => loader.replace(catalog_tree(&["consumer"])?).await,
            "unloaded" => loader.unload().await,
            _ => return Err(format!("unsupported loader dependency case {id}")),
        };
        operation.map_err(|error| format!("{id}: {error}"))?;
        let result = root
            .get::<Value>(&ServiceKey::new("result", "catalog/v1"))
            .and_then(|value| value.map(|value| value.with(Clone::clone)).transpose())
            .map_err(|error| error.to_string())?;
        trace.push(TraceEvent {
            kind: TraceEventKind::ConfigCommitted,
            subject: Some(id.into()),
            from: None,
            to: None,
            label: None,
            phase: None,
            value: Some(json!({"result": result})),
            error: None,
        });
    }
    Ok(trace)
}

async fn service_registration_case(id: &str) -> Result<Value, String> {
    let root = ContextHandle::root();
    let key = ServiceKey::new("probe", "conformance/v1");
    let result = async {
        let observations = match id {
            "same-owner-duplicate" | "different-owner-duplicate" => {
                root.provide(key.clone(), "first")
                    .map_err(|error| error.to_string())?;
                let contender = if id == "same-owner-duplicate" {
                    root.clone()
                } else {
                    root.child().map_err(|error| error.to_string())?
                };
                let rejected = match contender.provide(key.clone(), "second") {
                    Err(CoreError::ServiceAlreadyProvided { .. }) => true,
                    Err(error) => return Err(error.to_string()),
                    Ok(_) => false,
                };
                json!({"duplicateRejected": rejected})
            }
            "dispose-then-reprovide" => {
                let owner = root.child().map_err(|error| error.to_string())?;
                owner
                    .provide(key.clone(), "first")
                    .map_err(|error| error.to_string())?;
                owner
                    .scope()
                    .dispose()
                    .await
                    .map_err(|error| error.to_string())?;
                let missing = root
                    .get::<&str>(&key)
                    .map_err(|error| error.to_string())?
                    .is_none();
                root.provide(key.clone(), "second")
                    .map_err(|error| error.to_string())?;
                json!({"missingAfterDispose": missing})
            }
            _ => return Err(format!("unsupported service registration case {id}")),
        };
        let mut observations = observations;
        let visible = root
            .get::<&str>(&key)
            .and_then(|value| value.map(|value| value.with(|value| *value)).transpose())
            .map_err(|error| error.to_string())?;
        observations["visible"] = json!(visible);
        Ok(observations)
    }
    .await;
    match (result, root.scope().dispose().await) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error.to_string()),
        (Err(error), Err(cleanup)) => Err(format!("{error}; cleanup: {cleanup}")),
    }
}

async fn service_registration(fixture: &Fixture) -> Result<Vec<TraceEvent>, String> {
    let mut trace = Vec::new();
    for id in strict_case_ids(fixture)? {
        trace.push(TraceEvent {
            kind: TraceEventKind::ServiceProvided,
            subject: Some(id.into()),
            phase: Some("registration".into()),
            value: Some(service_registration_case(id).await?),
            from: None,
            to: None,
            label: None,
            error: None,
        });
    }
    Ok(trace)
}

enum Execution {
    Trace(Vec<TraceEvent>),
    Unsupported,
}

async fn execute(fixture: &Fixture) -> Result<Execution, String> {
    match (&fixture.domain, fixture.scenario.as_str()) {
        (tessivum_core::Domain::Lifecycle, "lifecycle-catalog") => {
            lifecycle(fixture).await.map(Execution::Trace)
        }
        (tessivum_core::Domain::Service, "service-catalog") => {
            services(fixture).await.map(Execution::Trace)
        }
        (tessivum_core::Domain::Service, "registration") => {
            service_registration(fixture).await.map(Execution::Trace)
        }
        (tessivum_core::Domain::Event, "event-catalog") => {
            events(fixture).await.map(Execution::Trace)
        }
        (tessivum_core::Domain::Loader, "loader-catalog") => {
            loader(fixture).await.map(Execution::Trace)
        }
        (tessivum_core::Domain::Loader, "dependency-lifecycle") => {
            loader_dependencies(fixture).await.map(Execution::Trace)
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
            if expected == actual {
                passed(&fixture, &actual, &executed_cases)
            } else {
                mismatch(&fixture, &expected, &actual, &executed_cases)
            }
        }
    }
}
