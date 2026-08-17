use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tessivum_core::{
    ContextHandle, CoreError, DynamicEvent, EventBus, EventKey, EventOptions, RealmLabel, Scope,
};

#[derive(Debug)]
struct NativeOnly(&'static str);

#[test]
fn typed_dispatch_is_native_while_dynamic_dispatch_preserves_json() {
    let bus = EventBus::new();
    let scope = Scope::root();
    let native_key = EventKey::<NativeOnly>::new("native/changed");
    let native_seen = Arc::new(Mutex::new(Vec::new()));
    let native_seen_by_listener = Arc::clone(&native_seen);

    bus.on(
        &scope,
        native_key.clone(),
        EventOptions::default(),
        move |event| {
            native_seen_by_listener
                .lock()
                .expect("native event log is available")
                .push(event.0);
            Ok(Value::Null)
        },
    )
    .expect("typed listener registration succeeds");
    bus.emit(&native_key, &NativeOnly("native value"))
        .expect("native event dispatch succeeds without serialization");
    assert_eq!(
        &*native_seen.lock().expect("native event log is available"),
        &["native value"],
        "typed events accept native payloads that have no JSON representation"
    );

    let dynamic_seen = Arc::new(Mutex::new(Vec::new()));
    let dynamic_seen_by_listener = Arc::clone(&dynamic_seen);
    bus.on_dynamic(
        &scope,
        "dynamic/changed",
        EventOptions::default(),
        move |event: &DynamicEvent| {
            dynamic_seen_by_listener
                .lock()
                .expect("dynamic event log is available")
                .push(event.clone());
            Ok(Value::Null)
        },
    )
    .expect("dynamic listener registration succeeds");

    let payload = json!({"id": 7, "tags": ["stable", "json"]});
    bus.emit_dynamic("dynamic/changed", &payload)
        .expect("dynamic event dispatch succeeds");
    assert_eq!(
        &*dynamic_seen.lock().expect("dynamic event log is available"),
        &[payload],
        "dynamic events deliver their JSON payload without reshaping it"
    );
}

#[test]
fn prepend_is_stable_and_listener_handles_have_distinct_resource_ids() {
    let bus = EventBus::new();
    let scope = Scope::root();
    let key = EventKey::<()>::new("ordered");
    let calls = Arc::new(Mutex::new(Vec::new()));

    let first_calls = Arc::clone(&calls);
    let first = bus
        .on(&scope, key.clone(), EventOptions::default(), move |_| {
            first_calls
                .lock()
                .expect("call log is available")
                .push("first");
            Ok(Value::Null)
        })
        .expect("first listener registration succeeds");
    let second_calls = Arc::clone(&calls);
    let second = bus
        .on(&scope, key.clone(), EventOptions::default(), move |_| {
            second_calls
                .lock()
                .expect("call log is available")
                .push("second");
            Ok(Value::Null)
        })
        .expect("second listener registration succeeds");
    let prepended_calls = Arc::clone(&calls);
    bus.on(
        &scope,
        key.clone(),
        EventOptions {
            prepend: true,
            ..EventOptions::default()
        },
        move |_| {
            prepended_calls
                .lock()
                .expect("call log is available")
                .push("prepended");
            Ok(Value::Null)
        },
    )
    .expect("prepended listener registration succeeds");

    assert_ne!(
        first.id(),
        second.id(),
        "each listener receives a distinct ResourceId"
    );
    bus.emit(&key, &())
        .expect("ordered event dispatch succeeds");
    assert_eq!(
        &*calls.lock().expect("call log is available"),
        &["prepended", "first", "second"],
        "prepend runs before ordinary listeners while ordinary registration remains stable"
    );
}

#[test]
fn realm_views_filter_local_listeners_but_global_listeners_cross_realms() {
    let bus = EventBus::new();
    let alpha = bus.in_realm(RealmLabel::new("alpha"));
    let beta = bus.in_realm(RealmLabel::new("beta"));
    let alpha_scope = Scope::root();
    let beta_scope = Scope::root();
    let key = EventKey::<()>::new("realm/filtered");
    let calls = Arc::new(Mutex::new(Vec::new()));

    let alpha_calls = Arc::clone(&calls);
    alpha
        .on(
            &alpha_scope,
            key.clone(),
            EventOptions::default(),
            move |_| {
                alpha_calls
                    .lock()
                    .expect("call log is available")
                    .push("alpha-local");
                Ok(Value::Null)
            },
        )
        .expect("alpha local listener registration succeeds");
    let beta_calls = Arc::clone(&calls);
    beta.on(
        &beta_scope,
        key.clone(),
        EventOptions::default(),
        move |_| {
            beta_calls
                .lock()
                .expect("call log is available")
                .push("beta-local");
            Ok(Value::Null)
        },
    )
    .expect("beta local listener registration succeeds");
    let global_calls = Arc::clone(&calls);
    alpha
        .on(
            &alpha_scope,
            key.clone(),
            EventOptions {
                global: true,
                ..EventOptions::default()
            },
            move |_| {
                global_calls
                    .lock()
                    .expect("call log is available")
                    .push("global");
                Ok(Value::Null)
            },
        )
        .expect("global listener registration succeeds");

    alpha.emit(&key, &()).expect("alpha dispatch succeeds");
    assert_eq!(
        &*calls.lock().expect("call log is available"),
        &["alpha-local", "global"],
        "an alpha dispatch excludes beta-local listeners"
    );
    calls.lock().expect("call log is available").clear();

    beta.emit(&key, &()).expect("beta dispatch succeeds");
    assert_eq!(
        &*calls.lock().expect("call log is available"),
        &["beta-local", "global"],
        "global listeners remain eligible across realm views"
    );
}

#[test]
fn emit_propagates_listener_errors_synchronously_and_stops_dispatch() {
    let bus = EventBus::new();
    let scope = Scope::root();
    let key = EventKey::<()>::new("emit/failure");
    let calls = Arc::new(Mutex::new(Vec::new()));

    let first_calls = Arc::clone(&calls);
    bus.on(&scope, key.clone(), EventOptions::default(), move |_| {
        first_calls
            .lock()
            .expect("call log is available")
            .push("first");
        Ok(Value::Null)
    })
    .expect("first listener registration succeeds");
    bus.on(&scope, key.clone(), EventOptions::default(), |_| {
        Err(CoreError::SetupFailed("listener exploded".into()))
    })
    .expect("failing listener registration succeeds");
    let after_calls = Arc::clone(&calls);
    bus.on(&scope, key.clone(), EventOptions::default(), move |_| {
        after_calls
            .lock()
            .expect("call log is available")
            .push("after-error");
        Ok(Value::Null)
    })
    .expect("trailing listener registration succeeds");

    let error = bus
        .emit(&key, &())
        .expect_err("emit returns the synchronous listener error");
    assert_eq!(error, CoreError::SetupFailed("listener exploded".into()));
    assert_eq!(
        &*calls.lock().expect("call log is available"),
        &["first"],
        "emit does not continue after a listener throws"
    );
}

#[test]
fn context_exposes_its_event_bus_without_requiring_product_services() {
    let context = ContextHandle::root();
    let bus = context.events();
    let key = EventKey::<u8>::new("context/events");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_by_listener = Arc::clone(&seen);

    bus.on(
        &context.scope(),
        key.clone(),
        EventOptions::default(),
        move |value| {
            seen_by_listener
                .lock()
                .expect("context event log is available")
                .push(*value);
            Ok(Value::Null)
        },
    )
    .expect("context bus listener registration succeeds");
    context
        .events()
        .emit(&key, &9)
        .expect("context bus dispatch succeeds");
    assert_eq!(
        &*seen.lock().expect("context event log is available"),
        &[9],
        "independent context bus views share the context-owned event bus"
    );
}

#[test]
fn scope_context_views_filter_local_listeners_unless_they_are_global() {
    let bus = EventBus::new();
    let alpha_scope = Scope::root();
    let beta_scope = Scope::root();
    let alpha = bus.in_context(&alpha_scope);
    let beta = bus.in_context(&beta_scope);
    let key = EventKey::<()>::new("context/filtered");
    let calls = Arc::new(Mutex::new(Vec::new()));

    let alpha_calls = Arc::clone(&calls);
    alpha
        .on(
            &alpha_scope,
            key.clone(),
            EventOptions::default(),
            move |_| {
                alpha_calls
                    .lock()
                    .expect("context call log is available")
                    .push("alpha");
                Ok(Value::Null)
            },
        )
        .expect("alpha context listener registration succeeds");
    let beta_calls = Arc::clone(&calls);
    beta.on(
        &beta_scope,
        key.clone(),
        EventOptions::default(),
        move |_| {
            beta_calls
                .lock()
                .expect("context call log is available")
                .push("beta");
            Ok(Value::Null)
        },
    )
    .expect("beta context listener registration succeeds");
    let global_calls = Arc::clone(&calls);
    alpha
        .on(
            &alpha_scope,
            key.clone(),
            EventOptions {
                global: true,
                ..EventOptions::default()
            },
            move |_| {
                global_calls
                    .lock()
                    .expect("context call log is available")
                    .push("global");
                Ok(Value::Null)
            },
        )
        .expect("global context listener registration succeeds");

    beta.emit(&key, &())
        .expect("beta context dispatch succeeds");
    assert_eq!(
        &*calls.lock().expect("context call log is available"),
        &["beta", "global"],
        "context-local listeners stay scoped while global listeners cross contexts"
    );
}
