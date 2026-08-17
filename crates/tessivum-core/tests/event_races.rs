use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use serde_json::{json, Value};
use tessivum_core::{
    CoreError, DispatchMode, DispatchOutcome, EventBus, EventKey, EventOptions, ListenerHandle,
    Scope,
};

#[tokio::test]
async fn parallel_waits_for_every_listener_and_aggregates_failures() {
    let bus = EventBus::new();
    let scope = Scope::root();
    let key = EventKey::<u32>::new("parallel/all-settle");
    let settled = Arc::new(Mutex::new(Vec::new()));

    let first_settled = Arc::clone(&settled);
    bus.on_async(&scope, key.clone(), EventOptions::default(), move |_| {
        let settled = Arc::clone(&first_settled);
        Box::pin(async move {
            settled
                .lock()
                .expect("settlement log is available")
                .push("first-error");
            Err(CoreError::SetupFailed("first failure".into()))
        })
    })
    .expect("first async listener registration succeeds");
    let success_settled = Arc::clone(&settled);
    bus.on_async(&scope, key.clone(), EventOptions::default(), move |_| {
        let settled = Arc::clone(&success_settled);
        Box::pin(async move {
            settled
                .lock()
                .expect("settlement log is available")
                .push("success");
            Ok(json!("completed"))
        })
    })
    .expect("successful async listener registration succeeds");
    let second_settled = Arc::clone(&settled);
    bus.on_async(&scope, key.clone(), EventOptions::default(), move |_| {
        let settled = Arc::clone(&second_settled);
        Box::pin(async move {
            settled
                .lock()
                .expect("settlement log is available")
                .push("second-error");
            Err(CoreError::SetupFailed("second failure".into()))
        })
    })
    .expect("second async listener registration succeeds");

    let error = bus
        .parallel(&key, Arc::new(7))
        .await
        .expect_err("parallel reports its aggregate after every listener settles");
    assert_eq!(
        error
            .cleanup_errors()
            .expect("parallel failure is represented as an aggregate")
            .len(),
        2,
        "both listener failures are retained"
    );
    let mut observed = settled.lock().expect("settlement log is available").clone();
    observed.sort_unstable();
    assert_eq!(
        observed,
        ["first-error", "second-error", "success"],
        "a failure does not prevent any parallel listener from settling"
    );
}

#[tokio::test]
async fn serial_and_bail_continue_through_null_and_false_then_stop_on_a_value() {
    let bus = EventBus::new();
    let scope = Scope::root();
    let serial_key = EventKey::<()>::new("serial/value");
    let serial_calls = Arc::new(Mutex::new(Vec::new()));

    for (label, value) in [
        ("null", Value::Null),
        ("false", json!(false)),
        ("stop", json!("stop")),
    ] {
        let calls = Arc::clone(&serial_calls);
        bus.on_async(
            &scope,
            serial_key.clone(),
            EventOptions::default(),
            move |_| {
                let calls = Arc::clone(&calls);
                let value = value.clone();
                Box::pin(async move {
                    calls
                        .lock()
                        .expect("serial call log is available")
                        .push(label);
                    Ok(value)
                })
            },
        )
        .expect("serial listener registration succeeds");
    }
    let late_serial_calls = Arc::clone(&serial_calls);
    bus.on_async(
        &scope,
        serial_key.clone(),
        EventOptions::default(),
        move |_| {
            let calls = Arc::clone(&late_serial_calls);
            Box::pin(async move {
                calls
                    .lock()
                    .expect("serial call log is available")
                    .push("late");
                Ok(json!("late"))
            })
        },
    )
    .expect("late serial listener registration succeeds");

    assert_eq!(
        bus.serial(&serial_key, Arc::new(()))
            .await
            .expect("serial dispatch succeeds"),
        Some(json!("stop")),
        "serial returns its first effective listener value"
    );
    assert_eq!(
        &*serial_calls.lock().expect("serial call log is available"),
        &["null", "false", "stop"],
        "serial treats only null and false as pass-through values"
    );

    let bail_key = EventKey::<()>::new("bail/value");
    let bail_calls = Arc::new(Mutex::new(Vec::new()));
    for (label, value) in [
        ("null", Value::Null),
        ("false", json!(false)),
        ("zero", json!(0)),
    ] {
        let calls = Arc::clone(&bail_calls);
        bus.on(
            &scope,
            bail_key.clone(),
            EventOptions::default(),
            move |_| {
                calls
                    .lock()
                    .expect("bail call log is available")
                    .push(label);
                Ok(value.clone())
            },
        )
        .expect("bail listener registration succeeds");
    }
    let late_bail_calls = Arc::clone(&bail_calls);
    bus.on(
        &scope,
        bail_key.clone(),
        EventOptions::default(),
        move |_| {
            late_bail_calls
                .lock()
                .expect("bail call log is available")
                .push("late");
            Ok(json!("late"))
        },
    )
    .expect("late bail listener registration succeeds");

    assert_eq!(
        bus.bail(&bail_key, &()).expect("bail dispatch succeeds"),
        Some(json!(0)),
        "zero is an effective Cordis value rather than a pass-through false"
    );
    assert_eq!(
        &*bail_calls.lock().expect("bail call log is available"),
        &["null", "false", "zero"],
        "bail stops before later listeners once it receives an effective value"
    );
}

#[test]
fn once_and_explicit_removal_are_idempotent_and_dispatch_uses_a_snapshot() {
    let bus = EventBus::new();
    let scope = Scope::root();
    let once_key = EventKey::<()>::new("once");
    let once_calls = Arc::new(Mutex::new(0usize));
    let once_calls_by_listener = Arc::clone(&once_calls);
    let once = bus
        .once(
            &scope,
            once_key.clone(),
            EventOptions::default(),
            move |_| {
                *once_calls_by_listener
                    .lock()
                    .expect("once counter is available") += 1;
                Ok(Value::Null)
            },
        )
        .expect("once listener registration succeeds");
    bus.emit(&once_key, &())
        .expect("first once dispatch succeeds");
    bus.emit(&once_key, &())
        .expect("second once dispatch succeeds");
    assert_eq!(
        *once_calls.lock().expect("once counter is available"),
        1,
        "a once listener is only dispatchable once"
    );
    assert!(
        !once.remove(),
        "removing a listener consumed by once is idempotently false"
    );

    let snapshot_key = EventKey::<()>::new("snapshot/removal");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let target = Arc::new(Mutex::new(None::<ListenerHandle>));
    let target_for_remover = Arc::clone(&target);
    let remover_calls = Arc::clone(&calls);
    bus.on(
        &scope,
        snapshot_key.clone(),
        EventOptions::default(),
        move |_| {
            if target_for_remover
                .lock()
                .expect("target listener slot is available")
                .as_ref()
                .expect("target listener is registered before dispatch")
                .remove()
            {
                remover_calls
                    .lock()
                    .expect("snapshot call log is available")
                    .push("removed");
            }
            remover_calls
                .lock()
                .expect("snapshot call log is available")
                .push("remover");
            Ok(Value::Null)
        },
    )
    .expect("removing listener registration succeeds");
    let target_calls = Arc::clone(&calls);
    let target_handle = bus
        .on(
            &scope,
            snapshot_key.clone(),
            EventOptions::default(),
            move |_| {
                target_calls
                    .lock()
                    .expect("snapshot call log is available")
                    .push("target");
                Ok(Value::Null)
            },
        )
        .expect("target listener registration succeeds");
    *target.lock().expect("target listener slot is available") = Some(target_handle);

    bus.emit(&snapshot_key, &())
        .expect("snapshot dispatch succeeds");
    assert_eq!(
        &*calls.lock().expect("snapshot call log is available"),
        &["removed", "remover", "target"],
        "a listener removed during dispatch remains in that dispatch snapshot"
    );
    calls
        .lock()
        .expect("snapshot call log is available")
        .clear();
    bus.emit(&snapshot_key, &())
        .expect("next dispatch succeeds");
    assert_eq!(
        &*calls.lock().expect("snapshot call log is available"),
        &["remover"],
        "the removed listener is excluded from later dispatches"
    );
}

#[tokio::test]
async fn owner_disposal_during_dispatch_keeps_the_current_snapshot_only() {
    let bus = EventBus::new();
    let owner = Scope::root();
    let key = EventKey::<()>::new("owner/disposal");
    let calls = Arc::new(Mutex::new(Vec::new()));

    let disposing_owner = owner.clone();
    let disposer_calls = Arc::clone(&calls);
    bus.on_async(&owner, key.clone(), EventOptions::default(), move |_| {
        let owner = disposing_owner.clone();
        let calls = Arc::clone(&disposer_calls);
        Box::pin(async move {
            owner.dispose().await.expect("owner disposal succeeds");
            calls
                .lock()
                .expect("owner call log is available")
                .push("dispose");
            Ok(Value::Null)
        })
    })
    .expect("disposing listener registration succeeds");
    let downstream_calls = Arc::clone(&calls);
    bus.on_async(&owner, key.clone(), EventOptions::default(), move |_| {
        let calls = Arc::clone(&downstream_calls);
        Box::pin(async move {
            calls
                .lock()
                .expect("owner call log is available")
                .push("downstream");
            Ok(Value::Null)
        })
    })
    .expect("downstream listener registration succeeds");

    bus.serial(&key, Arc::new(()))
        .await
        .expect("dispatch with owner disposal succeeds");
    assert_eq!(
        &*calls.lock().expect("owner call log is available"),
        &["dispose", "downstream"],
        "disposing an owner does not mutate the already captured dispatch snapshot"
    );
    calls.lock().expect("owner call log is available").clear();
    bus.serial(&key, Arc::new(()))
        .await
        .expect("post-disposal dispatch succeeds");
    assert!(
        calls
            .lock()
            .expect("owner call log is available")
            .is_empty(),
        "owner disposal removes all of its listeners from later dispatches"
    );
}

#[test]
fn slow_dispatch_diagnostics_report_duration_mode_and_outcome_without_payload() {
    let bus = EventBus::new();
    let scope = Scope::root();
    let key = EventKey::<()>::new("diagnostic/slow");
    bus.on(&scope, key.clone(), EventOptions::default(), |_| {
        std::thread::sleep(Duration::from_millis(5));
        Ok(json!({"must_not_escape_into_diagnostics": true}))
    })
    .expect("slow listener registration succeeds");

    bus.bail(&key, &())
        .expect("slow listener dispatch succeeds");
    let diagnostic = bus
        .take_diagnostics()
        .pop()
        .expect("dispatch records a diagnostic");
    assert_eq!(diagnostic.event, "diagnostic/slow");
    assert_eq!(diagnostic.mode, DispatchMode::Bail);
    assert_eq!(diagnostic.listener_count, 1);
    assert!(
        diagnostic.duration >= Duration::from_millis(5),
        "diagnostic duration includes the slow listener runtime"
    );
    assert_eq!(
        diagnostic.outcome,
        DispatchOutcome::Bailed,
        "diagnostics retain the dispatch outcome instead of an arbitrary listener payload"
    );
}
