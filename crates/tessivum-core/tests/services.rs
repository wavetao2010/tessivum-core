use std::sync::{Arc, Mutex};

use tessivum_core::{ContextHandle, CoreError, Dependency, ServiceKey};

fn key(name: &str) -> ServiceKey {
    ServiceKey::new(name, "test/v1")
}

#[derive(Debug)]
struct Named(&'static str);

#[test]
fn late_required_provider_gates_while_optional_dependency_does_not() {
    let context = ContextHandle::root();
    let clock = key("clock");
    let required_events = Arc::new(Mutex::new(Vec::new()));
    let required_events_for_listener = Arc::clone(&required_events);
    let required = context
        .subscribe(vec![Dependency::Required(clock.clone())], move |snapshot| {
            required_events_for_listener
                .lock()
                .expect("required event log is available")
                .push(snapshot.ready);
        })
        .expect("required dependency registration succeeds");
    let optional_events = Arc::new(Mutex::new(Vec::new()));
    let optional_events_for_listener = Arc::clone(&optional_events);
    let optional = context
        .subscribe(vec![Dependency::Optional(clock.clone())], move |snapshot| {
            optional_events_for_listener
                .lock()
                .expect("optional event log is available")
                .push(snapshot.ready);
        })
        .expect("optional dependency registration succeeds");

    assert!(
        !required.ready(),
        "a missing required service keeps its consumer pending"
    );
    assert!(
        optional.ready(),
        "a missing optional service never gates its consumer"
    );
    assert_eq!(
        &*required_events
            .lock()
            .expect("required event log is available"),
        &[false]
    );
    assert_eq!(
        &*optional_events
            .lock()
            .expect("optional event log is available"),
        &[true]
    );

    context
        .provide(clock.clone(), Named("wall"))
        .expect("late provider registration succeeds");

    assert!(
        required.ready(),
        "the late provider activates the required consumer"
    );
    assert!(optional.ready());
    assert_eq!(
        context
            .get::<Named>(&clock)
            .expect("service lookup succeeds")
            .expect("late provider becomes visible")
            .with(|service| service.0)
            .expect("current service handle remains callable"),
        "wall"
    );
    assert_eq!(
        &*required_events
            .lock()
            .expect("required event log is available"),
        &[false, true]
    );
    assert_eq!(
        &*optional_events
            .lock()
            .expect("optional event log is available"),
        &[true, true]
    );
}

#[test]
fn replacement_advances_generation_and_rejects_every_old_handle() {
    let context = ContextHandle::root();
    let theme = key("theme");
    let first_provider = context
        .provide(theme.clone(), Named("light"))
        .expect("initial provider registration succeeds");
    let old_handle = context
        .get::<Named>(&theme)
        .expect("initial lookup succeeds")
        .expect("initial provider is visible");
    let first_snapshot = context.snapshot(&theme);
    let first_generation = first_snapshot
        .generation
        .expect("visible provider has a generation");

    let replacement = context
        .provide(theme.clone(), Named("dark"))
        .expect("replacement registration succeeds");
    let replacement_snapshot = context.snapshot(&theme);
    let replacement_generation = replacement_snapshot
        .generation
        .expect("replacement has a generation");

    assert!(replacement_snapshot.available);
    assert_ne!(first_generation, replacement_generation);
    assert!(matches!(
        old_handle.with(|service| service.0),
        Err(CoreError::StaleServiceHandle { .. })
    ));
    assert!(matches!(
        first_provider.with(|service| service.0),
        Err(CoreError::StaleServiceHandle { .. })
    ));
    assert_eq!(
        replacement
            .with(|service| service.0)
            .expect("replacement handle is current"),
        "dark"
    );
    assert_eq!(
        context
            .get::<Named>(&theme)
            .expect("replacement lookup succeeds")
            .expect("replacement is visible")
            .with(|service| service.0)
            .expect("current lookup handle is callable"),
        "dark"
    );
}

#[test]
fn notifications_and_diagnostic_snapshots_follow_provider_commits() {
    let context = ContextHandle::root();
    let cache = key("cache");
    let notifications = Arc::new(Mutex::new(Vec::new()));
    let notifications_for_listener = Arc::clone(&notifications);
    let subscription = context
        .subscribe(vec![Dependency::Required(cache.clone())], move |snapshot| {
            let service = snapshot
                .services
                .first()
                .expect("dependency notification contains its service snapshot");
            notifications_for_listener
                .lock()
                .expect("notification log is available")
                .push((snapshot.ready, service.available, service.generation));
        })
        .expect("subscription succeeds");

    context
        .provide(cache.clone(), Named("first"))
        .expect("first cache provider succeeds");
    let first_generation = context
        .snapshot(&cache)
        .generation
        .expect("first provider has a generation");
    context
        .provide(cache.clone(), Named("second"))
        .expect("cache replacement succeeds");
    let latest = context.snapshot(&cache);
    let latest_generation = latest.generation.expect("replacement has a generation");

    assert!(subscription.ready());
    assert!(latest.available);
    assert_eq!(latest.key, cache.diagnostic_key());
    assert_eq!(context.snapshots().len(), 1);
    assert_ne!(first_generation, latest_generation);
    assert_eq!(
        &*notifications.lock().expect("notification log is available"),
        &[
            (false, false, None),
            (true, true, Some(first_generation)),
            (true, true, Some(latest_generation)),
        ]
    );
}
