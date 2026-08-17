use std::sync::{Arc, Mutex};

use tessivum_core::{ContextHandle, CoreError, Dependency, Scope, ServiceKey};

fn key(name: &str) -> ServiceKey {
    ServiceKey::new(name, "test/v1")
}

#[derive(Debug)]
struct Named(&'static str);

#[test]
fn required_consumer_reloads_when_a_provider_is_replaced() {
    let context = ContextHandle::root();
    let session = key("session");
    let observed_generations = Arc::new(Mutex::new(Vec::new()));
    let observed_generations_for_listener = Arc::clone(&observed_generations);
    let subscription = context
        .subscribe(
            vec![Dependency::Required(session.clone())],
            move |snapshot| {
                observed_generations_for_listener
                    .lock()
                    .expect("consumer event log is available")
                    .push(
                        snapshot
                            .services
                            .first()
                            .expect("dependency notification contains the session snapshot")
                            .generation,
                    );
            },
        )
        .expect("consumer dependency registration succeeds");

    context
        .provide(session.clone(), Named("first"))
        .expect("first provider registration succeeds");
    let first_generation = context
        .snapshot(&session)
        .generation
        .expect("first provider has a generation");
    context
        .provide(session.clone(), Named("replacement"))
        .expect("replacement provider registration succeeds");
    let replacement_generation = context
        .snapshot(&session)
        .generation
        .expect("replacement provider has a generation");

    assert!(subscription.ready());
    assert_ne!(first_generation, replacement_generation);
    assert_eq!(
        &*observed_generations
            .lock()
            .expect("consumer event log is available"),
        &[None, Some(first_generation), Some(replacement_generation)],
        "the managed consumer sees its initial pending state then both activations"
    );
    assert_eq!(
        context
            .get::<Named>(&session)
            .expect("replacement lookup succeeds")
            .expect("replacement is visible")
            .with(|service| service.0)
            .expect("replacement handle is callable"),
        "replacement"
    );
}

#[tokio::test]
async fn provider_owner_disposal_removes_service_and_invalidates_handles() {
    let owner = Scope::root();
    let context = ContextHandle::from_scope(owner.clone());
    let cache = key("cache");
    let provider_handle = context
        .provide(cache.clone(), Named("owned"))
        .expect("owned provider registration succeeds");
    let consumer_handle = context
        .get::<Named>(&cache)
        .expect("owned service lookup succeeds")
        .expect("owned service is visible");

    owner
        .dispose()
        .await
        .expect("provider owner disposal succeeds");

    assert!(!context.snapshot(&cache).available);
    assert!(context.snapshots().is_empty());
    assert!(
        context
            .get::<Named>(&cache)
            .expect("post-disposal lookup succeeds")
            .is_none(),
        "disposing the owning scope removes its provider"
    );
    assert!(matches!(
        provider_handle.with(|service| service.0),
        Err(CoreError::StaleServiceHandle { .. })
    ));
    assert!(matches!(
        consumer_handle.with(|service| service.0),
        Err(CoreError::StaleServiceHandle { .. })
    ));
}

#[test]
fn unsubscribed_consumers_stop_receiving_provider_commits() {
    let context = ContextHandle::root();
    let metrics = key("metrics");
    let notifications = Arc::new(Mutex::new(0usize));
    let notifications_for_listener = Arc::clone(&notifications);
    let subscription = context
        .subscribe(vec![Dependency::Optional(metrics.clone())], move |_| {
            *notifications_for_listener
                .lock()
                .expect("notification counter is available") += 1;
        })
        .expect("optional subscription succeeds");

    assert!(
        subscription.unsubscribe(),
        "first unsubscription removes the listener"
    );
    assert!(
        !subscription.unsubscribe(),
        "unsubscription is idempotent after the listener has been removed"
    );
    context
        .provide(metrics, Named("enabled"))
        .expect("provider registration succeeds");

    assert_eq!(
        *notifications
            .lock()
            .expect("notification counter is available"),
        1,
        "only synchronous registration notification is observed after unsubscription"
    );
}
