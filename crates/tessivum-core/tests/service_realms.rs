use serde_json::json;
use tessivum_core::{ContextHandle, Dependency, RealmLabel, ServiceKey};

fn key(name: &str) -> ServiceKey {
    ServiceKey::new(name, "test/v1")
}

#[derive(Debug)]
struct Named(&'static str);

#[test]
fn separate_isolated_realms_hide_equal_service_keys() {
    let root = ContextHandle::root();
    let storage = key("storage");
    let alpha = root.isolate(storage.clone(), None);
    let beta = root.isolate(storage.clone(), None);

    alpha
        .provide(storage.clone(), Named("alpha"))
        .expect("isolated provider registration succeeds");

    assert_eq!(
        alpha
            .get::<Named>(&storage)
            .expect("same-realm lookup succeeds")
            .expect("same-realm provider is visible")
            .with(|service| service.0)
            .expect("same-realm handle is callable"),
        "alpha"
    );
    assert!(
        beta.get::<Named>(&storage)
            .expect("other-realm lookup succeeds")
            .is_none(),
        "matching keys in separate isolated realms are invisible"
    );
}

#[test]
fn matching_shared_labels_coactivate_required_consumers() {
    let root = ContextHandle::root();
    let bus = key("bus");
    let alpha = root.isolate(bus.clone(), Some(RealmLabel::new("shared")));
    let beta = root.isolate(bus.clone(), Some(RealmLabel::new("shared")));
    let alpha_subscription = alpha
        .subscribe(vec![Dependency::Required(bus.clone())], |_| {})
        .expect("alpha dependency registration succeeds");
    let beta_subscription = beta
        .subscribe(vec![Dependency::Required(bus.clone())], |_| {})
        .expect("beta dependency registration succeeds");

    assert!(!alpha_subscription.ready());
    assert!(!beta_subscription.ready());

    alpha
        .provide(bus.clone(), Named("shared bus"))
        .expect("shared-realm provider registration succeeds");

    assert!(alpha_subscription.ready());
    assert!(beta_subscription.ready());
    assert_eq!(
        beta.get::<Named>(&bus)
            .expect("shared lookup succeeds")
            .expect("shared provider is visible")
            .with(|service| service.0)
            .expect("shared handle is callable"),
        "shared bus"
    );
    assert_eq!(alpha.snapshot(&bus).realm, "shared");
    assert_eq!(beta.snapshot(&bus).realm, "shared");
}

#[test]
fn intercepts_merge_in_context_derivation_order() {
    let root = ContextHandle::root();
    let config = key("config");
    let parent_intercept = json!({
        "retry": {"attempts": 1},
        "source": "parent"
    });
    let child_intercept = json!({
        "retry": {"backoff": "fast"},
        "source": "child"
    });
    let parent = root.intercept(config.clone(), parent_intercept.clone());
    let child = parent.intercept(config.clone(), child_intercept.clone());

    assert_eq!(
        child.snapshot(&config).intercepts,
        vec![parent_intercept, child_intercept],
        "derived contexts retain their parent intercept before their own"
    );
    assert!(
        root.snapshot(&config).intercepts.is_empty(),
        "intercepts are local to the derived context chain"
    );
}
