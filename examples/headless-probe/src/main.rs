use tessivum_core::{ContextHandle, FiberState, ServiceKey};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let context = ContextHandle::root();
    let key = ServiceKey::new("probe.health", "probe/v1");
    let provider = context
        .provide(key.clone(), String::from("ready"))
        .expect("public ContextHandle registers the service");
    let status = context
        .get::<String>(&key)
        .expect("public ContextHandle looks up the service")
        .expect("registered service is visible")
        .with(Clone::clone)
        .expect("current service handle is readable");
    assert_eq!(status, "ready");
    assert!(provider.is_current());

    let scope = context.scope();
    scope
        .dispose()
        .await
        .expect("public Scope teardown succeeds");
    assert_eq!(scope.state(), FiberState::Disposed);
    assert!(!context.snapshot(&key).available);

    println!("headless probe complete: public service lifecycle torn down");
}
