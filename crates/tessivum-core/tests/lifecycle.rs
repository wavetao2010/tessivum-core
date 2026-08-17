use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use tessivum_core::{BoxDisposer, CoreError, Fiber, FiberState, Scope};
use tokio::sync::oneshot;

fn disposer<F, Fut>(f: F) -> BoxDisposer
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), CoreError>> + Send + 'static,
{
    Box::new(move || Box::pin(f()))
}

fn setup_error(message: &str) -> CoreError {
    CoreError::SetupFailed(message.to_owned())
}

#[tokio::test]
async fn scope_ids_generations_and_effect_diagnostics_are_stable() {
    let root = Scope::root();
    let root_clone = root.clone();
    let other_root = Scope::root();
    let child = root.child().expect("root accepts child scopes");

    assert_eq!(root.id(), root_clone.id());
    assert_eq!(root.generation(), root_clone.generation());
    assert_ne!(root.id(), other_root.id());
    assert_ne!(root.generation(), other_root.generation());
    assert_ne!(root.id(), child.id());
    assert_ne!(root.generation(), child.generation());

    let first = root
        .add_effect("first", disposer(|| async { Ok(()) }))
        .expect("root accepts effects");
    let second = root
        .add_effect("second", disposer(|| async { Ok(()) }))
        .expect("root accepts effects");
    let child_effect = child
        .add_effect("child", disposer(|| async { Ok(()) }))
        .expect("child accepts effects");

    assert_ne!(first, second);
    assert_ne!(first, child_effect);
    assert_ne!(second, child_effect);

    let root_effects = root.effects();
    assert_eq!(root_effects.len(), 2);
    assert_eq!(root_effects[0].id, first);
    assert_eq!(root_effects[0].label, "first");
    assert!(root_effects[0].children.is_empty());
    assert_eq!(root_effects[1].label, "second");
    assert_eq!(root_effects[1].id, second);
    assert!(root_effects[1].children.is_empty());

    let child_effects = child.effects();
    assert_eq!(child_effects.len(), 1);
    assert_eq!(child_effects[0].label, "child");
    assert_eq!(child_effects[0].id, child_effect);
    assert!(child_effects[0].children.is_empty());

    root.dispose().await.expect("disposing root succeeds");
    assert_eq!(root.state(), FiberState::Disposed);
    assert_eq!(child.state(), FiberState::Disposed);
    assert!(root.effects().is_empty());
    assert!(child.effects().is_empty());
}

#[tokio::test]
async fn fiber_start_commits_pending_loading_active_then_disposed() {
    let parent = Scope::root();
    let fiber = Fiber::new(&parent, "normal").expect("parent accepts fibers");
    assert_eq!(fiber.state(), FiberState::Pending);
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let starting_fiber = fiber.clone();

    let start = tokio::spawn(async move {
        starting_fiber
            .start(move |_| async move {
                entered.send(()).expect("test observes setup entry");
                release_rx.await.expect("test releases setup");
                Ok(())
            })
            .await
    });

    entered_rx.await.expect("setup reached its gate");
    assert_eq!(fiber.name(), "normal");
    assert_eq!(fiber.state(), FiberState::Loading);
    assert_ne!(fiber.scope().id(), parent.id());

    release.send(()).expect("setup is still waiting");
    start
        .await
        .expect("start task did not panic")
        .expect("setup succeeds");
    assert_eq!(fiber.state(), FiberState::Active);

    fiber.dispose().await.expect("fiber disposal succeeds");
    assert_eq!(fiber.state(), FiberState::Disposed);
    assert_eq!(fiber.scope().state(), FiberState::Disposed);
}

#[tokio::test]
async fn cleanup_is_async_and_runs_local_effects_in_reverse_registration_order() {
    let scope = Scope::root();
    let order = Arc::new(Mutex::new(Vec::new()));
    let (second_started, second_started_rx) = oneshot::channel();
    let (release_second, release_second_rx) = oneshot::channel();

    let first_order = Arc::clone(&order);
    scope
        .add_effect(
            "first",
            disposer(move || async move {
                first_order
                    .lock()
                    .expect("order lock is available")
                    .push("first");
                Ok(())
            }),
        )
        .expect("effect registration succeeds");

    let second_order = Arc::clone(&order);
    scope
        .add_effect(
            "second",
            disposer(move || async move {
                second_order
                    .lock()
                    .expect("order lock is available")
                    .push("second-start");
                second_started
                    .send(())
                    .expect("test observes async cleanup entry");
                release_second_rx
                    .await
                    .expect("test releases async cleanup");
                second_order
                    .lock()
                    .expect("order lock is available")
                    .push("second-end");
                Ok(())
            }),
        )
        .expect("effect registration succeeds");

    let disposing_scope = scope.clone();
    let dispose = tokio::spawn(async move { disposing_scope.dispose().await });
    second_started_rx
        .await
        .expect("reverse-order cleanup reaches second effect first");
    assert!(!dispose.is_finished(), "dispose must await async cleanup");
    assert_eq!(
        &*order.lock().expect("order lock is available"),
        &["second-start"]
    );

    release_second
        .send(())
        .expect("async cleanup is still waiting");
    dispose
        .await
        .expect("dispose task did not panic")
        .expect("cleanup succeeds");
    assert_eq!(
        &*order.lock().expect("order lock is available"),
        &["second-start", "second-end", "first"]
    );
}

#[tokio::test]
async fn repeated_and_reentrant_disposal_share_one_cleanup() {
    let scope = Scope::root();
    let calls = Arc::new(Mutex::new(0usize));
    let (cleanup_started, cleanup_started_rx) = oneshot::channel();
    let (release_cleanup, release_cleanup_rx) = oneshot::channel();

    let calls_for_blocking_effect = Arc::clone(&calls);
    scope
        .add_effect(
            "blocking",
            disposer(move || async move {
                *calls_for_blocking_effect
                    .lock()
                    .expect("call counter lock is available") += 1;
                cleanup_started
                    .send(())
                    .expect("test observes cleanup entry");
                release_cleanup_rx.await.expect("test releases cleanup");
                Ok(())
            }),
        )
        .expect("effect registration succeeds");

    let first_scope = scope.clone();
    let first_dispose = tokio::spawn(async move { first_scope.dispose().await });
    cleanup_started_rx
        .await
        .expect("first dispose reached its cleanup gate");

    let (second_called, second_called_rx) = oneshot::channel();
    let second_scope = scope.clone();
    let second_dispose = tokio::spawn(async move {
        second_called
            .send(())
            .expect("test observes the second dispose call");
        second_scope.dispose().await
    });
    second_called_rx.await.expect("second dispose was invoked");
    tokio::task::yield_now().await;
    assert!(!first_dispose.is_finished());
    assert!(!second_dispose.is_finished());

    release_cleanup.send(()).expect("cleanup is still waiting");
    first_dispose
        .await
        .expect("first dispose task did not panic")
        .expect("first dispose succeeds");
    second_dispose
        .await
        .expect("second dispose task did not panic")
        .expect("second dispose joins the first one");
    assert_eq!(*calls.lock().expect("call counter lock is available"), 1);

    let reentrant = Scope::root();
    let reentrant_clone = reentrant.clone();
    let reentrant_calls = Arc::new(Mutex::new(0usize));
    let reentrant_calls_for_effect = Arc::clone(&reentrant_calls);
    reentrant
        .add_effect(
            "reentrant",
            disposer(move || async move {
                *reentrant_calls_for_effect
                    .lock()
                    .expect("call counter lock is available") += 1;
                reentrant_clone
                    .dispose()
                    .await
                    .expect("reentrant dispose joins without deadlocking");
                Ok(())
            }),
        )
        .expect("effect registration succeeds");

    reentrant
        .dispose()
        .await
        .expect("reentrant cleanup completes");
    assert_eq!(
        *reentrant_calls
            .lock()
            .expect("call counter lock is available"),
        1
    );
    assert_eq!(reentrant.state(), FiberState::Disposed);
}

#[tokio::test]
async fn failed_start_rolls_back_every_effect_and_leaves_fiber_failed() {
    let parent = Scope::root();
    let fiber = Fiber::new(&parent, "fails").expect("parent accepts fibers");
    let order = Arc::new(Mutex::new(Vec::new()));
    let first_order = Arc::clone(&order);
    let second_order = Arc::clone(&order);

    let result = fiber
        .start(move |scope| async move {
            scope
                .add_effect(
                    "first",
                    disposer(move || async move {
                        first_order
                            .lock()
                            .expect("order lock is available")
                            .push("first");
                        Ok(())
                    }),
                )
                .expect("setup can register its first effect");
            scope
                .add_effect(
                    "second",
                    disposer(move || async move {
                        second_order
                            .lock()
                            .expect("order lock is available")
                            .push("second");
                        Ok(())
                    }),
                )
                .expect("setup can register its second effect");
            Err(setup_error("setup failed after registering effects"))
        })
        .await;

    assert!(matches!(result, Err(CoreError::SetupFailed(_))));
    assert_eq!(fiber.state(), FiberState::Failed);
    assert_eq!(
        &*order.lock().expect("order lock is available"),
        &["second", "first"]
    );
    assert!(fiber.scope().effects().is_empty());
    assert!(
        fiber.dispose().await.is_ok(),
        "disposing an already failed fiber remains idempotent"
    );
}

#[tokio::test]
async fn setup_can_dispose_its_own_scope_or_its_parent_without_reactivation() {
    let parent = Scope::root();
    let self_disposing = Fiber::new(&parent, "self-disposing").expect("parent accepts fibers");

    let _ = self_disposing
        .start(|scope| async move { scope.dispose().await })
        .await;
    assert_eq!(self_disposing.scope().state(), FiberState::Disposed);
    assert_eq!(self_disposing.state(), FiberState::Disposed);

    let parent = Scope::root();
    let parent_for_setup = parent.clone();
    let parent_disposing = Fiber::new(&parent, "parent-disposing").expect("parent accepts fibers");

    let _ = parent_disposing
        .start(move |_| async move { parent_for_setup.dispose().await })
        .await;
    assert_eq!(parent.state(), FiberState::Disposed);
    assert_eq!(parent_disposing.scope().state(), FiberState::Disposed);
    assert_eq!(parent_disposing.state(), FiberState::Disposed);
}

#[tokio::test]
async fn cleanup_aggregates_sync_and_async_failures_after_running_every_effect() {
    let scope = Scope::root();
    let order = Arc::new(Mutex::new(Vec::new()));
    let (async_failure_started, async_failure_started_rx) = oneshot::channel();
    let (release_async_failure, release_async_failure_rx) = oneshot::channel();

    let first_order = Arc::clone(&order);
    scope
        .add_effect(
            "first",
            disposer(move || async move {
                first_order
                    .lock()
                    .expect("order lock is available")
                    .push("first");
                Err(setup_error("first"))
            }),
        )
        .expect("effect registration succeeds");
    let middle_order = Arc::clone(&order);
    scope
        .add_effect(
            "middle",
            disposer(move || async move {
                middle_order
                    .lock()
                    .expect("order lock is available")
                    .push("middle");
                Ok(())
            }),
        )
        .expect("effect registration succeeds");
    let last_order = Arc::clone(&order);
    scope
        .add_effect(
            "last",
            disposer(move || async move {
                last_order
                    .lock()
                    .expect("order lock is available")
                    .push("last");
                async_failure_started
                    .send(())
                    .expect("test observes asynchronous cleanup failure");
                release_async_failure_rx
                    .await
                    .expect("test releases asynchronous cleanup failure");
                Err(setup_error("last"))
            }),
        )
        .expect("effect registration succeeds");

    let disposing_scope = scope.clone();
    let dispose = tokio::spawn(async move { disposing_scope.dispose().await });
    async_failure_started_rx
        .await
        .expect("last cleanup reached its asynchronous failure gate");
    assert!(
        !dispose.is_finished(),
        "dispose awaits asynchronous cleanup failures"
    );

    release_async_failure
        .send(())
        .expect("asynchronous failure is still waiting");
    let error = dispose
        .await
        .expect("dispose task did not panic")
        .expect_err("two cleanups fail");
    match error {
        CoreError::Cleanup(errors) => assert_eq!(errors.len(), 2),
        other => panic!("expected aggregate cleanup error, got {other:?}"),
    }
    assert_eq!(
        &*order.lock().expect("order lock is available"),
        &["last", "middle", "first"]
    );
    assert_eq!(scope.state(), FiberState::Disposed);
    assert!(scope.effects().is_empty());
}

#[tokio::test]
async fn parent_disposal_quiesces_children_and_closes_registration() {
    let parent = Scope::root();
    let child = parent.child().expect("parent accepts child scopes");
    let (child_cleanup_started, child_cleanup_started_rx) = oneshot::channel();
    let (release_child_cleanup, release_child_cleanup_rx) = oneshot::channel();

    child
        .add_effect(
            "child-blocker",
            disposer(move || async move {
                child_cleanup_started
                    .send(())
                    .expect("test observes child cleanup entry");
                release_child_cleanup_rx
                    .await
                    .expect("test releases child cleanup");
                Ok(())
            }),
        )
        .expect("child accepts effects");

    let disposing_parent = parent.clone();
    let dispose = tokio::spawn(async move { disposing_parent.dispose().await });
    child_cleanup_started_rx
        .await
        .expect("parent disposal starts child cleanup");
    assert_eq!(parent.state(), FiberState::Unloading);
    assert_eq!(child.state(), FiberState::Unloading);
    assert!(!dispose.is_finished(), "parent must await child quiescence");

    assert!(
        parent
            .add_effect("late-parent-effect", disposer(|| async { Ok(()) }))
            .is_err(),
        "unloading scope rejects effects"
    );
    assert!(parent.child().is_err(), "unloading scope rejects children");
    assert!(
        Fiber::new(&parent, "late-fiber").is_err(),
        "unloading scope rejects fibers"
    );
    assert!(
        child
            .add_effect("late-child-effect", disposer(|| async { Ok(()) }))
            .is_err(),
        "unloading child rejects effects"
    );

    release_child_cleanup
        .send(())
        .expect("child cleanup is still waiting");
    dispose
        .await
        .expect("parent dispose task did not panic")
        .expect("parent dispose succeeds");
    assert_eq!(parent.state(), FiberState::Disposed);
    assert_eq!(child.state(), FiberState::Disposed);
    assert!(parent.effects().is_empty());
    assert!(child.effects().is_empty());
    assert!(
        parent
            .add_effect("disposed-effect", disposer(|| async { Ok(()) }))
            .is_err(),
        "disposed scope rejects effects"
    );
    assert!(parent.child().is_err(), "disposed scope rejects children");
    assert!(
        Fiber::new(&parent, "disposed-fiber").is_err(),
        "disposed scope rejects fibers"
    );
}
