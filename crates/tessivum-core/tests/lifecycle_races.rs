use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use tessivum_core::{BoxDisposer, CoreError, Fiber, FiberState, Scope};
use tokio::sync::{oneshot, Barrier};

fn disposer<F, Fut>(f: F) -> BoxDisposer
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), CoreError>> + Send + 'static,
{
    Box::new(move || Box::pin(f()))
}

#[tokio::test]
async fn cancellation_is_first_wins_and_wakes_all_waiters() {
    let scope = Scope::root();
    let token = scope.cancellation();
    let (first_waiting, first_waiting_rx) = oneshot::channel();
    let (second_waiting, second_waiting_rx) = oneshot::channel();

    let first_token = token.clone();
    let first_waiter = tokio::spawn(async move {
        first_waiting
            .send(())
            .expect("test observes first waiter registration");
        first_token.cancelled().await;
    });
    let second_token = token.clone();
    let second_waiter = tokio::spawn(async move {
        second_waiting
            .send(())
            .expect("test observes second waiter registration");
        second_token.cancelled().await;
    });

    first_waiting_rx
        .await
        .expect("first waiter reached cancellation gate");
    second_waiting_rx
        .await
        .expect("second waiter reached cancellation gate");
    tokio::task::yield_now().await;
    assert!(!token.is_cancelled());

    let cancellation_race = Arc::new(Barrier::new(2));
    let first_canceller_token = token.clone();
    let first_canceller_barrier = Arc::clone(&cancellation_race);
    let first_canceller = tokio::spawn(async move {
        first_canceller_barrier.wait().await;
        first_canceller_token.cancel()
    });
    let second_canceller_token = token.clone();
    let second_canceller_barrier = Arc::clone(&cancellation_race);
    let second_canceller = tokio::spawn(async move {
        second_canceller_barrier.wait().await;
        second_canceller_token.cancel()
    });

    let first_won = first_canceller
        .await
        .expect("first canceller task did not panic");
    let second_won = second_canceller
        .await
        .expect("second canceller task did not panic");
    assert_ne!(
        first_won, second_won,
        "exactly one racing cancellation commits"
    );
    assert!(token.is_cancelled());
    first_waiter
        .await
        .expect("first waiter wakes after cancellation");
    second_waiter
        .await
        .expect("second waiter wakes after cancellation");

    assert!(
        !token.cancel(),
        "later cancellation attempts cannot replace the first one"
    );
    scope
        .dispose()
        .await
        .expect("disposing a cancelled scope succeeds");
}

#[tokio::test]
async fn concurrent_completion_and_disposal_never_resurrect_or_leak_a_fiber() {
    for run in 0..8 {
        let parent = Scope::root();
        let fiber = Fiber::new(&parent, "racing").expect("parent accepts fibers");
        let cleanup_calls = Arc::new(Mutex::new(0usize));
        let cleanup_calls_for_effect = Arc::clone(&cleanup_calls);
        fiber
            .scope()
            .add_effect(
                "owned",
                disposer(move || async move {
                    *cleanup_calls_for_effect
                        .lock()
                        .expect("cleanup counter lock is available") += 1;
                    Ok(())
                }),
            )
            .expect("fiber scope accepts its owned effect");

        let (setup_entered, setup_entered_rx) = oneshot::channel();
        let (complete_setup, complete_setup_rx) = oneshot::channel();
        let race = Arc::new(Barrier::new(3));
        let starting_fiber = fiber.clone();
        let start_race = Arc::clone(&race);
        let start = tokio::spawn(async move {
            starting_fiber
                .start(move |scope| async move {
                    setup_entered.send(()).expect("test observes setup entry");
                    let cancellation = scope.cancellation();
                    start_race.wait().await;
                    tokio::select! {
                        _ = cancellation.cancelled() => Ok(()),
                        _ = complete_setup_rx => Ok(()),
                    }
                })
                .await
        });

        setup_entered_rx
            .await
            .expect("setup reached its deterministic race point");
        assert_eq!(fiber.state(), FiberState::Loading);

        let disposing_fiber = fiber.clone();
        let dispose_race = Arc::clone(&race);
        let dispose = tokio::spawn(async move {
            dispose_race.wait().await;
            disposing_fiber.dispose().await
        });
        let completion_race = Arc::clone(&race);
        let complete = tokio::spawn(async move {
            completion_race.wait().await;
            complete_setup
                .send(())
                .expect("setup still awaits its completion signal");
        });

        complete
            .await
            .expect("completion task did not panic on run {run}");
        dispose
            .await
            .expect("dispose task did not panic on run {run}")
            .expect("dispose succeeds on run {run}");
        let _ = start.await.expect("start task did not panic on run {run}");

        assert_eq!(fiber.state(), FiberState::Disposed, "run {run}");
        assert_eq!(fiber.scope().state(), FiberState::Disposed, "run {run}");
        assert!(
            fiber.scope().cancellation().is_cancelled(),
            "disposal cancels the owned scope on run {run}"
        );
        assert_eq!(
            *cleanup_calls
                .lock()
                .expect("cleanup counter lock is available"),
            1,
            "owned cleanup runs once on run {run}"
        );
        assert!(
            fiber.scope().effects().is_empty(),
            "no resource escapes the completion/disposal race on run {run}"
        );
    }
}

#[tokio::test]
async fn aborting_first_dispose_keeps_cleanup_available_to_joiners() {
    let scope = Scope::root();
    let cleanup_calls = Arc::new(Mutex::new(0usize));
    let (cleanup_started, cleanup_started_rx) = oneshot::channel();
    let (release_cleanup, release_cleanup_rx) = oneshot::channel();

    let cleanup_calls_for_effect = Arc::clone(&cleanup_calls);
    scope
        .add_effect(
            "blocking",
            disposer(move || async move {
                *cleanup_calls_for_effect
                    .lock()
                    .expect("cleanup counter lock is available") += 1;
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
    assert_eq!(scope.state(), FiberState::Unloading);

    let (joiner_called, joiner_called_rx) = oneshot::channel();
    let joining_scope = scope.clone();
    let joiner = tokio::spawn(async move {
        joiner_called.send(()).expect("test observes joiner entry");
        joining_scope.dispose().await
    });
    joiner_called_rx.await.expect("joiner starts disposing");
    tokio::task::yield_now().await;
    assert!(
        !joiner.is_finished(),
        "joiner waits for the retained cleanup"
    );

    first_dispose.abort();
    assert!(
        first_dispose
            .await
            .expect_err("aborted first dispose returns a join error")
            .is_cancelled(),
        "aborting the first dispose drops its caller future"
    );
    tokio::task::yield_now().await;
    assert!(
        !joiner.is_finished(),
        "joiner takes over but still awaits the cleanup gate"
    );

    release_cleanup.send(()).expect("cleanup remains retained");
    for _ in 0..10 {
        if joiner.is_finished() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(joiner.is_finished(), "released cleanup wakes the joiner");
    joiner
        .await
        .expect("joiner task did not panic")
        .expect("joiner completes the retained cleanup");
    assert_eq!(scope.state(), FiberState::Disposed);
    assert_eq!(
        *cleanup_calls
            .lock()
            .expect("cleanup counter lock is available"),
        1,
        "retained cleanup runs once"
    );
    assert!(
        scope.effects().is_empty(),
        "completed cleanup releases effects"
    );
}

#[tokio::test]
async fn aborting_sole_dispose_caller_keeps_cleanup_running() {
    let scope = Scope::root();
    let cleanup_calls = Arc::new(Mutex::new(0usize));
    let (cleanup_started, cleanup_started_rx) = oneshot::channel();
    let (release_cleanup, release_cleanup_rx) = oneshot::channel();
    let (cleanup_finished, cleanup_finished_rx) = oneshot::channel();

    let cleanup_calls_for_effect = Arc::clone(&cleanup_calls);
    scope
        .add_effect(
            "blocking",
            disposer(move || async move {
                *cleanup_calls_for_effect
                    .lock()
                    .expect("cleanup counter lock is available") += 1;
                cleanup_started
                    .send(())
                    .expect("test observes cleanup entry");
                release_cleanup_rx.await.expect("test releases cleanup");
                cleanup_finished
                    .send(())
                    .expect("test observes cleanup completion");
                Ok(())
            }),
        )
        .expect("effect registration succeeds");

    let first_scope = scope.clone();
    let first_dispose = tokio::spawn(async move { first_scope.dispose().await });
    cleanup_started_rx
        .await
        .expect("sole dispose reached its cleanup gate");
    first_dispose.abort();
    assert!(
        first_dispose
            .await
            .expect_err("aborted sole dispose returns a join error")
            .is_cancelled(),
        "aborting the sole dispose drops only its caller future"
    );

    release_cleanup
        .send(())
        .expect("detached cleanup retains its receiver");
    tokio::time::timeout(Duration::from_secs(1), cleanup_finished_rx)
        .await
        .expect("detached cleanup completes after release")
        .expect("cleanup driver sends completion");
    assert_eq!(scope.state(), FiberState::Disposed);
    assert_eq!(
        *cleanup_calls
            .lock()
            .expect("cleanup counter lock is available"),
        1,
        "cleanup runs once after its sole caller is aborted"
    );
    assert!(
        scope.effects().is_empty(),
        "completed cleanup releases effects"
    );
    scope
        .dispose()
        .await
        .expect("later disposal returns the published cleanup result");
}
