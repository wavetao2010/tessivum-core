use std::{
    future::Future,
    sync::{Arc, Mutex},
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
