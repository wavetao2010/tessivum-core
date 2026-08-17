use std::sync::{Arc, Mutex};

use serde_json::json;
use tessivum_core::{CoreError, EventBus, EventKey, EventOptions, Scope};

#[tokio::test]
async fn waterfall_modifies_wraps_and_returns_the_downstream_value() {
    let bus = EventBus::new();
    let scope = Scope::root();
    let key = EventKey::<String>::new("waterfall/wrap");

    bus.on_waterfall(
        &scope,
        key.clone(),
        EventOptions::default(),
        |value, next| {
            Box::pin(async move {
                let downstream = next.next(format!("{value}:outer")).await?;
                Ok(json!({"wrapped": downstream}))
            })
        },
    )
    .expect("outer waterfall listener registration succeeds");
    bus.on_waterfall(
        &scope,
        key.clone(),
        EventOptions::default(),
        |value, next| {
            Box::pin(async move {
                let downstream = next.next(format!("{value}:inner")).await?;
                Ok(json!(["middle", downstream]))
            })
        },
    )
    .expect("middle waterfall listener registration succeeds");
    bus.on_waterfall(&scope, key.clone(), EventOptions::default(), |value, _| {
        Box::pin(async move { Ok(json!(value)) })
    })
    .expect("terminal waterfall listener registration succeeds");

    assert_eq!(
        bus.waterfall(&key, "start".to_owned())
            .await
            .expect("waterfall dispatch succeeds"),
        json!({"wrapped": ["middle", "start:outer:inner"]}),
        "each listener can transform input, wrap its downstream result, and propagate that value"
    );
}

#[tokio::test]
async fn waterfall_veto_short_circuits_and_continuations_are_one_shot() {
    let bus = EventBus::new();
    let scope = Scope::root();
    let veto_key = EventKey::<String>::new("waterfall/veto");
    let downstream_calls = Arc::new(Mutex::new(0usize));

    bus.on_waterfall(
        &scope,
        veto_key.clone(),
        EventOptions::default(),
        |value, _| Box::pin(async move { Ok(json!({"veto": value})) }),
    )
    .expect("veto listener registration succeeds");
    let downstream_calls_by_listener = Arc::clone(&downstream_calls);
    bus.on_waterfall(
        &scope,
        veto_key.clone(),
        EventOptions::default(),
        move |value, _| {
            let calls = Arc::clone(&downstream_calls_by_listener);
            Box::pin(async move {
                *calls.lock().expect("downstream counter is available") += 1;
                Ok(json!(value))
            })
        },
    )
    .expect("downstream listener registration succeeds");

    assert_eq!(
        bus.waterfall(&veto_key, "blocked".to_owned())
            .await
            .expect("vetoed waterfall dispatch succeeds"),
        json!({"veto": "blocked"}),
        "a listener that does not call next supplies the waterfall result"
    );
    assert_eq!(
        *downstream_calls
            .lock()
            .expect("downstream counter is available"),
        0,
        "a veto prevents all downstream listeners from running"
    );

    let once_key = EventKey::<String>::new("waterfall/once");
    bus.on_waterfall(
        &scope,
        once_key.clone(),
        EventOptions::default(),
        |value, next| {
            Box::pin(async move {
                let result = next.next(value.clone()).await?;
                assert_eq!(
                    next.next(value)
                        .await
                        .expect_err("a continuation cannot be used twice"),
                    CoreError::WaterfallContinuationUsed
                );
                Ok(result)
            })
        },
    )
    .expect("one-shot wrapper registration succeeds");
    bus.on_waterfall(
        &scope,
        once_key.clone(),
        EventOptions::default(),
        |value, _| Box::pin(async move { Ok(json!(value)) }),
    )
    .expect("one-shot terminal registration succeeds");

    assert_eq!(
        bus.waterfall(&once_key, "once".to_owned())
            .await
            .expect("waterfall with one-shot continuation succeeds"),
        json!("once"),
        "the first continuation result remains available after a rejected second call"
    );
}

#[tokio::test]
async fn dynamic_waterfall_preserves_json_while_transforming_and_wrapping() {
    let bus = EventBus::new();
    let scope = Scope::root();

    bus.on_dynamic_waterfall(
        &scope,
        "dynamic/waterfall",
        EventOptions::default(),
        |mut payload, next| {
            Box::pin(async move {
                payload["stage"] = json!("outer");
                let downstream = next.next(payload).await?;
                Ok(json!({"wrapped": downstream}))
            })
        },
    )
    .expect("dynamic wrapper registration succeeds");
    bus.on_dynamic_waterfall(
        &scope,
        "dynamic/waterfall",
        EventOptions::default(),
        |payload, _| Box::pin(async move { Ok(payload) }),
    )
    .expect("dynamic terminal registration succeeds");

    assert_eq!(
        bus.waterfall_dynamic("dynamic/waterfall", json!({"id": 3}))
            .await
            .expect("dynamic waterfall dispatch succeeds"),
        json!({"wrapped": {"id": 3, "stage": "outer"}}),
        "dynamic waterfall listeners receive JSON, can transform it, and can wrap the result"
    );
}
