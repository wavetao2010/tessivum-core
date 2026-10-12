use serde_json::Value;
use std::sync::{Arc, Barrier, Mutex};
use tessivum_core::{ContextHandle, CoreError, Dependency, ServiceKey};
use tessivum_core::{
    FiberState, NativeConfigSchema, NativePlugin, NativePluginDescriptor, NativePluginError,
    NativePluginFuture, NativePluginInstance,
};

#[tokio::test]
async fn rejected_registration_preserves_provider_and_explicit_replacement_transfers_ownership() {
    let root = ContextHandle::root();
    let owner = root.child().unwrap();
    let contender = root.child().unwrap();
    let key = ServiceKey::new("registration", "test/v1");
    let notifications = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&notifications);
    root.subscribe(vec![Dependency::Required(key.clone())], move |snapshot| {
        seen.lock().unwrap().push(snapshot.services[0].generation);
    })
    .unwrap();
    assert!(matches!(
        contender.replace(key.clone(), 2_u8),
        Err(CoreError::ServiceNotProvided { .. })
    ));
    let first = owner.provide(key.clone(), 1_u8).unwrap();
    for context in [&owner, &contender] {
        assert!(matches!(
            context.provide(key.clone(), 2_u8),
            Err(CoreError::ServiceAlreadyProvided { .. })
        ));
        assert_eq!(first.with(|value| *value), Ok(1));
    }
    assert_eq!(
        *notifications.lock().unwrap(),
        [None, Some(first.generation())]
    );
    let replacement = contender.replace(key.clone(), 2_u8).unwrap();
    assert!(matches!(
        first.with(|value| *value),
        Err(CoreError::StaleServiceHandle { .. })
    ));
    assert_eq!(
        *notifications.lock().unwrap(),
        [
            None,
            Some(first.generation()),
            Some(replacement.generation())
        ]
    );
    owner.scope().dispose().await.unwrap();
    assert_eq!(replacement.with(|value| *value), Ok(2));
    contender.scope().dispose().await.unwrap();
    assert!(root.get::<u8>(&key).unwrap().is_none());
    assert!(matches!(
        replacement.with(|value| *value),
        Err(CoreError::StaleServiceHandle { .. })
    ));
    root.scope().dispose().await.unwrap();
}

#[tokio::test]
async fn disposed_contender_cannot_destroy_a_live_provider() {
    let root = ContextHandle::root();
    let contender = root.child().unwrap();
    let key = ServiceKey::new("live", "test/v1");
    let live = root.provide(key.clone(), 1_u8).unwrap();
    contender.scope().dispose().await.unwrap();
    assert!(matches!(
        contender.provide(key.clone(), 2_u8),
        Err(CoreError::InvalidState { .. })
    ));
    assert!(matches!(
        contender.replace(key.clone(), 2_u8),
        Err(CoreError::InvalidState { .. })
    ));
    assert_eq!(live.with(|value| *value), Ok(1));
    assert_eq!(
        root.get::<u8>(&key).unwrap().unwrap().with(|value| *value),
        Ok(1)
    );
    root.scope().dispose().await.unwrap();
}

#[tokio::test]
async fn simultaneous_registrations_have_exactly_one_winner() {
    let root = ContextHandle::root();
    let key = ServiceKey::new("race", "test/v1");
    let gate = Arc::new(Barrier::new(2));
    let attempts = std::thread::scope(|threads| {
        let threads: Vec<_> = (0..2_u8)
            .map(|value| {
                let gate = Arc::clone(&gate);
                let root = &root;
                let key = &key;
                threads.spawn(move || {
                    gate.wait();
                    root.provide(key.clone(), value)
                })
            })
            .collect();
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(attempts.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        attempts
            .iter()
            .filter(|result| matches!(result, Err(CoreError::ServiceAlreadyProvided { .. })))
            .count(),
        1
    );
    let winner = attempts.into_iter().find_map(Result::ok).unwrap();
    assert_eq!(
        root.get::<u8>(&key).unwrap().unwrap().with(|value| *value),
        winner.with(|value| *value)
    );
    root.scope().dispose().await.unwrap();
}

struct ConflictingUpdate {
    host: ContextHandle,
    updating: bool,
}

impl NativePlugin for ConflictingUpdate {
    fn descriptor(&self) -> NativePluginDescriptor {
        NativePluginDescriptor {
            name: "conflicting-update".into(),
            version: "1".into(),
            dependencies: vec![],
            config_schema: NativeConfigSchema::Any,
        }
    }

    fn start<'a>(&'a mut self, context: ContextHandle, _: &'a Value) -> NativePluginFuture<'a> {
        Box::pin(async move {
            if !self.updating {
                context.provide(ServiceKey::new("current", "test/v1"), 1_u8)?;
            }
            Ok(())
        })
    }

    fn update<'a>(&'a mut self, context: ContextHandle, _: &'a Value) -> NativePluginFuture<'a> {
        Box::pin(async move {
            self.updating = true;
            let key = ServiceKey::new("current", "test/v1");
            context.replace(key.clone(), 2_u8)?;
            context.provide(ServiceKey::new("candidate-only", "test/v1"), 9_u8)?;
            // Deterministically intervene between staging and commit, without timing a race.
            self.host.replace(key, 3_u8)?;
            Ok(())
        })
    }

    fn stop<'a>(&'a mut self, _: ContextHandle) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn native_commit_conflict_disposes_candidate_and_retains_active_instance() {
    let host = ContextHandle::root();
    let factory_host = host.clone();
    let mut instance = NativePluginInstance::instantiate(
        Arc::new(move || {
            Box::new(ConflictingUpdate {
                host: factory_host.clone(),
                updating: false,
            })
        }),
        host.clone(),
        Value::Null,
    )
    .unwrap();
    instance.start().await.unwrap();
    let active_id = instance.fiber().id();
    assert!(matches!(
        instance.update(Value::Bool(true)).await,
        Err(NativePluginError::Core(CoreError::ServiceChanged { .. }))
    ));
    assert_eq!(instance.fiber().id(), active_id);
    assert_eq!(instance.fiber().state(), FiberState::Active);
    assert_eq!(instance.config(), &Value::Null);
    assert!(host
        .get::<u8>(&ServiceKey::new("candidate-only", "test/v1"))
        .unwrap()
        .is_none());
    assert_eq!(
        host.get::<u8>(&ServiceKey::new("current", "test/v1"))
            .unwrap()
            .unwrap()
            .with(|v| *v),
        Ok(3)
    );
    instance.dispose().await.unwrap();
    assert_eq!(
        host.get::<u8>(&ServiceKey::new("current", "test/v1"))
            .unwrap()
            .unwrap()
            .with(|v| *v),
        Ok(3)
    );
    host.scope().dispose().await.unwrap();
}
