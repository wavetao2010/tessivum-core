use std::{any::Any, collections::BTreeMap, sync::Arc};

use serde_json::{Map, Value};

use crate::{
    ids::next_generation,
    service::{
        DependencySnapshot, Realm, Registry, ResolvedDependency, ResolvedService, ServiceHandle,
    },
    BoxDisposer, CoreError, Dependency, DependencySubscription, EventBus, RealmLabel, Scope,
    ServiceKey, ServiceSnapshot,
};

/// A lightweight, immutable service view bound to one ownership scope.
#[derive(Clone)]
pub struct ContextHandle {
    scope: Scope,
    registry: Arc<Registry>,
    host_registry: Arc<Registry>,
    realms: Arc<BTreeMap<ServiceKey, Realm>>,
    events: EventBus,
    intercepts: Arc<BTreeMap<ServiceKey, Vec<Value>>>,
}

impl std::fmt::Debug for ContextHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ContextHandle")
            .field("scope", &self.scope.id())
            .field("isolated_services", &self.realms.len())
            .field("intercepted_services", &self.intercepts.len())
            .finish()
    }
}

impl Default for ContextHandle {
    fn default() -> Self {
        Self::root()
    }
}

impl ContextHandle {
    pub fn root() -> Self {
        Self::from_scope(Scope::root())
    }

    /// Starts a new context tree rooted at `scope`.
    pub fn new(scope: Scope) -> Self {
        Self::from_scope(scope)
    }

    pub fn from_scope(scope: Scope) -> Self {
        let events = EventBus::new().in_context(&scope);
        let registry = Registry::new();
        Self {
            scope,
            registry: Arc::clone(&registry),
            host_registry: registry,
            realms: Arc::new(BTreeMap::new()),
            intercepts: Arc::new(BTreeMap::new()),
            events,
        }
    }

    /// Returns the scope which owns providers and subscriptions made through this handle.
    pub fn scope(&self) -> Scope {
        self.scope.clone()
    }

    /// Returns this context's filtered view of its shared event bus.
    pub fn events(&self) -> EventBus {
        self.events.clone()
    }

    /// Rebinds this immutable view to an existing scope in the same context tree.
    pub fn with_scope(&self, scope: Scope) -> Self {
        let events = self.events.in_context(&scope);
        Self {
            scope,
            registry: Arc::clone(&self.registry),
            host_registry: Arc::clone(&self.host_registry),
            realms: Arc::clone(&self.realms),
            intercepts: Arc::clone(&self.intercepts),
            events,
        }
    }

    /// Creates a service staging view which reads host providers while keeping writes local.
    pub(crate) fn staged(&self) -> Self {
        Self {
            scope: self.scope.clone(),
            registry: Registry::staged(Arc::clone(&self.host_registry)),
            host_registry: Arc::clone(&self.host_registry),
            realms: Arc::clone(&self.realms),
            intercepts: Arc::clone(&self.intercepts),
            events: self.events.clone(),
        }
    }

    pub(crate) fn commit_staged(&self) -> Result<(), CoreError> {
        self.registry.commit()
    }

    /// Creates a child scope without changing service visibility.
    pub fn child(&self) -> Result<Self, CoreError> {
        Ok(self.with_scope(self.scope.child()?))
    }

    /// Returns an immutable child view that resolves `service` in another realm.
    /// Equal labels deliberately coactivate the same isolated realm.
    pub fn isolate(&self, service: ServiceKey, label: Option<RealmLabel>) -> Self {
        let mut realms = (*self.realms).clone();
        let realm = label
            .map(Realm::Shared)
            .unwrap_or_else(|| Realm::Private(next_generation()));
        realms.insert(service, realm);
        Self {
            scope: self.scope.clone(),
            registry: Arc::clone(&self.registry),
            host_registry: Arc::clone(&self.host_registry),
            realms: Arc::new(realms),
            intercepts: Arc::clone(&self.intercepts),
            events: self.events.clone(),
        }
    }

    /// Returns an immutable child view with one later intercept configuration.
    /// Intercept chains keep ancestor-to-descendant order.
    pub fn intercept(&self, service: ServiceKey, value: Value) -> Self {
        let mut intercepts = (*self.intercepts).clone();
        intercepts.entry(service).or_default().push(value);
        Self {
            scope: self.scope.clone(),
            registry: Arc::clone(&self.registry),
            host_registry: Arc::clone(&self.host_registry),
            realms: Arc::clone(&self.realms),
            intercepts: Arc::new(intercepts),
            events: self.events.clone(),
        }
    }

    pub fn intercept_chain(&self, service: &ServiceKey) -> Vec<Value> {
        self.intercepts.get(service).cloned().unwrap_or_default()
    }

    /// Shallowly merges object intercepts, with child contexts taking precedence.
    pub fn intercept_config(&self, service: &ServiceKey) -> Value {
        let mut merged = Map::new();
        for value in self.intercept_chain(service) {
            if let Value::Object(config) = value {
                merged.extend(config);
            }
        }
        Value::Object(merged)
    }

    /// Registers a scope-owned provider; a visible provider in this realm is an error.
    pub fn provide<T>(&self, service: ServiceKey, value: T) -> Result<ServiceHandle<T>, CoreError>
    where
        T: Send + Sync + 'static,
    {
        self.publish_service(service, value, false)
    }

    /// Replaces an existing provider and transfers cleanup ownership to this scope.
    /// Missing providers are errors. Successful replacement invalidates old handles.
    pub fn replace<T>(&self, service: ServiceKey, value: T) -> Result<ServiceHandle<T>, CoreError>
    where
        T: Send + Sync + 'static,
    {
        self.publish_service(service, value, true)
    }

    fn publish_service<T>(
        &self,
        service: ServiceKey,
        value: T,
        replace: bool,
    ) -> Result<ServiceHandle<T>, CoreError>
    where
        T: Send + Sync + 'static,
    {
        let resolved = self.resolve(&service);
        let value = Arc::new(value);
        let erased: Arc<dyn Any + Send + Sync> = value.clone();
        let (_, (generation, retired)) =
            self.scope
                .add_effect_with(format!("service:{}", service.diagnostic_key()), || {
                    let (generation, retired) =
                        self.registry.provide(resolved.clone(), erased, replace)?;
                    let registry = Arc::clone(&self.registry);
                    let cleanup_service = resolved.clone();
                    let effect: BoxDisposer = Box::new(move || {
                        Box::pin(async move {
                            registry.remove_if_current(&cleanup_service, generation);
                            Ok(())
                        })
                    });
                    Ok(((generation, retired), effect))
                })?;
        drop(retired);
        self.registry.notify(&resolved);
        Ok(ServiceHandle {
            registry: Arc::downgrade(&self.registry),
            service: resolved,
            generation,
            value,
        })
    }

    /// Looks up a typed service in this context's selected realm.
    pub fn get<T>(&self, service: &ServiceKey) -> Result<Option<ServiceHandle<T>>, CoreError>
    where
        T: Send + Sync + 'static,
    {
        let resolved = self.resolve(service);
        let Some(entry) = self.registry.entry(&resolved) else {
            return Ok(None);
        };
        let generation = entry.generation;
        let value =
            Arc::downcast::<T>(entry.value).map_err(|_| CoreError::ServiceTypeMismatch {
                key: service.diagnostic_key(),
            })?;
        Ok(Some(ServiceHandle {
            registry: Arc::downgrade(&self.registry),
            service: resolved,
            generation,
            value,
        }))
    }

    /// Returns one stable diagnostic record for `service`, including its intercept chain.
    pub fn snapshot(&self, service: &ServiceKey) -> ServiceSnapshot {
        self.registry
            .snapshot(&self.resolve(service), self.intercept_chain(service))
    }

    /// Returns all providers visible from this context, sorted by their stable keys.
    pub fn snapshots(&self) -> Vec<ServiceSnapshot> {
        self.registry
            .provider_services()
            .into_iter()
            .filter(|service| self.resolve(&service.key) == *service)
            .map(|service| {
                self.registry
                    .snapshot(&service, self.intercept_chain(&service.key))
            })
            .collect()
    }

    /// Subscribes to availability changes. Required dependencies determine `ready`;
    /// optional dependencies are reported but never gate it. The listener is removed
    /// automatically when this context's scope disposes.
    pub fn subscribe<F>(
        &self,
        dependencies: Vec<Dependency>,
        listener: F,
    ) -> Result<DependencySubscription, CoreError>
    where
        F: Fn(DependencySnapshot) + Send + Sync + 'static,
    {
        let dependencies = dependencies
            .into_iter()
            .map(|dependency| {
                let service = dependency.key().clone();
                ResolvedDependency {
                    service: self.resolve(&service),
                    required: dependency.is_required(),
                    intercepts: self.intercept_chain(&service),
                }
            })
            .collect::<Vec<_>>();
        let dependencies: Arc<[ResolvedDependency]> = dependencies.into();
        let listener: Arc<dyn Fn(DependencySnapshot) + Send + Sync> = Arc::new(listener);
        let registry = Arc::clone(&self.host_registry);
        let id = registry.register_listener(Arc::clone(&dependencies), Arc::clone(&listener));
        let subscription = DependencySubscription {
            registry: Arc::downgrade(&registry),
            id,
            dependencies,
        };

        let effect: BoxDisposer = Box::new(move || {
            Box::pin(async move {
                registry.unregister_listener(id);
                Ok(())
            })
        });
        if let Err(error) = self.scope.add_effect("service-subscription", effect) {
            subscription.unsubscribe();
            return Err(error);
        }

        listener(subscription.snapshot());
        Ok(subscription)
    }

    fn resolve(&self, service: &ServiceKey) -> ResolvedService {
        ResolvedService {
            key: service.clone(),
            realm: self.realms.get(service).cloned().unwrap_or(Realm::Root),
        }
    }
}

#[cfg(test)]
mod registration_tests {
    use super::*;

    #[tokio::test]
    async fn staged_conflict_preserves_host_and_never_commits_a_partial_candidate() {
        let host = ContextHandle::root();
        let key = ServiceKey::new("current", "test/v1");
        let extra = ServiceKey::new("candidate-only", "test/v1");
        let original = host.provide(key.clone(), 1_u8).unwrap();
        let candidate = host.child().unwrap().staged();
        assert!(matches!(
            candidate.provide(key.clone(), 2_u8),
            Err(CoreError::ServiceAlreadyProvided { .. })
        ));
        candidate.replace(key.clone(), 2_u8).unwrap();
        candidate.provide(extra.clone(), 9_u8).unwrap();
        assert_eq!(original.with(|value| *value), Ok(1));
        assert!(host.get::<u8>(&extra).unwrap().is_none());
        let concurrent = host.replace(key.clone(), 3_u8).unwrap();
        assert!(matches!(
            candidate.commit_staged(),
            Err(CoreError::ServiceChanged { .. })
        ));
        assert_eq!(concurrent.with(|value| *value), Ok(3));
        assert!(host.get::<u8>(&extra).unwrap().is_none());
        candidate.scope().dispose().await.unwrap();
        assert_eq!(concurrent.with(|value| *value), Ok(3));
        host.scope().dispose().await.unwrap();
    }

    #[tokio::test]
    async fn staged_commit_is_visible_before_notifications_and_keeps_registration_strict() {
        let host = ContextHandle::root();
        let key = ServiceKey::new("current", "test/v1");
        let extra = ServiceKey::new("candidate-only", "test/v1");
        let old_owner = host.child().unwrap();
        let old = old_owner.provide(key.clone(), 1_u8).unwrap();
        let candidate = host.child().unwrap().staged();
        let updated = candidate.replace(key.clone(), 2_u8).unwrap();
        candidate.provide(extra.clone(), 9_u8).unwrap();
        let observer = host.clone();
        let extra_key = extra.clone();
        let subscription = host
            .subscribe(vec![Dependency::Required(key.clone())], move |snapshot| {
                if snapshot.services[0].generation != Some(old.generation()) {
                    assert_eq!(
                        observer
                            .get::<u8>(&extra_key)
                            .unwrap()
                            .unwrap()
                            .with(|v| *v),
                        Ok(9)
                    );
                }
            })
            .unwrap();
        candidate.commit_staged().unwrap();
        old_owner.scope().dispose().await.unwrap();
        assert_eq!(updated.with(|value| *value), Ok(2));
        assert!(matches!(
            candidate.provide(key.clone(), 4_u8),
            Err(CoreError::ServiceAlreadyProvided { .. })
        ));
        let latest = candidate.replace(key.clone(), 4_u8).unwrap();
        assert_eq!(
            host.get::<u8>(&key).unwrap().unwrap().with(|value| *value),
            Ok(4)
        );
        assert!(matches!(
            updated.with(|value| *value),
            Err(CoreError::StaleServiceHandle { .. })
        ));
        // Stop the observation before cleanup removes the candidate-only service.
        subscription.unsubscribe();
        host.scope().dispose().await.unwrap();
        assert!(!latest.is_current());
    }
}
