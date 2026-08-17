use std::{
    any::Any,
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, Weak},
};

use serde_json::Value;

use crate::{
    ids::{next_generation, next_resource_id},
    CoreError, Generation, RealmLabel, ResourceId, ScopeId, ServiceKey,
};

/// A dependency which may gate a managed service consumer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Dependency {
    Required(ServiceKey),
    Optional(ServiceKey),
}

impl Dependency {
    pub fn key(&self) -> &ServiceKey {
        match self {
            Self::Required(key) | Self::Optional(key) => key,
        }
    }

    pub fn is_required(&self) -> bool {
        matches!(self, Self::Required(_))
    }
}

/// A stable diagnostic description of one service in a context.
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceSnapshot {
    pub key: String,
    pub realm: String,
    pub generation: Option<Generation>,
    pub available: bool,
    pub intercepts: Vec<Value>,
}

/// The current activation input of a dependency subscription.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencySnapshot {
    pub ready: bool,
    pub services: Vec<ServiceSnapshot>,
}

/// A generation-checked handle to a native service.
#[derive(Clone)]
pub struct ServiceHandle<T> {
    pub(crate) registry: Weak<Registry>,
    pub(crate) service: ResolvedService,
    pub(crate) generation: Generation,
    pub(crate) value: Arc<T>,
}

impl<T> std::fmt::Debug for ServiceHandle<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServiceHandle")
            .field("key", &self.service.key.diagnostic_key())
            .field("realm", &self.service.realm.diagnostic_key())
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl<T> ServiceHandle<T> {
    pub fn key(&self) -> &ServiceKey {
        &self.service.key
    }

    pub fn generation(&self) -> Generation {
        self.generation
    }

    pub fn is_current(&self) -> bool {
        self.registry
            .upgrade()
            .is_some_and(|registry| registry.is_current(&self.service, self.generation))
    }

    /// Executes an operation only while this handle still names the current provider.
    pub fn with<R>(&self, operation: impl FnOnce(&T) -> R) -> Result<R, CoreError> {
        if self.is_current() {
            Ok(operation(&self.value))
        } else {
            Err(CoreError::StaleServiceHandle {
                key: self.service.key.diagnostic_key(),
                generation: self.generation,
            })
        }
    }
}

/// A scope-owned dependency subscription.
#[derive(Clone, Debug)]
pub struct DependencySubscription {
    pub(crate) registry: Weak<Registry>,
    pub(crate) id: ResourceId,
    pub(crate) dependencies: Arc<[ResolvedDependency]>,
}

impl DependencySubscription {
    pub fn ready(&self) -> bool {
        self.snapshot().ready
    }

    pub fn snapshot(&self) -> DependencySnapshot {
        self.registry
            .upgrade()
            .map(|registry| registry.dependency_snapshot(&self.dependencies))
            .unwrap_or_else(|| unavailable_snapshot(&self.dependencies))
    }

    /// Detaches this listener before its owning scope is disposed.
    pub fn unsubscribe(&self) -> bool {
        self.registry
            .upgrade()
            .is_some_and(|registry| registry.unregister_listener(self.id))
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ResolvedService {
    pub(crate) key: ServiceKey,
    pub(crate) realm: Realm,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum Realm {
    Root,
    Shared(RealmLabel),
    Private(Generation),
}

impl Realm {
    pub(crate) fn diagnostic_key(&self) -> String {
        match self {
            Self::Root => "root".into(),
            Self::Shared(label) => label.to_string(),
            Self::Private(generation) => format!("private-{}", generation.get()),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedDependency {
    pub(crate) service: ResolvedService,
    pub(crate) required: bool,
    pub(crate) intercepts: Vec<Value>,
}

#[derive(Clone)]
pub(crate) struct ServiceEntry {
    pub(crate) generation: Generation,
    pub(crate) value: Arc<dyn Any + Send + Sync>,
}

struct Listener {
    dependencies: Arc<[ResolvedDependency]>,
    callback: Arc<dyn Fn(DependencySnapshot) + Send + Sync>,
}

#[derive(Default)]
struct RegistryState {
    providers: BTreeMap<ResolvedService, ServiceEntry>,
    listeners: BTreeMap<ResourceId, Listener>,
    committed: bool,
}

/// Shared, lock-protected native service registry for a context tree.
pub(crate) struct Registry {
    state: Mutex<RegistryState>,
    parent: Option<Arc<Self>>,
}

impl Registry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(RegistryState::default()),
            parent: None,
        })
    }

    pub(crate) fn staged(parent: Arc<Self>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(RegistryState::default()),
            parent: Some(parent),
        })
    }

    pub(crate) fn commit(&self) {
        let Some(parent) = &self.parent else {
            return;
        };
        let services = {
            let mut state = lock(&self.state);
            if state.committed {
                return;
            }
            for (service, entry) in &state.providers {
                parent.insert(service.clone(), entry.clone());
            }
            state.committed = true;
            state.providers.keys().cloned().collect::<Vec<_>>()
        };
        for service in services {
            parent.notify(&service);
        }
    }

    pub(crate) fn provide(
        &self,
        service: ResolvedService,
        _owner: ScopeId,
        value: Arc<dyn Any + Send + Sync>,
    ) -> Generation {
        let generation = next_generation();
        let entry = ServiceEntry { generation, value };
        let parent = {
            let mut state = lock(&self.state);
            if state.committed {
                self.parent.clone()
            } else {
                state.providers.insert(service.clone(), entry.clone());
                None
            }
        };
        if let Some(parent) = parent {
            parent.insert(service.clone(), entry);
            parent.notify(&service);
        } else {
            self.notify(&service);
        }
        generation
    }

    fn insert(&self, service: ResolvedService, entry: ServiceEntry) {
        lock(&self.state).providers.insert(service, entry);
    }

    pub(crate) fn remove_if_current(
        &self,
        service: &ResolvedService,
        generation: Generation,
    ) -> bool {
        let (removed, parent) = {
            let mut state = lock(&self.state);
            if state.committed {
                (false, self.parent.clone())
            } else {
                let is_current = state
                    .providers
                    .get(service)
                    .is_some_and(|entry| entry.generation == generation);
                if is_current {
                    state.providers.remove(service);
                }
                (is_current, None)
            }
        };
        if let Some(parent) = parent {
            return parent.remove_if_current(service, generation);
        }
        if removed {
            self.notify(service);
        }
        removed
    }

    pub(crate) fn entry(&self, service: &ResolvedService) -> Option<ServiceEntry> {
        let (entry, parent) = {
            let state = lock(&self.state);
            (
                (!state.committed)
                    .then(|| state.providers.get(service).cloned())
                    .flatten(),
                self.parent.clone(),
            )
        };
        entry.or_else(|| parent.and_then(|parent| parent.entry(service)))
    }

    fn is_current(&self, service: &ResolvedService, generation: Generation) -> bool {
        self.entry(service)
            .is_some_and(|entry| entry.generation == generation)
    }

    pub(crate) fn snapshot(
        &self,
        service: &ResolvedService,
        intercepts: Vec<Value>,
    ) -> ServiceSnapshot {
        match self.entry(service) {
            Some(entry) => ServiceSnapshot {
                key: service.key.diagnostic_key(),
                realm: service.realm.diagnostic_key(),
                generation: Some(entry.generation),
                available: true,
                intercepts,
            },
            None => ServiceSnapshot {
                key: service.key.diagnostic_key(),
                realm: service.realm.diagnostic_key(),
                generation: None,
                available: false,
                intercepts,
            },
        }
    }

    pub(crate) fn provider_services(&self) -> Vec<ResolvedService> {
        let (mut services, parent, committed) = {
            let state = lock(&self.state);
            (
                state.providers.keys().cloned().collect::<Vec<_>>(),
                self.parent.clone(),
                state.committed,
            )
        };
        if let Some(parent) = parent {
            let mut parent_services = parent.provider_services();
            if !committed {
                parent_services.append(&mut services);
            }
            parent_services.sort();
            parent_services.dedup();
            parent_services
        } else {
            services
        }
    }

    pub(crate) fn register_listener(
        &self,
        dependencies: Arc<[ResolvedDependency]>,
        callback: Arc<dyn Fn(DependencySnapshot) + Send + Sync>,
    ) -> ResourceId {
        let id = next_resource_id();
        lock(&self.state).listeners.insert(
            id,
            Listener {
                dependencies,
                callback,
            },
        );
        id
    }

    pub(crate) fn unregister_listener(&self, id: ResourceId) -> bool {
        lock(&self.state).listeners.remove(&id).is_some()
    }

    pub(crate) fn dependency_snapshot(
        &self,
        dependencies: &[ResolvedDependency],
    ) -> DependencySnapshot {
        dependency_snapshot_from(&lock(&self.state), dependencies)
    }

    fn notify(&self, changed: &ResolvedService) {
        let deliveries = {
            let state = lock(&self.state);
            state
                .listeners
                .values()
                .filter(|listener| {
                    listener
                        .dependencies
                        .iter()
                        .any(|dependency| dependency.service == *changed)
                })
                .map(|listener| {
                    (
                        Arc::clone(&listener.callback),
                        dependency_snapshot_from(&state, &listener.dependencies),
                    )
                })
                .collect::<Vec<_>>()
        };
        for (listener, snapshot) in deliveries {
            listener(snapshot);
        }
    }
}

fn snapshot_from(
    state: &RegistryState,
    service: &ResolvedService,
    intercepts: Vec<Value>,
) -> ServiceSnapshot {
    let entry = state.providers.get(service);
    ServiceSnapshot {
        key: service.key.diagnostic_key(),
        realm: service.realm.diagnostic_key(),
        generation: entry.map(|entry| entry.generation),
        available: entry.is_some(),
        intercepts,
    }
}

fn dependency_snapshot_from(
    state: &RegistryState,
    dependencies: &[ResolvedDependency],
) -> DependencySnapshot {
    let services = dependencies
        .iter()
        .map(|dependency| snapshot_from(state, &dependency.service, dependency.intercepts.clone()))
        .collect::<Vec<_>>();
    let ready = dependencies
        .iter()
        .zip(&services)
        .all(|(dependency, snapshot)| !dependency.required || snapshot.available);
    DependencySnapshot { ready, services }
}

fn unavailable_snapshot(dependencies: &[ResolvedDependency]) -> DependencySnapshot {
    let services = dependencies
        .iter()
        .map(|dependency| ServiceSnapshot {
            key: dependency.service.key.diagnostic_key(),
            realm: dependency.service.realm.diagnostic_key(),
            generation: None,
            available: false,
            intercepts: dependency.intercepts.clone(),
        })
        .collect::<Vec<_>>();
    let ready = dependencies.iter().all(|dependency| !dependency.required);
    DependencySnapshot { ready, services }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}
