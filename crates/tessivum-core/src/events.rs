use std::{
    any::{type_name, Any},
    collections::BTreeMap,
    fmt,
    future::{poll_fn, Future},
    marker::PhantomData,
    mem,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, MutexGuard, Weak,
    },
    task::Poll,
    time::{Duration, Instant},
};

use serde_json::Value;

use crate::{BoxDisposer, CoreError, FiberState, RealmLabel, ResourceId, Scope, ScopeId};

/// A serialized event payload for dynamic event boundaries.
pub type DynamicEvent = Value;
/// A listener result using Cordis's JSON-compatible value convention.
pub type EventValue = Value;
/// The result produced by an event listener.
pub type EventResult = Result<EventValue, CoreError>;
/// A sendable asynchronous event listener result.
pub type EventFuture = Pin<Box<dyn Future<Output = EventResult> + Send>>;

type ErasedPayload = Arc<dyn Any + Send + Sync>;
type SyncListener = Arc<dyn Fn(&dyn Any) -> EventResult + Send + Sync>;
type AsyncListener = Arc<dyn Fn(ErasedPayload) -> EventFuture + Send + Sync>;
type WaterfallListener = Arc<dyn Fn(ErasedPayload, ErasedNext) -> EventFuture + Send + Sync>;
type ErasedContinuation = Box<dyn FnOnce(ErasedPayload) -> EventFuture + Send>;

/// A typed, native event name. Payloads stay native and are never serialized.
pub struct EventKey<T> {
    name: Arc<str>,
    marker: PhantomData<fn() -> T>,
}

impl<T> EventKey<T> {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Arc::from(name.into()),
            marker: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<T> Clone for EventKey<T> {
    fn clone(&self) -> Self {
        Self {
            name: Arc::clone(&self.name),
            marker: PhantomData,
        }
    }
}

impl<T> fmt::Debug for EventKey<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("EventKey").field(&self.name).finish()
    }
}

/// Registration controls that alter dispatch ordering and visibility.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EventOptions {
    /// Inserts this listener before ordinary registrations.
    pub prepend: bool,
    /// Makes this listener visible from every event context and realm.
    pub global: bool,
}

/// The Cordis event combination mode recorded in diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchMode {
    Emit,
    Parallel,
    Serial,
    Bail,
    Waterfall,
}

/// The payload-free terminal state of one event dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DispatchOutcome {
    Completed,
    Bailed,
    Failed { errors: usize },
}

/// A payload-free dispatch observation suitable for diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchDiagnostic {
    pub event: String,
    pub mode: DispatchMode,
    pub listener_count: usize,
    pub duration: Duration,
    pub outcome: DispatchOutcome,
}

/// A one-shot continuation supplied to a typed waterfall listener.
pub struct WaterfallNext<T> {
    continuation: ErasedNext,
    marker: PhantomData<fn(T)>,
}

impl<T> Clone for WaterfallNext<T> {
    fn clone(&self) -> Self {
        Self {
            continuation: self.continuation.clone(),
            marker: PhantomData,
        }
    }
}

impl<T> WaterfallNext<T>
where
    T: Send + Sync + 'static,
{
    /// Continues the captured dispatch exactly once with a possibly changed payload.
    pub fn next(&self, payload: T) -> EventFuture {
        self.continuation.next(Arc::new(payload))
    }
}

/// A scope-owned event registration.
pub struct ListenerHandle {
    id: ResourceId,
    event: Arc<str>,
    token: u64,
    bus: Weak<EventBusInner>,
}

impl ListenerHandle {
    pub fn id(&self) -> ResourceId {
        self.id
    }

    /// Stops future dispatches. Existing dispatch snapshots remain deterministic.
    pub fn remove(&self) -> bool {
        self.bus
            .upgrade()
            .is_some_and(|bus| remove_listener(&bus, &self.event, self.token))
    }
}

impl fmt::Debug for ListenerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ListenerHandle")
            .field("id", &self.id)
            .field("event", &self.event)
            .finish()
    }
}

/// A context-bound view of a shared event registry.
#[derive(Clone)]
pub struct EventBus {
    inner: Arc<EventBusInner>,
    filter: EventFilter,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for EventBus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventBus")
            .field("context", &self.filter.context)
            .field("realm", &self.filter.realm)
            .field("event_count", &lock(&self.inner.listeners).len())
            .finish()
    }
}

impl EventBus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(EventBusInner {
                listeners: Mutex::new(BTreeMap::new()),
                diagnostics: Mutex::new(Vec::new()),
                next_listener: AtomicU64::new(1),
            }),
            filter: EventFilter::default(),
        }
    }

    /// Rebinds this shared registry to `scope`'s context filter.
    pub fn in_context(&self, scope: &Scope) -> Self {
        let mut filter = self.filter.clone();
        filter.context = Some(scope.id());
        Self {
            inner: Arc::clone(&self.inner),
            filter,
        }
    }

    /// Rebinds this shared registry to one event realm.
    pub fn in_realm(&self, realm: RealmLabel) -> Self {
        let mut filter = self.filter.clone();
        filter.realm = Some(realm);
        Self {
            inner: Arc::clone(&self.inner),
            filter,
        }
    }

    pub fn on<T, F>(
        &self,
        owner: &Scope,
        key: EventKey<T>,
        options: EventOptions,
        listener: F,
    ) -> Result<ListenerHandle, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(&T) -> EventResult + Send + Sync + 'static,
    {
        self.register_sync(owner, key.name, options, false, listener)
    }

    pub fn once<T, F>(
        &self,
        owner: &Scope,
        key: EventKey<T>,
        options: EventOptions,
        listener: F,
    ) -> Result<ListenerHandle, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(&T) -> EventResult + Send + Sync + 'static,
    {
        self.register_sync(owner, key.name, options, true, listener)
    }

    pub fn on_async<T, F>(
        &self,
        owner: &Scope,
        key: EventKey<T>,
        options: EventOptions,
        listener: F,
    ) -> Result<ListenerHandle, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(Arc<T>) -> EventFuture + Send + Sync + 'static,
    {
        self.register_async(owner, key.name, options, false, listener)
    }

    pub fn once_async<T, F>(
        &self,
        owner: &Scope,
        key: EventKey<T>,
        options: EventOptions,
        listener: F,
    ) -> Result<ListenerHandle, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(Arc<T>) -> EventFuture + Send + Sync + 'static,
    {
        self.register_async(owner, key.name, options, true, listener)
    }

    pub fn on_dynamic<F>(
        &self,
        owner: &Scope,
        event: impl Into<String>,
        options: EventOptions,
        listener: F,
    ) -> Result<ListenerHandle, CoreError>
    where
        F: Fn(&DynamicEvent) -> EventResult + Send + Sync + 'static,
    {
        self.register_sync(owner, Arc::from(event.into()), options, false, listener)
    }

    pub fn once_dynamic<F>(
        &self,
        owner: &Scope,
        event: impl Into<String>,
        options: EventOptions,
        listener: F,
    ) -> Result<ListenerHandle, CoreError>
    where
        F: Fn(&DynamicEvent) -> EventResult + Send + Sync + 'static,
    {
        self.register_sync(owner, Arc::from(event.into()), options, true, listener)
    }

    pub fn on_dynamic_async<F>(
        &self,
        owner: &Scope,
        event: impl Into<String>,
        options: EventOptions,
        listener: F,
    ) -> Result<ListenerHandle, CoreError>
    where
        F: Fn(DynamicEvent) -> EventFuture + Send + Sync + 'static,
    {
        self.register_async_dynamic(owner, Arc::from(event.into()), options, false, listener)
    }

    pub fn once_dynamic_async<F>(
        &self,
        owner: &Scope,
        event: impl Into<String>,
        options: EventOptions,
        listener: F,
    ) -> Result<ListenerHandle, CoreError>
    where
        F: Fn(DynamicEvent) -> EventFuture + Send + Sync + 'static,
    {
        self.register_async_dynamic(owner, Arc::from(event.into()), options, true, listener)
    }

    pub fn on_dynamic_waterfall<F>(
        &self,
        owner: &Scope,
        event: impl Into<String>,
        options: EventOptions,
        listener: F,
    ) -> Result<ListenerHandle, CoreError>
    where
        F: Fn(DynamicEvent, WaterfallNext<DynamicEvent>) -> EventFuture + Send + Sync + 'static,
    {
        self.register_waterfall(owner, Arc::from(event.into()), options, listener)
    }

    pub fn on_waterfall<T, F>(
        &self,
        owner: &Scope,
        key: EventKey<T>,
        options: EventOptions,
        listener: F,
    ) -> Result<ListenerHandle, CoreError>
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(T, WaterfallNext<T>) -> EventFuture + Send + Sync + 'static,
    {
        self.register_waterfall(owner, key.name, options, listener)
    }

    /// Synchronously invokes every eligible synchronous listener in registration order.
    pub fn emit<T>(&self, key: &EventKey<T>, payload: &T) -> Result<(), CoreError>
    where
        T: Send + Sync + 'static,
    {
        self.emit_payload(&key.name, payload)
    }

    /// Synchronously returns the first Cordis-effective listener value.
    pub fn bail<T>(&self, key: &EventKey<T>, payload: &T) -> Result<Option<EventValue>, CoreError>
    where
        T: Send + Sync + 'static,
    {
        self.bail_payload(&key.name, payload)
    }

    /// Starts all eligible listeners, waits for them all, then aggregates their failures.
    pub async fn parallel<T>(
        &self,
        key: &EventKey<T>,
        payload: Arc<T>,
    ) -> Result<Vec<EventValue>, CoreError>
    where
        T: Send + Sync + 'static,
    {
        self.parallel_payload(&key.name, payload).await
    }

    /// Invokes listeners in order and stops on the first Cordis-effective value.
    pub async fn serial<T>(
        &self,
        key: &EventKey<T>,
        payload: Arc<T>,
    ) -> Result<Option<EventValue>, CoreError>
    where
        T: Send + Sync + 'static,
    {
        self.serial_payload(&key.name, payload).await
    }

    /// Runs the one-shot continuation chain for typed waterfall listeners.
    pub async fn waterfall<T>(&self, key: &EventKey<T>, payload: T) -> Result<EventValue, CoreError>
    where
        T: Send + Sync + 'static,
    {
        self.waterfall_payload(&key.name, Arc::new(payload)).await
    }

    pub fn emit_dynamic(
        &self,
        event: impl AsRef<str>,
        payload: &DynamicEvent,
    ) -> Result<(), CoreError> {
        self.emit_payload(event.as_ref(), payload)
    }

    pub fn bail_dynamic(
        &self,
        event: impl AsRef<str>,
        payload: &DynamicEvent,
    ) -> Result<Option<EventValue>, CoreError> {
        self.bail_payload(event.as_ref(), payload)
    }

    pub async fn parallel_dynamic(
        &self,
        event: impl AsRef<str>,
        payload: DynamicEvent,
    ) -> Result<Vec<EventValue>, CoreError> {
        self.parallel_payload(event.as_ref(), Arc::new(payload))
            .await
    }

    pub async fn serial_dynamic(
        &self,
        event: impl AsRef<str>,
        payload: DynamicEvent,
    ) -> Result<Option<EventValue>, CoreError> {
        self.serial_payload(event.as_ref(), Arc::new(payload)).await
    }

    pub async fn waterfall_dynamic(
        &self,
        event: impl AsRef<str>,
        payload: DynamicEvent,
    ) -> Result<EventValue, CoreError> {
        self.waterfall_payload(event.as_ref(), Arc::new(payload))
            .await
    }

    pub fn diagnostics(&self) -> Vec<DispatchDiagnostic> {
        lock(&self.inner.diagnostics).clone()
    }

    pub fn take_diagnostics(&self) -> Vec<DispatchDiagnostic> {
        mem::take(&mut *lock(&self.inner.diagnostics))
    }

    fn register_sync<T, F>(
        &self,
        owner: &Scope,
        event: Arc<str>,
        options: EventOptions,
        once: bool,
        listener: F,
    ) -> Result<ListenerHandle, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(&T) -> EventResult + Send + Sync + 'static,
    {
        let callback_event = Arc::clone(&event);
        let callback: SyncListener = Arc::new(move |payload| {
            payload
                .downcast_ref::<T>()
                .ok_or_else(|| type_mismatch(&callback_event, type_name::<T>()))
                .and_then(&listener)
        });
        self.register(owner, event, options, once, ListenerKind::Sync(callback))
    }

    fn register_async<T, F>(
        &self,
        owner: &Scope,
        event: Arc<str>,
        options: EventOptions,
        once: bool,
        listener: F,
    ) -> Result<ListenerHandle, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(Arc<T>) -> EventFuture + Send + Sync + 'static,
    {
        let callback_event = Arc::clone(&event);
        let callback: AsyncListener = Arc::new(move |payload| {
            let event = Arc::clone(&callback_event);
            match Arc::downcast::<T>(payload) {
                Ok(payload) => listener(payload),
                Err(_) => failed_future(type_mismatch(&event, type_name::<T>())),
            }
        });
        self.register(owner, event, options, once, ListenerKind::Async(callback))
    }

    fn register_async_dynamic<F>(
        &self,
        owner: &Scope,
        event: Arc<str>,
        options: EventOptions,
        once: bool,
        listener: F,
    ) -> Result<ListenerHandle, CoreError>
    where
        F: Fn(DynamicEvent) -> EventFuture + Send + Sync + 'static,
    {
        let callback_event = Arc::clone(&event);
        let callback: AsyncListener = Arc::new(move |payload| {
            let event = Arc::clone(&callback_event);
            match Arc::downcast::<DynamicEvent>(payload) {
                Ok(payload) => listener((*payload).clone()),
                Err(_) => failed_future(type_mismatch(&event, type_name::<DynamicEvent>())),
            }
        });
        self.register(owner, event, options, once, ListenerKind::Async(callback))
    }

    fn register_waterfall<T, F>(
        &self,
        owner: &Scope,
        event: Arc<str>,
        options: EventOptions,
        listener: F,
    ) -> Result<ListenerHandle, CoreError>
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(T, WaterfallNext<T>) -> EventFuture + Send + Sync + 'static,
    {
        let callback_event = Arc::clone(&event);
        let callback: WaterfallListener = Arc::new(move |payload, next| {
            let event = Arc::clone(&callback_event);
            let Ok(payload) = Arc::downcast::<T>(payload) else {
                return failed_future(type_mismatch(&event, type_name::<T>()));
            };
            match Arc::try_unwrap(payload) {
                Ok(payload) => listener(
                    payload,
                    WaterfallNext {
                        continuation: next,
                        marker: PhantomData,
                    },
                ),
                Err(payload) => listener(
                    (*payload).clone(),
                    WaterfallNext {
                        continuation: next,
                        marker: PhantomData,
                    },
                ),
            }
        });
        self.register(
            owner,
            event,
            options,
            false,
            ListenerKind::Waterfall(callback),
        )
    }

    fn register(
        &self,
        owner: &Scope,
        event: Arc<str>,
        options: EventOptions,
        once: bool,
        kind: ListenerKind,
    ) -> Result<ListenerHandle, CoreError> {
        if owner.state() != FiberState::Active {
            return Err(CoreError::InvalidState {
                operation: "register listener",
                state: owner.state(),
            });
        }

        let token = self.inner.next_listener.fetch_add(1, Ordering::Relaxed);
        let listener = Listener {
            token,
            owner: owner.clone(),
            filter: (!options.global).then(|| self.filter.clone()),
            once: once.then(|| Arc::new(AtomicBool::new(false))),
            kind,
        };
        {
            let mut listeners = lock(&self.inner.listeners);
            let entries = listeners.entry(event.to_string()).or_default();
            if options.prepend {
                entries.insert(0, listener);
            } else {
                entries.push(listener);
            }
        }

        let weak = Arc::downgrade(&self.inner);
        let cleanup_event = Arc::clone(&event);
        let disposer: BoxDisposer = Box::new(move || {
            Box::pin(async move {
                if let Some(bus) = weak.upgrade() {
                    remove_listener(&bus, &cleanup_event, token);
                }
                Ok(())
            })
        });
        let id = match owner.add_effect(format!("listener:{event}"), disposer) {
            Ok(id) => id,
            Err(error) => {
                remove_listener(&self.inner, &event, token);
                return Err(error);
            }
        };

        Ok(ListenerHandle {
            id,
            event,
            token,
            bus: Arc::downgrade(&self.inner),
        })
    }

    fn snapshot(&self, event: &str) -> Vec<Listener> {
        lock(&self.inner.listeners)
            .get(event)
            .map(|listeners| {
                listeners
                    .iter()
                    .filter(|listener| listener.matches(&self.filter))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    fn emit_payload(&self, event: &str, payload: &dyn Any) -> Result<(), CoreError> {
        let started = Instant::now();
        let listeners = self.snapshot(event);
        let listener_count = listeners.len();
        for listener in listeners {
            if !claim_once(&self.inner, event, &listener) {
                continue;
            }
            let result = match &listener.kind {
                ListenerKind::Sync(callback) => callback(payload),
                ListenerKind::Async(_) | ListenerKind::Waterfall(_) => {
                    Err(CoreError::AsyncEventListener {
                        operation: "emit",
                        event: event.into(),
                    })
                }
            };
            if let Err(error) = result {
                self.record(
                    event,
                    DispatchMode::Emit,
                    listener_count,
                    started.elapsed(),
                    DispatchOutcome::Failed { errors: 1 },
                );
                return Err(error);
            }
        }
        self.record(
            event,
            DispatchMode::Emit,
            listener_count,
            started.elapsed(),
            DispatchOutcome::Completed,
        );
        Ok(())
    }

    fn bail_payload(
        &self,
        event: &str,
        payload: &dyn Any,
    ) -> Result<Option<EventValue>, CoreError> {
        let started = Instant::now();
        let listeners = self.snapshot(event);
        let listener_count = listeners.len();
        for listener in listeners {
            if !claim_once(&self.inner, event, &listener) {
                continue;
            }
            let result = match &listener.kind {
                ListenerKind::Sync(callback) => callback(payload),
                ListenerKind::Async(_) | ListenerKind::Waterfall(_) => {
                    Err(CoreError::AsyncEventListener {
                        operation: "bail",
                        event: event.into(),
                    })
                }
            };
            match result {
                Ok(value) if is_effective(&value) => {
                    self.record(
                        event,
                        DispatchMode::Bail,
                        listener_count,
                        started.elapsed(),
                        DispatchOutcome::Bailed,
                    );
                    return Ok(Some(value));
                }
                Ok(_) => {}
                Err(error) => {
                    self.record(
                        event,
                        DispatchMode::Bail,
                        listener_count,
                        started.elapsed(),
                        DispatchOutcome::Failed { errors: 1 },
                    );
                    return Err(error);
                }
            }
        }
        self.record(
            event,
            DispatchMode::Bail,
            listener_count,
            started.elapsed(),
            DispatchOutcome::Completed,
        );
        Ok(None)
    }

    async fn parallel_payload<T>(
        &self,
        event: &str,
        payload: Arc<T>,
    ) -> Result<Vec<EventValue>, CoreError>
    where
        T: Send + Sync + 'static,
    {
        let payload: Arc<dyn Any + Send + Sync> = payload;
        let started = Instant::now();
        let listeners = self.snapshot(event);
        let listener_count = listeners.len();
        let futures = listeners
            .into_iter()
            .filter(|listener| claim_once(&self.inner, event, listener))
            .map(|listener| call_async(listener, Arc::clone(&payload)))
            .collect();
        let results = join_all(futures).await;
        let mut values = Vec::new();
        let mut errors = Vec::new();
        for result in results.into_iter().flatten() {
            match result {
                Ok(value) => values.push(value),
                Err(error) => errors.push(error),
            }
        }
        let outcome = if errors.is_empty() {
            DispatchOutcome::Completed
        } else {
            DispatchOutcome::Failed {
                errors: errors.len(),
            }
        };
        self.record(
            event,
            DispatchMode::Parallel,
            listener_count,
            started.elapsed(),
            outcome,
        );
        if errors.is_empty() {
            Ok(values)
        } else {
            Err(CoreError::cleanup(errors))
        }
    }

    async fn serial_payload<T>(
        &self,
        event: &str,
        payload: Arc<T>,
    ) -> Result<Option<EventValue>, CoreError>
    where
        T: Send + Sync + 'static,
    {
        let payload: Arc<dyn Any + Send + Sync> = payload;
        let started = Instant::now();
        let listeners = self.snapshot(event);
        let listener_count = listeners.len();
        for listener in listeners {
            if !claim_once(&self.inner, event, &listener) {
                continue;
            }
            let Some(result) = call_async(listener, Arc::clone(&payload)).await else {
                continue;
            };
            match result {
                Ok(value) if is_effective(&value) => {
                    self.record(
                        event,
                        DispatchMode::Serial,
                        listener_count,
                        started.elapsed(),
                        DispatchOutcome::Bailed,
                    );
                    return Ok(Some(value));
                }
                Ok(_) => {}
                Err(error) => {
                    self.record(
                        event,
                        DispatchMode::Serial,
                        listener_count,
                        started.elapsed(),
                        DispatchOutcome::Failed { errors: 1 },
                    );
                    return Err(error);
                }
            }
        }
        self.record(
            event,
            DispatchMode::Serial,
            listener_count,
            started.elapsed(),
            DispatchOutcome::Completed,
        );

        Ok(None)
    }
    async fn waterfall_payload(
        &self,
        event: &str,
        payload: ErasedPayload,
    ) -> Result<EventValue, CoreError> {
        let started = Instant::now();
        let listeners = self.snapshot(event);
        let listener_count = listeners.len();
        let result = waterfall_at(
            Arc::clone(&self.inner),
            Arc::from(event),
            listeners.into(),
            0,
            payload,
        )
        .await;
        self.record(
            event,
            DispatchMode::Waterfall,
            listener_count,
            started.elapsed(),
            match &result {
                Ok(_) => DispatchOutcome::Completed,
                Err(_) => DispatchOutcome::Failed { errors: 1 },
            },
        );
        result
    }

    fn record(
        &self,
        event: &str,
        mode: DispatchMode,
        listener_count: usize,
        duration: Duration,
        outcome: DispatchOutcome,
    ) {
        lock(&self.inner.diagnostics).push(DispatchDiagnostic {
            event: event.into(),
            mode,
            listener_count,
            duration,
            outcome,
        });
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct EventFilter {
    context: Option<ScopeId>,
    realm: Option<RealmLabel>,
}

struct EventBusInner {
    listeners: Mutex<BTreeMap<String, Vec<Listener>>>,
    diagnostics: Mutex<Vec<DispatchDiagnostic>>,
    next_listener: AtomicU64,
}

#[derive(Clone)]
struct Listener {
    token: u64,
    owner: Scope,
    filter: Option<EventFilter>,
    once: Option<Arc<AtomicBool>>,
    kind: ListenerKind,
}

impl Listener {
    fn matches(&self, filter: &EventFilter) -> bool {
        self.owner_active() && self.filter.as_ref().is_none_or(|current| current == filter)
    }

    fn owner_active(&self) -> bool {
        self.owner.state() == FiberState::Active
    }
}

#[derive(Clone)]
enum ListenerKind {
    Sync(SyncListener),
    Async(AsyncListener),
    Waterfall(WaterfallListener),
}

#[derive(Clone)]
struct ErasedNext {
    continuation: Arc<Mutex<Option<ErasedContinuation>>>,
}

impl ErasedNext {
    fn new(continuation: ErasedContinuation) -> Self {
        Self {
            continuation: Arc::new(Mutex::new(Some(continuation))),
        }
    }

    fn next(&self, payload: ErasedPayload) -> EventFuture {
        match lock(&self.continuation).take() {
            Some(continuation) => continuation(payload),
            None => failed_future(CoreError::WaterfallContinuationUsed),
        }
    }
}

fn call_async(listener: Listener, payload: ErasedPayload) -> ListenerFuture {
    Box::pin(async move {
        let result = match listener.kind {
            ListenerKind::Sync(callback) => callback(payload.as_ref()),
            ListenerKind::Async(callback) => callback(payload).await,
            ListenerKind::Waterfall(_) => Err(CoreError::AsyncEventListener {
                operation: "parallel or serial",
                event: "waterfall listener".into(),
            }),
        };
        Some(result)
    })
}

type ListenerFuture = Pin<Box<dyn Future<Output = Option<EventResult>> + Send>>;

fn waterfall_at(
    bus: Arc<EventBusInner>,
    event: Arc<str>,
    listeners: Arc<[Listener]>,
    start: usize,
    payload: ErasedPayload,
) -> EventFuture {
    Box::pin(async move {
        for index in start..listeners.len() {
            let listener = &listeners[index];
            if !claim_once(&bus, &event, listener) {
                continue;
            }
            let ListenerKind::Waterfall(callback) = &listener.kind else {
                return Err(CoreError::AsyncEventListener {
                    operation: "waterfall",
                    event: event.to_string(),
                });
            };
            let next_bus = Arc::clone(&bus);
            let next_event = Arc::clone(&event);
            let next_listeners = Arc::clone(&listeners);
            let continuation = ErasedNext::new(Box::new(move |payload| {
                waterfall_at(next_bus, next_event, next_listeners, index + 1, payload)
            }));
            return callback(payload, continuation).await;
        }
        Ok(Value::Null)
    })
}

fn claim_once(bus: &EventBusInner, event: &str, listener: &Listener) -> bool {
    let Some(once) = &listener.once else {
        return true;
    };
    if once.swap(true, Ordering::AcqRel) {
        return false;
    }
    remove_listener(bus, event, listener.token);
    true
}

fn remove_listener(bus: &EventBusInner, event: &str, token: u64) -> bool {
    let mut listeners = lock(&bus.listeners);
    let mut empty = false;
    let removed = if let Some(entries) = listeners.get_mut(event) {
        let count = entries.len();
        entries.retain(|listener| listener.token != token);
        empty = entries.is_empty();
        entries.len() != count
    } else {
        false
    };
    if empty {
        listeners.remove(event);
    }
    removed
}

async fn join_all(futures: Vec<ListenerFuture>) -> Vec<Option<EventResult>> {
    let mut futures = futures.into_iter().map(Some).collect::<Vec<_>>();
    let mut results = (0..futures.len())
        .map(|_| None)
        .collect::<Vec<Option<Option<EventResult>>>>();
    poll_fn(move |context| {
        let mut pending = false;
        for (index, slot) in futures.iter_mut().enumerate() {
            let poll = match slot.as_mut() {
                Some(future) => future.as_mut().poll(context),
                None => continue,
            };
            match poll {
                Poll::Ready(result) => {
                    results[index] = Some(result);
                    *slot = None;
                }
                Poll::Pending => pending = true,
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(results.drain(..).map(Option::unwrap).collect())
        }
    })
    .await
}

fn failed_future(error: CoreError) -> EventFuture {
    Box::pin(async move { Err(error) })
}

fn type_mismatch(event: &str, expected: &'static str) -> CoreError {
    CoreError::EventTypeMismatch {
        event: event.into(),
        expected,
    }
}

fn is_effective(value: &EventValue) -> bool {
    !matches!(value, Value::Null | Value::Bool(false))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}
