use std::{
    cell::RefCell,
    future::Future,
    mem,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc, Mutex, MutexGuard,
    },
    task::{Context, Poll, Waker},
};

use serde::{Deserialize, Serialize};

use crate::{
    error::CoreError,
    ids::{next_fiber_id, next_generation, next_resource_id, next_scope_id},
    FiberId, Generation, ResourceId, ScopeId,
};

/// The lifecycle state shared by scopes and fibers.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FiberState {
    Pending = 0,
    Loading = 1,
    Active = 2,
    Failed = 3,
    Disposed = 4,
    Unloading = 5,
}

impl FiberState {
    fn from_raw(value: u8) -> Self {
        match value {
            0 => Self::Pending,
            1 => Self::Loading,
            2 => Self::Active,
            3 => Self::Failed,
            4 => Self::Disposed,
            5 => Self::Unloading,
            _ => unreachable!("invalid lifecycle state"),
        }
    }
}

/// A boxed one-shot asynchronous cleanup operation.
pub type BoxDisposer =
    Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = Result<(), CoreError>> + Send>> + Send>;

type CleanupFuture = Pin<Box<dyn Future<Output = Result<(), CoreError>> + Send>>;

/// A stable diagnostic description of a registered effect.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectMeta {
    pub id: ResourceId,
    pub label: String,
    pub children: Vec<EffectMeta>,
}

/// A cooperative cancellation signal shared by a scope and its callers.
#[derive(Clone, Debug)]
pub struct CancellationToken {
    inner: Arc<CancellationInner>,
}

#[derive(Debug)]
struct CancellationInner {
    cancelled: AtomicBool,
    waiters: Mutex<Vec<Waker>>,
}

impl CancellationToken {
    fn new() -> Self {
        Self {
            inner: Arc::new(CancellationInner {
                cancelled: AtomicBool::new(false),
                waiters: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Cancels this token once, returning whether this caller won the race.
    pub fn cancel(&self) -> bool {
        if self
            .inner
            .cancelled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }

        let waiters = mem::take(&mut *lock(&self.inner.waiters));
        for waiter in waiters {
            waiter.wake();
        }
        true
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    /// Waits until [`Self::cancel`] succeeds in any clone.
    pub async fn cancelled(&self) {
        CancellationWait {
            token: self.clone(),
        }
        .await
    }
}

struct CancellationWait {
    token: CancellationToken,
}

impl Future for CancellationWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.token.is_cancelled() {
            return Poll::Ready(());
        }

        let mut waiters = lock(&self.token.inner.waiters);
        if self.token.is_cancelled() {
            Poll::Ready(())
        } else {
            register_waker(&mut waiters, context.waker());
            Poll::Pending
        }
    }
}

/// An ownership scope for effects and child scopes.
#[derive(Clone, Debug)]
pub struct Scope {
    inner: Arc<ScopeInner>,
}

#[derive(Debug)]
struct ScopeInner {
    id: ScopeId,
    generation: Generation,
    state: AtomicU8,
    cancellation: CancellationToken,
    resources: Mutex<ScopeResources>,
    completion: DisposeCompletion,
}

#[derive(Default)]
struct ScopeResources {
    effects: Vec<Effect>,
    children: Vec<Scope>,
}

impl std::fmt::Debug for ScopeResources {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScopeResources")
            .field("effect_count", &self.effects.len())
            .field("child_count", &self.children.len())
            .finish()
    }
}

struct Effect {
    meta: EffectMeta,
    disposer: BoxDisposer,
}

impl std::fmt::Debug for Effect {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.meta.fmt(formatter)
    }
}

impl Scope {
    pub fn root() -> Self {
        Self::new()
    }

    fn new() -> Self {
        Self {
            inner: Arc::new(ScopeInner {
                id: next_scope_id(),
                generation: next_generation(),
                state: AtomicU8::new(FiberState::Active as u8),
                cancellation: CancellationToken::new(),
                resources: Mutex::new(ScopeResources {
                    effects: Vec::new(),
                    children: Vec::new(),
                }),
                completion: DisposeCompletion::new(),
            }),
        }
    }

    pub fn child(&self) -> Result<Self, CoreError> {
        let mut resources = lock(&self.inner.resources);
        if self.state() != FiberState::Active {
            return Err(self.invalid_state("create child scope"));
        }

        let child = Self::new();
        resources.children.push(child.clone());
        Ok(child)
    }

    pub fn id(&self) -> ScopeId {
        self.inner.id
    }

    pub fn generation(&self) -> Generation {
        self.inner.generation
    }

    pub fn state(&self) -> FiberState {
        FiberState::from_raw(self.inner.state.load(Ordering::Acquire))
    }

    pub fn cancellation(&self) -> CancellationToken {
        self.inner.cancellation.clone()
    }

    pub fn add_effect(
        &self,
        label: impl Into<String>,
        disposer: BoxDisposer,
    ) -> Result<ResourceId, CoreError> {
        let mut resources = lock(&self.inner.resources);
        if self.state() != FiberState::Active {
            return Err(self.invalid_state("register effect"));
        }

        let id = next_resource_id();
        resources.effects.push(Effect {
            meta: EffectMeta {
                id,
                label: label.into(),
                children: Vec::new(),
            },
            disposer,
        });
        Ok(id)
    }

    pub fn effects(&self) -> Vec<EffectMeta> {
        lock(&self.inner.resources)
            .effects
            .iter()
            .map(|effect| effect.meta.clone())
            .collect()
    }

    /// Disposes children and local effects once, in reverse registration order.
    pub async fn dispose(&self) -> Result<(), CoreError> {
        let id = self.id();
        match self.claim_disposal() {
            DisposeClaim::Owner => {
                std::future::poll_fn(|context| self.inner.completion.wait(context)).await
            }
            DisposeClaim::Join if is_disposing(id) => Ok(()),
            DisposeClaim::Join => {
                self.inner.completion.inherit_ancestors(&disposing_scopes());
                std::future::poll_fn(|context| {
                    if is_disposing(id) {
                        Poll::Ready(Ok(()))
                    } else {
                        self.inner.completion.wait(context)
                    }
                })
                .await
            }
        }
    }

    fn invalid_state(&self, operation: &'static str) -> CoreError {
        CoreError::InvalidState {
            operation,
            state: self.state(),
        }
    }

    fn claim_disposal(&self) -> DisposeClaim {
        let mut resources = lock(&self.inner.resources);
        match self.state() {
            FiberState::Unloading | FiberState::Disposed => DisposeClaim::Join,
            _ => {
                self.inner
                    .state
                    .store(FiberState::Unloading as u8, Ordering::Release);
                self.inner.cancellation.cancel();

                let cleanup_resources = mem::take(&mut *resources);
                let id = self.id();
                let ancestor_scopes = disposing_scopes();
                let mut child_ancestors = ancestor_scopes.clone();
                child_ancestors.push(id);
                for child in &cleanup_resources.children {
                    child.inner.completion.inherit_ancestors(&child_ancestors);
                }
                let scope = Arc::downgrade(&self.inner);
                self.inner.completion.start(
                    ancestor_scopes,
                    id,
                    Box::pin(async move {
                        let result = Self::dispose_resources(cleanup_resources).await;
                        if let Some(scope) = scope.upgrade() {
                            scope
                                .state
                                .store(FiberState::Disposed as u8, Ordering::Release);
                        }
                        result
                    }),
                );
                DisposeClaim::Owner
            }
        }
    }

    async fn rollback_failed_setup(&self) -> Result<(), CoreError> {
        let resources = {
            let mut resources = lock(&self.inner.resources);
            if self.state() != FiberState::Active {
                return Ok(());
            }
            self.inner
                .state
                .store(FiberState::Failed as u8, Ordering::Release);
            self.inner.cancellation.cancel();
            mem::take(&mut *resources)
        };

        Self::dispose_resources(resources).await
    }

    async fn dispose_resources(resources: ScopeResources) -> Result<(), CoreError> {
        let mut errors = Vec::new();

        for child in resources.children.into_iter().rev() {
            if let Err(error) = child.dispose().await {
                collect_cleanup_error(&mut errors, error);
            }
        }
        for effect in resources.effects.into_iter().rev() {
            if let Err(error) = (effect.disposer)().await {
                collect_cleanup_error(&mut errors, error);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(CoreError::Cleanup(errors))
        }
    }
}

enum DisposeClaim {
    Owner,
    Join,
}

/// A named task owned by a child scope.
#[derive(Clone, Debug)]
pub struct Fiber {
    inner: Arc<FiberInner>,
}

#[derive(Debug)]
struct FiberInner {
    id: FiberId,
    name: String,
    scope: Scope,
    state: AtomicU8,
}

impl Fiber {
    pub fn new(parent: &Scope, name: impl Into<String>) -> Result<Self, CoreError> {
        Ok(Self {
            inner: Arc::new(FiberInner {
                id: next_fiber_id(),
                name: name.into(),
                scope: parent.child()?,
                state: AtomicU8::new(FiberState::Pending as u8),
            }),
        })
    }

    pub fn id(&self) -> FiberId {
        self.inner.id
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn scope(&self) -> Scope {
        self.inner.scope.clone()
    }

    pub fn state(&self) -> FiberState {
        match self.inner.scope.state() {
            FiberState::Failed => FiberState::Failed,
            FiberState::Unloading => FiberState::Unloading,
            FiberState::Disposed => FiberState::Disposed,
            _ => FiberState::from_raw(self.inner.state.load(Ordering::Acquire)),
        }
    }

    pub async fn start<F, Setup>(&self, setup: F) -> Result<(), CoreError>
    where
        F: FnOnce(Scope) -> Setup,
        Setup: Future<Output = Result<(), CoreError>>,
    {
        if self.inner.scope.state() != FiberState::Active {
            return Err(CoreError::InvalidState {
                operation: "start fiber",
                state: self.inner.scope.state(),
            });
        }
        if self
            .inner
            .state
            .compare_exchange(
                FiberState::Pending as u8,
                FiberState::Loading as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(CoreError::InvalidState {
                operation: "start fiber",
                state: self.state(),
            });
        }

        match setup(self.scope()).await {
            Ok(()) => match self.inner.scope.state() {
                FiberState::Active => {
                    self.inner
                        .state
                        .store(FiberState::Active as u8, Ordering::Release);
                    Ok(())
                }
                FiberState::Unloading | FiberState::Disposed => Ok(()),
                state => {
                    self.inner.state.store(state as u8, Ordering::Release);
                    Err(CoreError::InvalidState {
                        operation: "complete fiber start",
                        state,
                    })
                }
            },
            Err(error) => {
                self.inner
                    .state
                    .store(FiberState::Failed as u8, Ordering::Release);
                match self.inner.scope.rollback_failed_setup().await {
                    Ok(()) => Err(error),
                    Err(CoreError::Cleanup(mut cleanup_errors)) => {
                        cleanup_errors.insert(0, error);
                        Err(CoreError::Cleanup(cleanup_errors))
                    }
                    Err(cleanup_error) => Err(CoreError::Cleanup(vec![error, cleanup_error])),
                }
            }
        }
    }

    pub async fn dispose(&self) -> Result<(), CoreError> {
        loop {
            match FiberState::from_raw(self.inner.state.load(Ordering::Acquire)) {
                FiberState::Unloading | FiberState::Disposed => break,
                state => {
                    if self
                        .inner
                        .state
                        .compare_exchange(
                            state as u8,
                            FiberState::Unloading as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
            }
        }

        let result = self.inner.scope.dispose().await;
        if self.inner.scope.state() == FiberState::Disposed {
            self.inner
                .state
                .store(FiberState::Disposed as u8, Ordering::Release);
        }
        result
    }
}

#[derive(Debug)]
struct DisposeCompletion {
    inner: Arc<DisposeCompletionInner>,
}

struct DisposeCompletionInner {
    result: Mutex<Option<Result<(), CoreError>>>,
    waiters: Mutex<Vec<Waker>>,
    context: Arc<Mutex<DisposalContextState>>,
}

impl std::fmt::Debug for DisposeCompletionInner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DisposeCompletionInner")
            .finish_non_exhaustive()
    }
}

impl DisposeCompletion {
    fn new() -> Self {
        Self {
            inner: Arc::new(DisposeCompletionInner {
                result: Mutex::new(None),
                waiters: Mutex::new(Vec::new()),
                context: Arc::new(Mutex::new(DisposalContextState::default())),
            }),
        }
    }

    fn start(&self, ancestors: Vec<ScopeId>, id: ScopeId, cleanup: CleanupFuture) {
        self.inherit_ancestors(&ancestors);
        let cleanup = DisposalContext::new(Arc::clone(&self.inner.context), id, cleanup);
        let completion = self.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            drop(handle.spawn(async move {
                completion.complete(cleanup.await);
            }));
        } else {
            drop(std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("cleanup runtime initializes");
                runtime.block_on(async move {
                    completion.complete(cleanup.await);
                });
            }));
        }
    }

    fn inherit_ancestors(&self, ancestors: &[ScopeId]) {
        let wakers = {
            let mut context = lock(&self.inner.context);
            let original_len = context.ancestors.len();
            for ancestor in ancestors {
                if !context.ancestors.contains(ancestor) {
                    context.ancestors.push(*ancestor);
                }
            }
            if context.ancestors.len() == original_len {
                return;
            }
            mem::take(&mut context.wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }

    fn complete(&self, result: Result<(), CoreError>) {
        *lock(&self.inner.result) = Some(result);
        self.wake_waiters();
        self.wake_context_wakers();
    }

    fn result(&self) -> Option<Result<(), CoreError>> {
        lock(&self.inner.result).clone()
    }

    fn wait(&self, context: &mut Context<'_>) -> Poll<Result<(), CoreError>> {
        if let Some(result) = self.result() {
            return Poll::Ready(result);
        }

        let mut waiters = lock(&self.inner.waiters);
        if let Some(result) = self.result() {
            Poll::Ready(result)
        } else {
            register_waker(&mut waiters, context.waker());
            Poll::Pending
        }
    }

    fn wake_waiters(&self) {
        let waiters = mem::take(&mut *lock(&self.inner.waiters));
        for waiter in waiters {
            waiter.wake();
        }
    }

    fn wake_context_wakers(&self) {
        let wakers = mem::take(&mut lock(&self.inner.context).wakers);
        for waker in wakers {
            waker.wake();
        }
    }
}

impl Clone for DisposeCompletion {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

fn collect_cleanup_error(errors: &mut Vec<CoreError>, error: CoreError) {
    match error {
        CoreError::Cleanup(mut nested) => errors.append(&mut nested),
        error => errors.push(error),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

fn register_waker(waiters: &mut Vec<Waker>, waker: &Waker) {
    if let Some(existing) = waiters
        .iter_mut()
        .find(|existing| existing.will_wake(waker))
    {
        *existing = waker.clone();
    } else {
        waiters.push(waker.clone());
    }
}

#[derive(Default)]
struct DisposalContextState {
    ancestors: Vec<ScopeId>,
    wakers: Vec<Waker>,
}

thread_local! {
    static DISPOSING_SCOPES: RefCell<Vec<ScopeId>> = const { RefCell::new(Vec::new()) };
    static DISPOSAL_CONTEXTS: RefCell<Vec<Arc<Mutex<DisposalContextState>>>> = const { RefCell::new(Vec::new()) };
}

fn disposing_scopes() -> Vec<ScopeId> {
    let mut scopes = DISPOSAL_CONTEXTS.with(|contexts| {
        let contexts = contexts.borrow();
        let mut scopes = Vec::new();
        for context in contexts.iter() {
            for id in &lock(context).ancestors {
                if !scopes.contains(id) {
                    scopes.push(*id);
                }
            }
        }
        scopes
    });
    DISPOSING_SCOPES.with(|disposing_scopes| {
        for id in disposing_scopes.borrow().iter() {
            if !scopes.contains(id) {
                scopes.push(*id);
            }
        }
    });
    scopes
}

fn is_disposing(id: ScopeId) -> bool {
    DISPOSING_SCOPES.with(|scopes| scopes.borrow().contains(&id))
        || DISPOSAL_CONTEXTS.with(|contexts| {
            contexts
                .borrow()
                .iter()
                .any(|context| lock(context).ancestors.contains(&id))
        })
}

struct DisposalContext<F> {
    context: Arc<Mutex<DisposalContextState>>,
    id: ScopeId,
    future: Pin<Box<F>>,
}

impl<F> DisposalContext<F> {
    fn new(context: Arc<Mutex<DisposalContextState>>, id: ScopeId, future: F) -> Self {
        Self {
            context,
            id,
            future: Box::pin(future),
        }
    }
}

impl<F> Unpin for DisposalContext<F> {}

impl<F: Future> Future for DisposalContext<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let _guard = DisposalScopeGuard::enter(Arc::clone(&this.context), this.id, context.waker());
        this.future.as_mut().poll(context)
    }
}

struct DisposalScopeGuard {
    context: Arc<Mutex<DisposalContextState>>,
    scope_count: usize,
}

impl DisposalScopeGuard {
    fn enter(context: Arc<Mutex<DisposalContextState>>, id: ScopeId, waker: &Waker) -> Self {
        let scope_count = {
            let mut disposal_context = lock(&context);
            register_waker(&mut disposal_context.wakers, waker);
            DISPOSING_SCOPES.with(|disposing_scopes| {
                let mut disposing_scopes = disposing_scopes.borrow_mut();
                disposing_scopes.extend_from_slice(&disposal_context.ancestors);
                disposing_scopes.push(id);
            });
            disposal_context.ancestors.len() + 1
        };
        DISPOSAL_CONTEXTS.with(|contexts| contexts.borrow_mut().push(Arc::clone(&context)));
        Self {
            context,
            scope_count,
        }
    }
}

impl Drop for DisposalScopeGuard {
    fn drop(&mut self) {
        DISPOSAL_CONTEXTS.with(|contexts| {
            let popped = contexts.borrow_mut().pop();
            debug_assert!(matches!(popped, Some(popped) if Arc::ptr_eq(&popped, &self.context)));
        });
        DISPOSING_SCOPES.with(|disposing_scopes| {
            let mut disposing_scopes = disposing_scopes.borrow_mut();
            for _ in 0..self.scope_count {
                disposing_scopes.pop();
            }
        });
    }
}
