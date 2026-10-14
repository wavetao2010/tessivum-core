use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    future::{poll_fn, Future},
    io::{Read, Write},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    },
    task::Poll,
    thread::{self, JoinHandle},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    evaluate_config, ConfigExpression, ConfigScope, ContextHandle, LoaderError, Scope, TraceEvent,
    TraceEventKind,
};

pub type LoaderResult<T> = Result<T, LoaderError>;
pub type LoaderFuture<'a, T> = Pin<Box<dyn Future<Output = LoaderResult<T>> + Send + 'a>>;

/// A stable identifier for one configured runtime entry.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EntryId(pub String);

impl EntryId {
    pub fn new(value: impl Into<String>) -> LoaderResult<Self> {
        let value = value.into();
        if is_identifier(&value) {
            Ok(Self(value))
        } else {
            Err(LoaderError::Validation(format!(
                "entry id {value:?} must be a non-empty identifier"
            )))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for EntryId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The supported plugin execution backends.
#[derive(
    Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeKind {
    #[default]
    Native,
    Wasm,
    #[serde(alias = "legacy_node")]
    LegacyNode,
}

/// Declarative options shared by every entry runtime.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EntryOptions {
    pub id: EntryId,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub runtime: RuntimeKind,
    #[serde(default = "empty_object")]
    pub config: Value,
    #[serde(default)]
    pub inject: Vec<String>,
    #[serde(default)]
    pub isolate: Vec<String>,
    #[serde(default = "empty_object")]
    pub intercept: Value,
    #[serde(default)]
    pub disabled: bool,
    #[serde(default)]
    pub group: Option<String>,
}

impl EntryOptions {
    pub fn validate(&self) -> LoaderResult<()> {
        EntryId::new(self.id.0.clone())?;
        if self
            .name
            .as_deref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(LoaderError::Validation(format!(
                "entry {} has a blank name",
                self.id
            )));
        }
        for (field, values) in [("inject", &self.inject), ("isolate", &self.isolate)] {
            if values.iter().any(|value| value.trim().is_empty()) {
                return Err(LoaderError::Validation(format!(
                    "entry {} has a blank {field} item",
                    self.id
                )));
            }
        }
        if self
            .group
            .as_deref()
            .is_some_and(|group| !is_identifier(group))
        {
            return Err(LoaderError::Validation(format!(
                "entry {} has an invalid group",
                self.id
            )));
        }
        validate_config(&self.config)?;
        validate_config(&self.intercept)
    }
}

/// One package-backed entry. Package aliases are accepted only while parsing;
/// the public model keeps one unambiguous `package` field.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Entry {
    #[serde(alias = "module", alias = "plugin", alias = "source")]
    pub package: String,
    #[serde(flatten)]
    pub options: EntryOptions,
}

impl Entry {
    pub fn new(package: impl Into<String>, options: EntryOptions) -> Self {
        Self {
            package: package.into(),
            options,
        }
    }

    pub fn validate(&self) -> LoaderResult<()> {
        if self.package.trim().is_empty() {
            return Err(LoaderError::Validation(format!(
                "entry {} has a blank package",
                self.options.id
            )));
        }
        self.options.validate()
    }

    /// Resolves static configuration for runtimes without managed dependencies.
    pub fn evaluate_config(&mut self, scope: &ConfigScope) -> LoaderResult<()> {
        let config = evaluate_config(&self.options.config, scope)?;
        let intercept = evaluate_config(&self.options.intercept, scope)?;
        self.options.config = config;
        self.options.intercept = intercept;
        Ok(())
    }
}

/// A nested, ordered group of entries. A disabled group suppresses every
/// nested entry without changing its persisted configuration.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EntryGroup {
    pub id: String,
    #[serde(default)]
    pub entries: Vec<Entry>,
    #[serde(default)]
    pub groups: Vec<EntryGroup>,
    #[serde(default)]
    pub disabled: bool,
}

/// The persistable declarative loader configuration.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EntryTree {
    #[serde(default)]
    pub entries: Vec<Entry>,
    #[serde(default)]
    pub groups: Vec<EntryGroup>,
}

impl EntryTree {
    pub fn parse(document: &str) -> LoaderResult<Self> {
        parse_entry_tree(document)
    }

    pub fn validate(&self) -> LoaderResult<()> {
        let mut ids = BTreeSet::new();
        let mut groups = BTreeSet::new();
        self.validate_entries(&self.entries, &mut ids)?;
        for group in &self.groups {
            self.validate_group(group, "", &mut ids, &mut groups)?;
        }
        Ok(())
    }

    pub fn entries(&self) -> Vec<&Entry> {
        let mut entries = Vec::new();
        collect_entries(&self.entries, &self.groups, &mut entries);
        entries
    }

    pub fn active_entries(&self) -> Vec<Entry> {
        let mut entries = Vec::new();
        collect_active_entries(&self.entries, &self.groups, false, &mut entries);
        entries
    }

    /// Applies one patch to a detached clone. The source tree is never
    /// modified if targeting or validation fails.
    pub fn apply(&self, patch: &Patch) -> LoaderResult<Self> {
        let mut candidate = self.clone();
        patch.apply_to(&mut candidate)?;
        candidate.validate()?;
        Ok(candidate)
    }

    /// Applies patches in input order, so an insertion can be addressed by a
    /// later patch in the same list.
    pub fn apply_all(&self, patches: &[Patch]) -> LoaderResult<Self> {
        let mut candidate = self.clone();
        for patch in patches {
            patch.apply_to(&mut candidate)?;
        }
        candidate.validate()?;
        Ok(candidate)
    }

    pub fn persist(&self, path: impl AsRef<Path>) -> LoaderResult<()> {
        persist_entry_tree(path, self)
    }

    fn validate_entries(&self, entries: &[Entry], ids: &mut BTreeSet<EntryId>) -> LoaderResult<()> {
        for entry in entries {
            entry.validate()?;
            if !ids.insert(entry.options.id.clone()) {
                return Err(LoaderError::Validation(format!(
                    "duplicate entry id {}",
                    entry.options.id
                )));
            }
        }
        Ok(())
    }

    fn validate_group(
        &self,
        group: &EntryGroup,
        parent: &str,
        ids: &mut BTreeSet<EntryId>,
        groups: &mut BTreeSet<String>,
    ) -> LoaderResult<()> {
        if !is_identifier(&group.id) {
            return Err(LoaderError::Validation(format!(
                "group id {:?} must be a non-empty identifier",
                group.id
            )));
        }
        let qualified = if parent.is_empty() || group.id.starts_with(&format!("{parent}.")) {
            group.id.clone()
        } else {
            format!("{parent}.{}", group.id)
        };
        if !groups.insert(qualified.clone()) {
            return Err(LoaderError::Validation(format!(
                "duplicate group id {qualified:?}"
            )));
        }
        self.validate_entries(&group.entries, ids)?;
        for nested in &group.groups {
            self.validate_group(nested, &qualified, ids, groups)?;
        }
        Ok(())
    }
}

/// A change applied to a detached [`EntryTree`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Patch {
    Insert {
        entry: Entry,
        #[serde(default)]
        before: Option<EntryId>,
    },
    Update {
        id: EntryId,
        entry: Entry,
    },
    UpdateConfig {
        id: EntryId,
        config: Value,
    },
    Remove {
        id: EntryId,
    },
    Move {
        id: EntryId,
        #[serde(default)]
        before: Option<EntryId>,
    },
}

impl Patch {
    fn apply_to(&self, tree: &mut EntryTree) -> LoaderResult<()> {
        match self {
            Self::Insert { entry, before } => insert_entry(tree, entry.clone(), before.as_ref()),
            Self::Update { id, entry } => {
                if entry.options.id != *id {
                    return Err(LoaderError::Patch(format!(
                        "replacement id {} does not match target {id}",
                        entry.options.id
                    )));
                }
                let target = find_entry_mut(tree, id)
                    .ok_or_else(|| LoaderError::MissingEntry(id.clone()))?;
                *target = entry.clone();
                Ok(())
            }
            Self::UpdateConfig { id, config } => {
                let target = find_entry_mut(tree, id)
                    .ok_or_else(|| LoaderError::MissingEntry(id.clone()))?;
                target.options.config = config.clone();
                Ok(())
            }
            Self::Remove { id } => remove_entry(tree, id).map(|_| ()),
            Self::Move { id, before } => {
                if before.as_ref().is_some_and(|target| target == id) {
                    return Err(LoaderError::Patch("an entry cannot precede itself".into()));
                }
                let entry = remove_entry(tree, id)?;
                insert_entry(tree, entry, before.as_ref())
            }
        }
    }
}

/// Clones `tree` and applies patches in order.
pub fn apply_entry_patches(tree: &EntryTree, patches: &[Patch]) -> LoaderResult<EntryTree> {
    tree.apply_all(patches)
}

pub fn parse_entry_tree(document: &str) -> LoaderResult<EntryTree> {
    if document.contains("!!js") {
        return Err(LoaderError::Expression(
            "YAML !!js values must be handled by the legacy-node runtime".into(),
        ));
    }
    let value = match serde_json::from_str::<Value>(document) {
        Ok(value) => value,
        Err(json_error) => serde_yaml::from_str::<Value>(document).map_err(|yaml_error| {
            LoaderError::Parse(format!(
                "invalid JSON ({json_error}) or YAML ({yaml_error})"
            ))
        })?,
    };
    let tree = parse_tree_value(value)?;
    tree.validate()?;
    Ok(tree)
}

/// Persists JSON or YAML using a same-directory temporary file followed by a
/// rename, so a torn write never replaces the last known-good document.
pub fn persist_entry_tree(path: impl AsRef<Path>, tree: &EntryTree) -> LoaderResult<()> {
    tree.validate()?;
    let path = path.as_ref();
    let data = match path.extension().and_then(|extension| extension.to_str()) {
        Some("yaml" | "yml") => serde_yaml::to_string(tree)
            .map_err(|error| LoaderError::Persistence(error.to_string()))?
            .into_bytes(),
        _ => serde_json::to_vec_pretty(tree)
            .map_err(|error| LoaderError::Persistence(error.to_string()))?,
    };
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| LoaderError::Persistence("persistence path has no file name".into()))?;
    static TEMPORARY: AtomicU64 = AtomicU64::new(0);
    let temporary = parent.join(format!(
        ".{filename}.{}.{}.tmp",
        std::process::id(),
        TEMPORARY.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| LoaderError::Persistence(error.to_string()))?;
        file.write_all(&data)
            .map_err(|error| LoaderError::Persistence(error.to_string()))?;
        file.sync_all()
            .map_err(|error| LoaderError::Persistence(error.to_string()))?;
        fs::rename(&temporary, path).map_err(|error| LoaderError::Persistence(error.to_string()))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

/// A resolver returns a runtime-specific, imported package description without
/// giving the loader a package-manager implementation detail.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResolvedPackage {
    pub specifier: String,
    pub location: String,
}

pub trait PackageResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        specifier: &'a str,
        runtime: RuntimeKind,
    ) -> LoaderFuture<'a, ResolvedPackage>;
}

/// The state reached by one managed runtime activation pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivationState {
    Active,
    Pending,
}

/// A live plugin instance whose effects are still confined to its candidate
/// context until `activate` succeeds.
pub trait RuntimeHandle: Send {
    fn activate<'a>(&'a mut self) -> LoaderFuture<'a, ()>;

    /// Advances managed activation without making a missing required dependency
    /// an import failure. Existing runtimes stay eager by default.
    fn activation<'a>(&'a mut self) -> LoaderFuture<'a, ActivationState> {
        Box::pin(async move {
            self.activate().await?;
            Ok(ActivationState::Active)
        })
    }
    fn dispose<'a>(&'a mut self) -> LoaderFuture<'a, ()>;
}

/// Runtime lifecycle boundary used by the loader. Native, WASM, and legacy
/// Node implementations all enter through this same public contract.
pub trait LoaderRuntime: Send + Sync {
    fn kind(&self) -> RuntimeKind;

    fn instantiate<'a>(
        &'a self,
        package: ResolvedPackage,
        entry: Entry,
        context: ContextHandle,
    ) -> LoaderFuture<'a, Box<dyn RuntimeHandle>>;

    /// Imports source configuration. Managed runtimes retain it until dependencies
    /// are ready; eager runtimes resolve it before constructing their handle.
    fn instantiate_configured<'a>(
        &'a self,
        package: ResolvedPackage,
        mut entry: Entry,
        context: ContextHandle,
        config_scope: ConfigScope,
    ) -> LoaderFuture<'a, Box<dyn RuntimeHandle>> {
        Box::pin(async move {
            entry.evaluate_config(&config_scope)?;
            self.instantiate(package, entry, context).await
        })
    }
}

/// Optional source-change bridge. It is kept at the loader boundary so runtime
/// implementations need not depend on an HMR transport.
pub trait HmrDriver: Send + Sync {
    fn invalidate<'a>(&'a self, package: &'a ResolvedPackage) -> LoaderFuture<'a, ()>;
}

/// Configuration for [`EntryTreeWatcher`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntryTreeWatcherOptions {
    /// Interval between source reads while idle. After an in-flight transaction
    /// settles, the newest source is read within one additional interval.
    pub poll_interval: Duration,
    /// Maximum source size in bytes; larger files are rejected without reload.
    pub max_source_bytes: usize,
}

impl Default for EntryTreeWatcherOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            max_source_bytes: 4 * 1024 * 1024,
        }
    }
}

/// Consumer-visible state of an [`EntryTreeWatcher`].
#[derive(Clone, Debug, PartialEq)]
pub struct EntryTreeWatcherSnapshot {
    pub tree: EntryTree,
    pub committed_updates: u64,
    pub rejected_updates: u64,
    pub last_error: Option<LoaderError>,
    pub running: bool,
}

struct EntryTreeWatcherState {
    snapshot: EntryTreeWatcherSnapshot,
    context: ContextHandle,
}

struct EntryTreeWatcherControl {
    state: Mutex<EntryTreeWatcherState>,
    stop: tokio::sync::Notify,
    stopping: std::sync::atomic::AtomicBool,
}

/// Owns a [`Loader`] and keeps it synchronized with one JSON or YAML entry-tree
/// file. Updates are serialized through `Loader::replace`; rejected candidates
/// leave the last committed tree live. Missing/unreadable files are errors, not
/// requests to unload; write an empty entry tree to explicitly remove plugins.
///
/// The watcher uses bounded polling rather than a platform-specific filesystem
/// dependency. [`Self::stop`] wakes and joins its sole worker, waits for an
/// in-flight transaction, and unloads the committed tree. Dropping the watcher
/// performs the same join so it cannot detach an orphan worker.
/// Start/stop/Drop are blocking joins; async callers should use `spawn_blocking`
/// when plugin work needs their caller runtime to make progress.
pub struct EntryTreeWatcher {
    path: PathBuf,
    control: Arc<EntryTreeWatcherControl>,
    worker: Option<JoinHandle<LoaderResult<()>>>,
    stop_result: Option<LoaderResult<()>>,
}

impl std::fmt::Debug for EntryTreeWatcher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EntryTreeWatcher")
            .field("path", &self.path)
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl EntryTreeWatcher {
    /// Starts watching `path` and waits until its current contents have been
    /// applied or rejected. A rejected initial candidate is exposed in
    /// [`Self::snapshot`] and remains eligible for later recovery.
    pub fn start(
        loader: Loader,
        path: impl Into<PathBuf>,
        options: EntryTreeWatcherOptions,
    ) -> LoaderResult<Self> {
        if options.poll_interval.is_zero()
            || options.max_source_bytes == 0
            || options.max_source_bytes.checked_add(1).is_none()
        {
            return Err(LoaderError::Validation(
                "watch interval and source byte limit must be positive and bounded".into(),
            ));
        }
        let control = Arc::new(EntryTreeWatcherControl {
            state: Mutex::new(EntryTreeWatcherState {
                snapshot: EntryTreeWatcherSnapshot {
                    tree: loader.tree().clone(),
                    committed_updates: 0,
                    rejected_updates: 0,
                    last_error: None,
                    running: true,
                },
                context: loader.context(),
            }),
            stop: tokio::sync::Notify::new(),
            stopping: std::sync::atomic::AtomicBool::new(false),
        });
        let worker_control = Arc::clone(&control);
        let path = path.into();
        let worker_path = path.clone();
        let (ready_tx, ready_rx) = mpsc::sync_channel(0);
        let worker = thread::Builder::new()
            .name("tessivum-entry-tree-watcher".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| {
                        LoaderError::Validation(format!(
                            "failed to create entry-tree watcher runtime: {error}"
                        ))
                    })?;
                runtime.block_on(run_entry_tree_watcher(
                    loader,
                    worker_path,
                    options,
                    worker_control,
                    ready_tx,
                ))
            })
            .map_err(|error| {
                LoaderError::Validation(format!("failed to start entry-tree watcher: {error}"))
            })?;
        let mut watcher = Self {
            path,
            control,
            worker: Some(worker),
            stop_result: None,
        };
        if ready_rx.recv().is_err() {
            watcher.stop()?;
            return Err(LoaderError::Validation(
                "entry-tree watcher stopped during initial refresh".into(),
            ));
        }
        Ok(watcher)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn snapshot(&self) -> EntryTreeWatcherSnapshot {
        lock_watcher(&self.control.state).snapshot.clone()
    }

    /// Returns the context belonging to the last committed tree.
    pub fn context(&self) -> ContextHandle {
        lock_watcher(&self.control.state).context.clone()
    }

    /// Stops once, joins the worker, and returns the same result on later calls.
    pub fn stop(&mut self) -> LoaderResult<()> {
        if let Some(result) = &self.stop_result {
            return result.clone();
        }
        self.control.stopping.store(true, Ordering::Release);
        self.control.stop.notify_one();
        let result = match self.worker.take() {
            Some(worker) => worker.join().unwrap_or_else(|_| {
                Err(LoaderError::Validation(
                    "entry-tree watcher worker panicked".into(),
                ))
            }),
            None => Ok(()),
        };
        self.stop_result = Some(result.clone());
        result
    }
}

impl Drop for EntryTreeWatcher {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[derive(PartialEq)]
enum WatchedSource {
    Present(Vec<u8>),
    Unreadable(String),
}

struct LiveEntry {
    entry: Entry,
    handle: Box<dyn RuntimeHandle>,
}

/// Transactional owner of the live entry tree.
pub struct Loader {
    resolver: Arc<dyn PackageResolver>,
    runtimes: BTreeMap<RuntimeKind, Arc<dyn LoaderRuntime>>,
    tree: EntryTree,
    live: Vec<LiveEntry>,
    context: ContextHandle,
    config_scope: ConfigScope,
    trace: Vec<TraceEvent>,
}

impl std::fmt::Debug for Loader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Loader")
            .field("entries", &self.tree.entries().len())
            .field("live_entries", &self.live.len())
            .finish()
    }
}

impl Loader {
    pub fn try_new(
        resolver: Arc<dyn PackageResolver>,
        runtimes: impl IntoIterator<Item = Arc<dyn LoaderRuntime>>,
    ) -> LoaderResult<Self> {
        let mut by_kind = BTreeMap::new();
        for runtime in runtimes {
            let kind = runtime.kind();
            if by_kind.insert(kind, runtime).is_some() {
                return Err(LoaderError::Validation(format!(
                    "more than one runtime registered for {kind:?}"
                )));
            }
        }
        Ok(Self {
            resolver,
            runtimes: by_kind,
            tree: EntryTree::default(),
            live: Vec::new(),
            context: ContextHandle::root(),
            config_scope: ConfigScope::default(),
            trace: Vec::new(),
        })
    }

    pub fn with_context(mut self, context: ContextHandle) -> Self {
        self.context = context;
        self
    }

    pub fn with_config_scope(mut self, scope: ConfigScope) -> Self {
        self.config_scope = scope;
        self
    }

    pub fn tree(&self) -> &EntryTree {
        &self.tree
    }

    pub fn trace(&self) -> &[TraceEvent] {
        &self.trace
    }

    pub fn context(&self) -> ContextHandle {
        self.context.clone()
    }

    pub async fn load(&mut self, tree: EntryTree) -> LoaderResult<()> {
        if !self.live.is_empty() {
            return Err(LoaderError::Validation(
                "loader already has a committed tree; use replace or update".into(),
            ));
        }
        self.replace(tree).await
    }

    pub async fn update(&mut self, patches: &[Patch]) -> LoaderResult<()> {
        let candidate = self.tree.apply_all(patches)?;
        self.replace(candidate).await
    }

    /// Imports and starts a detached candidate before changing the current
    /// tree. On any failure the old tree remains the committed tree.
    pub async fn replace(&mut self, tree: EntryTree) -> LoaderResult<()> {
        tree.validate()?;
        let candidate_context = self.detached_context();
        let mut candidate = match self
            .import_candidates(&tree, candidate_context.clone())
            .await
        {
            Ok(candidate) => candidate,
            Err(error) => {
                self.rolled_back(error.to_string());
                return Err(error);
            }
        };
        let removal_errors = dispose_all(&mut self.live).await;
        if !removal_errors.is_empty() {
            let mut rollback = dispose_all(&mut candidate).await;
            self.restore_committed_tree(&mut rollback).await;

            let error = LoaderError::transaction(LoaderError::aggregate(removal_errors), rollback);
            self.rolled_back(error.to_string());
            return Err(error);
        }
        self.live.clear();

        let start_errors = activate_all(&mut candidate).await;
        if start_errors.iter().any(Result::is_err) {
            let failure = LoaderError::aggregate(start_errors.into_iter().filter_map(Result::err));
            let mut rollback = dispose_all(&mut candidate).await;
            self.restore_committed_tree(&mut rollback).await;

            let error = LoaderError::transaction(failure, rollback);
            self.rolled_back(error.to_string());
            return Err(error);
        }

        self.live = candidate;
        self.tree = tree;
        self.context = candidate_context;
        self.committed();
        Ok(())
    }

    pub async fn unload(&mut self) -> LoaderResult<()> {
        let errors = dispose_all(&mut self.live).await;
        if errors.is_empty() {
            self.live.clear();
            self.tree = EntryTree::default();
            Ok(())
        } else {
            Err(LoaderError::aggregate(errors))
        }
    }

    pub fn persist(&self, path: impl AsRef<Path>) -> LoaderResult<()> {
        self.tree.persist(path)
    }

    async fn import_candidates(
        &self,
        tree: &EntryTree,
        context: ContextHandle,
    ) -> LoaderResult<Vec<LiveEntry>> {
        let resolver = &*self.resolver;
        let runtimes = &self.runtimes;
        let scope = &self.config_scope;
        let futures = tree
            .active_entries()
            .into_iter()
            .map(|entry| {
                Box::pin(import_entry(
                    resolver,
                    runtimes,
                    scope,
                    entry,
                    context.clone(),
                )) as LoaderFuture<'_, LiveEntry>
            })
            .collect();
        let results = all_settle(futures).await;
        let mut candidates = Vec::with_capacity(results.len());
        let mut errors = Vec::new();
        for result in results {
            match result {
                Ok(candidate) => candidates.push(candidate),
                Err(error) => errors.push(error),
            }
        }
        if errors.is_empty() {
            Ok(candidates)
        } else {
            let rollback = dispose_all(&mut candidates).await;
            Err(LoaderError::transaction(
                LoaderError::aggregate(errors),
                rollback,
            ))
        }
    }

    fn detached_context(&self) -> ContextHandle {
        self.context.with_scope(Scope::root())
    }

    async fn restore_committed(&self, context: ContextHandle) -> LoaderResult<Vec<LiveEntry>> {
        let mut restored = self.import_candidates(&self.tree, context).await?;
        let activation_errors = activate_all(&mut restored).await;
        if activation_errors.is_empty() {
            Ok(restored)
        } else {
            let failure =
                LoaderError::aggregate(activation_errors.into_iter().filter_map(Result::err));
            let rollback = dispose_all(&mut restored).await;
            Err(LoaderError::transaction(failure, rollback))
        }
    }

    async fn restore_committed_tree(&mut self, rollback: &mut Vec<LoaderError>) {
        self.live.clear();
        let restoration_context = self.detached_context();
        match self.restore_committed(restoration_context.clone()).await {
            Ok(restored) => {
                self.live = restored;
                self.context = restoration_context;
            }
            Err(error) => collect_rollback_errors(error, rollback),
        }
    }

    fn committed(&mut self) {
        self.trace.push(TraceEvent {
            kind: TraceEventKind::ConfigCommitted,
            subject: None,
            from: None,
            to: None,
            label: None,
            phase: None,
            value: None,
            error: None,
        });
    }

    fn rolled_back(&mut self, error: String) {
        self.trace.push(TraceEvent {
            kind: TraceEventKind::ConfigRolledBack,
            subject: None,
            from: None,
            to: None,
            label: None,
            phase: None,
            value: None,
            error: Some(error),
        });
    }
}

async fn run_entry_tree_watcher(
    mut loader: Loader,
    path: PathBuf,
    options: EntryTreeWatcherOptions,
    control: Arc<EntryTreeWatcherControl>,
    ready: mpsc::SyncSender<()>,
) -> LoaderResult<()> {
    let mut observed = observe_source(&path, options.max_source_bytes);
    apply_watched_source(&mut loader, &observed, &control).await;
    let _ = ready.send(());
    // ponytail: whole-file polling coalesces intermediate writes; use OS notifications
    // if idle I/O for many watched files becomes material. The runtime stays driven.
    while !control.stopping.load(Ordering::Acquire) {
        let _ = tokio::time::timeout(options.poll_interval, control.stop.notified()).await;
        if control.stopping.load(Ordering::Acquire) {
            break;
        }
        let source = observe_source(&path, options.max_source_bytes);
        if source != observed {
            apply_watched_source(&mut loader, &source, &control).await;
            observed = source;
        }
    }
    let result = loader.unload().await;
    let mut state = lock_watcher(&control.state);
    state.snapshot.running = false;
    state.snapshot.tree = loader.tree().clone();
    state.context = loader.context();
    if let Err(error) = &result {
        state.snapshot.last_error = Some(error.clone());
    }
    result
}

fn observe_source(path: &Path, limit: usize) -> WatchedSource {
    let read = || -> std::io::Result<Vec<u8>> {
        if !fs::metadata(path)?.is_file() {
            return Err(std::io::Error::other("watch source must be a regular file"));
        }
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            // Avoid blocking if a regular source is swapped for a FIFO before open.
            options.custom_flags(libc::O_NONBLOCK);
        }
        let file = options.open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > limit as u64 {
            return Err(std::io::Error::other(
                "watch source is not a regular file within its byte limit",
            ));
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            return Err(std::io::Error::other("watch source exceeds its byte limit"));
        }
        Ok(bytes)
    };
    match read() {
        Ok(bytes) => WatchedSource::Present(bytes),
        Err(error) => WatchedSource::Unreadable(error.to_string()),
    }
}

async fn apply_watched_source(
    loader: &mut Loader,
    source: &WatchedSource,
    control: &EntryTreeWatcherControl,
) {
    let candidate = match source {
        WatchedSource::Present(bytes) => std::str::from_utf8(bytes)
            .map_err(|error| LoaderError::Parse(error.to_string()))
            .and_then(parse_entry_tree),
        WatchedSource::Unreadable(error) => Err(LoaderError::Persistence(error.clone())),
    };
    let result = match candidate {
        Ok(tree) => loader.replace(tree).await,
        Err(error) => Err(error),
    };
    let mut state = lock_watcher(&control.state);
    state.context = loader.context();
    match result {
        Ok(()) => {
            state.snapshot.tree = loader.tree().clone();
            state.snapshot.committed_updates = state.snapshot.committed_updates.saturating_add(1);
        }
        Err(error) => {
            state.snapshot.rejected_updates = state.snapshot.rejected_updates.saturating_add(1);
            state.snapshot.last_error = Some(error);
        }
    }
}

fn lock_watcher<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

async fn import_entry(
    resolver: &dyn PackageResolver,
    runtimes: &BTreeMap<RuntimeKind, Arc<dyn LoaderRuntime>>,
    scope: &ConfigScope,
    entry: Entry,
    context: ContextHandle,
) -> LoaderResult<LiveEntry> {
    let package = resolver
        .resolve(&entry.package, entry.options.runtime)
        .await
        .map_err(|error| error.in_entry("import", &entry))?;
    let runtime = runtimes
        .get(&entry.options.runtime)
        .ok_or_else(|| LoaderError::Runtime {
            stage: "import",
            entry: entry.options.id.clone(),
            name: entry.options.name.clone(),
            message: format!("no {:?} runtime is registered", entry.options.runtime),
        })?;
    let handle = runtime
        .instantiate_configured(package, entry.clone(), context, scope.clone())
        .await
        .map_err(|error| error.in_entry("import", &entry))?;
    Ok(LiveEntry { entry, handle })
}

async fn activate_all(entries: &mut [LiveEntry]) -> Vec<LoaderResult<()>> {
    let mut pending = (0..entries.len()).collect::<Vec<_>>();
    let mut errors = Vec::new();

    while !pending.is_empty() {
        let pending_set = pending.iter().copied().collect::<BTreeSet<_>>();
        let results = all_settle(
            entries
                .iter_mut()
                .enumerate()
                .filter_map(|(index, entry)| {
                    pending_set
                        .contains(&index)
                        .then(|| entry.handle.activation())
                })
                .collect(),
        )
        .await;
        let mut next = Vec::new();
        let mut progressed = false;

        for (index, result) in pending.into_iter().zip(results) {
            match result {
                Ok(ActivationState::Active) => progressed = true,
                Ok(ActivationState::Pending) => next.push(index),
                Err(error) => errors.push(error),
            }
        }
        // Pending is a managed dependency wait, not an activation failure.
        // A later provider notification can activate the retained handle.
        if !errors.is_empty() || !progressed {
            break;
        }
        pending = next;
    }

    errors.into_iter().map(Err).collect()
}

async fn dispose_all(entries: &mut [LiveEntry]) -> Vec<LoaderError> {
    let mut errors = Vec::new();
    for entry in entries.iter_mut().rev() {
        if let Err(error) = entry.handle.dispose().await {
            errors.push(error.in_entry("dispose", &entry.entry));
        }
    }
    errors
}

fn collect_rollback_errors(error: LoaderError, rollback: &mut Vec<LoaderError>) {
    match error {
        LoaderError::Aggregate(errors) => {
            for error in errors {
                collect_rollback_errors(error, rollback);
            }
        }
        LoaderError::Transaction {
            failure,
            rollback: errors,
        } => {
            collect_rollback_errors(*failure, rollback);
            for error in errors {
                collect_rollback_errors(error, rollback);
            }
        }
        error => rollback.push(error),
    }
}

async fn all_settle<'a, T>(futures: Vec<LoaderFuture<'a, T>>) -> Vec<LoaderResult<T>> {
    let mut futures = futures.into_iter().map(Some).collect::<Vec<_>>();
    let mut results = std::iter::repeat_with(|| None)
        .take(futures.len())
        .collect::<Vec<Option<LoaderResult<T>>>>();
    poll_fn(|context| {
        let mut pending = 0;
        for index in 0..futures.len() {
            let Some(future) = futures[index].as_mut() else {
                continue;
            };
            match future.as_mut().poll(context) {
                Poll::Ready(result) => {
                    futures[index] = None;
                    results[index] = Some(result);
                }
                Poll::Pending => pending += 1,
            }
        }
        if pending == 0 {
            Poll::Ready(
                results
                    .iter_mut()
                    .map(|result| result.take().expect("settled future has a result"))
                    .collect(),
            )
        } else {
            Poll::Pending
        }
    })
    .await
}

fn parse_tree_value(value: Value) -> LoaderResult<EntryTree> {
    if value.is_array() {
        let entries =
            serde_json::from_value(value).map_err(|error| LoaderError::Parse(error.to_string()))?;
        return Ok(EntryTree {
            entries,
            groups: Vec::new(),
        });
    }
    let mut object = value
        .as_object()
        .cloned()
        .ok_or_else(|| LoaderError::Parse("entry tree must be an object or entry list".into()))?;
    if !object.contains_key("entries") {
        if let Some(entries) = object.remove("plugins") {
            object.insert("entries".into(), normalize_entries(entries)?);
        }
    } else if let Some(entries) = object.remove("entries") {
        object.insert("entries".into(), normalize_entries(entries)?);
    }
    serde_json::from_value(Value::Object(object))
        .map_err(|error| LoaderError::Parse(error.to_string()))
}

fn normalize_entries(value: Value) -> LoaderResult<Value> {
    let Value::Object(entries) = value else {
        return Ok(value);
    };
    entries
        .into_iter()
        .map(|(id, value)| {
            let mut value = value.as_object().cloned().unwrap_or_default();
            value
                .entry("id")
                .or_insert_with(|| Value::String(id.clone()));
            if !value.contains_key("package") {
                let package = value.get("name").cloned().unwrap_or(Value::String(id));
                value.insert("package".into(), package);
            }
            Ok(Value::Object(value))
        })
        .collect::<LoaderResult<Vec<_>>>()
        .map(Value::Array)
}

fn validate_config(value: &Value) -> LoaderResult<()> {
    match value {
        Value::Array(values) => {
            for value in values {
                validate_config(value)?;
            }
        }
        Value::Object(values) => {
            if values.keys().any(|key| key == "$js" || key == "!!js") {
                return Err(LoaderError::Expression(
                    "arbitrary JavaScript expressions are not supported".into(),
                ));
            }
            if let Some(expression) = values.get("$expr") {
                if values.len() != 1 {
                    return Err(LoaderError::Expression(
                        "an expression object may contain only $expr".into(),
                    ));
                }
                ConfigExpression::parse(
                    expression
                        .as_str()
                        .ok_or_else(|| LoaderError::Expression("$expr must be a string".into()))?,
                )?;
            } else {
                for value in values.values() {
                    validate_config(value)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn collect_entries<'a>(root: &'a [Entry], groups: &'a [EntryGroup], output: &mut Vec<&'a Entry>) {
    output.extend(root);
    for group in groups {
        output.extend(&group.entries);
        collect_entries(&[], &group.groups, output);
    }
}

fn collect_active_entries(
    root: &[Entry],
    groups: &[EntryGroup],
    parent_disabled: bool,
    output: &mut Vec<Entry>,
) {
    output.extend(
        root.iter()
            .filter(|entry| !entry.options.disabled && !parent_disabled)
            .cloned(),
    );
    for group in groups {
        let disabled = parent_disabled || group.disabled;
        output.extend(
            group
                .entries
                .iter()
                .filter(|entry| !entry.options.disabled && !disabled)
                .cloned(),
        );
        collect_active_entries(&[], &group.groups, disabled, output);
    }
}

fn find_entry_mut<'a>(tree: &'a mut EntryTree, id: &EntryId) -> Option<&'a mut Entry> {
    find_in_entries_mut(&mut tree.entries, id).or_else(|| find_in_groups_mut(&mut tree.groups, id))
}

fn find_in_entries_mut<'a>(entries: &'a mut [Entry], id: &EntryId) -> Option<&'a mut Entry> {
    entries.iter_mut().find(|entry| entry.options.id == *id)
}

fn find_in_groups_mut<'a>(groups: &'a mut [EntryGroup], id: &EntryId) -> Option<&'a mut Entry> {
    for group in groups {
        if let Some(entry) = find_in_entries_mut(&mut group.entries, id) {
            return Some(entry);
        }
        if let Some(entry) = find_in_groups_mut(&mut group.groups, id) {
            return Some(entry);
        }
    }
    None
}

fn remove_entry(tree: &mut EntryTree, id: &EntryId) -> LoaderResult<Entry> {
    remove_from_entries(&mut tree.entries, id)
        .or_else(|| remove_from_groups(&mut tree.groups, id))
        .ok_or_else(|| LoaderError::MissingEntry(id.clone()))
}

fn remove_from_entries(entries: &mut Vec<Entry>, id: &EntryId) -> Option<Entry> {
    entries
        .iter()
        .position(|entry| entry.options.id == *id)
        .map(|index| entries.remove(index))
}

fn remove_from_groups(groups: &mut [EntryGroup], id: &EntryId) -> Option<Entry> {
    for group in groups {
        if let Some(entry) = remove_from_entries(&mut group.entries, id) {
            return Some(entry);
        }
        if let Some(entry) = remove_from_groups(&mut group.groups, id) {
            return Some(entry);
        }
    }
    None
}

fn insert_entry(tree: &mut EntryTree, entry: Entry, before: Option<&EntryId>) -> LoaderResult<()> {
    let Some(before) = before else {
        tree.entries.push(entry);
        return Ok(());
    };
    if let Some(position) = tree
        .entries
        .iter()
        .position(|current| current.options.id == *before)
    {
        tree.entries.insert(position, entry);
        return Ok(());
    }
    if insert_before_in_groups(&mut tree.groups, entry, before) {
        return Ok(());
    }
    Err(LoaderError::MissingEntry(before.clone()))
}

fn insert_before_in_groups(groups: &mut [EntryGroup], entry: Entry, before: &EntryId) -> bool {
    for group in groups {
        if let Some(position) = group
            .entries
            .iter()
            .position(|current| current.options.id == *before)
        {
            group.entries.insert(position, entry);
            return true;
        }
        if insert_before_in_groups(&mut group.groups, entry.clone(), before) {
            return true;
        }
    }
    false
}

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

fn is_identifier(value: &str) -> bool {
    !value.is_empty()
        && !matches!(value.as_bytes().first(), Some(b'.' | b'-' | b'_' | b':'))
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b':'))
}
