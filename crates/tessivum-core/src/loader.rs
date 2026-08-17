use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    future::{poll_fn, Future},
    io::Write,
    path::Path,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::Poll,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    evaluate_config, ConfigExpression, ConfigScope, ContextHandle, LoaderError, TraceEvent,
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

/// A live plugin instance whose effects are still confined to its candidate
/// context until `activate` succeeds.
pub trait RuntimeHandle: Send {
    fn activate<'a>(&'a mut self) -> LoaderFuture<'a, ()>;
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
}

/// Optional source-change bridge. It is kept at the loader boundary so runtime
/// implementations need not depend on an HMR transport.
pub trait HmrDriver: Send + Sync {
    fn invalidate<'a>(&'a self, package: &'a ResolvedPackage) -> LoaderFuture<'a, ()>;
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
        let candidate_context = ContextHandle::root();
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
        let start_errors = activate_all(&mut candidate).await;
        if start_errors.iter().any(Result::is_err) {
            let failure = LoaderError::aggregate(start_errors.into_iter().filter_map(Result::err));
            let rollback = dispose_all(&mut candidate).await;
            let error = LoaderError::transaction(failure, rollback);
            self.rolled_back(error.to_string());
            return Err(error);
        }

        let removal_errors = dispose_all(&mut self.live).await;
        if !removal_errors.is_empty() {
            let restored = activate_all(&mut self.live).await;
            let mut rollback = dispose_all(&mut candidate).await;
            rollback.extend(restored.into_iter().filter_map(Result::err));
            let error = LoaderError::transaction(LoaderError::aggregate(removal_errors), rollback);
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

async fn import_entry(
    resolver: &dyn PackageResolver,
    runtimes: &BTreeMap<RuntimeKind, Arc<dyn LoaderRuntime>>,
    scope: &ConfigScope,
    mut entry: Entry,
    context: ContextHandle,
) -> LoaderResult<LiveEntry> {
    entry.options.config = evaluate_config(&entry.options.config, scope)
        .map_err(|error| error.in_entry("import", &entry))?;
    entry.options.intercept = evaluate_config(&entry.options.intercept, scope)
        .map_err(|error| error.in_entry("import", &entry))?;
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
        .instantiate(package, entry.clone(), context)
        .await
        .map_err(|error| error.in_entry("import", &entry))?;
    Ok(LiveEntry { entry, handle })
}

async fn activate_all(entries: &mut [LiveEntry]) -> Vec<LoaderResult<()>> {
    let futures = entries
        .iter_mut()
        .map(|entry| entry.handle.activate())
        .collect::<Vec<_>>();
    all_settle(futures).await
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
