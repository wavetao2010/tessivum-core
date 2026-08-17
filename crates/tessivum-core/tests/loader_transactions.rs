use std::{
    collections::BTreeSet,
    fs,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use serde_json::json;
use tessivum_core::{
    ContextHandle, Entry, EntryGroup, EntryId, EntryOptions, EntryTree, HmrDriver, Loader,
    LoaderError, LoaderFuture, LoaderRuntime, PackageResolver, Patch, ResolvedPackage,
    RuntimeHandle, RuntimeKind, ServiceKey, TraceEventKind,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

fn id(value: &str) -> EntryId {
    EntryId::new(value).expect("test identifier is valid")
}

fn entry(value: &str) -> Entry {
    Entry {
        package: format!("test/{value}"),
        options: EntryOptions {
            id: id(value),
            name: Some(format!("plugin.{value}")),
            runtime: RuntimeKind::Native,
            config: json!({"version": value}),
            inject: Vec::new(),
            isolate: Vec::new(),
            intercept: json!({}),
            disabled: false,
            group: None,
        },
    }
}

fn tree(entries: &[&str]) -> EntryTree {
    EntryTree {
        entries: entries.iter().map(|value| entry(value)).collect(),
        groups: Vec::new(),
    }
}

fn entry_ids(tree: &EntryTree) -> Vec<EntryId> {
    tree.entries
        .iter()
        .map(|entry| entry.options.id.clone())
        .collect()
}

#[test]
fn ordered_patches_clone_then_insert_and_target_later_entries() {
    let original = tree(&["first", "second", "third"]);
    let candidate = original
        .apply_all(&[
            Patch::Insert {
                entry: entry("inserted"),
                before: Some(id("second")),
            },
            Patch::UpdateConfig {
                id: id("inserted"),
                config: json!({"patched": true}),
            },
            Patch::Move {
                id: id("third"),
                before: Some(id("first")),
            },
            Patch::Update {
                id: id("second"),
                entry: Entry {
                    package: "test/replaced".into(),
                    options: EntryOptions {
                        config: json!({"replacement": true}),
                        ..entry("second").options
                    },
                },
            },
            Patch::Remove { id: id("first") },
        ])
        .expect("ordered patch set produces a validated candidate tree");

    assert_eq!(
        entry_ids(&candidate),
        vec![id("third"), id("inserted"), id("second")],
        "an inserted entry is immediately targetable by a later patch and moves are stable"
    );
    assert_eq!(
        candidate.entries[1].options.config,
        json!({"patched": true}),
        "config patch replaces the complete config value rather than shallow-merging it"
    );
    assert_eq!(candidate.entries[2].package, "test/replaced");
    assert_eq!(
        entry_ids(&original),
        vec![id("first"), id("second"), id("third")],
        "patch application changes the detached candidate and never mutates the source tree"
    );
}

#[test]
fn malformed_candidate_patch_never_changes_the_last_known_good_tree() {
    let original = tree(&["stable"]);
    assert!(
        original
            .apply_all(&[
                Patch::UpdateConfig {
                    id: id("stable"),
                    config: json!({"candidate": "changed"}),
                },
                Patch::Remove { id: id("missing") },
            ])
            .is_err(),
        "a patch that targets no entry is rejected before it can become active"
    );
    assert_eq!(
        original.entries[0].options.config,
        json!({"version": "stable"}),
        "the committed tree remains intact when a detached candidate fails validation"
    );
}

#[test]
fn nested_groups_validate_and_atomic_persistence_replaces_only_committed_trees() {
    let tree = EntryTree {
        entries: vec![entry("root")],
        groups: vec![EntryGroup {
            id: "root".into(),
            entries: vec![entry("child")],
            groups: vec![EntryGroup {
                id: "root.child".into(),
                entries: vec![entry("leaf")],
                groups: Vec::new(),
                disabled: false,
            }],
            disabled: false,
        }],
    };
    tree.validate()
        .expect("nested groups form one valid detached tree");
    assert_eq!(
        tree.active_entries().len(),
        3,
        "active entry traversal includes root entries and each enabled nested group in stable order"
    );

    let directory = std::env::temp_dir().join(format!(
        "tessivum-loader-persist-{}-{}",
        std::process::id(),
        NEXT_PATH.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&directory).expect("test persistence directory is created");
    let path = directory.join("loader.json");
    fs::write(&path, "obsolete").expect("old configuration is present before replacement");
    tree.persist(&path)
        .expect("validated tree is persisted through an atomic replacement");
    let persisted = fs::read_to_string(&path).expect("committed configuration remains readable");
    let restored = tessivum_core::parse_entry_tree(&persisted)
        .expect("atomic replacement leaves a complete parseable configuration file");
    assert_eq!(restored.active_entries().len(), 3);

    assert_eq!(
        fs::read_dir(&directory)
            .expect("persistence directory is readable")
            .count(),
        1,
        "successful atomic replacement leaves no temporary artifact beside the committed file"
    );
    fs::remove_dir_all(directory).expect("test persistence directory is removed");
}

#[derive(Default)]
struct MockState {
    calls: Mutex<Vec<String>>,
    fail_resolve: Mutex<BTreeSet<String>>,
    fail_instantiate: Mutex<BTreeSet<String>>,
    fail_activate: Mutex<BTreeSet<String>>,
    fail_dispose: Mutex<BTreeSet<String>>,
    required_service: Mutex<Option<ServiceKey>>,
    yield_activations: bool,
}

impl MockState {
    fn record(&self, call: impl Into<String>) {
        self.calls
            .lock()
            .expect("mock call log is available")
            .push(call.into());
    }

    fn calls(&self) -> Vec<String> {
        self.calls
            .lock()
            .expect("mock call log is available")
            .clone()
    }

    fn clear_calls(&self) {
        self.calls
            .lock()
            .expect("mock call log is available")
            .clear();
    }

    fn fails(set: &Mutex<BTreeSet<String>>, id: &EntryId) -> bool {
        set.lock()
            .expect("mock failure configuration is available")
            .contains(id.as_str())
    }
}

struct MockResolver {
    state: Arc<MockState>,
}

impl PackageResolver for MockResolver {
    fn resolve<'a>(
        &'a self,
        specifier: &'a str,
        _runtime: RuntimeKind,
    ) -> LoaderFuture<'a, ResolvedPackage> {
        let state = Arc::clone(&self.state);
        let specifier = specifier.to_owned();
        Box::pin(async move {
            state.record(format!("resolve:{specifier}"));
            if state
                .fail_resolve
                .lock()
                .expect("mock failure configuration is available")
                .contains(&specifier)
            {
                return Err(LoaderError::Validation(format!(
                    "resolver rejected {specifier}"
                )));
            }
            Ok(ResolvedPackage {
                location: format!("resolved:{specifier}"),
                specifier,
            })
        })
    }
}

struct MockRuntime {
    state: Arc<MockState>,
    kind: RuntimeKind,
}

impl LoaderRuntime for MockRuntime {
    fn kind(&self) -> RuntimeKind {
        self.kind
    }

    fn instantiate<'a>(
        &'a self,
        package: ResolvedPackage,
        entry: Entry,
        context: ContextHandle,
    ) -> LoaderFuture<'a, Box<dyn RuntimeHandle>> {
        let state = Arc::clone(&self.state);
        let required_service = state
            .required_service
            .lock()
            .expect("mock service requirement is available")
            .clone();
        Box::pin(async move {
            state.record(format!(
                "instantiate:{}:{}",
                entry.options.id, package.location
            ));
            if MockState::fails(&state.fail_instantiate, &entry.options.id) {
                return Err(LoaderError::Validation(format!(
                    "runtime rejected {}",
                    entry.options.id
                )));
            }
            if let Some(service) = required_service {
                if context
                    .get::<String>(&service)
                    .map_err(|error| LoaderError::Validation(error.to_string()))?
                    .is_none()
                {
                    return Err(LoaderError::Validation(format!(
                        "host service {service} is unavailable"
                    )));
                }
                state.record(format!("context:service:{service}"));
            }
            let handle: Box<dyn RuntimeHandle> = Box::new(MockHandle {
                id: entry.options.id,
                state,
                disposed: false,
            });
            Ok(handle)
        })
    }
}

struct MockHandle {
    id: EntryId,
    state: Arc<MockState>,
    disposed: bool,
}

impl RuntimeHandle for MockHandle {
    fn activate<'a>(&'a mut self) -> LoaderFuture<'a, ()> {
        let id = self.id.clone();
        let state = Arc::clone(&self.state);
        let disposed = self.disposed;
        Box::pin(async move {
            state.record(format!("activate:start:{id}"));
            if disposed {
                state.record(format!("activate:disposed:{id}"));
                return Err(LoaderError::Validation(format!(
                    "activation used disposed handle for {id}"
                )));
            }
            if state.yield_activations {
                tokio::task::yield_now().await;
            }
            if MockState::fails(&state.fail_activate, &id) {
                state.record(format!("activate:failed:{id}"));
                Err(LoaderError::Validation(format!(
                    "activation failed for {id}"
                )))
            } else {
                state.record(format!("activate:ok:{id}"));
                Ok(())
            }
        })
    }

    fn dispose<'a>(&'a mut self) -> LoaderFuture<'a, ()> {
        self.disposed = true;
        let id = self.id.clone();
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            state.record(format!("dispose:{id}"));
            if MockState::fails(&state.fail_dispose, &id) {
                Err(LoaderError::Validation(format!("disposal failed for {id}")))
            } else {
                Ok(())
            }
        })
    }
}

struct MockHmr {
    state: Arc<MockState>,
}

impl HmrDriver for MockHmr {
    fn invalidate<'a>(&'a self, package: &'a ResolvedPackage) -> LoaderFuture<'a, ()> {
        let state = Arc::clone(&self.state);
        let specifier = package.specifier.clone();
        Box::pin(async move {
            state.record(format!("hmr:{specifier}"));
            Ok(())
        })
    }
}

fn loader(state: Arc<MockState>) -> Loader {
    let resolver: Arc<dyn PackageResolver> = Arc::new(MockResolver {
        state: Arc::clone(&state),
    });
    let runtime: Arc<dyn LoaderRuntime> = Arc::new(MockRuntime {
        state,
        kind: RuntimeKind::Native,
    });
    Loader::try_new(resolver, [runtime]).expect("one native runtime makes a valid loader")
}

fn grouped_tree(entries: &[&str]) -> EntryTree {
    EntryTree {
        entries: Vec::new(),
        groups: vec![EntryGroup {
            id: "batch".into(),
            entries: entries.iter().map(|value| entry(value)).collect(),
            groups: Vec::new(),
            disabled: false,
        }],
    }
}

#[tokio::test]
async fn loader_imports_candidate_then_disposes_old_before_activation() {
    let state = Arc::new(MockState::default());
    let mut loader = loader(Arc::clone(&state));
    loader
        .load(tree(&["old"]))
        .await
        .expect("initial tree loads");
    state.clear_calls();

    loader
        .replace(tree(&["new"]))
        .await
        .expect("replacement candidate imports and activates");
    let calls = state.calls();
    let candidate_import = calls
        .iter()
        .position(|call| call == "instantiate:new:resolved:test/new")
        .expect("replacement candidate imports");
    let old_disposal = calls
        .iter()
        .position(|call| call == "dispose:old")
        .expect("previous plugin disposes before candidate activation");
    let candidate_activation = calls
        .iter()
        .position(|call| call == "activate:ok:new")
        .expect("replacement candidate activates after old disposal");
    assert!(
        candidate_import < old_disposal,
        "candidate setup stays detached until the committed tree is retired"
    );
    assert!(
        old_disposal < candidate_activation,
        "a candidate cannot activate against services from the retiring tree"
    );
    assert_eq!(entry_ids(loader.tree()), vec![id("new")]);
    assert_eq!(
        loader.trace().last().map(|event| &event.kind),
        Some(&TraceEventKind::ConfigCommitted),
        "only a complete candidate appends a committed configuration trace"
    );

    state.clear_calls();
    loader
        .update(&[Patch::UpdateConfig {
            id: id("new"),
            config: json!({"revision": 2}),
        }])
        .await
        .expect("config-only update commits through the same transaction");
    assert_eq!(
        loader.tree().entries[0].options.config,
        json!({"revision": 2})
    );
    let calls = state.calls();
    assert!(
        calls
            .iter()
            .any(|call| call == "instantiate:new:resolved:test/new"),
        "a config update reimports a detached candidate instead of mutating a live handle"
    );
    assert!(
        calls.iter().any(|call| call == "dispose:new"),
        "the previous configuration retires before the new candidate starts"
    );
}

#[tokio::test]
async fn replacement_disposal_failure_reimports_fresh_old_handles() {
    let state = Arc::new(MockState::default());
    state
        .fail_dispose
        .lock()
        .expect("mock failure configuration is available")
        .insert("old-fails".into());
    let mut loader = loader(Arc::clone(&state));
    loader
        .load(tree(&["old-live", "old-fails"]))
        .await
        .expect("initial tree loads");
    state.clear_calls();

    loader
        .replace(tree(&["new"]))
        .await
        .expect_err("one old disposal rejects the replacement");
    let calls = state.calls();
    let candidate_disposal = calls
        .iter()
        .position(|call| call == "dispose:new")
        .expect("active candidate is disposed before restoration");
    let restoration_import = calls
        .iter()
        .position(|call| call == "instantiate:old-live:resolved:test/old-live")
        .expect("old tree is freshly imported for restoration");
    assert!(
        candidate_disposal < restoration_import,
        "candidate effects are removed before old resources are rebuilt"
    );
    assert!(
        calls.iter().any(|call| call == "dispose:old-live")
            && calls.iter().any(|call| call == "dispose:old-fails"),
        "the replacement attempts every old disposal despite one failure"
    );
    assert_eq!(
        calls
            .iter()
            .filter(|call| *call == "instantiate:old-live:resolved:test/old-live")
            .count(),
        1,
        "the restoration creates one fresh old-live handle after the failed removal"
    );
    assert_eq!(
        calls
            .iter()
            .filter(|call| *call == "instantiate:old-fails:resolved:test/old-fails")
            .count(),
        1,
        "the failed old-fails disposal is restored through a fresh handle too"
    );
    assert!(
        calls.iter().any(|call| call == "activate:ok:old-live")
            && calls.iter().any(|call| call == "activate:ok:old-fails"),
        "fresh old handles reactivate after their failed replacement"
    );
    assert!(
        !calls
            .iter()
            .any(|call| call.starts_with("activate:disposed:")),
        "restoration never activates a handle after it has been disposed"
    );
    assert_eq!(
        entry_ids(loader.tree()),
        vec![id("old-live"), id("old-fails")]
    );
    assert_eq!(
        loader.trace().last().map(|event| &event.kind),
        Some(&TraceEventKind::ConfigRolledBack),
        "a restored old tree still records the rejected replacement"
    );
}

#[tokio::test]
async fn concurrent_group_candidates_all_settle_and_aggregate_rollback_failures() {
    let state = Arc::new(MockState {
        yield_activations: true,
        ..MockState::default()
    });
    state
        .fail_activate
        .lock()
        .expect("mock failure configuration is available")
        .insert("broken".into());
    state
        .fail_dispose
        .lock()
        .expect("mock failure configuration is available")
        .extend(["broken".into(), "settled".into()]);
    let mut loader = loader(Arc::clone(&state));
    loader
        .load(tree(&["old"]))
        .await
        .expect("initial tree loads before candidate failure");
    state.clear_calls();

    let error = loader
        .replace(grouped_tree(&["broken", "settled"]))
        .await
        .expect_err("one candidate activation rolls the whole group back");
    let calls = state.calls();
    let starts = calls
        .iter()
        .enumerate()
        .filter(|(_, call)| *call == "activate:start:broken" || *call == "activate:start:settled")
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let completions = calls
        .iter()
        .enumerate()
        .filter(|(_, call)| {
            call.starts_with("activate:ok:") || call.starts_with("activate:failed:")
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    assert_eq!(
        starts.len(),
        2,
        "every enabled group member starts despite a sibling failure"
    );
    assert!(
        starts.iter().all(|start| *start < completions[0]),
        "candidate group activation starts concurrently before any result is accepted"
    );
    assert_eq!(
        error.rollback_errors().len(),
        2,
        "all rollback disposal errors are retained instead of masking the original activation failure"
    );
    assert!(
        calls.iter().any(|call| call == "dispose:settled")
            && calls.iter().any(|call| call == "dispose:broken"),
        "every candidate handle receives rollback disposal in reverse stable order"
    );
    assert!(
        calls.iter().any(|call| call == "dispose:old"),
        "the committed tree retires before the candidate begins activation"
    );
    assert!(
        calls
            .iter()
            .any(|call| call == "instantiate:old:resolved:test/old")
            && calls.iter().any(|call| call == "activate:ok:old"),
        "a failed candidate activation rebuilds and reactivates the committed tree"
    );
    assert!(
        !calls
            .iter()
            .any(|call| call.starts_with("activate:disposed:")),
        "activation rollback never reuses the retired old handle"
    );
    assert_eq!(entry_ids(loader.tree()), vec![id("old")]);
    assert_eq!(
        loader.trace().last().map(|event| &event.kind),
        Some(&TraceEventKind::ConfigRolledBack),
        "failed candidate transactions leave an explicit rollback trace"
    );
}

#[tokio::test]
async fn import_failure_rolls_back_detached_handles_without_touching_committed_tree() {
    let state = Arc::new(MockState::default());
    state
        .fail_resolve
        .lock()
        .expect("mock failure configuration is available")
        .insert("test/import-fails".into());
    let mut loader = loader(Arc::clone(&state));
    loader
        .load(tree(&["old"]))
        .await
        .expect("initial tree loads");
    state.clear_calls();

    assert!(
        loader
            .replace(tree(&["survivor", "import-fails"]))
            .await
            .is_err(),
        "a resolver failure rejects the detached candidate tree"
    );
    let calls = state.calls();
    assert!(
        calls.iter().any(|call| call == "dispose:survivor"),
        "already-imported candidate handles are rolled back after another import fails"
    );
    assert!(
        !calls.iter().any(|call| call == "activate:start:survivor"),
        "candidate handles are not activated once group import has a failure"
    );
    assert!(
        !calls.iter().any(|call| call == "dispose:old"),
        "the known-good tree remains live after a detached import failure"
    );
    assert_eq!(entry_ids(loader.tree()), vec![id("old")]);
}

#[tokio::test]
async fn restoration_failures_are_aggregated_with_candidate_cleanup_failures() {
    let state = Arc::new(MockState::default());
    state
        .fail_dispose
        .lock()
        .expect("mock failure configuration is available")
        .insert("old-fails".into());
    let mut loader = loader(Arc::clone(&state));
    loader
        .load(tree(&["old-live", "old-fails"]))
        .await
        .expect("initial tree loads");
    state
        .fail_dispose
        .lock()
        .expect("mock failure configuration is available")
        .insert("new".into());
    state
        .fail_resolve
        .lock()
        .expect("mock failure configuration is available")
        .extend(["test/old-live".into(), "test/old-fails".into()]);
    state.clear_calls();

    let error = loader
        .replace(tree(&["new"]))
        .await
        .expect_err("cleanup and fresh restoration failures reject the replacement");
    assert_eq!(
        error.rollback_errors().len(),
        3,
        "candidate cleanup plus each fresh old import failure is retained"
    );
    assert!(
        error.rollback_errors().iter().any(|error| {
            matches!(
                error,
                LoaderError::Runtime {
                    stage: "dispose",
                    entry,
                    ..
                } if entry == &id("new")
            )
        }),
        "the candidate disposal failure remains a rollback error"
    );
    assert_eq!(
        error
            .rollback_errors()
            .iter()
            .filter(|error| matches!(
                error,
                LoaderError::Runtime {
                    stage: "import",
                    ..
                }
            ))
            .count(),
        2,
        "all fresh old-tree import failures are aggregated into the rollback"
    );
    assert_eq!(
        entry_ids(loader.tree()),
        vec![id("old-live"), id("old-fails")]
    );
}

#[tokio::test]
async fn detached_candidates_inherit_host_service_realm_and_intercepts() {
    let state = Arc::new(MockState::default());
    let service = ServiceKey::new("host-storage", "test/v1");
    let host = ContextHandle::root()
        .isolate(service.clone(), None)
        .intercept(service.clone(), json!({"origin": "host"}));
    let _provider = host
        .provide(service.clone(), "available".to_owned())
        .expect("host provides an isolated service");
    *state
        .required_service
        .lock()
        .expect("mock service requirement is available") = Some(service.clone());
    let mut loader = loader(Arc::clone(&state)).with_context(host);

    loader
        .load(tree(&["consumer"]))
        .await
        .expect("candidate resolves the host-provided isolated service");
    assert!(
        state
            .calls()
            .iter()
            .any(|call| call == "context:service:host-storage@test/v1"),
        "the runtime sees services from the context supplied to Loader::with_context"
    );
    assert_eq!(
        loader.context().intercept_config(&service),
        json!({"origin": "host"}),
        "the detached candidate keeps host intercept configuration"
    );
}

#[tokio::test]
async fn resolver_and_hmr_are_public_runtime_boundaries() {
    let state = Arc::new(MockState::default());
    let resolver = MockResolver {
        state: Arc::clone(&state),
    };
    let package = resolver
        .resolve("test/hot", RuntimeKind::Native)
        .await
        .expect("resolver produces a runtime-specific imported package");
    assert_eq!(package.location, "resolved:test/hot");
    MockHmr {
        state: Arc::clone(&state),
    }
    .invalidate(&package)
    .await
    .expect("HMR driver is invoked only through its package boundary");
    assert_eq!(
        state.calls(),
        vec!["resolve:test/hot", "hmr:test/hot"],
        "the loader-facing interfaces pass resolved packages rather than runtime internals"
    );
}
