use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use serde_json::{json, Value};
use tessivum_core::{
    ContextHandle, Entry, EntryId, EntryOptions, EntryTree, EntryTreeWatcher,
    EntryTreeWatcherOptions, Loader, LoaderFuture, LoaderRuntime, NativeConfigSchema, NativePlugin,
    NativePluginDescriptor, NativePluginError, NativePluginFuture, NativePluginRuntime,
    PackageResolver, ResolvedPackage, RuntimeKind, ServiceKey,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

fn config_service() -> ServiceKey {
    ServiceKey::new("watcher.config", "test/v1")
}

struct Resolver;

impl PackageResolver for Resolver {
    fn resolve<'a>(
        &'a self,
        specifier: &'a str,
        _runtime: RuntimeKind,
    ) -> LoaderFuture<'a, ResolvedPackage> {
        let specifier = specifier.to_owned();
        Box::pin(async move {
            Ok(ResolvedPackage {
                location: specifier.clone(),
                specifier,
            })
        })
    }
}

#[derive(Default)]
struct ProbeState {
    observed: Mutex<Vec<Value>>,
    stops: AtomicUsize,
    blocking_start: AtomicBool,
    release: (Mutex<bool>, Condvar),
}

struct Probe {
    state: Arc<ProbeState>,
}

impl NativePlugin for Probe {
    fn descriptor(&self) -> NativePluginDescriptor {
        NativePluginDescriptor {
            name: "watcher-probe".into(),
            version: "1".into(),
            dependencies: Vec::new(),
            config_schema: NativeConfigSchema::Object {
                properties: BTreeMap::from([
                    ("value".into(), NativeConfigSchema::String),
                    ("removed".into(), NativeConfigSchema::String),
                ]),
                required: BTreeSet::from(["value".into()]),
                allow_additional: false,
            },
        }
    }

    fn start<'a>(
        &'a mut self,
        context: ContextHandle,
        config: &'a Value,
    ) -> NativePluginFuture<'a> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            context.provide(config_service(), config.clone())?;
            state
                .observed
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(config.clone());
            if config["value"] == "reject" {
                return Err(NativePluginError::Runtime {
                    package: "native/watcher-probe".into(),
                    message: "candidate activation rejected".into(),
                });
            }
            if config["value"] == "blocked" {
                state.blocking_start.store(true, Ordering::Release);
                let mut released = state
                    .release
                    .0
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                while !*released {
                    released = state
                        .release
                        .1
                        .wait(released)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            }
            Ok(())
        })
    }

    fn update<'a>(
        &'a mut self,
        _context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn stop<'a>(&'a mut self, _context: ContextHandle) -> NativePluginFuture<'a> {
        self.state.stops.fetch_add(1, Ordering::AcqRel);
        Box::pin(async { Ok(()) })
    }
}

fn loader(state: Arc<ProbeState>) -> Loader {
    let mut runtime = NativePluginRuntime::new();
    runtime
        .register("native/watcher-probe", move || Probe {
            state: Arc::clone(&state),
        })
        .expect("the Native probe factory registers");
    Loader::try_new(
        Arc::new(Resolver),
        [Arc::new(runtime) as Arc<dyn LoaderRuntime>],
    )
    .expect("the watcher owns a valid Loader")
}

fn tree(config: Value) -> EntryTree {
    EntryTree {
        entries: vec![Entry {
            package: "native/watcher-probe".into(),
            options: EntryOptions {
                id: EntryId::new("probe").unwrap(),
                name: Some("watcher probe".into()),
                runtime: RuntimeKind::Native,
                config,
                inject: Vec::new(),
                isolate: Vec::new(),
                intercept: json!({}),
                disabled: false,
                group: None,
            },
        }],
        groups: Vec::new(),
    }
}

fn write_tree(path: &Path, config: Value) {
    fs::write(path, serde_json::to_vec(&tree(config)).unwrap()).unwrap();
}

fn temp_path() -> (PathBuf, PathBuf) {
    let directory = std::env::temp_dir().join(format!(
        "tessivum-entry-tree-watcher-{}-{}",
        std::process::id(),
        NEXT_PATH.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join("plugins.json");
    (directory, path)
}

fn eventually(description: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < deadline, "{description}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn current_config(watcher: &EntryTreeWatcher) -> Option<Value> {
    watcher
        .context()
        .get::<Value>(&config_service())
        .unwrap()
        .and_then(|handle| handle.with(Clone::clone).ok())
}

fn live_config(watcher: &EntryTreeWatcher) -> Value {
    current_config(watcher).expect("the committed Native plugin provides its live config")
}

#[test]
fn real_file_changes_are_transactional_convergent_and_quiescent_on_stop() {
    let (directory, path) = temp_path();
    let state = Arc::new(ProbeState::default());
    write_tree(&path, json!({"value": "initial", "removed": "old"}));
    let mut watcher = EntryTreeWatcher::start(
        loader(Arc::clone(&state)),
        path.clone(),
        EntryTreeWatcherOptions {
            poll_interval: Duration::from_millis(20),
            max_source_bytes: 1024,
        },
    )
    .expect("the owned watcher starts after applying the existing file");
    assert_eq!(
        live_config(&watcher),
        json!({"value": "initial", "removed": "old"})
    );

    let rejected_before = watcher.snapshot().rejected_updates;
    write_tree(&path, json!({"value": 7}));
    eventually("invalid Native config was not exposed", || {
        watcher.snapshot().rejected_updates > rejected_before
    });
    assert_eq!(
        live_config(&watcher),
        json!({"value": "initial", "removed": "old"})
    );
    assert_eq!(
        watcher.snapshot().tree.entries[0].options.config["value"],
        "initial"
    );
    assert!(watcher.snapshot().last_error.is_some());

    let rejected_before = watcher.snapshot().rejected_updates;
    write_tree(&path, json!({"value": "reject"}));
    eventually("activation failure was not exposed", || {
        watcher.snapshot().rejected_updates > rejected_before
    });
    assert_eq!(
        live_config(&watcher),
        json!({"value": "initial", "removed": "old"})
    );

    let rejected_before = watcher.snapshot().rejected_updates;
    fs::write(&path, vec![b' '; 1025]).unwrap();
    eventually("oversized source was not rejected", || {
        watcher.snapshot().rejected_updates > rejected_before
    });
    assert_eq!(
        live_config(&watcher),
        json!({"value": "initial", "removed": "old"})
    );

    #[cfg(unix)]
    {
        let rejected_before = watcher.snapshot().rejected_updates;
        let device_link = directory.join("device-link");
        std::os::unix::fs::symlink("/dev/zero", &device_link).unwrap();
        fs::rename(&device_link, &path).unwrap();
        eventually("non-regular source was not rejected", || {
            watcher.snapshot().rejected_updates > rejected_before
        });
        assert_eq!(
            live_config(&watcher),
            json!({"value": "initial", "removed": "old"})
        );
        fs::remove_file(&path).unwrap();
    }

    write_tree(
        &path,
        json!({"value": "intermediate", "removed": "discard"}),
    );
    write_tree(&path, json!({"value": "latest"}));
    eventually(
        "rapid replacements did not converge on the newest valid source",
        || current_config(&watcher) == Some(json!({"value": "latest"})),
    );
    assert_eq!(
        watcher.snapshot().tree.entries[0].options.config,
        json!({"value": "latest"}),
        "whole-source replacement drops fields removed by the latest file"
    );

    let rejected_before = watcher.snapshot().rejected_updates;
    fs::remove_file(&path).unwrap();
    eventually("missing source was not exposed as a read error", || {
        watcher.snapshot().rejected_updates > rejected_before
    });
    assert_eq!(live_config(&watcher), json!({"value": "latest"}));

    let replacement = directory.join("replacement.json");
    write_tree(&replacement, json!({"value": "recreated"}));
    fs::rename(&replacement, &path).unwrap();
    eventually("atomic recreation did not activate the new tree", || {
        current_config(&watcher) == Some(json!({"value": "recreated"}))
    });

    write_tree(&path, json!({"value": "blocked"}));
    eventually("the in-flight Native activation did not begin", || {
        state.blocking_start.load(Ordering::Acquire)
    });
    let stopped = Arc::new(AtomicBool::new(false));
    let stop_finished = Arc::clone(&stopped);
    let stopper = thread::spawn(move || {
        watcher
            .stop()
            .expect("stop joins the in-flight replacement");
        watcher.stop().expect("repeated stop is safe");
        stop_finished.store(true, Ordering::Release);
        watcher
    });
    thread::sleep(Duration::from_millis(60));
    assert!(
        !stopped.load(Ordering::Acquire),
        "stop must wait for the transaction it owns"
    );
    {
        let mut released = state
            .release
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *released = true;
        state.release.1.notify_all();
    }
    let watcher = stopper.join().expect("the stopper thread does not panic");
    assert!(!watcher.snapshot().running);
    let callbacks_after_stop = state
        .observed
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .len();
    write_tree(&path, json!({"value": "after-stop"}));
    thread::sleep(Duration::from_millis(80));
    assert_eq!(
        state
            .observed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len(),
        callbacks_after_stop,
        "joined shutdown leaves no detached watcher callback"
    );
    assert!(state.stops.load(Ordering::Acquire) > 0);

    drop(watcher);
    fs::remove_dir_all(directory).unwrap();
}

struct TickingRuntime(Arc<AtomicUsize>);
struct TickingHandle(tokio::task::JoinHandle<()>);

impl LoaderRuntime for TickingRuntime {
    fn kind(&self) -> RuntimeKind {
        RuntimeKind::Native
    }

    fn instantiate<'a>(
        &'a self,
        _: ResolvedPackage,
        _: Entry,
        _: ContextHandle,
    ) -> LoaderFuture<'a, Box<dyn tessivum_core::RuntimeHandle>> {
        Box::pin(async move {
            let ticks = Arc::clone(&self.0);
            let task = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    ticks.fetch_add(1, Ordering::Release);
                }
            });
            Ok(Box::new(TickingHandle(task)) as Box<dyn tessivum_core::RuntimeHandle>)
        })
    }
}

impl tessivum_core::RuntimeHandle for TickingHandle {
    fn activate<'a>(&'a mut self) -> LoaderFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn dispose<'a>(&'a mut self) -> LoaderFuture<'a, ()> {
        Box::pin(async move {
            self.0.abort();
            if let Err(error) = (&mut self.0).await {
                if !error.is_cancelled() {
                    return Err(tessivum_core::LoaderError::Validation(error.to_string()));
                }
            }
            Ok(())
        })
    }
}

#[test]
fn watcher_drives_plugin_tasks_while_idle_and_drop_joins_cleanup() {
    let (directory, path) = temp_path();
    write_tree(&path, json!({"value": "idle"}));
    let ticks = Arc::new(AtomicUsize::new(0));
    let loader = Loader::try_new(
        Arc::new(Resolver),
        [Arc::new(TickingRuntime(Arc::clone(&ticks))) as Arc<dyn LoaderRuntime>],
    )
    .unwrap();
    let watcher = EntryTreeWatcher::start(
        loader,
        &path,
        EntryTreeWatcherOptions {
            poll_interval: Duration::from_secs(60),
            ..EntryTreeWatcherOptions::default()
        },
    )
    .unwrap();
    eventually("plugin tasks were suspended between source reads", || {
        ticks.load(Ordering::Acquire) >= 2
    });
    drop(watcher);
    let after_drop = ticks.load(Ordering::Acquire);
    thread::sleep(Duration::from_millis(50));
    assert_eq!(ticks.load(Ordering::Acquire), after_drop);
    fs::remove_dir_all(directory).unwrap();
}
