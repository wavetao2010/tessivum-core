use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};

use serde_json::{json, Value};
use tessivum_core::{
    Entry, EntryId, EntryOptions, EntryTree, Loader, LoaderFuture, LoaderRuntime, PackageResolver,
    ResolvedPackage, RuntimeKind,
};
use tessivum_node_bridge::{
    BridgeClient, ClientConfig, FrameKind, HostCommand, LegacyNodeRuntime, NodeSupervisor,
};

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park(),
        }
    }
}

fn host_command() -> HostCommand {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = manifest
        .parent()
        .and_then(|path| path.parent())
        .expect("bridge crate is nested below the workspace root");
    HostCommand::new("bun")
        .arg("run")
        .arg(root.join("node/compat-host/src/index.ts"))
        .current_dir(root.join("node/compat-host"))
}

fn fixture_path(name: &str) -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(|path| path.parent())
        .expect("bridge crate is nested below the workspace root")
        .join("fixtures/legacy")
        .join(name)
        .to_string_lossy()
        .into_owned()
}

fn load(
    client: &BridgeClient,
    plugin_id: &str,
    path: &str,
    export: Option<&str>,
    config: Value,
) -> Value {
    let mut payload = json!({
        "pluginId": plugin_id,
        "package": { "specifier": path, "location": path },
        "config": config,
    });
    if let Some(export) = export {
        payload["export"] = json!(export);
    }
    client
        .request(FrameKind::PluginLoad, payload, Duration::from_secs(2))
        .expect("real host loads the legacy plugin")
}

#[test]
fn real_host_runs_function_object_class_service_inject_events_waterfall_and_async_disposers() {
    let supervisor = NodeSupervisor::new(host_command(), ClientConfig::default())
        .expect("supervisor accepts a Bun host command");
    let client = supervisor.start().expect("real compat host handshakes");
    let logs = Arc::new(Mutex::new(Vec::new()));
    let received_logs = Arc::clone(&logs);
    client.set_log_handler(move |record| {
        received_logs
            .lock()
            .expect("test log collector is available")
            .push(record);
    });

    let function_path = fixture_path("function-plugin.ts");
    let function = load(
        &client,
        "function",
        &function_path,
        None,
        json!({ "prefix": "function" }),
    );
    assert_eq!(function["pluginId"], "function");
    assert_eq!(function["state"], "ACTIVE");
    assert!(function.get("effects").is_some());
    let node_api = client
        .request(
            FrameKind::ServiceCall,
            json!({ "name": "legacy.function", "method": "inspect", "args": ["node-api"] }),
            Duration::from_secs(1),
        )
        .expect("function plugin exposes a service through the host");
    assert_eq!(node_api["prefix"], "function");
    assert_eq!(node_api["value"], "node-api");
    assert_eq!(node_api["file"], "function-plugin.ts");
    assert_eq!(
        node_api["readable"], true,
        "Bun ran the fixture's Node fs/path probe"
    );

    let event_result = client.request(
        FrameKind::EventEmit,
        json!({ "event": "legacy.event", "args": ["event-value"] }),
        Duration::from_secs(1),
    );
    if let Err(error) = event_result {
        panic!(
            "function listener receives emitted events: {error:?}; logs: {:?}",
            logs.lock().expect("test log collector is available")
        );
    }
    assert_eq!(
        client
            .request(
                FrameKind::ServiceCall,
                json!({ "name": "legacy.function", "method": "events" }),
                Duration::from_secs(1),
            )
            .expect("event state remains available through service proxy"),
        json!(["event-value"])
    );
    assert_eq!(
        client
            .request(
                FrameKind::EventEmit,
                json!({
                    "event": "legacy.waterfall",
                    "mode": "waterfall",
                    "args": ["water"],
                    "next": "tail",
                }),
                Duration::from_secs(1),
            )
            .expect("waterfall crosses the bridge"),
        json!({ "prefix": "function", "value": "water", "next": "tail" })
    );

    let object = load(
        &client,
        "object",
        &function_path,
        Some("objectPlugin"),
        json!({ "prefix": "object" }),
    );
    assert_eq!(object["state"], "ACTIVE");
    assert_eq!(
        client
            .request(
                FrameKind::ServiceCall,
                json!({ "name": "legacy.object", "method": "echo", "args": ["object-value"] }),
                Duration::from_secs(1),
            )
            .expect("object plugin exports its service"),
        json!({ "prefix": "object", "value": "object-value" })
    );

    client
        .request(
            FrameKind::ServiceProvide,
            json!({ "name": "legacy.required", "value": { "value": "injected" } }),
            Duration::from_secs(1),
        )
        .expect("host registers the class plugin's required service");
    let class_path = fixture_path("service-plugin.ts");
    let class = load(
        &client,
        "class",
        &class_path,
        Some("ClassPlugin"),
        json!({ "prefix": "class" }),
    );
    assert_eq!(class["state"], "ACTIVE");
    assert_eq!(
        client
            .request(
                FrameKind::ServiceCall,
                json!({ "name": "legacy.class", "method": "required" }),
                Duration::from_secs(1),
            )
            .expect("class injection observes its required service"),
        json!("injected")
    );
    client
        .request(
            FrameKind::EventEmit,
            json!({ "event": "legacy.event", "args": ["class-event"] }),
            Duration::from_secs(1),
        )
        .expect("class listener receives emitted events");
    assert_eq!(
        client
            .request(
                FrameKind::ServiceCall,
                json!({ "name": "legacy.bridge", "method": "inspect", "args": ["service"] }),
                Duration::from_secs(1),
            )
            .expect("Cordis Service subclass remains callable"),
        json!({ "prefix": "class", "value": "service", "eventCount": 1 })
    );
    let waterfall = client
        .request(
            FrameKind::EventEmit,
            json!({
                "event": "legacy.waterfall",
                "mode": "waterfall",
                "args": ["chain"],
                "next": "tail",
            }),
            Duration::from_secs(1),
        )
        .expect("waterfall reaches function and class listeners in registration order");
    assert_eq!(waterfall["prefix"], "function");
    assert_eq!(waterfall["next"]["class"], "class");
    assert_eq!(waterfall["next"]["next"], "tail");

    for plugin_id in ["class", "object", "function"] {
        assert_eq!(
            client
                .request(
                    FrameKind::PluginDispose,
                    json!({ "pluginId": plugin_id }),
                    Duration::from_secs(2),
                )
                .expect("disposal waits for the plugin's async cleanup")["disposed"],
            true
        );
        assert!(client
            .request(
                FrameKind::PluginSnapshot,
                json!({ "pluginId": plugin_id }),
                Duration::from_secs(1),
            )
            .is_err());
    }
    assert!(
        logs.lock()
            .expect("test log collector is available")
            .iter()
            .any(|record| record.to_string().contains("legacy:function-plugin")),
        "plugin console output is delivered as a log frame instead of corrupting stdout"
    );
    supervisor
        .shutdown()
        .expect("host drains async disposers and exits without an orphan");
}

#[test]
fn loader_runtime_loads_and_unloads_a_real_function_plugin() {
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

    let supervisor = NodeSupervisor::new(host_command(), ClientConfig::default())
        .expect("supervisor accepts a Bun host command");
    let client = supervisor.start().expect("real compat host handshakes");
    let runtime = Arc::new(LegacyNodeRuntime::new(client).with_timeout(Duration::from_secs(2)));
    let runtime_for_loader: Arc<dyn LoaderRuntime> = runtime.clone();
    let mut loader = Loader::try_new(Arc::new(Resolver), [runtime_for_loader])
        .expect("legacy runtime registers with the common loader");
    let entry = Entry {
        package: fixture_path("function-plugin.ts"),
        options: EntryOptions {
            id: EntryId::new("legacy-function").expect("stable entry id is valid"),
            name: Some("legacy-function".into()),
            runtime: RuntimeKind::LegacyNode,
            config: json!({ "prefix": "loader" }),
            inject: Vec::new(),
            isolate: Vec::new(),
            intercept: json!({}),
            disabled: false,
            group: None,
        },
    };
    block_on(loader.load(EntryTree {
        entries: vec![entry],
        groups: Vec::new(),
    }))
    .expect("LoaderRuntime activates a plugin through the Node bridge");
    assert_eq!(
        runtime
            .client()
            .request(
                FrameKind::PluginSnapshot,
                json!({ "pluginId": "legacy-function" }),
                Duration::from_secs(1),
            )
            .expect("loader-managed plugin has a host snapshot")["state"],
        "ACTIVE"
    );
    let root = loader.context();
    block_on(loader.unload()).expect("loader disposes the Node runtime handle");
    block_on(root.scope().dispose()).expect("loader root has no orphaned scope work");
    supervisor
        .shutdown()
        .expect("loader test reaps its real host");
}

#[test]
fn crash_cleans_generation_resources_restarts_the_host_and_rejects_stale_clients() {
    let supervisor = NodeSupervisor::new(host_command(), ClientConfig::default())
        .expect("supervisor accepts a Bun host command");
    let stale = supervisor.start().expect("first real host handshakes");
    let first_generation = supervisor.generation().expect("first generation is active");
    let cleanups = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&cleanups);
    supervisor
        .register_cleanup(first_generation, move || {
            observed.fetch_add(1, Ordering::SeqCst);
        })
        .expect("generation-owned registration succeeds");
    let crashing_fixture = fixture_path("function-plugin.ts");
    assert!(stale
        .request(
            FrameKind::PluginLoad,
            json!({
                "pluginId": "crash",
                "package": {
                    "specifier": crashing_fixture,
                    "location": crashing_fixture,
                },
                "config": { "crash": true },
            }),
            Duration::from_secs(2),
        )
        .is_err());
    let deadline = Instant::now() + Duration::from_secs(1);
    while cleanups.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        cleanups.load(Ordering::SeqCst),
        1,
        "crash releases every generation cleanup"
    );
    assert_eq!(
        supervisor.generation(),
        None,
        "disconnection clears the dead generation"
    );
    assert!(
        stale.heartbeat().is_err(),
        "a stale client cannot reach a replacement host"
    );

    let fresh = supervisor
        .start()
        .expect("supervisor starts a replacement child");
    assert!(
        fresh.generation() > first_generation,
        "restart allocates a fresh generation rather than reusing stale handles"
    );
    fresh
        .heartbeat()
        .expect("replacement host is independently usable");
    supervisor
        .shutdown()
        .expect("replacement child exits without an orphan");
}
