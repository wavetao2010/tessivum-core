use std::{
    fs,
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

static NEXT_RACE_FIXTURE: AtomicUsize = AtomicUsize::new(0);

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

fn wait_for_file(path: &std::path::Path, label: &str) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !path.exists() {
        assert!(Instant::now() < deadline, "timed out waiting for {label}");
        thread::sleep(Duration::from_millis(1));
    }
}

fn host_command() -> HostCommand {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = manifest
        .parent()
        .and_then(|path| path.parent())
        .expect("bridge crate is nested below the workspace root");
    let command = HostCommand::new("bun")
        .arg("run")
        .arg(root.join("node/compat-host/src/index.ts"))
        .current_dir(root.join("node/compat-host"));
    if let Some(vendor_root) = std::env::var_os("CORDIS_VENDOR_ROOT") {
        command.env("CORDIS_VENDOR_ROOT", vendor_root)
    } else {
        command
    }
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
    let inventory = runtime
        .client()
        .request(
            FrameKind::PluginSnapshot,
            json!({ "loader": true }),
            Duration::from_secs(1),
        )
        .expect("compat host exposes its active Loader inventory");
    assert_eq!(inventory["entries"][0]["options"]["id"], "legacy-function");
    assert_eq!(
        inventory["entries"][0]["options"]["name"],
        "legacy-function"
    );
    assert_eq!(inventory["entries"][0]["state"], "ACTIVE");
    let root = loader.context();
    block_on(loader.unload()).expect("loader disposes the Node runtime handle");
    assert_eq!(
        runtime
            .client()
            .request(
                FrameKind::PluginSnapshot,
                json!({ "loader": true }),
                Duration::from_secs(1),
            )
            .expect("compat host Loader inventory follows unload")["entries"],
        json!([])
    );
    block_on(root.scope().dispose()).expect("loader root has no orphaned scope work");
    supervisor
        .shutdown()
        .expect("loader test reaps its real host");
}

#[test]
fn cancelled_loader_load_and_delayed_disposers_leave_no_stale_handles() {
    let race_file = std::env::temp_dir().join(format!(
        "tessivum-node-bridge-race-{}-{}.ts",
        std::process::id(),
        NEXT_RACE_FIXTURE.fetch_add(1, Ordering::Relaxed),
    ));
    let loader_marker = race_file.with_extension("started");
    let loader_release = race_file.with_extension("release");
    let marker_json = serde_json::to_string(&loader_marker).expect("marker path serializes");
    let release_json = serde_json::to_string(&loader_release).expect("release path serializes");
    let race_source = r#"import { Context } from '@deepseek-ai/cordis'
import { existsSync, writeFileSync } from 'node:fs'

const pause = () => new Promise<void>(resolve => setTimeout(resolve, 500))
writeFileSync("__MARKER__", 'started')
await pause()
while (!existsSync("__RELEASE__")) await pause()
let patched = false

export default function racePlugin(ctx: any, config: any = {}) {
  if (config.mode === 'loader') {
    console.log('race:loader:start')
    return (async () => {
      writeFileSync(config.marker, 'started')
      while (!existsSync(config.release)) await pause()
      ctx.provide('legacy.race.loader', { active: true })
      return () => console.log('race:loader:dispose')
    })()
  }

  if (config.mode === 'plugin-disposer') {
    return async () => {
      console.log('race:plugin-dispose:start')
      await pause()
      console.log('race:plugin-dispose:done')
    }
  }

  if (config.mode === 'patch' && !patched) {
    patched = true
    const prototype = Context.prototype as Record<string, any>
    const provide = prototype.provide
    prototype.provide = function (name: string, value: unknown, ...args: unknown[]) {
      const dispose = provide.call(this, name, value, ...args)
      if (name !== 'legacy.race.service') return dispose
      return async () => {
        console.log('race:service-dispose:start')
        await pause()
        await dispose()
        console.log('race:service-dispose:done')
      }
    }
    const on = prototype.on
    prototype.on = function (name: string, listener: (...values: unknown[]) => unknown, ...args: unknown[]) {
      const dispose = on.call(this, name, listener, ...args)
      if (name !== 'legacy.race.event') return dispose
      return async () => {
        console.log('race:registration-dispose:start')
        await pause()
        await dispose()
        console.log('race:registration-dispose:done')
      }
    }
  }
}
"#
    .replace("\"__MARKER__\"", &marker_json)
    .replace("\"__RELEASE__\"", &release_json);
    fs::write(&race_file, race_source).expect("delayed legacy race fixture is writable");
    let race_path = race_file.to_string_lossy().into_owned();

    let supervisor = NodeSupervisor::new(host_command(), ClientConfig::default())
        .expect("supervisor accepts a Bun host command");
    let client = supervisor.start().expect("real compat host handshakes");
    let loader_started = Arc::new(AtomicUsize::new(0));
    let loader_disposed = Arc::new(AtomicUsize::new(0));
    let plugin_dispose_started = Arc::new(AtomicUsize::new(0));
    let plugin_dispose_done = Arc::new(AtomicUsize::new(0));
    let service_dispose_started = Arc::new(AtomicUsize::new(0));
    let service_dispose_done = Arc::new(AtomicUsize::new(0));
    let registration_dispose_started = Arc::new(AtomicUsize::new(0));
    let registration_dispose_done = Arc::new(AtomicUsize::new(0));
    let observed_loader_started = Arc::clone(&loader_started);
    let observed_loader_disposed = Arc::clone(&loader_disposed);
    let observed_plugin_dispose_started = Arc::clone(&plugin_dispose_started);
    let observed_plugin_dispose_done = Arc::clone(&plugin_dispose_done);
    let observed_service_dispose_started = Arc::clone(&service_dispose_started);
    let observed_service_dispose_done = Arc::clone(&service_dispose_done);
    let observed_registration_dispose_started = Arc::clone(&registration_dispose_started);
    let observed_registration_dispose_done = Arc::clone(&registration_dispose_done);
    client.set_log_handler(move |record| {
        let record = record.to_string();
        if record.contains("race:loader:start") {
            observed_loader_started.fetch_add(1, Ordering::SeqCst);
        }
        if record.contains("race:loader:dispose") {
            observed_loader_disposed.fetch_add(1, Ordering::SeqCst);
        }
        if record.contains("race:plugin-dispose:start") {
            observed_plugin_dispose_started.fetch_add(1, Ordering::SeqCst);
        }
        if record.contains("race:plugin-dispose:done") {
            observed_plugin_dispose_done.fetch_add(1, Ordering::SeqCst);
        }
        if record.contains("race:service-dispose:start") {
            observed_service_dispose_started.fetch_add(1, Ordering::SeqCst);
        }
        if record.contains("race:service-dispose:done") {
            observed_service_dispose_done.fetch_add(1, Ordering::SeqCst);
        }
        if record.contains("race:registration-dispose:start") {
            observed_registration_dispose_started.fetch_add(1, Ordering::SeqCst);
        }
        if record.contains("race:registration-dispose:done") {
            observed_registration_dispose_done.fetch_add(1, Ordering::SeqCst);
        }
    });

    let cancelled_loader = client
        .begin_request(
            FrameKind::PluginLoad,
            json!({
                "pluginId": "race-loader",
                "package": { "specifier": race_path, "location": race_path },
                "loader": true,
                "config": { "mode": "loader", "marker": loader_marker, "release": loader_release },
            }),
        )
        .expect("loader race request reaches the host");
    wait_for_file(&loader_marker, "loader plugin activation");
    assert!(
        cancelled_loader.cancel(),
        "loader request is still pending at cancellation"
    );
    fs::write(&loader_release, b"release").expect("loader activation release is written");
    assert!(
        client
            .request(
                FrameKind::PluginSnapshot,
                json!({ "pluginId": "race-loader" }),
                Duration::from_secs(2),
            )
            .is_err(),
        "cancelled loader plugin has no host handle"
    );
    assert_eq!(
        client
            .request(
                FrameKind::PluginSnapshot,
                json!({ "loader": true }),
                Duration::from_secs(2),
            )
            .expect("loader snapshot follows rollback")["entries"],
        json!([]),
        "cancelled loader plugin leaves no loader entry"
    );
    assert_eq!(
        client
            .request(
                FrameKind::PluginLoad,
                json!({
                    "pluginId": "race-loader",
                    "package": { "specifier": race_path, "location": race_path },
                    "loader": true,
                    "config": { "mode": "loader", "marker": loader_marker, "release": loader_release },
                }),
                Duration::from_secs(2),
            )
            .expect("rolled-back loader plugin can reload")["state"],
        "ACTIVE"
    );
    assert_eq!(
        client
            .request(
                FrameKind::PluginDispose,
                json!({ "pluginId": "race-loader" }),
                Duration::from_secs(2),
            )
            .expect("reloaded loader plugin disposes once")["disposed"],
        true
    );
    assert!(
        client
            .request(
                FrameKind::PluginDispose,
                json!({ "pluginId": "race-loader" }),
                Duration::from_secs(2),
            )
            .is_err(),
        "reloaded loader plugin cannot double-dispose"
    );

    load(
        &client,
        "race-plugin",
        &race_path,
        None,
        json!({ "mode": "plugin-disposer" }),
    );
    assert_eq!(
        client
            .request(
                FrameKind::PluginDispose,
                json!({ "pluginId": "race-plugin" }),
                Duration::from_secs(2),
            )
            .expect("plugin disposal completes")["disposed"],
        true
    );
    assert!(
        client
            .request(
                FrameKind::PluginSnapshot,
                json!({ "pluginId": "race-plugin" }),
                Duration::from_secs(2),
            )
            .is_err(),
        "disposed plugin removes its host handle"
    );
    load(
        &client,
        "race-plugin",
        &race_path,
        None,
        json!({ "mode": "plugin-disposer" }),
    );
    assert_eq!(
        client
            .request(
                FrameKind::PluginDispose,
                json!({ "pluginId": "race-plugin" }),
                Duration::from_secs(2),
            )
            .expect("reloaded plugin disposes once")["disposed"],
        true
    );
    assert!(
        client
            .request(
                FrameKind::PluginDispose,
                json!({ "pluginId": "race-plugin" }),
                Duration::from_secs(2),
            )
            .is_err(),
        "reloaded plugin cannot double-dispose"
    );

    load(
        &client,
        "race-patcher",
        &race_path,
        None,
        json!({ "mode": "patch" }),
    );
    client
        .request(
            FrameKind::ServiceProvide,
            json!({
                "name": "legacy.race.service",
                "registrationId": "race-service",
                "value": { "generation": 1 },
            }),
            Duration::from_secs(2),
        )
        .expect("delayed service registration succeeds");
    client
        .request(
            FrameKind::ServiceRemove,
            json!({ "registrationId": "race-service" }),
            Duration::from_secs(2),
        )
        .expect("service registration disposes");
    assert!(
        client
            .request(
                FrameKind::ServiceRemove,
                json!({ "registrationId": "race-service" }),
                Duration::from_secs(2),
            )
            .is_err(),
        "disposed service registration is invalidated"
    );
    client
        .request(
            FrameKind::ServiceProvide,
            json!({
                "name": "legacy.race.service",
                "registrationId": "race-service",
                "value": { "generation": 2 },
            }),
            Duration::from_secs(2),
        )
        .expect("removed service registration can be recreated");
    client
        .request(
            FrameKind::ServiceRemove,
            json!({ "registrationId": "race-service" }),
            Duration::from_secs(2),
        )
        .expect("recreated service registration disposes once");
    client
        .request(
            FrameKind::EventSubscribe,
            json!({
                "event": "legacy.race.event",
                "callbackId": "race-callback",
                "registrationId": "race-registration",
            }),
            Duration::from_secs(2),
        )
        .expect("delayed event registration succeeds");
    client
        .request(
            FrameKind::RegistrationDispose,
            json!({ "registrationId": "race-registration" }),
            Duration::from_secs(2),
        )
        .expect("event registration disposes");
    assert!(
        client
            .request(
                FrameKind::RegistrationDispose,
                json!({ "registrationId": "race-registration" }),
                Duration::from_secs(2),
            )
            .is_err(),
        "disposed event registration is invalidated"
    );
    client
        .request(
            FrameKind::EventSubscribe,
            json!({
                "event": "legacy.race.event",
                "callbackId": "race-callback",
                "registrationId": "race-registration",
            }),
            Duration::from_secs(2),
        )
        .expect("removed event registration can be recreated");
    client
        .request(
            FrameKind::RegistrationDispose,
            json!({ "registrationId": "race-registration" }),
            Duration::from_secs(2),
        )
        .expect("recreated event registration disposes once");

    supervisor
        .shutdown()
        .expect("race host drains without stale resources");
    fs::remove_file(race_file).expect("temporary race fixture is removed");
    fs::remove_file(loader_marker).expect("temporary loader marker is removed");
    fs::remove_file(loader_release).expect("temporary loader release is removed");
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
