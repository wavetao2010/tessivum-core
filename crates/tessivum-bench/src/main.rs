use std::{
    future::Future,
    hint::black_box,
    mem::size_of,
    path::{Path, PathBuf},
    pin::Pin,
    process::{self, Command},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};

use serde_json::{json, Value};
use tessivum_core::{
    ContextHandle, Entry, EntryId, EntryOptions, EntryTree, EventKey, EventOptions, Fiber, Loader,
    LoaderFuture, LoaderRuntime, NativeConfigSchema, NativePlugin, NativePluginDescriptor,
    NativePluginFuture, NativePluginInstance, NativePluginRuntime, PackageResolver, Patch,
    ResolvedPackage, RuntimeKind,
};
use tessivum_extism::{
    CapabilityRegistry, ExtismGuestEngine, PluginManifest, ResourceLimits, WasmPackage,
    WasmPluginInstance, ABI_VERSION,
};
use tessivum_node_bridge::{
    ClientConfig, FrameKind, HostCommand, NodeSupervisor, PROTOCOL_VERSION,
};

const DEFAULT_SAMPLES: usize = 15;
const MAX_SAMPLES: usize = 100;
const DEFAULT_BATCH: usize = 256;
const MAX_BATCH: usize = 1_024;
const LOADER_ENTRIES: usize = 16;
const ROOT_CHILDREN: usize = 32;
const NODE_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

struct Options {
    samples: usize,
    batch: usize,
    wasm: PathBuf,
    node_host: PathBuf,
}

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

struct NoopPlugin;

impl NativePlugin for NoopPlugin {
    fn descriptor(&self) -> NativePluginDescriptor {
        NativePluginDescriptor {
            name: "benchmark.noop".into(),
            version: "0.1.0".into(),
            dependencies: Vec::new(),
            config_schema: NativeConfigSchema::Any,
        }
    }

    fn start<'a>(
        &'a mut self,
        _context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn update<'a>(
        &'a mut self,
        _context: ContextHandle,
        _config: &'a Value,
    ) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn stop<'a>(&'a mut self, _context: ContextHandle) -> NativePluginFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

struct BenchResolver;

impl PackageResolver for BenchResolver {
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

fn main() {
    match parse_options().and_then(run) {
        Ok(report) => println!(
            "{}",
            serde_json::to_string_pretty(&report).expect("benchmark report is serializable")
        ),
        Err(error) => {
            eprintln!("{}", json!({ "error": error }));
            process::exit(1);
        }
    }
}

fn parse_options() -> Result<Options, String> {
    let mut samples = DEFAULT_SAMPLES;
    let mut batch = DEFAULT_BATCH;
    let mut wasm = None;
    let mut node_host = PathBuf::from("bun");
    let mut arguments = std::env::args_os().skip(1);

    while let Some(argument) = arguments.next() {
        let argument = argument
            .to_str()
            .ok_or_else(|| "benchmark arguments must be valid UTF-8".to_owned())?;
        match argument {
            "--samples" => {
                samples = parse_bounded_argument(&mut arguments, "--samples", 1, MAX_SAMPLES)?;
            }
            "--batch" => {
                batch = parse_bounded_argument(&mut arguments, "--batch", 1, MAX_BATCH)?;
            }
            "--wasm" => {
                wasm = Some(PathBuf::from(next_argument(&mut arguments, "--wasm")?));
            }
            "--node-host" => {
                node_host = PathBuf::from(next_argument(&mut arguments, "--node-host")?);
            }
            "--help" | "-h" => {
                println!(
                    "Usage: cargo run -p tessivum-bench --release -- --wasm <guest.wasm> [--samples 1..{MAX_SAMPLES}] [--batch 1..{MAX_BATCH}] [--node-host bun]"
                );
                process::exit(0);
            }
            _ => {
                return Err(format!(
                    "unknown argument {argument:?}; pass --help for usage"
                ))
            }
        }
    }

    let wasm = wasm.ok_or_else(|| "--wasm must name the built Rust guest module".to_owned())?;
    if !wasm.is_file() {
        return Err(format!("WASM guest does not exist: {}", wasm.display()));
    }

    Ok(Options {
        samples,
        batch,
        wasm,
        node_host,
    })
}

fn next_argument(
    arguments: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &str,
) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("{flag} requires a value"))?
        .into_string()
        .map_err(|_| format!("{flag} value must be valid UTF-8"))
}

fn parse_bounded_argument(
    arguments: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &str,
    minimum: usize,
    maximum: usize,
) -> Result<usize, String> {
    let value = next_argument(arguments, flag)?;
    let value = value
        .parse::<usize>()
        .map_err(|_| format!("{flag} must be an integer"))?;
    if !(minimum..=maximum).contains(&value) {
        return Err(format!("{flag} must be in {minimum}..={maximum}"));
    }
    Ok(value)
}

fn run(options: Options) -> Result<Value, String> {
    let wasm = std::fs::read(&options.wasm)
        .map_err(|error| format!("cannot read {}: {error}", options.wasm.display()))?;
    let root = workspace_root()?;
    let mut benchmarks = Vec::new();

    benchmarks.push(benchmark_context_fiber_memory_proxy(options.samples)?);
    benchmarks.push(benchmark_native_lifecycle(options.samples)?);
    benchmarks.push(benchmark_service_throughput(
        options.samples,
        options.batch,
    )?);
    benchmarks.push(benchmark_event_throughput(options.samples, options.batch)?);
    benchmarks.push(benchmark_wasm_cold_call(options.samples, &wasm)?);
    benchmarks.push(benchmark_wasm_warm_call(options.samples, &wasm)?);
    benchmarks.push(benchmark_node_cold(options.samples, &options, &root)?);
    benchmarks.push(benchmark_node_warm(options.samples, &options, &root)?);
    benchmarks.push(benchmark_node_batch(
        options.samples,
        options.batch,
        &options,
        &root,
    )?);
    benchmarks.push(benchmark_loader_tree(options.samples)?);
    benchmarks.push(benchmark_loader_update(options.samples)?);
    benchmarks.push(benchmark_root_dispose(options.samples)?);

    Ok(json!({
        "environment": {
            "uname": command_output("uname", ["-a"]),
            "rust": command_output("rustc", ["--version"]),
            "profile": build_profile(),
        },
        "sampleCount": options.samples,
        "batchSize": options.batch,
        "abi": {
            "wasm": ABI_VERSION,
            "node": PROTOCOL_VERSION,
        },
        "benchmarks": benchmarks,
    }))
}

fn command_output<const N: usize>(program: &str, arguments: [&str; N]) -> String {
    match Command::new(program).args(arguments).output() {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        }
        Ok(output) => format!(
            "unavailable (exit {})",
            output
                .status
                .code()
                .map_or_else(|| "signal".to_owned(), |code| code.to_string())
        ),
        Err(error) => format!("unavailable ({error})"),
    }
}

fn build_profile() -> &'static str {
    option_env!("PROFILE").unwrap_or(if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    })
}

fn workspace_root() -> Result<PathBuf, String> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| "benchmark manifest is not nested under a workspace root".to_owned())
}

fn benchmark_context_fiber_memory_proxy(samples: usize) -> Result<Value, String> {
    let mut values = Vec::with_capacity(samples);
    for _ in 0..samples {
        let context = ContextHandle::root();
        let fiber = Fiber::new(&context.scope(), "benchmark.memory-proxy")
            .map_err(|error| error.to_string())?;
        black_box(&context);
        black_box(&fiber);
        values.push((size_of::<ContextHandle>() + size_of::<Fiber>()) as u64);
        dispose_root(&context)?;
    }
    summarize(
        "context_fiber_memory_proxy",
        "bytes",
        1,
        values,
        Some("shallow ContextHandle + Fiber layout; excludes allocator and Arc targets"),
    )
}

fn benchmark_native_lifecycle(samples: usize) -> Result<Value, String> {
    let mut values = Vec::with_capacity(samples);
    for _ in 0..samples {
        let context = ContextHandle::root();
        let mut instance = NativePluginInstance::instantiate(
            Arc::new(|| Box::new(NoopPlugin) as Box<dyn NativePlugin>),
            context.clone(),
            json!({}),
        )
        .map_err(|error| error.to_string())?;
        let start = Instant::now();
        let lifecycle = (|| -> Result<(), String> {
            block_on(instance.start()).map_err(|error| error.to_string())?;
            block_on(instance.update(json!({ "revision": 1 })))
                .map_err(|error| error.to_string())?;
            block_on(instance.dispose()).map_err(|error| error.to_string())
        })();
        let elapsed = start.elapsed();
        let disposed = dispose_root(&context);
        lifecycle?;
        disposed?;
        values.push(nanoseconds(elapsed)?);
    }
    summarize("native_lifecycle", "ns", 1, values, None)
}

fn benchmark_service_throughput(samples: usize, batch: usize) -> Result<Value, String> {
    let mut values = Vec::with_capacity(samples);
    let service = tessivum_core::ServiceKey::new("benchmark.service", "native/v1");
    for _ in 0..samples {
        let context = ContextHandle::root();
        context
            .provide(service.clone(), 42_u64)
            .map_err(|error| error.to_string())?;
        let start = Instant::now();
        let operations = (|| -> Result<(), String> {
            for _ in 0..batch {
                let handle = context
                    .get::<u64>(&service)
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "benchmark service disappeared".to_owned())?;
                black_box(
                    handle
                        .with(|value| *value)
                        .map_err(|error| error.to_string())?,
                );
            }
            Ok(())
        })();
        let elapsed = start.elapsed();
        let disposed = dispose_root(&context);
        operations?;
        disposed?;
        values.push(operations_per_second(batch, elapsed)?);
    }
    summarize("service_lookup", "operations/s", batch, values, None)
}

fn benchmark_event_throughput(samples: usize, batch: usize) -> Result<Value, String> {
    let mut values = Vec::with_capacity(samples);
    for _ in 0..samples {
        let context = ContextHandle::root();
        let key = EventKey::<u64>::new("benchmark.event");
        let seen = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&seen);
        context
            .events()
            .on(
                &context.scope(),
                key.clone(),
                EventOptions::default(),
                move |_| {
                    observed.fetch_add(1, Ordering::Relaxed);
                    Ok(Value::Null)
                },
            )
            .map_err(|error| error.to_string())?;
        let start = Instant::now();
        let operations = (|| -> Result<(), String> {
            for value in 0..batch {
                context
                    .events()
                    .emit(&key, &(value as u64))
                    .map_err(|error| error.to_string())?;
            }
            Ok(())
        })();
        let elapsed = start.elapsed();
        let observed = seen.load(Ordering::Relaxed);
        let disposed = dispose_root(&context);
        operations?;
        disposed?;
        if observed != batch {
            return Err(format!(
                "benchmark event listener saw {observed} of {batch} events"
            ));
        }
        values.push(operations_per_second(batch, elapsed)?);
    }
    summarize("event_emit", "operations/s", batch, values, None)
}

fn benchmark_wasm_cold_call(samples: usize, wasm: &[u8]) -> Result<Value, String> {
    let mut values = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        let instance = wasm_instance(wasm)?;
        let initialized = instance
            .init(json!({ "benchmark": "cold" }))
            .map_err(|error| error.to_string());
        let called = if initialized.is_ok() {
            instance
                .call(json!({ "benchmark": "cold" }), json!({ "request": "cold" }))
                .map(|response| {
                    black_box(response);
                })
                .map_err(|error| error.to_string())
        } else {
            Ok(())
        };
        let elapsed = start.elapsed();
        let stopped = instance.stop().map_err(|error| error.to_string());
        initialized?;
        called?;
        stopped?;
        values.push(nanoseconds(elapsed)?);
    }
    summarize("wasm_cold_call", "ns", 1, values, None)
}

fn benchmark_wasm_warm_call(samples: usize, wasm: &[u8]) -> Result<Value, String> {
    let instance = wasm_instance(wasm)?;
    instance
        .init(json!({ "benchmark": "warm" }))
        .map_err(|error| error.to_string())?;
    let mut values = Vec::with_capacity(samples);
    let calls = (|| -> Result<(), String> {
        for sequence in 0..samples {
            let start = Instant::now();
            let response = instance
                .call(
                    json!({ "benchmark": "warm" }),
                    json!({ "sequence": sequence }),
                )
                .map_err(|error| error.to_string())?;
            black_box(response);
            values.push(nanoseconds(start.elapsed())?);
        }
        Ok(())
    })();
    let stopped = instance.stop().map_err(|error| error.to_string());
    calls?;
    stopped?;
    summarize("wasm_warm_call", "ns", 1, values, None)
}

fn wasm_instance(wasm: &[u8]) -> Result<WasmPluginInstance, String> {
    let manifest: PluginManifest = serde_json::from_value(json!({
        "id": "benchmark.wasm",
        "version": "0.1.0",
        "entry": "tessivum_wasm_minimal.wasm",
        "abi": ABI_VERSION,
        "inject": [],
        "permissions": [],
        "configSchema": {
            "type": "object",
            "properties": {},
            "additionalProperties": false,
        },
        "exports": [
            "cordis_init",
            "cordis_call",
            "cordis_event",
            "cordis_update",
            "cordis_stop",
        ],
    }))
    .map_err(|error| error.to_string())?;
    let package =
        WasmPackage::from_bytes(manifest, wasm.to_vec()).map_err(|error| error.to_string())?;
    WasmPluginInstance::instantiate(
        package,
        Arc::new(ExtismGuestEngine),
        Arc::new(CapabilityRegistry::default()),
        ResourceLimits::default(),
        json!({}),
    )
    .map_err(|error| error.to_string())
}

fn benchmark_node_cold(samples: usize, options: &Options, root: &Path) -> Result<Value, String> {
    let mut values = Vec::with_capacity(samples);
    for _ in 0..samples {
        let supervisor = node_supervisor(options, root, 1)?;
        let start = Instant::now();
        let started = supervisor.start().map_err(|error| error.to_string());
        let elapsed = start.elapsed();
        let shutdown = if started.is_ok() {
            supervisor.shutdown().map_err(|error| error.to_string())
        } else {
            Ok(())
        };
        started?;
        shutdown?;
        values.push(nanoseconds(elapsed)?);
    }
    summarize("node_bridge_cold", "ns", 1, values, None)
}

fn benchmark_node_warm(samples: usize, options: &Options, root: &Path) -> Result<Value, String> {
    let supervisor = node_supervisor(options, root, 1)?;
    let client = supervisor.start().map_err(|error| error.to_string())?;
    let plugin_id = "benchmark-warm";
    let loaded = load_legacy_plugin(&client, root, plugin_id);
    let mut values = Vec::with_capacity(samples);
    let calls = if loaded.is_ok() {
        (|| -> Result<(), String> {
            for _ in 0..samples {
                let start = Instant::now();
                let response = client
                    .request(
                        FrameKind::ServiceCall,
                        json!({
                            "name": "legacy.function",
                            "method": "inspect",
                            "args": ["warm"],
                        }),
                        NODE_REQUEST_TIMEOUT,
                    )
                    .map_err(|error| error.to_string())?;
                black_box(response);
                values.push(operations_per_second(1, start.elapsed())?);
            }
            Ok(())
        })()
    } else {
        Ok(())
    };
    let disposed = if loaded.is_ok() {
        dispose_legacy_plugin(&client, plugin_id)
    } else {
        Ok(())
    };
    let shutdown = supervisor.shutdown().map_err(|error| error.to_string());
    loaded?;
    calls?;
    disposed?;
    shutdown?;
    summarize("node_bridge_warm", "operations/s", 1, values, None)
}

fn benchmark_node_batch(
    samples: usize,
    batch: usize,
    options: &Options,
    root: &Path,
) -> Result<Value, String> {
    let supervisor = node_supervisor(options, root, batch)?;
    let client = supervisor.start().map_err(|error| error.to_string())?;
    let plugin_id = "benchmark-batch";
    let loaded = load_legacy_plugin(&client, root, plugin_id);
    let mut values = Vec::with_capacity(samples);
    let calls = if loaded.is_ok() {
        (|| -> Result<(), String> {
            for request_index in 0..samples {
                let start = Instant::now();
                let mut pending = Vec::with_capacity(batch);
                for item in 0..batch {
                    pending.push(
                        client
                            .begin_request(
                                FrameKind::ServiceCall,
                                json!({
                                    "name": "legacy.function",
                                    "method": "inspect",
                                    "args": [format!("{request_index}-{item}")],
                                }),
                            )
                            .map_err(|error| error.to_string())?,
                    );
                }
                for request in pending {
                    black_box(
                        request
                            .wait(NODE_REQUEST_TIMEOUT)
                            .map_err(|error| error.to_string())?,
                    );
                }
                values.push(operations_per_second(batch, start.elapsed())?);
            }
            Ok(())
        })()
    } else {
        Ok(())
    };
    let disposed = if loaded.is_ok() {
        dispose_legacy_plugin(&client, plugin_id)
    } else {
        Ok(())
    };
    let shutdown = supervisor.shutdown().map_err(|error| error.to_string());
    loaded?;
    calls?;
    disposed?;
    shutdown?;
    summarize("node_bridge_batch", "operations/s", batch, values, None)
}

fn node_supervisor(options: &Options, root: &Path, batch: usize) -> Result<NodeSupervisor, String> {
    let host = HostCommand::new(options.node_host.clone())
        .arg("run")
        .arg(root.join("node/compat-host/src/index.ts"))
        .current_dir(root.join("node/compat-host"));
    let host = if let Some(vendor_root) = std::env::var_os("CORDIS_VENDOR_ROOT") {
        host.env("CORDIS_VENDOR_ROOT", vendor_root)
    } else {
        host
    };
    let config = ClientConfig {
        queue_capacity: batch.saturating_add(8).max(64),
        handshake_timeout: Duration::from_secs(5),
        request_timeout: NODE_REQUEST_TIMEOUT,
        shutdown_timeout: Duration::from_secs(5),
        ..ClientConfig::default()
    };
    NodeSupervisor::new(host, config).map_err(|error| error.to_string())
}

fn load_legacy_plugin(
    client: &tessivum_node_bridge::BridgeClient,
    root: &Path,
    plugin_id: &str,
) -> Result<(), String> {
    let fixture = root
        .join("fixtures/legacy/function-plugin.ts")
        .to_string_lossy()
        .into_owned();
    client
        .request(
            FrameKind::PluginLoad,
            json!({
                "pluginId": plugin_id,
                "package": {
                    "specifier": fixture.clone(),
                    "location": fixture,
                },
                "config": { "prefix": "benchmark" },
            }),
            NODE_REQUEST_TIMEOUT,
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn dispose_legacy_plugin(
    client: &tessivum_node_bridge::BridgeClient,
    plugin_id: &str,
) -> Result<(), String> {
    client
        .request(
            FrameKind::PluginDispose,
            json!({ "pluginId": plugin_id }),
            NODE_REQUEST_TIMEOUT,
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn benchmark_loader_tree(samples: usize) -> Result<Value, String> {
    let mut values = Vec::with_capacity(samples);
    for _ in 0..samples {
        let mut loader = new_loader()?;
        let start = Instant::now();
        let loaded = block_on(loader.load(benchmark_tree())).map_err(|error| error.to_string());
        let elapsed = start.elapsed();
        let root = loader.context();
        let unloaded = if loaded.is_ok() {
            block_on(loader.unload()).map_err(|error| error.to_string())
        } else {
            Ok(())
        };
        let disposed = dispose_root(&root);
        loaded?;
        unloaded?;
        disposed?;
        values.push(nanoseconds(elapsed)?);
    }
    summarize("loader_tree", "ns", LOADER_ENTRIES, values, None)
}

fn benchmark_loader_update(samples: usize) -> Result<Value, String> {
    let mut values = Vec::with_capacity(samples);
    for _ in 0..samples {
        let mut loader = new_loader()?;
        block_on(loader.load(benchmark_tree())).map_err(|error| error.to_string())?;
        let patch = Patch::UpdateConfig {
            id: EntryId::new("benchmark-0").map_err(|error| error.to_string())?,
            config: json!({ "revision": 2 }),
        };
        let start = Instant::now();
        let updated = block_on(loader.update(&[patch])).map_err(|error| error.to_string());
        let elapsed = start.elapsed();
        let root = loader.context();
        let unloaded = if updated.is_ok() {
            block_on(loader.unload()).map_err(|error| error.to_string())
        } else {
            Ok(())
        };
        let disposed = dispose_root(&root);
        updated?;
        unloaded?;
        disposed?;
        values.push(nanoseconds(elapsed)?);
    }
    summarize("loader_update", "ns", LOADER_ENTRIES, values, None)
}

fn new_loader() -> Result<Loader, String> {
    let mut runtime = NativePluginRuntime::new();
    runtime
        .register("benchmark/noop", || NoopPlugin)
        .map_err(|error| error.to_string())?;
    let runtime: Arc<dyn LoaderRuntime> = Arc::new(runtime);
    Loader::try_new(Arc::new(BenchResolver), [runtime]).map_err(|error| error.to_string())
}

fn benchmark_tree() -> EntryTree {
    EntryTree {
        entries: (0..LOADER_ENTRIES)
            .map(|index| Entry {
                package: "benchmark/noop".into(),
                options: EntryOptions {
                    id: EntryId::new(format!("benchmark-{index}"))
                        .expect("fixed benchmark entry IDs are valid"),
                    name: None,
                    runtime: RuntimeKind::Native,
                    config: json!({ "revision": 1 }),
                    inject: Vec::new(),
                    isolate: Vec::new(),
                    intercept: json!({}),
                    disabled: false,
                    group: None,
                },
            })
            .collect(),
        groups: Vec::new(),
    }
}

fn benchmark_root_dispose(samples: usize) -> Result<Value, String> {
    let mut values = Vec::with_capacity(samples);
    for _ in 0..samples {
        let context = ContextHandle::root();
        for _ in 0..ROOT_CHILDREN {
            context.child().map_err(|error| error.to_string())?;
        }
        let start = Instant::now();
        dispose_root(&context)?;
        values.push(nanoseconds(start.elapsed())?);
    }
    summarize("root_dispose", "ns", ROOT_CHILDREN, values, None)
}

fn dispose_root(context: &ContextHandle) -> Result<(), String> {
    block_on(context.scope().dispose()).map_err(|error| error.to_string())
}

fn nanoseconds(duration: Duration) -> Result<u64, String> {
    u64::try_from(duration.as_nanos())
        .map_err(|_| "sample duration exceeds u64 nanoseconds".to_owned())
}

fn operations_per_second(operations: usize, elapsed: Duration) -> Result<u64, String> {
    let elapsed_ns = elapsed.as_nanos();
    if elapsed_ns == 0 {
        return Err(
            "timer resolution produced a zero-duration sample; increase --batch".to_owned(),
        );
    }
    let operations =
        u128::try_from(operations).map_err(|_| "operation count exceeds u128".to_owned())?;
    u64::try_from(operations.saturating_mul(1_000_000_000) / elapsed_ns)
        .map_err(|_| "throughput exceeds u64 operations/s".to_owned())
}

fn summarize(
    name: &str,
    unit: &str,
    operations_per_sample: usize,
    samples: Vec<u64>,
    note: Option<&str>,
) -> Result<Value, String> {
    if samples.is_empty() {
        return Err(format!("{name} collected no samples"));
    }
    let mut sorted = samples.clone();
    sorted.sort_unstable();
    let median = sorted[sorted.len() / 2];
    let p95_index = (sorted.len() * 95).div_ceil(100).saturating_sub(1);
    let mut result = json!({
        "name": name,
        "unit": unit,
        "operationsPerSample": operations_per_sample,
        "samples": samples,
        "median": median,
        "p95": sorted[p95_index],
    });
    if let Some(note) = note {
        result["note"] = Value::String(note.to_owned());
    }
    Ok(result)
}
