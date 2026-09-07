use std::{
    error::Error,
    fs, io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use serde_json::{json, Value};
use tessivum_core::{
    ContextHandle, Entry, EntryId, EntryOptions, EntryTree, Loader, LoaderFuture, LoaderRuntime,
    NativeConfigSchema, NativePlugin, NativePluginDescriptor, NativePluginFuture,
    NativePluginRuntime, PackageResolver, ResolvedPackage, RuntimeKind,
};
use tessivum_extism::{
    CapabilityRegistry, GuestExport, PluginManifest, ResourceLimits, WasmPackage,
    WasmPluginRuntime, ABI_VERSION,
};
use tessivum_node_bridge::{
    ClientConfig, FrameKind, HostCommand, LegacyNodeRuntime, NodeSupervisor,
};

#[derive(Clone)]
struct NativeCounters {
    starts: Arc<AtomicUsize>,
    stops: Arc<AtomicUsize>,
}

struct NativeSmoke(NativeCounters);

impl NativePlugin for NativeSmoke {
    fn descriptor(&self) -> NativePluginDescriptor {
        NativePluginDescriptor {
            name: "runtime-smoke-native".into(),
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
        let starts = Arc::clone(&self.0.starts);
        Box::pin(async move {
            starts.fetch_add(1, Ordering::SeqCst);
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
        let stops = Arc::clone(&self.0.stops);
        Box::pin(async move {
            stops.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
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

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("runtime smoke is nested under examples/")
        .to_path_buf()
}

fn compiled_wasm(root: &Path) -> Result<Vec<u8>, Box<dyn Error>> {
    let path = std::env::var_os("TESSIVUM_WASM_GUEST")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            root.join("target/wasm32-unknown-unknown/debug/tessivum_wasm_minimal.wasm")
        });
    fs::read(&path).map_err(|source| {
        io::Error::new(
            source.kind(),
            format!(
                "cannot read compiled WASM guest at {} (build tessivum-wasm-minimal for wasm32-unknown-unknown or set TESSIVUM_WASM_GUEST): {source}",
                path.display()
            ),
        )
        .into()
    })
}

fn wasm_package(root: &Path) -> Result<WasmPackage, Box<dyn Error>> {
    Ok(WasmPackage::from_bytes(
        PluginManifest {
            id: "runtime-smoke-wasm".into(),
            version: "0.1.0".into(),
            entry: "tessivum_wasm_minimal.wasm".into(),
            abi: ABI_VERSION.into(),
            inject: Vec::new(),
            permissions: Vec::new(),
            config_schema: json!({}),
            exports: GuestExport::ALL.to_vec(),
        },
        compiled_wasm(root)?,
    )?)
}

fn host_command(root: &Path) -> HostCommand {
    let command = HostCommand::new("bun")
        .arg("run")
        .arg(root.join("node/compat-host/src/index.ts"))
        .current_dir(root.join("node/compat-host"))
        .env("BUN_RUNTIME_TRANSPILER_CACHE_PATH", "0");
    if let Some(vendor_root) = std::env::var_os("CORDIS_VENDOR_ROOT") {
        command.env("CORDIS_VENDOR_ROOT", vendor_root)
    } else {
        command
    }
}

fn entry(id: &str, package: impl Into<String>, runtime: RuntimeKind, config: Value) -> Entry {
    Entry::new(
        package,
        EntryOptions {
            id: EntryId::new(id).expect("smoke entry id is valid"),
            name: Some(id.into()),
            runtime,
            config,
            inject: Vec::new(),
            isolate: Vec::new(),
            intercept: json!({}),
            disabled: false,
            group: None,
        },
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let root = workspace_root();
    let counters = NativeCounters {
        starts: Arc::new(AtomicUsize::new(0)),
        stops: Arc::new(AtomicUsize::new(0)),
    };

    let mut native = NativePluginRuntime::new();
    native.register("smoke/native", {
        let counters = counters.clone();
        move || NativeSmoke(counters.clone())
    })?;
    let native = Arc::new(native);

    let wasm = Arc::new(WasmPluginRuntime::new(
        Arc::new(CapabilityRegistry::default()),
        ResourceLimits::default(),
    ));
    wasm.register("smoke/wasm", wasm_package(&root)?)?;

    let supervisor = NodeSupervisor::new(host_command(&root), ClientConfig::default())?;
    let client = supervisor.start()?;
    let legacy =
        Arc::new(LegacyNodeRuntime::new(client.clone()).with_timeout(Duration::from_secs(2)));
    let runtimes: [Arc<dyn LoaderRuntime>; 3] = [native, wasm, legacy];
    let mut loader = Loader::try_new(Arc::new(Resolver), runtimes)?;

    let legacy_plugin = root
        .join("fixtures/legacy/function-plugin.ts")
        .to_string_lossy()
        .into_owned();
    loader
        .load(EntryTree {
            entries: vec![
                entry("native", "smoke/native", RuntimeKind::Native, Value::Null),
                entry("wasm", "smoke/wasm", RuntimeKind::Wasm, Value::Null),
                entry(
                    "legacy",
                    legacy_plugin,
                    RuntimeKind::LegacyNode,
                    json!({ "prefix": "smoke" }),
                ),
            ],
            groups: Vec::new(),
        })
        .await?;

    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        client.request(
            FrameKind::PluginSnapshot,
            json!({ "pluginId": "legacy" }),
            Duration::from_secs(2),
        )?["state"],
        "ACTIVE"
    );

    let context = loader.context();
    loader.unload().await?;
    assert_eq!(counters.stops.load(Ordering::SeqCst), 1);
    assert!(
        client
            .request(
                FrameKind::PluginSnapshot,
                json!({ "pluginId": "legacy" }),
                Duration::from_secs(2),
            )
            .is_err(),
        "the legacy runtime must dispose its host plugin"
    );

    let scope = context.scope();
    scope.dispose().await?;
    assert!(
        scope.effects().is_empty(),
        "root scope owns no residual effects"
    );

    supervisor.shutdown()?;
    assert_eq!(supervisor.generation(), None, "the Node host is reaped");

    println!("runtime smoke complete: native, Extism WASM, and Legacy Node tore down");
    Ok(())
}
