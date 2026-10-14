use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    future::Future,
    io::ErrorKind,
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex},
    task::{Context, Poll, Waker},
    thread,
    time::Duration,
};

use serde_json::{json, Value};
use tessivum_core::{ContextHandle, Entry, LoaderRuntime, ResolvedPackage, RuntimeHandle};
use tessivum_extism::{
    Capability, CapabilityRegistry, CapabilityRequest, ExtismGuestEngine, GuestExport,
    PluginManifest, ResourceLimits, WasmPackage, WasmPluginInstance, WasmPluginRuntime, WasmResult,
    ABI_VERSION,
};
#[derive(Clone, Copy)]
enum GuestLanguage {
    Rust,
    TypeScript,
    Go,
    Python,
}

impl GuestLanguage {
    fn name(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::TypeScript => "typescript",
            Self::Go => "go",
            Self::Python => "python",
        }
    }

    fn manifest_id(self) -> &'static str {
        match self {
            Self::Rust => "real-rust-matrix",
            Self::TypeScript => "real-typescript-matrix",
            Self::Go => "real-go-matrix",
            Self::Python => "real-python-matrix",
        }
    }

    fn manifest_entry(self) -> &'static str {
        match self {
            Self::Rust => "tessivum_wasm_minimal.wasm",
            Self::TypeScript => "tessivum_wasm_typescript.wasm",
            Self::Go => "tessivum_wasm_go.wasm",
            Self::Python => "tessivum_wasm_python.wasm",
        }
    }

    fn package_specifier(self) -> &'static str {
        match self {
            Self::Rust => "real-rust",
            Self::TypeScript => "real-typescript",
            Self::Go => "real-go",
            Self::Python => "real-python",
        }
    }
}

static GUEST_LANGUAGE: LazyLock<GuestLanguage> = LazyLock::new(|| {
    match std::env::var("TESSIVUM_GUEST_LANGUAGE") {
        Ok(language) if language == "rust" => GuestLanguage::Rust,
        Ok(language) if language == "typescript" => GuestLanguage::TypeScript,
        Ok(language) if language == "go" => GuestLanguage::Go,
        Ok(language) if language == "python" => GuestLanguage::Python,
        Ok(language) => {
            panic!("unsupported TESSIVUM_GUEST_LANGUAGE={language:?}; expected rust, typescript, go or python")
        }
        Err(std::env::VarError::NotPresent) => GuestLanguage::Rust,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("TESSIVUM_GUEST_LANGUAGE must be valid UTF-8: rust, typescript, go or python")
        }
    }
});

static WASM: LazyLock<Vec<u8>> = LazyLock::new(|| {
    let path = guest_path();
    fs::read(&path).unwrap_or_else(|error| {
        panic!(
            "cannot read selected {} guest at {}: {error}; set TESSIVUM_WASM_GUEST to a compiled guest artifact",
            GUEST_LANGUAGE.name(),
            path.display()
        )
    })
});

fn guest_path() -> PathBuf {
    let language = *GUEST_LANGUAGE;
    if let Some(path) = std::env::var_os("TESSIVUM_WASM_GUEST") {
        return PathBuf::from(path);
    }
    match language {
        GuestLanguage::Rust => Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/wasm32-unknown-unknown/debug/tessivum_wasm_minimal.wasm"),
        _ => panic!(
            "TESSIVUM_WASM_GUEST is required when TESSIVUM_GUEST_LANGUAGE={}",
            language.name()
        ),
    }
}

fn wasm() -> Vec<u8> {
    WASM.clone()
}

fn manifest() -> PluginManifest {
    PluginManifest {
        id: GUEST_LANGUAGE.manifest_id().into(),
        version: "1.0.0".into(),
        entry: GUEST_LANGUAGE.manifest_entry().into(),
        abi: ABI_VERSION.into(),
        inject: Vec::new(),
        permissions: Capability::ALL.to_vec(),
        config_schema: json!({ "type": "object" }),
        exports: GuestExport::ALL.to_vec(),
    }
}

fn package() -> WasmPackage {
    WasmPackage::from_bytes(manifest(), wasm()).expect("compiled guest satisfies the v1 ABI")
}

fn instance(
    registry: Arc<CapabilityRegistry>,
    limits: ResourceLimits,
    config: Value,
) -> WasmPluginInstance {
    WasmPluginInstance::instantiate(
        package(),
        Arc::new(ExtismGuestEngine),
        registry,
        limits,
        config,
    )
    .expect("compiled guest instantiates in the real Extism engine")
}

#[derive(Default)]
struct Effects {
    logs: Vec<String>,
    config: BTreeMap<String, Value>,
    service_calls: usize,
    emitted: Vec<(String, Value)>,
    subscriptions: BTreeMap<String, String>,
    next_subscription: usize,
    kv: BTreeMap<String, Value>,
    seen: Vec<(Capability, String, String)>,
}

struct HostState(Mutex<Effects>);

impl HostState {
    fn new() -> Self {
        let mut effects = Effects::default();
        effects.config.insert("theme".into(), json!("dark"));
        Self(Mutex::new(effects))
    }

    fn handle(&self, request: CapabilityRequest) -> WasmResult<Value> {
        assert_eq!(request.plugin_id, GUEST_LANGUAGE.manifest_id());
        assert_ne!(request.instance_id, request.plugin_id);
        let mut effects = self.0.lock().expect("host effects are available");
        effects
            .seen
            .push((request.capability, request.plugin_id, request.instance_id));
        match request.capability {
            Capability::Log => {
                assert_eq!(request.payload["level"], "info");
                let message = request.payload["message"]
                    .as_str()
                    .expect("log message is a string")
                    .to_owned();
                effects.logs.push(message);
                Ok(Value::Null)
            }
            Capability::ConfigGet => {
                let key = request.payload["key"]
                    .as_str()
                    .expect("config key is a string");
                Ok(effects.config.get(key).cloned().unwrap_or(Value::Null))
            }
            Capability::ServiceCall => {
                assert_eq!(request.payload["service"], "math");
                assert_eq!(request.payload["method"], "increment");
                effects.service_calls += 1;
                Ok(json!(
                    request.payload["payload"]["value"]
                        .as_i64()
                        .expect("service value is an integer")
                        + 1
                ))
            }
            Capability::EventEmit => {
                let topic = request.payload["topic"]
                    .as_str()
                    .expect("event topic is a string")
                    .to_owned();
                effects
                    .emitted
                    .push((topic, request.payload["payload"].clone()));
                Ok(Value::Null)
            }
            Capability::EventSubscribe => {
                let topic = request.payload["topic"]
                    .as_str()
                    .expect("subscription topic is a string")
                    .to_owned();
                effects.next_subscription += 1;
                let id = format!("sub-{}", effects.next_subscription);
                effects.subscriptions.insert(id.clone(), topic);
                Ok(json!({ "id": id }))
            }
            Capability::RegistrationDispose => {
                let id = request.payload["id"]
                    .as_str()
                    .expect("registration id is a string");
                assert!(
                    effects.subscriptions.remove(id).is_some(),
                    "disposing a real subscription mutates host state"
                );
                Ok(Value::Null)
            }
            Capability::KvGet => {
                let key = request.payload["key"].as_str().expect("kv key is a string");
                Ok(effects.kv.get(key).cloned().unwrap_or(Value::Null))
            }
            Capability::KvSet => {
                let key = request.payload["key"]
                    .as_str()
                    .expect("kv key is a string")
                    .to_owned();
                effects.kv.insert(key, request.payload["value"].clone());
                Ok(Value::Null)
            }
        }
    }
}

fn registry(grant_all: bool) -> (Arc<CapabilityRegistry>, Arc<HostState>) {
    let registry = Arc::new(CapabilityRegistry::new());
    let state = Arc::new(HostState::new());
    for capability in Capability::ALL {
        let state = Arc::clone(&state);
        registry
            .register(capability, move |request| state.handle(request))
            .expect("each host capability registers once");
        if grant_all {
            registry.grant(capability);
        }
    }
    (registry, state)
}

fn capability_call(instance: &WasmPluginInstance, capability: Capability) -> Value {
    instance
        .call(
            json!({ "source": GUEST_LANGUAGE.manifest_id() }),
            json!({ "op": "capability", "capability": capability.as_str() }),
        )
        .expect("the guest reports host capability outcomes as a result")
}

fn assert_denied(result: &Value) {
    assert_eq!(result["ok"], false);
    assert_eq!(result["error"]["code"], "PERMISSION_DENIED");
    assert_eq!(result["error"]["phase"], "call");
}

#[test]
#[ignore = "requires a compiled WASM guest"]
fn real_guest_rejects_invalid_manifest_and_requests() {
    let mut invalid = manifest();
    invalid.abi = "cordis.plugin/v2".into();
    assert_eq!(
        WasmPackage::from_bytes(invalid, wasm()).unwrap_err().code,
        "ABI_UNSUPPORTED"
    );
    let mut missing = manifest();
    missing.exports.pop();
    assert_eq!(
        WasmPackage::from_bytes(missing, wasm()).unwrap_err().code,
        "MANIFEST_INVALID"
    );
    let functions = Capability::ALL.into_iter().map(|capability| {
        extism::Function::new(
            capability.as_str(),
            [extism::PTR],
            [extism::PTR],
            extism::UserData::new(()),
            |_, _, _, _| panic!("malformed requests must not reach a host function"),
        )
        .with_namespace("extism:host/user")
    });
    let mut raw = extism::PluginBuilder::new(
        extism::Manifest::new([extism::Wasm::data(wasm())]).disallow_all_hosts(),
    )
    .with_functions(functions)
    .with_wasi(false)
    .build()
    .unwrap();
    for input in [
        "{not-json",
        r#"{"requestId":"bad","context":{},"payload":{},"extra":true}"#,
        r#"{"RequestId":"bad","context":{},"payload":{}}"#,
        r#"{"requestId":null,"context":{},"payload":{}}"#,
        r#"{"context":{},"payload":{}}"#,
    ] {
        let output: &str = raw.call("cordis_call", input).unwrap();
        let response: Value = serde_json::from_str(output).unwrap();
        assert_eq!(response["error"]["code"], "invalid_request");
        assert_eq!(response["error"]["phase"], "call");
        assert!(response.get("result").is_none());
    }
    if matches!(*GUEST_LANGUAGE, GuestLanguage::Python) {
        for input in [
            r#"{"requestId":"bad","context":{},"payload":"\ud83d"}"#,
            r#"{"requestId":"bad","context":{},"payload":"\ude00"}"#,
            r#"{"requestId":"bad","context":{},"payload":"\ud83d\u0041"}"#,
        ] {
            let output: &str = raw.call("cordis_call", input).unwrap();
            let response: Value = serde_json::from_str(output).unwrap();
            assert_eq!(response["error"]["code"], "invalid_request");
        }
    }
    let output: &str = raw
        .call("cordis_call", r#"{"requestId":"unicode","context":{},"payload":{"text":"\ud83d\ude00","literal":"\\ud83d\\ude00"}}"#)
        .unwrap();
    let response: Value = serde_json::from_str(output).unwrap();
    assert_eq!(
        response["result"]["echo"],
        json!({"text":"\u{1f600}","literal":r"\ud83d\ude00"})
    );
}

#[test]
#[ignore = "requires a compiled WASM guest"]
fn real_guest_manifest_envelopes_five_exports_update_and_unload() {
    let bytes = wasm();
    for forbidden_import in [b"wasi_snapshot_preview1".as_slice(), b"wasi_unstable"] {
        assert!(
            !bytes
                .windows(forbidden_import.len())
                .any(|window| window == forbidden_import),
            "selected guest must not depend on WASI"
        );
    }

    let (registry, state) = registry(true);
    let guest = instance(
        registry,
        ResourceLimits::default(),
        json!({ "matrix": true, "revision": 1 }),
    );
    assert_eq!(guest.manifest(), &manifest());
    assert_eq!(guest.manifest().exports, GuestExport::ALL.to_vec());

    assert_eq!(
        guest
            .init(json!({ "entry": "matrix" }))
            .expect("init succeeds"),
        json!({ "abi": ABI_VERSION, "initialized": true })
    );
    let call = guest
        .call(
            json!({ "trace": "ctx-1" }),
            json!({ "op": "inspect", "answer": 42 }),
        )
        .expect("call succeeds");
    assert_eq!(call["context"], json!({ "trace": "ctx-1" }));
    assert_eq!(call["payload"], json!({ "op": "inspect", "answer": 42 }));
    assert!(
        call["requestId"]
            .as_str()
            .is_some_and(|request_id| request_id.starts_with(guest.instance_id())),
        "real request and response envelopes preserve the generated request id"
    );
    assert_eq!(
        guest
            .event(json!({ "trace": "ctx-2" }), json!({ "topic": "notice" }))
            .expect("event succeeds"),
        json!({ "accepted": true })
    );
    assert_eq!(
        guest
            .update(
                json!({ "trace": "ctx-3" }),
                json!({ "matrix": true, "revision": 2 }),
            )
            .expect("update succeeds"),
        json!({ "updated": true })
    );
    assert_eq!(guest.config(), json!({ "matrix": true, "revision": 2 }));
    guest.stop().expect("stop export succeeds");
    let stopped = guest
        .call(json!({}), Value::Null)
        .expect_err("unloaded instance rejects its old handle");
    assert_eq!(stopped.code, "INSTANCE_STOPPED");

    let effects = state.0.lock().expect("host effects are available");
    assert_eq!(
        effects.logs,
        [
            "lifecycle:init",
            "lifecycle:call",
            "lifecycle:event",
            "lifecycle:update",
            "lifecycle:stop",
        ]
    );
    drop(effects);

    for phase in ["init", "call", "event", "update", "stop"] {
        let (registry, state) = self::registry(phase != "init");
        let guest = instance(
            Arc::clone(&registry),
            ResourceLimits::default(),
            json!({ "matrix": true, "revision": 1 }),
        );
        if phase != "init" {
            guest.init(Value::Null).unwrap();
            registry.revoke(Capability::Log);
        }
        let error = match phase {
            "init" => guest.init(Value::Null).unwrap_err(),
            "call" => guest.call(Value::Null, Value::Null).unwrap_err(),
            "event" => guest.event(Value::Null, Value::Null).unwrap_err(),
            "update" => guest
                .update(Value::Null, json!({ "matrix": true, "revision": 2 }))
                .unwrap_err(),
            "stop" => guest.stop().unwrap_err(),
            _ => unreachable!(),
        };
        assert_eq!(error.code, "GUEST_REJECTED", "denied {phase}");
        assert_eq!(error.phase, phase);
        assert_eq!(guest.config(), json!({ "matrix": true, "revision": 1 }));
        let effects = state.0.lock().unwrap();
        assert_eq!(
            effects.logs,
            if phase == "init" {
                vec![]
            } else {
                vec!["lifecycle:init"]
            }
        );
    }
}

#[test]
#[ignore = "requires a compiled WASM guest"]
fn real_guest_all_host_functions_grant_deny_revoke_and_mutate_state() {
    let (registry, state) = registry(false);
    let guest = instance(Arc::clone(&registry), ResourceLimits::default(), json!({}));

    let mut undeclared = manifest();
    undeclared.permissions.clear();
    let denied = WasmPluginInstance::instantiate(
        WasmPackage::from_bytes(undeclared, wasm()).unwrap(),
        Arc::new(ExtismGuestEngine),
        Arc::clone(&registry),
        ResourceLimits::default(),
        json!({}),
    )
    .unwrap();
    for capability in Capability::ALL {
        assert_denied(&capability_call(&guest, capability));
        registry.grant(capability);
        assert_denied(&capability_call(&denied, capability));
        let granted = capability_call(&guest, capability);
        assert_eq!(granted["ok"], true, "{} is callable", capability.as_str());
        match capability {
            Capability::ConfigGet => assert_eq!(granted["value"], "dark"),
            Capability::ServiceCall => assert_eq!(granted["value"], 42),
            Capability::EventSubscribe => assert_eq!(granted["value"]["id"], "sub-1"),
            Capability::KvGet => assert_eq!(granted["value"], Value::Null),
            _ => {}
        }
        registry.revoke(capability);
        assert_denied(&capability_call(&guest, capability));
    }

    registry.grant(Capability::KvGet);
    let stored = capability_call(&guest, Capability::KvGet);
    assert_eq!(stored, json!({ "ok": true, "value": 9 }));
    registry.revoke(Capability::KvGet);

    let effects = state.0.lock().expect("host effects are available");
    assert_eq!(effects.logs, ["matrix-log"]);
    assert_eq!(effects.service_calls, 1);
    assert_eq!(
        effects.emitted,
        [("matrix.notice".into(), json!({ "value": 7 }))]
    );
    assert!(effects.subscriptions.is_empty());
    assert_eq!(effects.kv.get("matrix-key"), Some(&json!(9)));
    let seen_capabilities = effects
        .seen
        .iter()
        .map(|(capability, _, _)| *capability)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        seen_capabilities,
        Capability::ALL.into_iter().collect::<BTreeSet<_>>()
    );
    let instance_ids = effects
        .seen
        .iter()
        .map(|(_, _, instance_id)| instance_id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(instance_ids, BTreeSet::from([guest.instance_id()]));
}

fn assert_fault(limits: ResourceLimits, payload: Value, code: &str) {
    let guest = instance(Arc::new(CapabilityRegistry::default()), limits, json!({}));
    let error = guest
        .call(json!({}), payload)
        .expect_err("deliberate guest fault must fail closed");
    assert_eq!(error.code, code);
    assert_eq!(error.phase, "call");
    if matches!(code, "GUEST_TRAP" | "FUEL_LIMIT_EXCEEDED") {
        assert_interrupted_python(&guest);
    }
}

fn assert_interrupted_python(guest: &WasmPluginInstance) {
    if matches!(*GUEST_LANGUAGE, GuestLanguage::Python) {
        assert_eq!(
            guest.call(json!({}), json!({})).unwrap_err().code,
            "GUEST_TRAP"
        );
        assert_eq!(guest.stop().unwrap_err().code, "GUEST_TRAP");
        assert!(guest.is_stopped());
        assert_eq!(
            guest.call(json!({}), json!({})).unwrap_err().code,
            "INSTANCE_STOPPED"
        );
    }
}

#[test]
#[ignore = "requires a compiled WASM guest"]
fn real_guest_errors_malformed_trap_and_resource_limits_fail_closed() {
    assert_fault(
        ResourceLimits::default(),
        json!({ "op": "reject" }),
        "GUEST_REJECTED",
    );
    for op in ["malformed", "unknown-field", "wrong-id", "both-results"] {
        assert_fault(
            ResourceLimits::default(),
            json!({ "op": op }),
            "PROTOCOL_INVALID",
        );
    }
    assert_fault(
        ResourceLimits::default(),
        json!({ "op": "trap" }),
        "GUEST_TRAP",
    );
    if !matches!(*GUEST_LANGUAGE, GuestLanguage::Rust) {
        assert_fault(
            ResourceLimits::default(),
            json!({ "op": "wasi-clock" }),
            "GUEST_TRAP",
        );
    }
    let listener = TcpListener::bind("127.0.0.1:0").expect("loopback probe binds");
    listener
        .set_nonblocking(true)
        .expect("loopback probe is nonblocking");
    let address = listener
        .local_addr()
        .expect("loopback address is available");
    let network = instance(
        Arc::new(CapabilityRegistry::default()),
        ResourceLimits::default(),
        json!({}),
    )
    .call(
        json!({}),
        json!({ "op": "network", "url": format!("http://{address}/") }),
    );
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == ErrorKind::WouldBlock),
        "default sandbox must block network before a connection is made"
    );
    let network = network.expect_err("default sandbox denies guest network");
    assert_eq!(network.code, "GUEST_TRAP");
    assert_eq!(network.phase, "call");

    let input = ResourceLimits {
        max_input_bytes: 128,
        ..ResourceLimits::default()
    };
    assert_fault(
        input,
        json!({ "op": "inspect", "data": "x".repeat(256) }),
        "INPUT_LIMIT_EXCEEDED",
    );

    let output = ResourceLimits {
        max_output_bytes: 512,
        ..ResourceLimits::default()
    };
    assert_fault(
        output,
        json!({ "op": "output", "bytes": 4096 }),
        "OUTPUT_LIMIT_EXCEEDED",
    );
    let allowed = instance(
        Arc::new(CapabilityRegistry::default()),
        ResourceLimits::default(),
        json!({}),
    );
    assert_eq!(
        allowed
            .call(json!({}), json!({"op":"output", "bytes":4096}))
            .unwrap()["data"],
        "x".repeat(4096)
    );
    assert_eq!(
        allowed
            .call(
                json!({}),
                json!({"op":"allocate", "bytes":16 * 1024 * 1024})
            )
            .unwrap()["allocated"],
        16 * 1024 * 1024
    );
    if matches!(*GUEST_LANGUAGE, GuestLanguage::Python) {
        let allowed = instance(
            Arc::new(CapabilityRegistry::default()),
            ResourceLimits {
                fuel: 100_000_000,
                ..ResourceLimits::default()
            },
            json!({}),
        );
        // Explicit collection must preserve live Python objects and subsequent exports.
        assert_eq!(
            allowed.call(json!({}), json!({"op":"gc"})).unwrap(),
            json!({"checksum":2016,"last":63})
        );
        assert_eq!(
            allowed.event(json!({}), json!({})).unwrap(),
            json!({"accepted":true})
        );
        assert_eq!(
            allowed.update(json!({}), json!({"revision":2})).unwrap(),
            json!({"updated":true})
        );
        assert_eq!(allowed.config(), json!({"revision":2}));
        allowed.stop().unwrap();
        assert_eq!(
            allowed.call(json!({}), Value::Null).unwrap_err().code,
            "INSTANCE_STOPPED"
        );
    }

    let memory = ResourceLimits {
        memory_pages: 128,
        ..ResourceLimits::default()
    };
    assert_fault(
        memory,
        json!({ "op": "allocate", "bytes": 16 * 1024 * 1024 }),
        "GUEST_TRAP",
    );

    let fuel = ResourceLimits {
        fuel: 10_000,
        ..ResourceLimits::default()
    };
    assert_fault(fuel, json!({ "op": "spin" }), "FUEL_LIMIT_EXCEEDED");

    let timeout = ResourceLimits {
        timeout: Duration::from_millis(20),
        fuel: 10_000_000_000,
        ..ResourceLimits::default()
    };
    let timed = instance(Arc::new(CapabilityRegistry::default()), timeout, json!({}));
    let started = std::time::Instant::now();
    assert_eq!(
        timed
            .call(json!({}), json!({"op":"spin"}))
            .unwrap_err()
            .code,
        "TIMEOUT"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "timeout must interrupt, not wait for fuel exhaustion"
    );
    assert_interrupted_python(&timed);
}

#[test]
#[ignore = "requires a compiled WASM guest"]
fn real_guest_cancellation_interrupts_running_wasm() {
    let limits = ResourceLimits {
        fuel: 10_000_000_000,
        ..ResourceLimits::default()
    };
    let registry = Arc::new(CapabilityRegistry::default());
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    registry
        .register(Capability::Log, move |request: CapabilityRequest| {
            assert_eq!(request.payload["message"], "spin-ready");
            ready_tx.send(()).unwrap();
            Ok(Value::Null)
        })
        .unwrap();
    registry.grant(Capability::Log);
    let guest = Arc::new(instance(registry, limits, json!({})));
    let running = Arc::clone(&guest);
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let call = thread::spawn(move || {
        finished_tx
            .send(running.call(json!({}), json!({ "op": "spin", "signalReady": true })))
            .unwrap();
    });
    ready_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("guest reached real WASM execution");
    assert!(guest.cancel_current(), "running Extism call is cancellable");
    let error = finished_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("cancellation interrupts before the five-second execution backstop")
        .expect_err("cancelled guest call cannot report success");
    call.join().expect("guest call thread does not panic");
    assert_eq!(error.code, "CANCELLED");
    assert_eq!(error.phase, "call");
    assert_interrupted_python(&guest);
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker: &Waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::yield_now(),
        }
    }
}

#[test]
#[ignore = "requires a compiled WASM guest"]
fn real_runtime_dispose_invokes_stop_and_invalidates_old_handle() {
    let (registry, state) = registry(true);
    let runtime = WasmPluginRuntime::new(registry, ResourceLimits::default());
    runtime
        .register(GUEST_LANGUAGE.package_specifier(), package())
        .expect("real guest package registers");
    let entry: Entry = serde_json::from_value(json!({
        "package": GUEST_LANGUAGE.package_specifier(),
        "id": format!("{}-entry", GUEST_LANGUAGE.package_specifier()),
        "runtime": "wasm",
        "config": { "matrix": true }
    }))
    .expect("runtime entry parses");
    let mut handle: Box<dyn RuntimeHandle> = block_on(runtime.instantiate(
        ResolvedPackage {
            specifier: GUEST_LANGUAGE.package_specifier().into(),
            location: String::new(),
        },
        entry,
        ContextHandle::root(),
    ))
    .expect("real runtime handle instantiates");

    block_on(handle.activate()).expect("real runtime activates init");
    block_on(handle.dispose()).expect("real runtime unloads stop");
    assert!(
        block_on(handle.activate()).is_err(),
        "disposed runtime handle cannot be reactivated"
    );
    assert_eq!(
        state.0.lock().expect("host effects are available").logs,
        ["lifecycle:init", "lifecycle:stop"]
    );
}
