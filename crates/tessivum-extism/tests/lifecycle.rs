use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::Duration,
};

use serde_json::{json, Value};
use tessivum_extism::{
    Capability, CapabilityRegistry, GuestCancellation, GuestEngine, GuestExport, GuestInstance,
    HostBindings, InMemoryGuestEngine, PluginError, PluginManifest, RequestEnvelope,
    ResourceLimits, ResponseEnvelope, WasmPackage, WasmPluginInstance, WasmResult,
};

fn manifest() -> PluginManifest {
    serde_json::from_value(json!({
        "id": "lifecycle.wasm",
        "version": "1.0.0",
        "entry": "plugin.wasm",
        "abi": "cordis.plugin/v1",
        "inject": [],
        "permissions": ["cordis.log"],
        "configSchema": { "type": "object" },
        "exports": [
            "cordis_init",
            "cordis_call",
            "cordis_event",
            "cordis_update",
            "cordis_stop"
        ]
    }))
    .expect("test manifest parses")
}

fn package() -> WasmPackage {
    WasmPackage::in_memory(manifest()).expect("test package validates")
}

fn response(input: &[u8], result: Value) -> WasmResult<Vec<u8>> {
    let request: RequestEnvelope = serde_json::from_slice(input)
        .map_err(|error| PluginError::new("INVALID_REQUEST", error.to_string(), "call"))?;
    serde_json::to_vec(&ResponseEnvelope {
        request_id: request.request_id,
        result: Some(result),
        error: None,
    })
    .map_err(|error| PluginError::new("INVALID_RESPONSE", error.to_string(), "call"))
}

fn instance(engine: Arc<dyn GuestEngine>, limits: ResourceLimits) -> WasmPluginInstance {
    WasmPluginInstance::instantiate(
        package(),
        engine,
        Arc::new(CapabilityRegistry::default()),
        limits,
        json!({ "enabled": true }),
    )
    .expect("test instance instantiates")
}

#[derive(Default)]
struct TestCancellation {
    cancelled: AtomicBool,
    calls: AtomicUsize,
}

impl GuestCancellation for TestCancellation {
    fn cancel(&self) -> WasmResult<()> {
        self.cancelled.store(true, Ordering::SeqCst);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct GateEngine {
    entered: mpsc::Sender<GuestExport>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
    cancellation: Arc<TestCancellation>,
}

impl GuestEngine for GateEngine {
    fn instantiate(
        &self,
        _package: &WasmPackage,
        _host: HostBindings,
        _limits: ResourceLimits,
    ) -> WasmResult<Box<dyn GuestInstance>> {
        Ok(Box::new(GateGuest {
            entered: self.entered.clone(),
            release: Arc::clone(&self.release),
            cancellation: Arc::clone(&self.cancellation),
        }))
    }
}

struct GateGuest {
    entered: mpsc::Sender<GuestExport>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
    cancellation: Arc<TestCancellation>,
}

impl GuestInstance for GateGuest {
    fn call(
        &mut self,
        export: GuestExport,
        input: &[u8],
        _max_output_bytes: usize,
    ) -> WasmResult<Vec<u8>> {
        self.entered
            .send(export)
            .expect("test observes guest entry");
        self.release
            .lock()
            .expect("test release lock is available")
            .recv()
            .expect("test releases guest call");
        if self.cancellation.cancelled.load(Ordering::SeqCst) {
            return Err(PluginError::new(
                "CANCELLED",
                "guest call cancelled",
                "call",
            ));
        }
        response(input, json!({ "done": true }))
    }

    fn cancellation(&self) -> Arc<dyn GuestCancellation> {
        self.cancellation.clone()
    }
}

struct ImmediateGuest;

impl GuestInstance for ImmediateGuest {
    fn call(
        &mut self,
        _export: GuestExport,
        input: &[u8],
        _max_output_bytes: usize,
    ) -> WasmResult<Vec<u8>> {
        response(input, json!({ "done": true }))
    }

    fn cancellation(&self) -> Arc<dyn GuestCancellation> {
        Arc::new(TestCancellation::default())
    }
}
struct BoundedOutputEngine {
    seen_limit: Arc<AtomicUsize>,
    allocation_attempts: Arc<AtomicUsize>,
}

impl GuestEngine for BoundedOutputEngine {
    fn instantiate(
        &self,
        _package: &WasmPackage,
        _host: HostBindings,
        _limits: ResourceLimits,
    ) -> WasmResult<Box<dyn GuestInstance>> {
        Ok(Box::new(BoundedOutputGuest {
            seen_limit: Arc::clone(&self.seen_limit),
            allocation_attempts: Arc::clone(&self.allocation_attempts),
        }))
    }
}

struct BoundedOutputGuest {
    seen_limit: Arc<AtomicUsize>,
    allocation_attempts: Arc<AtomicUsize>,
}

impl GuestInstance for BoundedOutputGuest {
    fn call(
        &mut self,
        _export: GuestExport,
        _input: &[u8],
        max_output_bytes: usize,
    ) -> WasmResult<Vec<u8>> {
        self.seen_limit.store(max_output_bytes, Ordering::SeqCst);
        if max_output_bytes < 2 {
            return Err(PluginError::new(
                "OUTPUT_LIMIT_EXCEEDED",
                "guest output exceeds limit",
                "call",
            ));
        }
        self.allocation_attempts.fetch_add(1, Ordering::SeqCst);
        Ok(vec![b'{', b'}'])
    }

    fn cancellation(&self) -> Arc<dyn GuestCancellation> {
        Arc::new(TestCancellation::default())
    }
}

struct StopGateEngine {
    entered: mpsc::Sender<()>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
    cancellation: Arc<TestCancellation>,
    exports: Arc<Mutex<Vec<GuestExport>>>,
}

impl GuestEngine for StopGateEngine {
    fn instantiate(
        &self,
        _package: &WasmPackage,
        _host: HostBindings,
        _limits: ResourceLimits,
    ) -> WasmResult<Box<dyn GuestInstance>> {
        Ok(Box::new(StopGateGuest {
            entered: self.entered.clone(),
            release: Arc::clone(&self.release),
            cancellation: Arc::clone(&self.cancellation),
            exports: Arc::clone(&self.exports),
        }))
    }
}

struct StopGateGuest {
    entered: mpsc::Sender<()>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
    cancellation: Arc<TestCancellation>,
    exports: Arc<Mutex<Vec<GuestExport>>>,
}

impl GuestInstance for StopGateGuest {
    fn call(
        &mut self,
        export: GuestExport,
        input: &[u8],
        _max_output_bytes: usize,
    ) -> WasmResult<Vec<u8>> {
        self.exports
            .lock()
            .expect("export log is available")
            .push(export);
        if export == GuestExport::Stop {
            return response(input, json!({ "stopped": true }));
        }
        self.entered.send(()).expect("test observes guest entry");
        self.release
            .lock()
            .expect("test release lock is available")
            .recv()
            .expect("test releases guest call");
        if self.cancellation.cancelled.load(Ordering::SeqCst) {
            return Err(PluginError::new(
                "CANCELLED",
                "guest call cancelled",
                "call",
            ));
        }
        response(input, json!({ "done": true }))
    }

    fn cancellation(&self) -> Arc<dyn GuestCancellation> {
        self.cancellation.clone()
    }
}

#[test]
fn calls_are_serialized_before_the_guest_engine_is_entered() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let engine: Arc<dyn GuestEngine> = Arc::new(GateEngine {
        entered: entered_tx,
        release: Arc::new(Mutex::new(release_rx)),
        cancellation: Arc::new(TestCancellation::default()),
    });
    let instance = Arc::new(instance(engine, ResourceLimits::default()));

    let first_instance = Arc::clone(&instance);
    let first = thread::spawn(move || first_instance.call(json!({}), json!("first")));
    assert_eq!(
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first call reaches guest"),
        GuestExport::Call
    );

    let (second_started_tx, second_started_rx) = mpsc::channel();
    let second_instance = Arc::clone(&instance);
    let second = thread::spawn(move || {
        second_started_tx
            .send(())
            .expect("test observes second call attempt");
        second_instance.call(json!({}), json!("second"))
    });
    second_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("second call task starts");
    assert!(
        entered_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "the second call must wait behind the first before entering the guest"
    );

    release_tx.send(()).expect("first guest call is waiting");
    assert_eq!(
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second call reaches guest after first settles"),
        GuestExport::Call
    );
    release_tx.send(()).expect("second guest call is waiting");
    first
        .join()
        .expect("first call thread does not panic")
        .expect("first call succeeds");
    second
        .join()
        .expect("second call thread does not panic")
        .expect("second call succeeds");
}

#[test]
fn cancellation_is_first_wins_and_settles_the_blocked_call() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let cancellation = Arc::new(TestCancellation::default());
    let engine: Arc<dyn GuestEngine> = Arc::new(GateEngine {
        entered: entered_tx,
        release: Arc::new(Mutex::new(release_rx)),
        cancellation: Arc::clone(&cancellation),
    });
    let instance = Arc::new(instance(engine, ResourceLimits::default()));

    let calling_instance = Arc::clone(&instance);
    let call = thread::spawn(move || calling_instance.call(json!({}), json!("blocked")));
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("call reaches the cancellation gate");

    assert!(instance.cancel_current(), "the first cancellation wins");
    assert!(
        !instance.cancel_current(),
        "later cancellation cannot replace the winner"
    );
    assert_eq!(cancellation.calls.load(Ordering::SeqCst), 1);
    release_tx.send(()).expect("blocked guest call is waiting");
    let error = call
        .join()
        .expect("call thread does not panic")
        .expect_err("cancelled guest call fails");
    assert_eq!(error.code, "CANCELLED");
    assert_eq!(error.phase, "call");
}

#[test]
fn stop_rejects_new_calls_cancels_in_flight_work_and_calls_the_guest_stop_export() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let cancellation = Arc::new(TestCancellation::default());
    let exports = Arc::new(Mutex::new(Vec::new()));
    let instance = Arc::new(instance(
        Arc::new(StopGateEngine {
            entered: entered_tx,
            release: Arc::new(Mutex::new(release_rx)),
            cancellation: Arc::clone(&cancellation),
            exports: Arc::clone(&exports),
        }),
        ResourceLimits::default(),
    ));

    let active_instance = Arc::clone(&instance);
    let active = thread::spawn(move || active_instance.call(json!({}), Value::Null));
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("active call reaches guest");

    let stopping_instance = Arc::clone(&instance);
    let stopping = thread::spawn(move || stopping_instance.stop());
    for _ in 0..1_000 {
        if cancellation.calls.load(Ordering::SeqCst) == 1 {
            break;
        }
        thread::yield_now();
    }
    assert_eq!(
        cancellation.calls.load(Ordering::SeqCst),
        1,
        "stop cancels active work"
    );

    let rejected_instance = Arc::clone(&instance);
    let rejected = thread::spawn(move || rejected_instance.call(json!({}), Value::Null));
    release_tx.send(()).expect("active guest call is waiting");
    active
        .join()
        .expect("active thread does not panic")
        .expect_err("stop cancels active call");
    stopping
        .join()
        .expect("stop thread does not panic")
        .expect("stop succeeds after cancellation settles");
    assert!(
        rejected
            .join()
            .expect("rejected thread does not panic")
            .is_err(),
        "calls begun after stop starts are rejected"
    );
    assert_eq!(
        *exports.lock().expect("export log is available"),
        vec![GuestExport::Call, GuestExport::Stop]
    );
}

#[test]
fn resource_limits_are_forwarded_and_reject_oversized_input_and_output() {
    struct LimitEngine {
        seen: Arc<Mutex<Vec<ResourceLimits>>>,
    }
    impl GuestEngine for LimitEngine {
        fn instantiate(
            &self,
            _package: &WasmPackage,
            _host: HostBindings,
            limits: ResourceLimits,
        ) -> WasmResult<Box<dyn GuestInstance>> {
            self.seen
                .lock()
                .expect("limit log is available")
                .push(limits);
            Ok(Box::new(ImmediateGuest))
        }
    }

    let limits = ResourceLimits {
        memory_pages: 7,
        timeout: Duration::from_millis(7),
        fuel: 77,
        max_concurrency: 1,
        max_input_bytes: 1,
        max_output_bytes: 1,
    };
    let seen = Arc::new(Mutex::new(Vec::new()));
    let engine: Arc<dyn GuestEngine> = Arc::new(LimitEngine {
        seen: Arc::clone(&seen),
    });
    let limited_instance = instance(engine, limits);
    let forwarded = seen
        .lock()
        .expect("limit log is available")
        .pop()
        .expect("engine receives limits");
    assert_eq!(forwarded.memory_pages, 7);
    assert_eq!(forwarded.timeout, Duration::from_millis(7));
    assert_eq!(forwarded.fuel, 77);
    assert_eq!(forwarded.max_concurrency, 1);

    let error = limited_instance
        .call(json!({}), json!("larger than one byte"))
        .expect_err("input limit rejects before guest execution");
    assert!(!error.code.is_empty());
    assert_eq!(error.phase, "call");

    let engine = InMemoryGuestEngine::new(move |_export, request, _host| {
        Ok(ResponseEnvelope {
            request_id: request.request_id,
            result: Some(json!("larger than one byte")),
            error: None,
        })
    });

    let instance = instance(
        Arc::new(engine),
        ResourceLimits {
            max_output_bytes: 1,
            ..ResourceLimits::default()
        },
    );
    let error = instance
        .call(json!({}), Value::Null)
        .expect_err("output limit rejects oversized guest response");
    assert!(!error.code.is_empty());
    assert_eq!(error.phase, "call");
}
#[test]
fn lifecycle_output_limit_reaches_the_engine_before_output_allocation() {
    let seen_limit = Arc::new(AtomicUsize::new(0));
    let allocation_attempts = Arc::new(AtomicUsize::new(0));
    let instance = instance(
        Arc::new(BoundedOutputEngine {
            seen_limit: Arc::clone(&seen_limit),
            allocation_attempts: Arc::clone(&allocation_attempts),
        }),
        ResourceLimits {
            max_output_bytes: 1,
            ..ResourceLimits::default()
        },
    );

    let error = instance
        .call(json!({}), Value::Null)
        .expect_err("engine rejects oversized output before copying it");
    assert_eq!(error.code, "OUTPUT_LIMIT_EXCEEDED");
    assert_eq!(seen_limit.load(Ordering::SeqCst), 1);
    assert_eq!(allocation_attempts.load(Ordering::SeqCst), 0);
}

#[test]
fn a_guest_that_exceeds_its_timeout_fails_without_turning_into_success() {
    let engine = InMemoryGuestEngine::new(move |_export, request, _host| {
        thread::sleep(Duration::from_millis(10));
        Ok(ResponseEnvelope {
            request_id: request.request_id,
            result: Some(json!({ "late": true })),
            error: None,
        })
    });
    let instance = instance(
        Arc::new(engine),
        ResourceLimits {
            timeout: Duration::from_millis(1),
            ..ResourceLimits::default()
        },
    );
    let error = instance
        .call(json!({}), Value::Null)
        .expect_err("timed out guest result is rejected");
    assert!(!error.code.is_empty());
    assert_eq!(error.phase, "call");
}

#[test]
fn trapped_guests_do_not_poison_an_independent_instance() {
    let trapped = InMemoryGuestEngine::new(move |_export, _request, _host| {
        Err(PluginError::new("GUEST_TRAP", "unreachable", "call"))
    });
    let trapped = instance(Arc::new(trapped), ResourceLimits::default());
    let error = trapped
        .call(json!({}), Value::Null)
        .expect_err("trap surfaces as a stable failure");
    assert_eq!(error.code, "GUEST_TRAP");

    let healthy = InMemoryGuestEngine::new(move |_export, request, _host| {
        Ok(ResponseEnvelope {
            request_id: request.request_id,
            result: Some(json!({ "healthy": true })),
            error: None,
        })
    });
    let healthy = instance(Arc::new(healthy), ResourceLimits::default());
    assert_eq!(
        healthy
            .call(json!({}), Value::Null)
            .expect("independent guest remains usable"),
        json!({ "healthy": true })
    );
}

#[test]
fn update_event_and_stop_follow_the_guest_lifecycle_and_invalidate_host_handles() {
    let exports = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&exports);
    let engine = InMemoryGuestEngine::new(move |export, request, _host| {
        observed
            .lock()
            .expect("export log is available")
            .push(export);
        Ok(ResponseEnvelope {
            request_id: request.request_id,
            result: Some(json!({ "ok": true })),
            error: None,
        })
    });
    let instance = instance(Arc::new(engine), ResourceLimits::default());

    assert_eq!(
        instance
            .init(json!({ "boot": true }))
            .expect("init succeeds"),
        json!({ "ok": true })
    );
    assert_eq!(
        instance
            .event(json!({ "event": "notice" }), json!({ "value": 1 }))
            .expect("event reaches guest"),
        json!({ "ok": true })
    );
    assert_eq!(
        instance
            .update(json!({ "source": "reload" }), json!({ "enabled": false }))
            .expect("config update reaches guest"),
        json!({ "ok": true })
    );
    instance.stop().expect("stop succeeds");
    let error = instance
        .call(json!({}), Value::Null)
        .expect_err("stopped instances reject new calls");
    assert!(!error.code.is_empty());
    assert_eq!(
        *exports.lock().expect("export log is available"),
        vec![
            GuestExport::Init,
            GuestExport::Event,
            GuestExport::Update,
            GuestExport::Stop,
        ]
    );

    let registry = Arc::new(CapabilityRegistry::default());
    registry.grant(Capability::Log);
    registry
        .register(Capability::Log, |_request| Ok(Value::Null))
        .expect("log capability registers");
    let handle = Arc::new(Mutex::new(None));
    struct HandleEngine(Arc<Mutex<Option<HostBindings>>>);
    impl GuestEngine for HandleEngine {
        fn instantiate(
            &self,
            _package: &WasmPackage,
            host: HostBindings,
            _limits: ResourceLimits,
        ) -> WasmResult<Box<dyn GuestInstance>> {
            *self.0.lock().expect("handle slot is available") = Some(host);
            Ok(Box::new(ImmediateGuest))
        }
    }
    let instance = WasmPluginInstance::instantiate(
        package(),
        Arc::new(HandleEngine(Arc::clone(&handle))),
        Arc::clone(&registry),
        ResourceLimits::default(),
        json!({}),
    )
    .expect("handle guest instantiates");
    let host = handle
        .lock()
        .expect("handle slot is available")
        .take()
        .expect("guest receives a host binding");
    instance.stop().expect("stopping handle guest succeeds");
    assert!(
        host.invoke(Capability::Log, json!({ "message": "after stop" }))
            .is_err(),
        "host bindings become unusable after their instance stops"
    );
}
