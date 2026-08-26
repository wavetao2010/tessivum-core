use std::{
    fs,
    io::Write,
    os::unix::net::UnixStream,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Condvar, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use serde_json::json;
use tessivum_node_bridge::{
    BridgeClient, ClientConfig, Frame, FrameCodec, FrameKind, HostCommand, NodeSupervisor,
};

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

extern "C" {
    fn kill(process: i32, signal: i32) -> i32;
}

struct GrandchildGuard(u32);

impl Drop for GrandchildGuard {
    fn drop(&mut self) {
        unsafe {
            let _ = kill(self.0 as i32, 9);
        }
    }
}

fn process_is_alive(process: u32) -> bool {
    unsafe { kill(process as i32, 0) == 0 }
}

#[test]
fn supervisor_owns_a_real_host_generation_and_runs_cleanup_before_graceful_exit() {
    let supervisor = NodeSupervisor::new(host_command(), ClientConfig::default())
        .expect("supervisor accepts a Bun host command");
    assert_eq!(supervisor.generation(), None);
    assert!(supervisor.client().is_none());

    let client = supervisor.start().expect("real compat host handshakes");
    let generation = supervisor
        .generation()
        .expect("a successful handshake owns one nonzero generation");
    assert!(generation > 0);
    assert!(supervisor.client().is_some());
    client
        .heartbeat()
        .expect("a live host accepts a framed heartbeat");
    assert!(
        client
            .request(
                FrameKind::PluginSnapshot,
                json!({ "id": "not-loaded" }),
                Duration::from_secs(1),
            )
            .is_err(),
        "the real host responds to a correlated request rather than treating stdout as logs"
    );

    let cleanups = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&cleanups);
    supervisor
        .register_cleanup(generation, move || {
            observed.fetch_add(1, Ordering::SeqCst);
        })
        .expect("generation-owned cleanup registers while the host is connected");
    supervisor
        .shutdown()
        .expect("graceful exit waits for cleanup and reaps the child");
    assert_eq!(cleanups.load(Ordering::SeqCst), 1);
    assert_eq!(supervisor.generation(), None);
    assert!(supervisor.client().is_none());
}

#[test]
fn client_sends_hello_before_requests_and_rejects_wrong_version_or_generation() {
    for (protocol_version, connection_generation) in [("cordis.node/v2", 9), ("cordis.node/v1", 10)]
    {
        let (socket, mut peer) = UnixStream::pair().expect("in-process stream pair opens");
        let reader = socket.try_clone().expect("client read side clones");
        let client = BridgeClient::from_io(reader, socket, 9, ClientConfig::default())
            .expect("client owns the stream pair");
        let host = thread::spawn(move || {
            let codec = FrameCodec::default();
            let hello = codec
                .read_frame(&mut peer)
                .expect("client emits hello before doing work");
            assert_eq!(hello.kind, FrameKind::Hello);
            assert_eq!(hello.connection_generation, 9);
            let body = format!(
                r#"{{"protocolVersion":"{protocol_version}","connectionGeneration":{connection_generation},"kind":"ready","payload":{{}}}}"#
            );
            peer.write_all(&(body.len() as u32).to_be_bytes())
                .expect("test host writes invalid frame length");
            peer.write_all(body.as_bytes())
                .expect("test host writes its incompatible ready frame");
        });

        assert!(
            client.handshake(Duration::from_secs(1)).is_err(),
            "an incompatible ready frame fails before any plugin request"
        );
        client.close();
        host.join().expect("test host thread settles");
    }
}

#[test]
fn client_accepts_a_log_before_ready() {
    let (socket, mut peer) = UnixStream::pair().expect("in-process stream pair opens");
    let reader = socket.try_clone().expect("client read side clones");
    let client = BridgeClient::from_io(reader, socket, 9, ClientConfig::default())
        .expect("client owns the stream pair");
    let (logged, received_log) = mpsc::channel();
    client.set_log_handler(move |payload| {
        logged.send(payload).expect("startup log is observed");
    });
    let host = thread::spawn(move || {
        let codec = FrameCodec::default();
        assert_eq!(
            codec
                .read_frame(&mut peer)
                .expect("client sends hello")
                .kind,
            FrameKind::Hello
        );
        codec
            .write_frame(
                &mut peer,
                &Frame::new(9, FrameKind::Log, json!({ "message": "starting" })),
            )
            .expect("test host sends startup log");
        codec
            .write_frame(
                &mut peer,
                &Frame::new(9, FrameKind::Ready, json!({ "capabilities": ["web.route/v1"] })),
            )
            .expect("test host sends ready capability");
    });

    client
        .handshake(Duration::from_secs(1))
        .expect("a startup log does not abort the handshake");
    assert!(client.supports_extension("web.route/v1"));
    assert_eq!(
        received_log
            .recv_timeout(Duration::from_secs(1))
            .expect("startup log reaches the configured handler"),
        json!({ "message": "starting" })
    );
    client.close();
    host.join().expect("test host thread settles");
}

#[test]
fn client_correlates_a_response_after_a_valid_handshake() {
    let (socket, mut peer) = UnixStream::pair().expect("in-process stream pair opens");
    let reader = socket.try_clone().expect("client read side clones");
    let client = BridgeClient::from_io(reader, socket, 17, ClientConfig::default())
        .expect("client owns the stream pair");
    let (release_host, hold_host) = mpsc::channel();
    let host = thread::spawn(move || {
        let codec = FrameCodec::default();
        assert_eq!(
            codec
                .read_frame(&mut peer)
                .expect("client sends hello")
                .kind,
            FrameKind::Hello
        );
        codec
            .write_frame(&mut peer, &Frame::ready(17))
            .expect("test host sends ready");
        let request = codec
            .read_frame(&mut peer)
            .expect("client request follows ready");
        assert_eq!(request.kind, FrameKind::PluginSnapshot);
        let request_id = request.request_id.expect("plugin request is correlated");
        assert_eq!(request_id % 2, 1, "Rust-originated requests use odd correlations");
        codec
            .write_frame(
                &mut peer,
                &Frame::response(
                    17,
                    request_id,
                    json!({ "id": "fixture", "state": "ACTIVE" }),
                ),
            )
            .expect("test host sends a correlated response");
        hold_host
            .recv()
            .expect("client releases test host after response");
    });

    client
        .handshake(Duration::from_secs(1))
        .expect("matching ready succeeds");
    assert!(!client.supports_extension("web.route/v1"), "old empty ready advertises no extensions");
    assert_eq!(
        client
            .request(
                FrameKind::PluginSnapshot,
                json!({ "id": "fixture" }),
                Duration::from_secs(1),
            )
            .expect("matching response resolves the pending request"),
        json!({ "id": "fixture", "state": "ACTIVE" })
    );
    client.close();
    release_host
        .send(())
        .expect("test host release is delivered");
    host.join().expect("test host thread settles");
}

#[test]
fn cancellation_is_first_wins_and_a_late_response_cannot_reopen_the_correlation() {
    let (socket, mut peer) = UnixStream::pair().expect("in-process stream pair opens");
    let reader = socket.try_clone().expect("client read side clones");
    let client = BridgeClient::from_io(reader, socket, 23, ClientConfig::default())
        .expect("client owns the stream pair");
    let host = thread::spawn(move || {
        let codec = FrameCodec::default();
        assert_eq!(
            codec
                .read_frame(&mut peer)
                .expect("client sends hello")
                .kind,
            FrameKind::Hello
        );
        codec
            .write_frame(&mut peer, &Frame::ready(23))
            .expect("test host sends ready");
        let request = codec
            .read_frame(&mut peer)
            .expect("client sends the cancellable request");
        let request_id = request.request_id.expect("request has correlation");
        assert_eq!(
            codec
                .read_frame(&mut peer)
                .expect("client sends cancel")
                .kind,
            FrameKind::Cancel
        );
        codec
            .write_frame(
                &mut peer,
                &Frame::response(23, request_id, json!({ "late": true })),
            )
            .expect("test host sends a deliberately late response");
    });

    client
        .handshake(Duration::from_secs(1))
        .expect("matching ready succeeds");
    let request = client
        .begin_request(FrameKind::PluginSnapshot, json!({ "pluginId": "fixture" }))
        .expect("ready client creates a correlated request");
    let request_id = request.request_id();
    assert!(
        request.cancel(),
        "the cancel that removes the pending request wins"
    );
    assert!(
        !client.cancel(request_id),
        "once cancelled, the same correlation id has no second terminal transition"
    );
    client.close();
    host.join().expect("test host thread settles");
}

#[test]
fn cleanup_panic_does_not_prevent_later_generation_cleanup() {
    let supervisor = NodeSupervisor::new(host_command(), ClientConfig::default())
        .expect("supervisor accepts a Bun host command");
    supervisor.start().expect("real compat host handshakes");
    let generation = supervisor.generation().expect("host generation is active");
    let completed = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&completed);
    supervisor
        .register_cleanup(generation, || panic!("first cleanup deliberately panics"))
        .expect("panicking cleanup registers");
    supervisor
        .register_cleanup(generation, move || {
            observed.fetch_add(1, Ordering::SeqCst);
        })
        .expect("later cleanup registers");

    supervisor
        .shutdown()
        .expect("a cleanup panic cannot abort graceful shutdown");
    assert_eq!(completed.load(Ordering::SeqCst), 1);
}

#[test]
fn host_command_redacts_values_and_clears_ambient_environment() {
    let debug = format!(
        "{:?}",
        HostCommand::new("bun").env("TESSIVUM_TEST_API_KEY", "top-secret-value")
    );
    assert!(!debug.contains("top-secret-value"));

    let secret_key = format!("TESSIVUM_AMBIENT_SECRET_{}", std::process::id());
    std::env::set_var(&secret_key, "ambient-secret");
    let script = [
        r#"const secretKey = "#,
        &serde_json::to_string(&secret_key).expect("secret key serializes for Bun"),
        r#";
let buffered = Buffer.alloc(0);
let generation = 0;
function send(kind, requestId, payload, exitAfter = false) {
  const frame = { protocolVersion: "cordis.node/v1", connectionGeneration: generation, kind, payload };
  if (requestId !== undefined) frame.requestId = requestId;
  const body = Buffer.from(JSON.stringify(frame));
  const message = Buffer.concat([Buffer.from([(body.length >>> 24) & 255, (body.length >>> 16) & 255, (body.length >>> 8) & 255, body.length & 255]), body]);
  if (exitAfter) process.stdout.write(message, () => process.exit(0)); else process.stdout.write(message);
}
process.stdin.on("data", (chunk) => {
  buffered = Buffer.concat([buffered, chunk]);
  while (buffered.length >= 4) {
    const length = buffered.readUInt32BE(0);
    if (buffered.length < length + 4) return;
    const frame = JSON.parse(buffered.subarray(4, length + 4).toString());
    buffered = buffered.subarray(length + 4);
    if (frame.kind === "hello") {
      generation = frame.connectionGeneration;
      send("ready", undefined, {});
    } else if (frame.kind === "plugin.snapshot") {
      send("response", frame.requestId, { allowed: process.env.TESSIVUM_ALLOWED ?? null, ambient: process.env[secretKey] ?? null, path: process.env.PATH ?? null });
    } else if (frame.kind === "exit") {
      send("response", frame.requestId, {}, true);
    }
  }
});
"#,
    ]
    .concat();
    let supervisor = NodeSupervisor::new(
        HostCommand::new("bun")
            .arg("-e")
            .arg(script.as_str())
            .env("TESSIVUM_ALLOWED", "allowed-value"),
        ClientConfig::default(),
    )
    .expect("supervisor accepts an explicit environment allowlist");
    let client = supervisor
        .start()
        .expect("environment test host handshakes");
    std::env::remove_var(&secret_key);
    assert_eq!(
        client
            .request(FrameKind::PluginSnapshot, json!({}), Duration::from_secs(1),)
            .expect("host reports only its allowlisted environment"),
        json!({ "allowed": "allowed-value", "ambient": null, "path": null })
    );
    supervisor
        .shutdown()
        .expect("environment test host exits after its exit response");
}

#[test]
fn inbound_hello_before_ready_disconnects_without_invoking_handler() {
    let (socket, mut peer) = UnixStream::pair().expect("in-process stream pair opens");
    let reader = socket.try_clone().expect("client read side clones");
    let client = BridgeClient::from_io(reader, socket, 31, ClientConfig::default())
        .expect("client owns the stream pair");
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    client.set_handler(Arc::new(move |_| {
        observed.fetch_add(1, Ordering::SeqCst);
        Ok(json!({}))
    }));
    let host = thread::spawn(move || {
        let codec = FrameCodec::default();
        assert_eq!(
            codec
                .read_frame(&mut peer)
                .expect("client sends hello before the peer frame")
                .kind,
            FrameKind::Hello
        );
        codec
            .write_frame(&mut peer, &Frame::hello(31))
            .expect("peer sends an invalid reciprocal hello");
    });

    assert!(
        client.handshake(Duration::from_secs(1)).is_err(),
        "a peer cannot promote the Rust client with a reciprocal hello"
    );
    host.join().expect("test host thread settles");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn correlated_response_wins_when_peer_closes_immediately_after_writing_it() {
    let (socket, mut peer) = UnixStream::pair().expect("in-process stream pair opens");
    let reader = socket.try_clone().expect("client read side clones");
    let client = BridgeClient::from_io(reader, socket, 37, ClientConfig::default())
        .expect("client owns the stream pair");
    let host = thread::spawn(move || {
        let codec = FrameCodec::default();
        assert_eq!(
            codec
                .read_frame(&mut peer)
                .expect("client sends hello")
                .kind,
            FrameKind::Hello
        );
        codec
            .write_frame(&mut peer, &Frame::ready(37))
            .expect("test host sends ready");
        let request = codec
            .read_frame(&mut peer)
            .expect("client sends a correlated request");
        codec
            .write_frame(
                &mut peer,
                &Frame::response(
                    37,
                    request.request_id.expect("request has an id"),
                    json!({ "beforeEof": true }),
                ),
            )
            .expect("test host writes the response before dropping the stream");
    });

    client
        .handshake(Duration::from_secs(1))
        .expect("matching ready succeeds");
    assert_eq!(
        client
            .request(
                FrameKind::PluginSnapshot,
                json!({ "id": "race" }),
                Duration::from_secs(1),
            )
            .expect("the queued response resolves before EOF disconnects the client"),
        json!({ "beforeEof": true })
    );
    host.join().expect("test host thread settles");
}

#[test]
fn shutdown_terminates_bun_grandchildren_with_the_host_process_group() {
    let pid_file = std::env::temp_dir().join(format!(
        "tessivum-node-bridge-grandchild-{}",
        std::process::id()
    ));
    let _ = fs::remove_file(&pid_file);
    let script = r#"
const grandchild = Bun.spawn(["/bin/sleep", "60"]);
await Bun.write(process.env.TESSIVUM_GRANDCHILD_PID_FILE, String(grandchild.pid));
let buffered = Buffer.alloc(0);
let generation = 0;
function sendReady() {
  const body = Buffer.from(JSON.stringify({ protocolVersion: "cordis.node/v1", connectionGeneration: generation, kind: "ready", payload: {} }));
  process.stdout.write(Buffer.concat([Buffer.from([(body.length >>> 24) & 255, (body.length >>> 16) & 255, (body.length >>> 8) & 255, body.length & 255]), body]));
}
process.stdin.on("data", (chunk) => {
  buffered = Buffer.concat([buffered, chunk]);
  if (buffered.length < 4) return;
  const length = buffered.readUInt32BE(0);
  if (buffered.length < length + 4) return;
  const frame = JSON.parse(buffered.subarray(4, length + 4).toString());
  buffered = buffered.subarray(length + 4);
  if (frame.kind === "hello") {
    generation = frame.connectionGeneration;
    sendReady();
  }
});
"#;
    let supervisor = NodeSupervisor::new(
        HostCommand::new("bun")
            .arg("-e")
            .arg(script)
            .env("TESSIVUM_GRANDCHILD_PID_FILE", &pid_file),
        ClientConfig {
            shutdown_timeout: Duration::from_millis(100),
            ..ClientConfig::default()
        },
    )
    .expect("supervisor accepts the process-tree fixture");
    supervisor.start().expect("fixture host handshakes");
    let deadline = Instant::now() + Duration::from_secs(1);
    let grandchild = loop {
        match fs::read_to_string(&pid_file) {
            Ok(pid) => {
                break pid
                    .trim()
                    .parse::<u32>()
                    .expect("fixture writes a numeric pid")
            }
            Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
            Err(error) => panic!("fixture did not report its grandchild pid: {error}"),
        }
    };
    let _guard = GrandchildGuard(grandchild);

    assert!(
        supervisor.shutdown().is_err(),
        "an uncooperative host reaches the bounded termination path"
    );
    let deadline = Instant::now() + Duration::from_secs(1);
    while process_is_alive(grandchild) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !process_is_alive(grandchild),
        "killing the host process group also kills its Bun grandchild"
    );
    let _ = fs::remove_file(pid_file);
}

#[test]
fn inbound_pnpm_run_does_not_block_its_generic_cancel() {
    let (socket, mut peer) = UnixStream::pair().expect("in-process stream pair opens");
    let reader = socket.try_clone().expect("client read side clones");
    let client = BridgeClient::from_io(reader, socket, 41, ClientConfig::default())
        .expect("client owns the stream pair");
    let state = Arc::new((Mutex::new(false), Condvar::new()));
    let observed = Arc::clone(&state);
    client.set_handler(Arc::new(move |frame| {
        let (cancelled, changed) = &*observed;
        let mut cancelled = cancelled.lock().expect("state lock holds");
        match frame.kind {
            FrameKind::PnpmRun => {
                while !*cancelled {
                    cancelled = changed.wait(cancelled).expect("state wait holds");
                }
                Ok(json!({ "exitCode": 130 }))
            }
            FrameKind::Cancel => {
                *cancelled = true;
                changed.notify_all();
                Ok(json!({}))
            }
            _ => Err(tessivum_node_bridge::BridgeError::InvalidFrame("unexpected test frame".into())),
        }
    }));
    let host = thread::spawn(move || {
        let codec = FrameCodec::default();
        assert_eq!(codec.read_frame(&mut peer).expect("client sends hello").kind, FrameKind::Hello);
        codec.write_frame(&mut peer, &Frame::ready(41)).expect("test host sends ready");
        codec.write_frame(
            &mut peer,
            &Frame::request(41, 9, FrameKind::PnpmRun, json!({ "operationId": "op" })),
        ).expect("test host sends pnpm run");
        codec.write_frame(&mut peer, &Frame::cancel(41, 9)).expect("test host cancels pnpm run");
        let response = codec.read_frame(&mut peer).expect("cancelled pnpm run responds");
        assert_eq!(response.kind, FrameKind::Response);
        assert_eq!(response.request_id, Some(9));
    });
    client.handshake(Duration::from_secs(1)).expect("matching ready succeeds");
    host.join().expect("test host settles");
    client.close();
}
