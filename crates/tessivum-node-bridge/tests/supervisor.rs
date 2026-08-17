use std::{
    io::Write,
    os::unix::net::UnixStream,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    },
    thread,
    time::Duration,
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
    HostCommand::new("bun")
        .arg("run")
        .arg(root.join("node/compat-host/src/index.ts"))
        .current_dir(root.join("node/compat-host"))
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
