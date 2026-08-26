use std::io::{Cursor, Read};

use serde_json::json;
use tessivum_node_bridge::{
    BridgeError, Frame, FrameCodec, FrameKind, DEFAULT_MAX_FRAME_SIZE, PROTOCOL_VERSION,
};

struct FragmentedReader {
    bytes: Cursor<Vec<u8>>,
    chunk: usize,
}

impl FragmentedReader {
    fn new(bytes: Vec<u8>, chunk: usize) -> Self {
        Self {
            bytes: Cursor::new(bytes),
            chunk,
        }
    }
}

impl Read for FragmentedReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let limit = output.len().min(self.chunk);
        self.bytes.read(&mut output[..limit])
    }
}

#[test]
fn codec_reassembles_fragmented_concatenated_frames_without_losing_boundaries() {
    let codec = FrameCodec::default();
    let hello = Frame::hello(7);
    let request = Frame::request(
        7,
        11,
        FrameKind::PluginLoad,
        json!({ "id": "function", "specifier": "fixture.ts" }),
    );
    let mut wire = codec.encode(&hello).expect("hello encodes");
    wire.extend(codec.encode(&request).expect("request encodes"));

    let mut reader = FragmentedReader::new(wire, 2);
    assert_eq!(
        codec.read_frame(&mut reader).expect("fragmented hello"),
        hello
    );
    assert_eq!(
        codec
            .read_frame(&mut reader)
            .expect("next frame keeps its boundary"),
        request
    );
}

#[test]
fn codec_checks_announced_size_before_allocating_or_reading_a_payload() {
    let codec = FrameCodec::new(8).expect("small test limit is valid");
    let oversized_prefix = (9_u32).to_be_bytes();
    let error = codec
        .read_frame(&mut Cursor::new(oversized_prefix))
        .expect_err("announced oversize frame is rejected before its absent body is read");
    assert_eq!(
        error,
        BridgeError::FrameTooLarge {
            announced: 9,
            max: 8,
        }
    );

    let error = codec
        .encode(&Frame::hello(1))
        .expect_err("encoded data also observes the configured frame limit");
    assert!(matches!(error, BridgeError::FrameTooLarge { max: 8, .. }));
    assert_eq!(
        FrameCodec::default().max_frame_size(),
        DEFAULT_MAX_FRAME_SIZE
    );
}

#[test]
fn frames_require_the_negotiated_version_generation_and_request_correlation_shape() {
    let codec = FrameCodec::default();
    let hello = Frame::hello(42);
    let ready = Frame::ready(42);
    assert_eq!(hello.protocol_version, PROTOCOL_VERSION);
    assert_eq!(ready.protocol_version, PROTOCOL_VERSION);
    assert_eq!(hello.request_id, None);
    assert_eq!(ready.request_id, None);

    let encoded = codec
        .encode(&hello)
        .expect("protocol field uses wire spelling");
    let wire: serde_json::Value = serde_json::from_slice(&encoded[4..]).expect("wire is JSON");
    assert_eq!(wire["protocolVersion"], PROTOCOL_VERSION);
    assert_eq!(wire["connectionGeneration"], 42);
    assert_eq!(wire["kind"], "hello");

    let mut wrong_version = Frame::hello(42);
    wrong_version.protocol_version = "cordis.node/v2".into();
    assert!(matches!(
        wrong_version.validate(),
        Err(BridgeError::ProtocolVersion { expected, received })
            if expected == PROTOCOL_VERSION && received == "cordis.node/v2"
    ));

    let zero_generation = Frame::hello(0);
    assert!(matches!(
        zero_generation.validate(),
        Err(BridgeError::InvalidFrame(message)) if message.contains("connectionGeneration")
    ));

    let missing_correlation = Frame::new(42, FrameKind::PluginLoad, json!({}));
    assert!(matches!(
        missing_correlation.validate(),
        Err(BridgeError::InvalidFrame(message)) if message.contains("requestId")
    ));

    let bad_handshake = Frame::request(42, 1, FrameKind::Hello, json!({}));
    assert!(matches!(
        bad_handshake.validate(),
        Err(BridgeError::InvalidFrame(message)) if message.contains("hello")
    ));
}

#[test]
fn frozen_extension_kinds_serialize_exactly_as_protocol_names() {
    let codec = FrameCodec::default();
    for (kind, name) in [
        (FrameKind::WebRouteRegister, "web.route.register"),
        (FrameKind::WebRouteRemove, "web.route.unregister"),
        (FrameKind::WebRouteInvoke, "web.route.request"),
        (FrameKind::PnpmRun, "pnpm.run"),
    ] {
        assert!(kind.is_request());
        assert_eq!(kind.as_str(), name);
        let frame = Frame::request(7, 11, kind, json!({}));
        let encoded = codec.encode(&frame).expect("extension request encodes");
        let wire: serde_json::Value = serde_json::from_slice(&encoded[4..]).expect("wire is JSON");
        assert_eq!(wire["kind"], name);
        assert_eq!(codec.decode(&encoded).expect("extension request decodes"), frame);
    }
}

#[test]
fn pnpm_output_is_an_uncorrelated_notification() {
    let codec = FrameCodec::default();
    let frame = Frame::new(7, FrameKind::PnpmOutput, json!({ "operationId": "op", "stream": "stdout", "chunkBase64": "" }));
    assert!(!frame.kind.is_request());
    let encoded = codec.encode(&frame).expect("notification encodes");
    let wire: serde_json::Value = serde_json::from_slice(&encoded[4..]).expect("wire is JSON");
    assert_eq!(wire["kind"], "pnpm.output");
    assert!(wire.get("requestId").is_none());
    assert_eq!(codec.decode(&encoded).expect("notification decodes"), frame);
}
