//! Legacy Node transport and Loader runtime for `cordis.node/v1`.

pub mod protocol;
pub mod supervisor;

pub use protocol::{
    BridgeError, BridgeResult, Frame, FrameCodec, FrameKind, RemoteError, DEFAULT_MAX_FRAME_SIZE,
    PROTOCOL_VERSION,
};
pub use supervisor::{
    BridgeClient, BridgeHandler, BridgeRequest, ClientConfig, ConnectionStatus, HostCommand,
    LegacyNodeHandle, LegacyNodeRuntime, NodeSupervisor,
};
