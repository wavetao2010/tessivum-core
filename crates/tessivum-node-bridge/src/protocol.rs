use std::{
    error::Error,
    fmt,
    io::{Read, Write},
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The only frame protocol understood by the legacy Node bridge.
pub const PROTOCOL_VERSION: &str = "cordis.node/v1";
/// A conservative default which keeps an untrusted peer from forcing a large allocation.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 1024 * 1024;

pub type BridgeResult<T> = Result<T, BridgeError>;

/// Every operation that can appear in a bridge frame.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FrameKind {
    #[serde(rename = "hello")]
    Hello,
    #[serde(rename = "ready")]
    Ready,
    #[serde(rename = "response")]
    Response,
    #[serde(rename = "error")]
    Error,
    #[serde(rename = "cancel")]
    Cancel,
    #[serde(rename = "heartbeat")]
    Heartbeat,
    #[serde(rename = "exit")]
    Exit,
    #[serde(rename = "log")]
    Log,
    #[serde(rename = "plugin.load")]
    PluginLoad,
    #[serde(rename = "plugin.update")]
    PluginUpdate,
    #[serde(rename = "plugin.dispose")]
    PluginDispose,
    #[serde(rename = "plugin.snapshot")]
    PluginSnapshot,
    #[serde(rename = "service.call")]
    ServiceCall,
    #[serde(rename = "service.provide")]
    ServiceProvide,
    #[serde(rename = "service.remove")]
    ServiceRemove,
    #[serde(rename = "event.subscribe")]
    EventSubscribe,
    #[serde(rename = "event.emit")]
    EventEmit,
    #[serde(rename = "event.callback")]
    EventCallback,
    #[serde(rename = "registration.dispose")]
    RegistrationDispose,
    #[serde(rename = "web.route.register")]
    WebRouteRegister,
    #[serde(rename = "web.route.unregister")]
    WebRouteRemove,
    #[serde(rename = "web.route.request")]
    WebRouteInvoke,
    #[serde(rename = "pnpm.run")]
    PnpmRun,
    #[serde(rename = "pnpm.output")]
    PnpmOutput,
}

impl FrameKind {
    pub const fn is_request(self) -> bool {
        matches!(
            self,
            Self::Exit
                | Self::PluginLoad
                | Self::PluginUpdate
                | Self::PluginDispose
                | Self::PluginSnapshot
                | Self::ServiceCall
                | Self::ServiceProvide
                | Self::ServiceRemove
                | Self::EventSubscribe
                | Self::EventEmit
                | Self::EventCallback
                | Self::RegistrationDispose
                | Self::WebRouteRegister
                | Self::WebRouteRemove
                | Self::WebRouteInvoke
                | Self::PnpmRun
        )
    }

    const fn requires_request_id(self) -> bool {
        self.is_request() || matches!(self, Self::Response | Self::Error | Self::Cancel)
    }
}

/// A JSON error returned by the other bridge endpoint.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoteError {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl RemoteError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }

    pub(crate) fn from_payload(payload: Value) -> Self {
        serde_json::from_value(payload.clone()).unwrap_or_else(|_| {
            Self::new(
                "remote_error",
                "remote endpoint returned an invalid error payload",
            )
            .with_details(payload)
        })
    }
}

impl fmt::Display for RemoteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

/// One length-prefixed JSON transport unit.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Frame {
    pub protocol_version: String,
    pub connection_generation: u64,
    pub kind: FrameKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<u64>,
    pub payload: Value,
}

impl Frame {
    pub fn new(connection_generation: u64, kind: FrameKind, payload: Value) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION.into(),
            connection_generation,
            kind,
            request_id: None,
            payload,
        }
    }

    pub fn request(
        connection_generation: u64,
        request_id: u64,
        kind: FrameKind,
        payload: Value,
    ) -> Self {
        let mut frame = Self::new(connection_generation, kind, payload);
        frame.request_id = Some(request_id);
        frame
    }

    pub fn response(connection_generation: u64, request_id: u64, payload: Value) -> Self {
        Self::request(
            connection_generation,
            request_id,
            FrameKind::Response,
            payload,
        )
    }

    pub fn error(connection_generation: u64, request_id: u64, error: RemoteError) -> Self {
        Self::request(
            connection_generation,
            request_id,
            FrameKind::Error,
            serde_json::to_value(error).expect("remote errors are serializable"),
        )
    }

    pub fn cancel(connection_generation: u64, request_id: u64) -> Self {
        Self::request(
            connection_generation,
            request_id,
            FrameKind::Cancel,
            json!({"requestId": request_id}),
        )
    }

    pub fn hello(connection_generation: u64) -> Self {
        Self::new(connection_generation, FrameKind::Hello, json!({}))
    }

    pub fn ready(connection_generation: u64) -> Self {
        Self::new(connection_generation, FrameKind::Ready, json!({}))
    }

    pub fn validate(&self) -> BridgeResult<()> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(BridgeError::ProtocolVersion {
                expected: PROTOCOL_VERSION.into(),
                received: self.protocol_version.clone(),
            });
        }
        if self.connection_generation == 0 {
            return Err(BridgeError::InvalidFrame(
                "connectionGeneration must be greater than zero".into(),
            ));
        }
        if self.kind.requires_request_id() && self.request_id.is_none() {
            return Err(BridgeError::InvalidFrame(format!(
                "{} requires requestId",
                self.kind.as_str()
            )));
        }
        if !self.kind.requires_request_id() && self.request_id.is_some() {
            return Err(BridgeError::InvalidFrame(format!(
                "{} must not carry requestId",
                self.kind.as_str()
            )));
        }
        if self.request_id == Some(0) {
            return Err(BridgeError::InvalidFrame(
                "requestId must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

impl FrameKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hello => "hello",
            Self::Ready => "ready",
            Self::Response => "response",
            Self::Error => "error",
            Self::Cancel => "cancel",
            Self::Heartbeat => "heartbeat",
            Self::Exit => "exit",
            Self::Log => "log",
            Self::PluginLoad => "plugin.load",
            Self::PluginUpdate => "plugin.update",
            Self::PluginDispose => "plugin.dispose",
            Self::PluginSnapshot => "plugin.snapshot",
            Self::ServiceCall => "service.call",
            Self::ServiceProvide => "service.provide",
            Self::ServiceRemove => "service.remove",
            Self::EventSubscribe => "event.subscribe",
            Self::EventEmit => "event.emit",
            Self::EventCallback => "event.callback",
            Self::RegistrationDispose => "registration.dispose",
            Self::WebRouteRegister => "web.route.register",
            Self::WebRouteRemove => "web.route.unregister",
            Self::WebRouteInvoke => "web.route.request",
            Self::PnpmRun => "pnpm.run",
            Self::PnpmOutput => "pnpm.output",
        }
    }
}

/// Failures reported by the bounded transport and its peer.
#[derive(Clone, Debug, PartialEq)]
pub enum BridgeError {
    Io(String),
    FrameTooLarge { announced: usize, max: usize },
    InvalidFrame(String),
    ProtocolVersion { expected: String, received: String },
    Generation { expected: u64, received: u64 },
    QueueFull,
    Timeout,
    Handshake(String),
    Disconnected(String),
    Cancelled,
    Remote(RemoteError),
    Process(String),
}

impl fmt::Display for BridgeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(formatter, "bridge I/O failed: {message}"),
            Self::FrameTooLarge { announced, max } => {
                write!(
                    formatter,
                    "bridge frame of {announced} bytes exceeds {max} byte limit"
                )
            }
            Self::InvalidFrame(message) => write!(formatter, "invalid bridge frame: {message}"),
            Self::ProtocolVersion { expected, received } => {
                write!(
                    formatter,
                    "bridge protocol {received:?} is incompatible with {expected:?}"
                )
            }
            Self::Generation { expected, received } => write!(
                formatter,
                "bridge generation {received} does not match active generation {expected}"
            ),
            Self::QueueFull => formatter.write_str("bridge queue is full"),
            Self::Timeout => formatter.write_str("bridge request timed out"),
            Self::Handshake(message) => write!(formatter, "bridge handshake failed: {message}"),
            Self::Disconnected(message) => write!(formatter, "bridge disconnected: {message}"),
            Self::Cancelled => formatter.write_str("bridge request cancelled"),
            Self::Remote(error) => write!(formatter, "bridge peer failed: {error}"),
            Self::Process(message) => write!(formatter, "bridge process failed: {message}"),
        }
    }
}

impl Error for BridgeError {}

impl From<std::io::Error> for BridgeError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

/// Stateless codec for bounded u32 big-endian UTF-8 JSON frames.
#[derive(Clone, Debug)]
pub struct FrameCodec {
    max_frame_size: usize,
}

impl Default for FrameCodec {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_FRAME_SIZE).expect("default bridge frame size is valid")
    }
}

impl FrameCodec {
    pub fn new(max_frame_size: usize) -> BridgeResult<Self> {
        if max_frame_size == 0 || max_frame_size > u32::MAX as usize {
            return Err(BridgeError::InvalidFrame(
                "max frame size must be between 1 and u32::MAX".into(),
            ));
        }
        Ok(Self { max_frame_size })
    }

    pub const fn max_frame_size(&self) -> usize {
        self.max_frame_size
    }

    pub fn encode(&self, frame: &Frame) -> BridgeResult<Vec<u8>> {
        frame.validate()?;
        let json = serde_json::to_vec(frame)
            .map_err(|error| BridgeError::InvalidFrame(error.to_string()))?;
        self.check_size(json.len())?;
        let mut output = Vec::with_capacity(4 + json.len());
        output.extend_from_slice(&(json.len() as u32).to_be_bytes());
        output.extend_from_slice(&json);
        Ok(output)
    }

    pub fn decode(&self, bytes: &[u8]) -> BridgeResult<Frame> {
        if bytes.len() < 4 {
            return Err(BridgeError::InvalidFrame(
                "length-prefixed frame is shorter than four bytes".into(),
            ));
        }
        let announced = u32::from_be_bytes(bytes[..4].try_into().expect("four bytes")) as usize;
        self.check_size(announced)?;
        if bytes.len() != announced + 4 {
            return Err(BridgeError::InvalidFrame(format!(
                "frame declares {announced} bytes but carries {}",
                bytes.len() - 4
            )));
        }
        self.decode_json(&bytes[4..])
    }

    pub fn read_frame<R: Read>(&self, reader: &mut R) -> BridgeResult<Frame> {
        let mut prefix = [0; 4];
        reader.read_exact(&mut prefix)?;
        let announced = u32::from_be_bytes(prefix) as usize;
        self.check_size(announced)?;
        let mut json = vec![0; announced];
        reader.read_exact(&mut json)?;
        self.decode_json(&json)
    }

    pub fn write_frame<W: Write>(&self, writer: &mut W, frame: &Frame) -> BridgeResult<()> {
        let bytes = self.encode(frame)?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        Ok(())
    }

    fn check_size(&self, size: usize) -> BridgeResult<()> {
        if size > self.max_frame_size {
            Err(BridgeError::FrameTooLarge {
                announced: size,
                max: self.max_frame_size,
            })
        } else {
            Ok(())
        }
    }

    fn decode_json(&self, json: &[u8]) -> BridgeResult<Frame> {
        let frame: Frame = serde_json::from_slice(json)
            .map_err(|error| BridgeError::InvalidFrame(error.to_string()))?;
        frame.validate()?;
        Ok(frame)
    }
}
