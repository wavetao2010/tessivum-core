use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{OsStr, OsString},
    io::{Read, Write},
    panic::{catch_unwind, AssertUnwindSafe},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, Sender, SyncSender, TryRecvError, TrySendError},
        Arc, Condvar, Mutex, MutexGuard, Weak,
    },
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use serde_json::{json, Value};
use tessivum_core::{
    ContextHandle, Entry, LoaderError, LoaderFuture, LoaderRuntime, ResolvedPackage, RuntimeHandle,
    RuntimeKind,
};

use crate::protocol::{BridgeError, BridgeResult, Frame, FrameCodec, FrameKind, RemoteError};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_BRIDGE_REQUEST_ID: u64 = 9_007_199_254_740_991;

const MAX_STARTUP_STDERR_BYTES: usize = 4 * 1024;

struct StartupStderr {
    output: Arc<Mutex<Vec<u8>>>,
    done: Receiver<()>,
}

impl StartupStderr {
    fn capture<R>(mut reader: R) -> Self
    where
        R: Read + Send + 'static,
    {
        let output = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&output);
        let (done_tx, done) = mpsc::sync_channel(1);
        thread::spawn(move || {
            let mut buffer = [0; 1024];
            loop {
                let count = match reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(count) => count,
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                };
                let mut output = lock(&captured);
                let remaining = MAX_STARTUP_STDERR_BYTES.saturating_sub(output.len());
                output.extend_from_slice(&buffer[..count.min(remaining)]);
            }
            let _ = done_tx.send(());
        });
        Self { output, done }
    }

    fn attach(&self, error: BridgeError) -> BridgeError {
        let _ = self.done.recv_timeout(Duration::from_millis(100));
        let output = lock(&self.output);
        let diagnostic = String::from_utf8_lossy(&output)
            .trim()
            .escape_default()
            .to_string();
        if diagnostic.is_empty() {
            return error;
        }
        let diagnostic = |message: String| format!("{message}; host stderr: {diagnostic}");
        match error {
            BridgeError::Io(message) => BridgeError::Io(diagnostic(message)),
            BridgeError::InvalidFrame(message) => BridgeError::InvalidFrame(diagnostic(message)),
            BridgeError::Handshake(message) => BridgeError::Handshake(diagnostic(message)),
            BridgeError::Disconnected(message) => BridgeError::Disconnected(diagnostic(message)),
            BridgeError::Process(message) => BridgeError::Process(diagnostic(message)),
            error => error,
        }
    }
}

#[cfg(test)]
mod startup_stderr_tests {
    use super::*;

    struct InterruptedThenData(u8);

    impl Read for InterruptedThenData {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.0 += 1;
            match self.0 {
                1 => Err(std::io::ErrorKind::Interrupted.into()),
                2 => {
                    buffer[..7].copy_from_slice(b"warning");
                    Ok(7)
                }
                _ => Ok(0),
            }
        }
    }

    #[test]
    fn startup_stderr_retries_interrupts_without_erasing_typed_errors() {
        let stderr = StartupStderr::capture(InterruptedThenData(0));
        assert_eq!(
            stderr.attach(BridgeError::Handshake("invalid ready".into())),
            BridgeError::Handshake("invalid ready; host stderr: warning".into())
        );

        let stderr = StartupStderr::capture(std::io::Cursor::new(b"warning"));
        let version = BridgeError::ProtocolVersion {
            expected: crate::PROTOCOL_VERSION.into(),
            received: "cordis.node/v2".into(),
        };
        assert_eq!(stderr.attach(version.clone()), version);
    }
}

/// Limits and deadlines for one bounded Node transport.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub max_frame_size: usize,
    pub queue_capacity: usize,
    pub handshake_timeout: Duration,
    pub request_timeout: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            max_frame_size: crate::DEFAULT_MAX_FRAME_SIZE,
            queue_capacity: 64,
            handshake_timeout: DEFAULT_TIMEOUT,
            request_timeout: DEFAULT_TIMEOUT,
            shutdown_timeout: DEFAULT_TIMEOUT,
        }
    }
}

impl ClientConfig {
    fn codec(&self) -> BridgeResult<FrameCodec> {
        if self.queue_capacity == 0 {
            return Err(BridgeError::InvalidFrame(
                "bridge queue capacity must be greater than zero".into(),
            ));
        }
        FrameCodec::new(self.max_frame_size)
    }
}

/// The state externally visible for one Node connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionStatus {
    Handshaking,
    Ready,
    Disconnected,
}

enum ConnectionState {
    Handshaking,
    Ready,
    Disconnected(BridgeError),
}

enum Outbound {
    Frame(Frame),
    Close,
}

/// Handles Node-initiated generic service, event, and registration requests.
pub trait BridgeHandler: Send + Sync {
    fn handle(&self, frame: Frame) -> BridgeResult<Value>;
}

impl<F> BridgeHandler for F
where
    F: Fn(Frame) -> BridgeResult<Value> + Send + Sync,
{
    fn handle(&self, frame: Frame) -> BridgeResult<Value> {
        self(frame)
    }
}

type DisconnectHandler = Arc<dyn Fn(BridgeError) + Send + Sync>;
type LogHandler = Arc<dyn Fn(Value) + Send + Sync>;
type PendingReply = Sender<BridgeResult<Value>>;
type Inbound = (Frame, Sender<()>);

struct ClientInner {
    generation: u64,
    config: ClientConfig,
    state: Mutex<ConnectionState>,
    state_changed: Condvar,
    outgoing: SyncSender<Outbound>,
    pending: Mutex<BTreeMap<u64, PendingReply>>,
    next_request_id: AtomicU64,
    handler: Mutex<Option<Arc<dyn BridgeHandler>>>,
    on_disconnect: Mutex<Option<DisconnectHandler>>,
    on_log: Mutex<Option<LogHandler>>,
    extensions: Mutex<BTreeSet<String>>,
    last_heartbeat: Mutex<Instant>,
    pnpm_runs: Arc<Mutex<BTreeMap<u64, bool>>>,
}

struct PnpmRunPermit {
    active: Arc<Mutex<BTreeMap<u64, bool>>>,
    request_id: u64,
}

impl Drop for PnpmRunPermit {
    fn drop(&mut self) {
        lock(&self.active).remove(&self.request_id);
    }
}

impl ClientInner {
    fn state_error(&self) -> Option<BridgeError> {
        match &*lock(&self.state) {
            ConnectionState::Disconnected(error) => Some(error.clone()),
            ConnectionState::Handshaking | ConnectionState::Ready => None,
        }
    }

    fn send(&self, frame: Frame) -> BridgeResult<()> {
        if let Some(error) = self.state_error() {
            return Err(error);
        }
        match self.outgoing.try_send(Outbound::Frame(frame)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(BridgeError::QueueFull),
            Err(TrySendError::Disconnected(_)) => {
                let error = BridgeError::Disconnected("writer thread stopped".into());
                self.disconnect(error.clone());
                Err(error)
            }
        }
    }

    fn disconnect(&self, error: BridgeError) {
        let disconnect_handler = {
            let mut state = lock(&self.state);
            if matches!(&*state, ConnectionState::Disconnected(_)) {
                return;
            }
            *state = ConnectionState::Disconnected(error.clone());
            self.state_changed.notify_all();
            lock(&self.on_disconnect).clone()
        };
        let pending = std::mem::take(&mut *lock(&self.pending));
        for (_, reply) in pending {
            let _ = reply.send(Err(error.clone()));
        }
        if let Some(handler) = disconnect_handler {
            handler(error);
        }
    }
    fn mark_ready(&self, payload: Value) -> BridgeResult<()> {
        let capabilities = match payload {
            Value::Object(payload) => match payload.get("capabilities") {
                None => BTreeSet::new(),
                Some(Value::Array(values)) => values
                    .iter()
                    .map(|value| value.as_str().map(str::to_owned))
                    .collect::<Option<BTreeSet<_>>>()
                    .ok_or_else(|| BridgeError::Handshake("ready capabilities must be strings".into()))?,
                Some(_) => return Err(BridgeError::Handshake("ready capabilities must be an array".into())),
            },
            _ => return Err(BridgeError::Handshake("ready payload must be an object".into())),
        };
        let mut state = lock(&self.state);
        if matches!(&*state, ConnectionState::Handshaking) {
            *lock(&self.extensions) = capabilities;
            *state = ConnectionState::Ready;
            self.state_changed.notify_all();
        }
        Ok(())
    }

    fn resolve_pending(&self, request_id: u64, result: BridgeResult<Value>) -> bool {
        let reply = lock(&self.pending).remove(&request_id);
        if let Some(reply) = reply {
            let _ = reply.send(result);
            true
        } else {
            false
        }
    }

    fn acquire_pnpm_run(&self, request_id: u64) -> Option<PnpmRunPermit> {
        let mut active = lock(&self.pnpm_runs);
        if active.len() >= self.config.queue_capacity {
            return None;
        }
        active.insert(request_id, false);
        Some(PnpmRunPermit {
            active: Arc::clone(&self.pnpm_runs),
            request_id,
        })
    }

    fn cancel_pnpm_run(&self, frame: Frame) {
        let request_id = frame.request_id.expect("validated request id");
        let should_dispatch = lock(&self.pnpm_runs)
            .get_mut(&request_id)
            .is_some_and(|cancelled| {
                if *cancelled {
                    false
                } else {
                    *cancelled = true;
                    true
                }
            });
        if !should_dispatch {
            return;
        }
        if let Some(handler) = lock(&self.handler).clone() {
            thread::spawn(move || {
                let _ = handler.handle(frame);
            });
        }
    }
    fn dispatch(self: &Arc<Self>, frame: Frame) {
        let state = lock(&self.state);
        let handshaking = matches!(&*state, ConnectionState::Handshaking);
        let disconnected = matches!(&*state, ConnectionState::Disconnected(_));
        drop(state);
        if disconnected {
            return;
        }
        if handshaking && !matches!(frame.kind, FrameKind::Ready | FrameKind::Log) {
            self.disconnect(BridgeError::Handshake(format!(
                "received {} instead of ready",
                frame.kind.as_str()
            )));
            return;
        }
        if !handshaking && matches!(frame.kind, FrameKind::Hello | FrameKind::Ready) {
            self.disconnect(BridgeError::Handshake(format!(
                "received duplicate {} after ready",
                frame.kind.as_str()
            )));
            return;
        }
        match frame.kind {
            FrameKind::Ready => {
                if let Err(error) = self.mark_ready(frame.payload) {
                    self.disconnect(error);
                }
            }
            FrameKind::Heartbeat => *lock(&self.last_heartbeat) = Instant::now(),
            FrameKind::Log => {
                if let Some(handler) = lock(&self.on_log).clone() {
                    handler(frame.payload);
                }
            }
            FrameKind::Response => {
                self.resolve_pending(
                    frame.request_id.expect("validated request id"),
                    Ok(frame.payload),
                );
            }
            FrameKind::Error => {
                self.resolve_pending(
                    frame.request_id.expect("validated request id"),
                    Err(BridgeError::Remote(RemoteError::from_payload(
                        frame.payload,
                    ))),
                );
            }
            FrameKind::Cancel => {
                if !self.resolve_pending(
                    frame.request_id.expect("validated request id"),
                    Err(BridgeError::Cancelled),
                ) {
                    self.cancel_pnpm_run(frame);
                }
            }
            _ => self.dispatch_request(frame),
        }
    }

    fn dispatch_request(self: &Arc<Self>, frame: Frame) {
        if !matches!(&*lock(&self.state), ConnectionState::Ready) {
            self.disconnect(BridgeError::Handshake(format!(
                "received {} before ready",
                frame.kind.as_str()
            )));
            return;
        }
        if frame.kind == FrameKind::PnpmRun {
            let request_id = frame.request_id.expect("validated request id");
            let Some(permit) = self.acquire_pnpm_run(request_id) else {
                self.respond_request(frame, Err(BridgeError::QueueFull));
                return;
            };
            let inner = Arc::clone(self);
            thread::spawn(move || {
                let _permit = permit;
                inner.dispatch_request_serial(frame);
            });
            return;
        }
        self.dispatch_request_serial(frame);
    }

    fn dispatch_request_serial(&self, frame: Frame) {
        let result = lock(&self.handler)
            .clone()
            .ok_or_else(|| {
                BridgeError::Remote(RemoteError::new(
                    "unsupported_request",
                    format!("no Rust handler is registered for {}", frame.kind.as_str()),
                ))
            })
            .and_then(|handler| handler.handle(frame.clone()));
        self.respond_request(frame, result);
    }

    fn respond_request(&self, frame: Frame, result: BridgeResult<Value>) {
        let request_id = frame.request_id.expect("validated request id");
        let response = match result {
            Ok(payload) => Frame::response(self.generation, request_id, payload),
            Err(BridgeError::Remote(error)) => Frame::error(self.generation, request_id, error),
            Err(error) => Frame::error(
                self.generation,
                request_id,
                RemoteError::new("bridge_error", error.to_string()),
            ),
        };
        let _ = self.send(response);
    }
 }

/// A bounded, generation-checked connection to a single Node compat host.
#[derive(Clone)]
pub struct BridgeClient {
    inner: Arc<ClientInner>,
}

impl std::fmt::Debug for BridgeClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BridgeClient")
            .field("generation", &self.generation())
            .field("status", &self.status())
            .finish()
    }
}

impl BridgeClient {
    /// Attaches independent reader and writer halves to a fresh connection.
    pub fn from_io<R, W>(
        reader: R,
        writer: W,
        connection_generation: u64,
        config: ClientConfig,
    ) -> BridgeResult<Self>
    where
        R: Read + Send + 'static,
        W: Write + Send + 'static,
    {
        if connection_generation == 0 {
            return Err(BridgeError::InvalidFrame(
                "connection generation must be greater than zero".into(),
            ));
        }
        let codec = config.codec()?;
        let (outgoing, outgoing_rx) = mpsc::sync_channel(config.queue_capacity);
        let (incoming_tx, incoming_rx) = mpsc::sync_channel(config.queue_capacity);
        let inner = Arc::new(ClientInner {
            generation: connection_generation,
            config,
            state: Mutex::new(ConnectionState::Handshaking),
            state_changed: Condvar::new(),
            outgoing,
            pending: Mutex::new(BTreeMap::new()),
            next_request_id: AtomicU64::new(1),
            handler: Mutex::new(None),
            on_disconnect: Mutex::new(None),
            on_log: Mutex::new(None),
            last_heartbeat: Mutex::new(Instant::now()),
            extensions: Mutex::new(BTreeSet::new()),
            pnpm_runs: Arc::new(Mutex::new(BTreeMap::new())),
        });
        spawn_writer(Arc::clone(&inner), codec.clone(), writer, outgoing_rx);
        spawn_reader(Arc::clone(&inner), codec, reader, incoming_tx);
        spawn_dispatcher(Arc::clone(&inner), incoming_rx);
        Ok(Self { inner })
    }

    pub fn generation(&self) -> u64 {
        self.inner.generation
    }

    pub fn status(&self) -> ConnectionStatus {
        match &*lock(&self.inner.state) {
            ConnectionState::Handshaking => ConnectionStatus::Handshaking,
            ConnectionState::Ready => ConnectionStatus::Ready,
            ConnectionState::Disconnected(_) => ConnectionStatus::Disconnected,
        }
    }

    pub fn disconnect_error(&self) -> Option<BridgeError> {
        self.inner.state_error()
    }

    pub fn last_heartbeat(&self) -> Instant {
        *lock(&self.inner.last_heartbeat)
    }

    pub fn set_handler(&self, handler: Arc<dyn BridgeHandler>) {
        *lock(&self.inner.handler) = Some(handler);
    }

    pub fn set_log_handler(&self, handler: impl Fn(Value) + Send + Sync + 'static) {
        *lock(&self.inner.on_log) = Some(Arc::new(handler));
    }

    pub fn set_disconnect_handler(&self, handler: impl Fn(BridgeError) + Send + Sync + 'static) {
        let handler: DisconnectHandler = Arc::new(handler);
        let prior_error = self.inner.state_error();
        *lock(&self.inner.on_disconnect) = Some(Arc::clone(&handler));
        if let Some(error) = prior_error {
            handler(error);
        }
    }

    pub fn supports_extension(&self, extension: &str) -> bool {
        lock(&self.inner.extensions).contains(extension)
    }

    /// Sends `hello` and waits for a matching `ready`; no plugin request is
    /// admitted until this returns successfully.
    pub fn handshake(&self, timeout: Duration) -> BridgeResult<()> {
        match self.status() {
            ConnectionStatus::Ready => return Ok(()),
            ConnectionStatus::Disconnected => {
                return Err(self
                    .disconnect_error()
                    .unwrap_or_else(|| BridgeError::Disconnected("connection stopped".into())))
            }
            ConnectionStatus::Handshaking => {}
        }
        self.inner.send(Frame::hello(self.generation()))?;
        let deadline = Instant::now() + timeout;
        let mut state = lock(&self.inner.state);
        loop {
            match &*state {
                ConnectionState::Ready => return Ok(()),
                ConnectionState::Disconnected(error) => return Err(error.clone()),
                ConnectionState::Handshaking => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(BridgeError::Handshake("timed out waiting for ready".into()));
                    }
                    let (next, timeout) = self
                        .inner
                        .state_changed
                        .wait_timeout(state, remaining)
                        .unwrap_or_else(|poison| poison.into_inner());
                    state = next;
                    if timeout.timed_out() && matches!(&*state, ConnectionState::Handshaking) {
                        return Err(BridgeError::Handshake("timed out waiting for ready".into()));
                    }
                }
            }
        }
    }

    /// Emits one bounded, uncorrelated peer notification while ready.
    pub fn notify(&self, kind: FrameKind, payload: Value) -> BridgeResult<()> {
        if kind.is_request() || matches!(kind, FrameKind::Response | FrameKind::Error | FrameKind::Cancel) {
            return Err(BridgeError::InvalidFrame(format!(
                "{} is not a notification operation",
                kind.as_str()
            )));
        }
        self.require_ready()?;
        self.inner.send(Frame::new(self.generation(), kind, payload))
    }

    pub fn heartbeat(&self) -> BridgeResult<()> {
        self.require_ready()?;
        self.inner.send(Frame::new(
            self.generation(),
            FrameKind::Heartbeat,
            Value::Null,
        ))
    }

    pub fn begin_request(&self, kind: FrameKind, payload: Value) -> BridgeResult<BridgeRequest> {
        if !kind.is_request() {
            return Err(BridgeError::InvalidFrame(format!(
                "{} is not a request operation",
                kind.as_str()
            )));
        }
        self.require_ready()?;
        let request_id = self
            .inner
            .next_request_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value
                    .checked_add(2)
                    .filter(|next| *next <= MAX_BRIDGE_REQUEST_ID)
            })
            .map_err(|_| BridgeError::InvalidFrame("bridge request ids exhausted".into()))?;
        let (reply, receiver) = mpsc::channel();
        lock(&self.inner.pending).insert(request_id, reply);
        let frame = Frame::request(self.generation(), request_id, kind, payload);
        if let Err(error) = self.inner.send(frame) {
            self.inner.resolve_pending(request_id, Err(error.clone()));
            return Err(error);
        }
        Ok(BridgeRequest {
            client: self.clone(),
            request_id,
            receiver: Some(receiver),
            settled: false,
        })
    }

    pub fn request(
        &self,
        kind: FrameKind,
        payload: Value,
        timeout: Duration,
    ) -> BridgeResult<Value> {
        self.begin_request(kind, payload)?.wait(timeout)
    }

    pub fn request_default(&self, kind: FrameKind, payload: Value) -> BridgeResult<Value> {
        self.request(kind, payload, self.inner.config.request_timeout)
    }

    /// Cancels a still-pending local request. Removing the pending entry and a
    /// response race under one lock, so the first terminal event wins.
    pub fn cancel(&self, request_id: u64) -> bool {
        let cancelled = self
            .inner
            .resolve_pending(request_id, Err(BridgeError::Cancelled));
        if cancelled {
            let _ = self
                .inner
                .send(Frame::cancel(self.generation(), request_id));
        }
        cancelled
    }

    pub fn exit(&self, timeout: Duration) -> BridgeResult<()> {
        self.request(FrameKind::Exit, Value::Null, timeout)
            .map(|_| ())
    }

    /// Stops accepting work and tells the writer thread to release its stdin.
    pub fn close(&self) {
        self.inner
            .disconnect(BridgeError::Disconnected("client closed".into()));
        let _ = self.inner.outgoing.try_send(Outbound::Close);
    }

    fn require_ready(&self) -> BridgeResult<()> {
        match &*lock(&self.inner.state) {
            ConnectionState::Ready => Ok(()),
            ConnectionState::Handshaking => Err(BridgeError::Handshake(
                "hello/ready must complete before requests".into(),
            )),
            ConnectionState::Disconnected(error) => Err(error.clone()),
        }
    }
}

/// A pending request whose timeout, drop, or explicit cancellation is terminal.
pub struct BridgeRequest {
    client: BridgeClient,
    request_id: u64,
    receiver: Option<Receiver<BridgeResult<Value>>>,
    settled: bool,
}

impl BridgeRequest {
    pub const fn request_id(&self) -> u64 {
        self.request_id
    }

    pub fn wait(mut self, timeout: Duration) -> BridgeResult<Value> {
        let receiver = self.receiver.take().expect("request has one receiver");
        let result = match receiver.recv_timeout(timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if self.client.cancel(self.request_id) {
                    Err(BridgeError::Timeout)
                } else {
                    match receiver.try_recv() {
                        Ok(result) => result,
                        Err(TryRecvError::Empty | TryRecvError::Disconnected) => {
                            Err(BridgeError::Timeout)
                        }
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(self
                .client
                .disconnect_error()
                .unwrap_or_else(|| BridgeError::Disconnected("response channel closed".into()))),
        };
        self.settled = true;
        result
    }

    pub fn cancel(mut self) -> bool {
        self.settled = true;
        self.client.cancel(self.request_id)
    }
}

impl Drop for BridgeRequest {
    fn drop(&mut self) {
        if !self.settled {
            self.client.cancel(self.request_id);
        }
    }
}

fn spawn_writer<W: Write + Send + 'static>(
    inner: Arc<ClientInner>,
    codec: FrameCodec,
    mut writer: W,
    receiver: Receiver<Outbound>,
) {
    thread::spawn(move || {
        while let Ok(message) = receiver.recv() {
            match message {
                Outbound::Frame(frame) => {
                    if let Err(error) = codec.write_frame(&mut writer, &frame) {
                        inner.disconnect(error);
                        return;
                    }
                }
                Outbound::Close => return,
            }
        }
    });
}

fn spawn_reader<R: Read + Send + 'static>(
    inner: Arc<ClientInner>,
    codec: FrameCodec,
    mut reader: R,
    sender: SyncSender<Inbound>,
) {
    thread::spawn(move || loop {
        match codec.read_frame(&mut reader) {
            Ok(frame) if frame.connection_generation == inner.generation => {
                let (dispatched, handled) = mpsc::channel();
                if sender.send((frame, dispatched)).is_err() {
                    inner.disconnect(BridgeError::Disconnected(
                        "dispatcher thread stopped".into(),
                    ));
                    return;
                }
                if handled.recv().is_err() {
                    inner.disconnect(BridgeError::Disconnected(
                        "dispatcher thread stopped".into(),
                    ));
                    return;
                }
            }
            Ok(frame) => {
                inner.disconnect(BridgeError::Generation {
                    expected: inner.generation,
                    received: frame.connection_generation,
                });
                return;
            }
            Err(error) => {
                inner.disconnect(error);
                return;
            }
        }
    });
}

fn spawn_dispatcher(inner: Arc<ClientInner>, receiver: Receiver<Inbound>) {
    thread::spawn(move || {
        while let Ok((frame, handled)) = receiver.recv() {
            inner.dispatch(frame);
            let _ = handled.send(());
        }
    });
}

/// A restartable command line for exactly one Node host profile.
///
/// The child starts with an empty environment; [`HostCommand::env`] is its
/// explicit allowlist. Unix hosts run in a dedicated process group. Windows
/// hosts use a kill-on-close job object and fail startup if it cannot attach.
#[derive(Clone)]
pub struct HostCommand {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub env: Vec<(OsString, OsString)>,
    pub cwd: Option<PathBuf>,
}

impl HostCommand {
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env: Vec::new(),
            cwd: None,
        }
    }

    pub fn arg(mut self, arg: impl AsRef<OsStr>) -> Self {
        self.args.push(arg.as_ref().to_owned());
        self
    }

    /// Allows one environment variable into the otherwise empty child environment.
    pub fn env(mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> Self {
        self.env
            .push((key.as_ref().to_owned(), value.as_ref().to_owned()));
        self
    }

    pub fn current_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.cwd = Some(path.into());
        self
    }
}

impl std::fmt::Debug for HostCommand {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostCommand")
            .field("program", &self.program)
            .field("args", &self.args)
            .field(
                "env",
                &self.env.iter().map(|(key, _)| key).collect::<Vec<_>>(),
            )
            .field("cwd", &self.cwd)
            .finish()
    }
}

type Cleanup = Box<dyn FnOnce() + Send + 'static>;

#[cfg(unix)]
struct ProcessTree {
    process_group: i32,
}

#[cfg(unix)]
impl ProcessTree {
    fn attach(child: &Child) -> BridgeResult<Self> {
        Ok(Self {
            process_group: i32::try_from(child.id()).map_err(|_| {
                BridgeError::Process("Node host process id is outside the POSIX range".into())
            })?,
        })
    }

    fn terminate(&self, child: &mut Child) {
        terminate_process_group(self.process_group);
        let _ = child.kill();
        let _ = child.wait();
    }
}

#[cfg(windows)]
struct ProcessTree {
    job: usize,
}

#[cfg(windows)]
impl ProcessTree {
    fn attach(child: &Child) -> BridgeResult<Self> {
        use std::{
            mem::{size_of, zeroed},
            os::windows::io::AsRawHandle,
        };

        let job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if job.is_null() {
            return Err(BridgeError::Process(
                "could not create a Windows job object".into(),
            ));
        }
        let mut limits: JobObjectExtendedLimitInformation = unsafe { zeroed() };
        limits.basic.limit_flags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        if unsafe {
            SetInformationJobObject(
                job,
                JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
                (&limits as *const JobObjectExtendedLimitInformation).cast(),
                size_of::<JobObjectExtendedLimitInformation>() as u32,
            )
        } == 0
        {
            unsafe { CloseHandle(job) };
            return Err(BridgeError::Process(
                "could not configure a Windows job object".into(),
            ));
        }
        if unsafe { AssignProcessToJobObject(job, child.as_raw_handle().cast()) } == 0 {
            unsafe { CloseHandle(job) };
            return Err(BridgeError::Process(
                "could not attach Node host to a Windows job object".into(),
            ));
        }
        Ok(Self { job: job as usize })
    }

    fn terminate(&self, child: &mut Child) {
        if unsafe { TerminateJobObject(self.job as *mut _, 1) } == 0 {
            let _ = child.kill();
        }
        let _ = child.wait();
    }
}

#[cfg(windows)]
impl Drop for ProcessTree {
    fn drop(&mut self) {
        unsafe { CloseHandle(self.job as *mut _) };
    }
}

#[cfg(windows)]
#[repr(C)]
struct JobObjectBasicLimitInformation {
    per_process_user_time_limit: i64,
    per_job_user_time_limit: i64,
    limit_flags: u32,
    minimum_working_set_size: usize,
    maximum_working_set_size: usize,
    active_process_limit: u32,
    affinity: usize,
    priority_class: u32,
    scheduling_class: u32,
}

#[cfg(windows)]
#[repr(C)]
struct IoCounters {
    read_operation_count: u64,
    write_operation_count: u64,
    other_operation_count: u64,
    read_transfer_count: u64,
    write_transfer_count: u64,
    other_transfer_count: u64,
}

#[cfg(windows)]
#[repr(C)]
struct JobObjectExtendedLimitInformation {
    basic: JobObjectBasicLimitInformation,
    io: IoCounters,
    process_memory_limit: usize,
    job_memory_limit: usize,
    peak_process_memory_used: usize,
    peak_job_memory_used: usize,
}

#[cfg(windows)]
const JOB_OBJECT_EXTENDED_LIMIT_INFORMATION: u32 = 9;
#[cfg(windows)]
const JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: u32 = 0x0000_2000;

#[cfg(windows)]
extern "system" {
    fn CreateJobObjectW(
        attributes: *const std::ffi::c_void,
        name: *const u16,
    ) -> *mut std::ffi::c_void;
    fn SetInformationJobObject(
        job: *mut std::ffi::c_void,
        class: u32,
        information: *const std::ffi::c_void,
        length: u32,
    ) -> i32;
    fn AssignProcessToJobObject(job: *mut std::ffi::c_void, process: *mut std::ffi::c_void) -> i32;
    fn TerminateJobObject(job: *mut std::ffi::c_void, exit_code: u32) -> i32;
    fn CloseHandle(handle: *mut std::ffi::c_void) -> i32;
}

struct ActiveProcess {
    generation: u64,
    child: Child,
    tree: ProcessTree,
    client: BridgeClient,
}

#[derive(Default)]
struct SupervisorState {
    active: Option<ActiveProcess>,
    cleanups: BTreeMap<u64, Vec<Cleanup>>,
}

#[derive(Default)]
struct SupervisorInner {
    state: Mutex<SupervisorState>,
}

impl SupervisorInner {
    fn release_generation(&self, generation: u64) {
        let Some((mut process, cleanups)) = take_generation(self, generation) else {
            return;
        };
        process.client.close();
        terminate_process(&mut process);
        run_cleanups(cleanups);
    }
}

/// Owns one Node process for a profile and all resources created by its generation.
pub struct NodeSupervisor {
    command: HostCommand,
    config: ClientConfig,
    next_generation: AtomicU64,
    inner: Arc<SupervisorInner>,
}

impl std::fmt::Debug for NodeSupervisor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NodeSupervisor")
            .field("command", &self.command)
            .field("generation", &self.generation())
            .finish()
    }
}

impl NodeSupervisor {
    pub fn new(command: HostCommand, config: ClientConfig) -> BridgeResult<Self> {
        config.codec()?;
        Ok(Self {
            command,
            config,
            next_generation: AtomicU64::new(1),
            inner: Arc::new(SupervisorInner::default()),
        })
    }

    /// Starts and handshakes a host. A running profile must be shut down or
    /// disconnected before it can receive a new generation.
    pub fn start(&self) -> BridgeResult<BridgeClient> {
        if lock(&self.inner.state).active.is_some() {
            return Err(BridgeError::Process(
                "a Node host is already active for this profile".into(),
            ));
        }
        let generation = self
            .next_generation
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| BridgeError::Process("connection generations exhausted".into()))?;
        let program = resolve_program(&self.command.program)?;
        let mut command = Command::new(program);
        if let Some(cwd) = &self.command.cwd {
            command.current_dir(cwd);
        }
        configure_process_tree(&mut command);
        command
            .args(&self.command.args)
            .env_clear()
            .envs(self.command.env.iter().cloned())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .map_err(|error| BridgeError::Process(error.to_string()))?;
        let tree = ProcessTree::attach(&child).inspect_err(|_| {
            terminate_child(&mut child);
        })?;
        let stdin = child.stdin.take().ok_or_else(|| {
            tree.terminate(&mut child);
            BridgeError::Process("Node host stdin was not piped".into())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            tree.terminate(&mut child);
            BridgeError::Process("Node host stdout was not piped".into())
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            tree.terminate(&mut child);
            BridgeError::Process("Node host stderr was not piped".into())
        })?;
        let stderr = StartupStderr::capture(stderr);
        let client = match BridgeClient::from_io(stdout, stdin, generation, self.config.clone()) {
            Ok(client) => client,
            Err(error) => {
                tree.terminate(&mut child);
                return Err(error);
            }
        };
        let mut process = Some(ActiveProcess {
            generation,
            child,
            tree,
            client: client.clone(),
        });
        let weak_inner: Weak<SupervisorInner> = Arc::downgrade(&self.inner);
        client.set_disconnect_handler(move |_| {
            if let Some(inner) = weak_inner.upgrade() {
                inner.release_generation(generation);
            }
        });
        let concurrent_start = {
            let mut state = lock(&self.inner.state);
            if state.active.is_some() {
                true
            } else {
                state.active = process.take();
                false
            }
        };
        if concurrent_start {
            client.close();
            terminate_process(&mut process.expect("concurrent start keeps its child"));
            return Err(BridgeError::Process(
                "a Node host became active while starting this profile".into(),
            ));
        }
        if let Err(error) = client.handshake(self.config.handshake_timeout) {
            self.inner.release_generation(generation);
            return Err(stderr.attach(error));
        }
        Ok(client)
    }

    pub fn generation(&self) -> Option<u64> {
        lock(&self.inner.state)
            .active
            .as_ref()
            .map(|process| process.generation)
    }

    pub fn client(&self) -> Option<BridgeClient> {
        lock(&self.inner.state)
            .active
            .as_ref()
            .map(|process| process.client.clone())
    }

    /// Registers a local cleanup that runs exactly once if the owning Node
    /// generation exits, crashes, rejects the protocol, or is dropped.
    pub fn register_cleanup(
        &self,
        generation: u64,
        cleanup: impl FnOnce() + Send + 'static,
    ) -> BridgeResult<()> {
        let mut state = lock(&self.inner.state);
        let expected = state
            .active
            .as_ref()
            .map_or(0, |process| process.generation);
        let client = state
            .active
            .as_ref()
            .filter(|process| process.generation == generation)
            .map(|process| process.client.clone())
            .ok_or(BridgeError::Generation {
                expected,
                received: generation,
            })?;
        if client.status() == ConnectionStatus::Disconnected {
            return Err(client
                .disconnect_error()
                .unwrap_or_else(|| BridgeError::Disconnected("host stopped".into())));
        }
        state
            .cleanups
            .entry(generation)
            .or_default()
            .push(Box::new(cleanup));
        Ok(())
    }

    /// Gracefully asks the host to settle async disposers, then kills it only
    /// after the bounded grace window. Local generation cleanup always follows.
    pub fn shutdown(&self) -> BridgeResult<()> {
        let (mut process, cleanups) = {
            let mut state = lock(&self.inner.state);
            let Some(process) = state.active.take() else {
                return Ok(());
            };
            let cleanups = state
                .cleanups
                .remove(&process.generation)
                .unwrap_or_default();
            (process, cleanups)
        };
        let exit_error = process.client.exit(self.config.shutdown_timeout).err();
        let exited = match wait_for_exit(&mut process.child, self.config.shutdown_timeout) {
            Ok(exited) => exited,
            Err(error) => {
                process.client.close();
                terminate_process(&mut process);
                run_cleanups(cleanups);
                return Err(error);
            }
        };
        process.client.close();
        terminate_process(&mut process);
        run_cleanups(cleanups);
        if exited {
            Ok(())
        } else {
            Err(exit_error.unwrap_or_else(|| {
                BridgeError::Process("Node host did not exit before the shutdown deadline".into())
            }))
        }
    }
}

impl Drop for NodeSupervisor {
    fn drop(&mut self) {
        let active = {
            let mut state = lock(&self.inner.state);
            state.active.take().map(|process| {
                let cleanups = state
                    .cleanups
                    .remove(&process.generation)
                    .unwrap_or_default();
                (process, cleanups)
            })
        };
        if let Some((mut process, cleanups)) = active {
            process.client.close();
            terminate_process(&mut process);
            run_cleanups(cleanups);
        }
    }
}

fn take_generation(
    inner: &SupervisorInner,
    generation: u64,
) -> Option<(ActiveProcess, Vec<Cleanup>)> {
    let mut state = lock(&inner.state);
    if state
        .active
        .as_ref()
        .is_none_or(|process| process.generation != generation)
    {
        return None;
    }
    let process = state.active.take().expect("checked active process");
    let cleanups = state.cleanups.remove(&generation).unwrap_or_default();
    Some((process, cleanups))
}

fn wait_for_exit(child: &mut Child, timeout: Duration) -> BridgeResult<bool> {
    let deadline = Instant::now() + timeout;
    loop {
        if child
            .try_wait()
            .map_err(|error| BridgeError::Process(error.to_string()))?
            .is_some()
        {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn terminate_process(process: &mut ActiveProcess) {
    process.tree.terminate(&mut process.child);
}

fn terminate_child(child: &mut Child) {
    #[cfg(unix)]
    if let Ok(process_group) = i32::try_from(child.id()) {
        terminate_process_group(process_group);
    }
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(unix)]
fn terminate_process_group(process_group: i32) {
    extern "C" {
        fn kill(process: i32, signal: i32) -> i32;
    }
    unsafe {
        let _ = kill(-process_group, 9);
    }
}

fn run_cleanups(cleanups: Vec<Cleanup>) {
    for cleanup in cleanups {
        let _ = catch_unwind(AssertUnwindSafe(cleanup));
    }
}

fn resolve_program(program: &Path) -> BridgeResult<PathBuf> {
    if program.components().count() != 1 {
        return Ok(program.to_owned());
    }
    let path = std::env::var_os("PATH")
        .ok_or_else(|| BridgeError::Process("PATH is required to resolve the Node host".into()))?;
    std::env::split_paths(&path)
        .map(|directory| directory.join(program))
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| BridgeError::Process(format!("could not resolve Node host {program:?}")))
}

#[cfg(unix)]
fn configure_process_tree(command: &mut Command) {
    command.process_group(0);
}

#[cfg(windows)]
fn configure_process_tree(_: &mut Command) {}

/// Loader adapter for plugins executed by one checked and handshaken Node host.
#[derive(Clone, Debug)]
pub struct LegacyNodeRuntime {
    client: BridgeClient,
    timeout: Duration,
}

impl LegacyNodeRuntime {
    pub fn new(client: BridgeClient) -> Self {
        Self {
            client,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn client(&self) -> BridgeClient {
        self.client.clone()
    }
}

impl LoaderRuntime for LegacyNodeRuntime {
    fn kind(&self) -> RuntimeKind {
        RuntimeKind::LegacyNode
    }

    fn instantiate<'a>(
        &'a self,
        package: ResolvedPackage,
        entry: Entry,
        context: ContextHandle,
    ) -> LoaderFuture<'a, Box<dyn RuntimeHandle>> {
        Box::pin(async move {
            Ok(Box::new(LegacyNodeHandle {
                client: self.client.clone(),
                package,
                entry,
                _context: context,
                timeout: self.timeout,
                active: false,
            }) as Box<dyn RuntimeHandle>)
        })
    }
}

pub struct LegacyNodeHandle {
    client: BridgeClient,
    package: ResolvedPackage,
    entry: Entry,
    _context: ContextHandle,
    timeout: Duration,
    active: bool,
}

impl LegacyNodeHandle {
    fn plugin_id(&self) -> String {
        self.entry.options.id.to_string()
    }

    pub fn update<'a>(&'a mut self, config: Value) -> LoaderFuture<'a, ()> {
        Box::pin(async move {
            if !self.active {
                return Err(LoaderError::Validation(
                    "cannot update an inactive legacy Node plugin".into(),
                ));
            }
            self.client
                .request(
                    FrameKind::PluginUpdate,
                    json!({"pluginId": self.plugin_id(), "config": config}),
                    self.timeout,
                )
                .map(|_| ())
                .map_err(loader_error)
        })
    }

    pub fn snapshot<'a>(&'a self) -> LoaderFuture<'a, Value> {
        Box::pin(async move {
            self.client
                .request(
                    FrameKind::PluginSnapshot,
                    json!({"pluginId": self.plugin_id()}),
                    self.timeout,
                )
                .map_err(loader_error)
        })
    }
}

impl RuntimeHandle for LegacyNodeHandle {
    fn activate<'a>(&'a mut self) -> LoaderFuture<'a, ()> {
        Box::pin(async move {
            if self.active {
                return Ok(());
            }
            self.client
                .request(
                    FrameKind::PluginLoad,
                    json!({
                        "pluginId": self.plugin_id(),
                        "package": {
                            "specifier": self.package.specifier,
                            "location": self.package.location,
                        },
                        "entry": self.entry,
                    }),
                    self.timeout,
                )
                .map_err(loader_error)?;
            self.active = true;
            Ok(())
        })
    }

    fn dispose<'a>(&'a mut self) -> LoaderFuture<'a, ()> {
        Box::pin(async move {
            if !self.active {
                return Ok(());
            }
            self.client
                .request(
                    FrameKind::PluginDispose,
                    json!({"pluginId": self.plugin_id()}),
                    self.timeout,
                )
                .map_err(loader_error)?;
            self.active = false;
            Ok(())
        })
    }
}

fn loader_error(error: BridgeError) -> LoaderError {
    LoaderError::Validation(format!("legacy Node bridge failed: {error}"))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}
