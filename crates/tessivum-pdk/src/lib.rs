use std::fmt;

#[cfg(target_arch = "wasm32")]
use extism_pdk::Memory;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};

pub use extism_pdk;

pub const ABI: &str = "cordis.plugin/v1";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Envelope<T = Value> {
    pub request_id: String,
    #[serde(default)]
    pub context: Value,
    pub payload: T,
}

impl<T> Envelope<T> {
    pub fn new(request_id: impl Into<String>, context: Value, payload: T) -> Self {
        Self {
            request_id: request_id.into(),
            context,
            payload,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Init,
    Call,
    Event,
    Update,
    Stop,
    Input,
    Host,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Error {
    pub code: String,
    pub message: String,
    pub phase: Phase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl Error {
    pub fn new(code: impl Into<String>, message: impl Into<String>, phase: Phase) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            phase,
            details: None,
        }
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }

    fn host(code: &str, message: &str, details: Value) -> Self {
        Self::new(code, message, Phase::Host).with_details(details)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Response {
    Success {
        #[serde(rename = "requestId")]
        request_id: String,
        result: Value,
    },
    Failure {
        #[serde(rename = "requestId")]
        request_id: String,
        error: Error,
    },
}

impl Response {
    pub fn success(request_id: impl Into<String>, result: Value) -> Self {
        Self::Success {
            request_id: request_id.into(),
            result,
        }
    }

    pub fn failure(request_id: impl Into<String>, error: Error) -> Self {
        Self::Failure {
            request_id: request_id.into(),
            error,
        }
    }
}

pub trait Guest {
    fn init(request: Envelope) -> Result<Value>;
    fn call(request: Envelope) -> Result<Value>;
    fn event(request: Envelope) -> Result<Value>;
    fn update(request: Envelope) -> Result<Value>;
    fn stop(request: Envelope) -> Result<Value>;
}

pub fn dispatch<F>(input: Vec<u8>, phase: Phase, handler: F) -> extism_pdk::FnResult<String>
where
    F: FnOnce(Envelope) -> Result<Value>,
{
    let response = match serde_json::from_slice::<Envelope>(&input) {
        Ok(request) => {
            let request_id = request.request_id.clone();
            match handler(request) {
                Ok(result) => Response::success(request_id, result),
                Err(error) => Response::failure(request_id, error),
            }
        }
        Err(_) => Response::failure(
            request_id(&input),
            Error::new("invalid_request", "invalid request envelope", phase),
        ),
    };

    Ok(serde_json::to_string(&response).expect("JSON response is serializable"))
}

fn request_id(input: &[u8]) -> String {
    #[derive(Deserialize)]
    struct RequestId {
        #[serde(rename = "requestId")]
        request_id: Option<String>,
    }

    serde_json::from_slice::<RequestId>(input)
        .ok()
        .and_then(|request| request.request_id)
        .unwrap_or_default()
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Registration {
    pub id: String,
}

impl Registration {
    pub fn dispose(self) -> Result<()> {
        registration_dispose(&self.id)
    }
}

#[cfg(target_arch = "wasm32")]
#[derive(Deserialize)]
#[serde(untagged)]
enum CapabilityReply {
    Success { result: Value },
    Failure { error: Error },
}

#[cfg(target_arch = "wasm32")]
type HostFunction = unsafe extern "C" fn(u64) -> u64;

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "extism:host/user")]
extern "C" {
    #[link_name = "cordis.log"]
    fn cordis_log(input: u64) -> u64;
    #[link_name = "cordis.config.get"]
    fn cordis_config_get(input: u64) -> u64;
    #[link_name = "cordis.service.call"]
    fn cordis_service_call(input: u64) -> u64;
    #[link_name = "cordis.event.emit"]
    fn cordis_event_emit(input: u64) -> u64;
    #[link_name = "cordis.event.subscribe"]
    fn cordis_event_subscribe(input: u64) -> u64;
    #[link_name = "cordis.registration.dispose"]
    fn cordis_registration_dispose(input: u64) -> u64;
    #[link_name = "cordis.kv.get"]
    fn cordis_kv_get(input: u64) -> u64;
    #[link_name = "cordis.kv.set"]
    fn cordis_kv_set(input: u64) -> u64;
}

#[cfg(target_arch = "wasm32")]
fn capability(function: HostFunction, request: Value) -> Result<Value> {
    let bytes = serde_json::to_vec(&request).map_err(|cause| {
        Error::host(
            "host_request_encoding_failed",
            "could not encode host capability request",
            json!({ "cause": cause.to_string() }),
        )
    })?;
    let input = Memory::from_bytes(bytes).map_err(|cause| {
        Error::host(
            "host_request_allocation_failed",
            "could not allocate host capability request",
            json!({ "cause": cause.to_string() }),
        )
    })?;
    let output = unsafe { function(input.offset()) };
    input.free();

    let output = Memory::from(output);
    let response = output.to_vec();
    output.free();

    match serde_json::from_slice::<CapabilityReply>(&response).map_err(|cause| {
        Error::host(
            "host_protocol_error",
            "invalid host capability response",
            json!({ "cause": cause.to_string() }),
        )
    })? {
        CapabilityReply::Success { result } => Ok(result),
        CapabilityReply::Failure { error } => Err(error),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn unavailable() -> Result<Value> {
    Err(Error::new(
        "unsupported_runtime",
        "Cordis capabilities require a wasm32 guest",
        Phase::Host,
    ))
}

pub fn log(level: LogLevel, message: impl AsRef<str>) -> Result<()> {
    let request = json!({ "level": level, "message": message.as_ref() });
    #[cfg(target_arch = "wasm32")]
    {
        capability(cordis_log, request).map(|_| ())
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = request;
        unavailable().map(|_| ())
    }
}

pub fn config_get<T: DeserializeOwned>(key: impl AsRef<str>) -> Result<Option<T>> {
    let request = json!({ "key": key.as_ref() });
    #[cfg(target_arch = "wasm32")]
    let value = capability(cordis_config_get, request)?;
    #[cfg(not(target_arch = "wasm32"))]
    let value = {
        let _ = request;
        unavailable()?
    };
    decode_optional(value)
}

pub fn service_call<T: Serialize, R: DeserializeOwned>(
    service: impl AsRef<str>,
    method: impl AsRef<str>,
    payload: T,
) -> Result<R> {
    let request = json!({
        "service": service.as_ref(),
        "method": method.as_ref(),
        "payload": payload,
    });
    #[cfg(target_arch = "wasm32")]
    let value = capability(cordis_service_call, request)?;
    #[cfg(not(target_arch = "wasm32"))]
    let value = {
        let _ = request;
        unavailable()?
    };
    decode(value)
}

pub fn event_emit<T: Serialize>(topic: impl AsRef<str>, payload: T) -> Result<()> {
    let request = json!({ "topic": topic.as_ref(), "payload": payload });
    #[cfg(target_arch = "wasm32")]
    {
        capability(cordis_event_emit, request).map(|_| ())
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = request;
        unavailable().map(|_| ())
    }
}

pub fn event_subscribe(topic: impl AsRef<str>) -> Result<Registration> {
    let request = json!({ "topic": topic.as_ref() });
    #[cfg(target_arch = "wasm32")]
    let value = capability(cordis_event_subscribe, request)?;
    #[cfg(not(target_arch = "wasm32"))]
    let value = {
        let _ = request;
        unavailable()?
    };
    decode(value)
}

pub fn registration_dispose(id: impl AsRef<str>) -> Result<()> {
    let request = json!({ "id": id.as_ref() });
    #[cfg(target_arch = "wasm32")]
    {
        capability(cordis_registration_dispose, request).map(|_| ())
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = request;
        unavailable().map(|_| ())
    }
}

pub fn kv_get<T: DeserializeOwned>(key: impl AsRef<str>) -> Result<Option<T>> {
    let request = json!({ "key": key.as_ref() });
    #[cfg(target_arch = "wasm32")]
    let value = capability(cordis_kv_get, request)?;
    #[cfg(not(target_arch = "wasm32"))]
    let value = {
        let _ = request;
        unavailable()?
    };
    decode_optional(value)
}

pub fn kv_set<T: Serialize>(key: impl AsRef<str>, value: T) -> Result<()> {
    let request = json!({ "key": key.as_ref(), "value": value });
    #[cfg(target_arch = "wasm32")]
    {
        capability(cordis_kv_set, request).map(|_| ())
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = request;
        unavailable().map(|_| ())
    }
}

fn decode_optional<T: DeserializeOwned>(value: Value) -> Result<Option<T>> {
    if value.is_null() {
        Ok(None)
    } else {
        decode(value).map(Some)
    }
}

fn decode<T: DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(|cause| {
        Error::host(
            "host_protocol_error",
            "host capability returned an unexpected value",
            json!({ "cause": cause.to_string() }),
        )
    })
}

#[macro_export]
macro_rules! export_guest {
    ($guest:ty) => {
        #[cfg_attr(target_arch = "wasm32", extism_pdk::plugin_fn)]
        pub fn cordis_init(input: Vec<u8>) -> $crate::extism_pdk::FnResult<String> {
            $crate::dispatch(input, $crate::Phase::Init, <$guest as $crate::Guest>::init)
        }

        #[cfg_attr(target_arch = "wasm32", extism_pdk::plugin_fn)]
        pub fn cordis_call(input: Vec<u8>) -> $crate::extism_pdk::FnResult<String> {
            $crate::dispatch(input, $crate::Phase::Call, <$guest as $crate::Guest>::call)
        }

        #[cfg_attr(target_arch = "wasm32", extism_pdk::plugin_fn)]
        pub fn cordis_event(input: Vec<u8>) -> $crate::extism_pdk::FnResult<String> {
            $crate::dispatch(
                input,
                $crate::Phase::Event,
                <$guest as $crate::Guest>::event,
            )
        }

        #[cfg_attr(target_arch = "wasm32", extism_pdk::plugin_fn)]
        pub fn cordis_update(input: Vec<u8>) -> $crate::extism_pdk::FnResult<String> {
            $crate::dispatch(
                input,
                $crate::Phase::Update,
                <$guest as $crate::Guest>::update,
            )
        }

        #[cfg_attr(target_arch = "wasm32", extism_pdk::plugin_fn)]
        pub fn cordis_stop(input: Vec<u8>) -> $crate::extism_pdk::FnResult<String> {
            $crate::dispatch(input, $crate::Phase::Stop, <$guest as $crate::Guest>::stop)
        }
    };
}
