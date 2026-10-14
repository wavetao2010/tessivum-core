use std::sync::atomic::{AtomicBool, Ordering};

use serde::Serialize;
use serde_json::{json, Value};
use tessivum_pdk::{
    config_get, event_emit, event_subscribe, kv_get, kv_set, log, registration_dispose,
    service_call, Envelope, Error, Guest, LogLevel, Phase, Result, ABI,
};

static MATRIX: AtomicBool = AtomicBool::new(false);

struct MinimalGuest;

impl MinimalGuest {
    fn lifecycle(phase: &str) -> Result<()> {
        if MATRIX.load(Ordering::Relaxed) {
            log(LogLevel::Info, format!("lifecycle:{phase}"))?;
        }
        Ok(())
    }

    fn capability(payload: &Value) -> Result<Value> {
        let name = payload
            .get("capability")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("invalid_operation", "missing capability", Phase::Call))?;
        match name {
            "cordis.log" => outcome(log(LogLevel::Info, "matrix-log")),
            "cordis.config.get" => outcome(config_get::<Value>("theme")),
            "cordis.service.call" => outcome(service_call::<_, Value>(
                "math",
                "increment",
                json!({ "value": 41 }),
            )),
            "cordis.event.emit" => outcome(event_emit("matrix.notice", json!({ "value": 7 }))),
            "cordis.event.subscribe" => outcome(event_subscribe("matrix.notice")),
            "cordis.registration.dispose" => outcome(registration_dispose("sub-1")),
            "cordis.kv.get" => outcome(kv_get::<Value>("matrix-key")),
            "cordis.kv.set" => outcome(kv_set("matrix-key", json!(9))),
            _ => Err(Error::new(
                "invalid_operation",
                "unknown capability",
                Phase::Call,
            )),
        }
    }

    #[cfg(target_arch = "wasm32")]
    fn network_probe(payload: &Value) -> Result<Value> {
        let url = payload
            .get("url")
            .and_then(Value::as_str)
            .unwrap_or("http://127.0.0.1:9/");
        let document = serde_json::to_vec(&json!({
            "url": url,
            "headers": {},
            "method": "GET",
        }))
        .map_err(|error| Error::new("network_probe_failed", error.to_string(), Phase::Call))?;
        let request = tessivum_pdk::extism_pdk::Memory::from_bytes(document)
            .map_err(|error| Error::new("network_probe_failed", error.to_string(), Phase::Call))?;
        unsafe {
            tessivum_pdk::extism_pdk::extism::http_request(request.offset(), 0);
        }
        Ok(json!({ "networkAllowed": true }))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn network_probe(_: &Value) -> Result<Value> {
        Err(Error::new(
            "network_probe_unavailable",
            "network probe requires wasm32",
            Phase::Call,
        ))
    }
}

fn outcome<T: Serialize>(result: Result<T>) -> Result<Value> {
    Ok(match result {
        Ok(value) => json!({ "ok": true, "value": value }),
        Err(error) => json!({ "ok": false, "error": error }),
    })
}

impl Guest for MinimalGuest {
    fn init(request: Envelope) -> Result<Value> {
        MATRIX.store(
            request
                .payload
                .get("matrix")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            Ordering::Relaxed,
        );
        Self::lifecycle("init")?;
        Ok(json!({ "abi": ABI, "initialized": true }))
    }

    fn call(request: Envelope) -> Result<Value> {
        Self::lifecycle("call")?;
        match request.payload.get("op").and_then(Value::as_str) {
            Some("inspect") => Ok(json!({
                "requestId": request.request_id,
                "context": request.context,
                "payload": request.payload,
            })),
            Some("capability") => Self::capability(&request.payload),
            Some("reject") => Err(Error::new(
                "matrix_rejected",
                "deliberate guest rejection",
                Phase::Call,
            )),
            Some("trap") => panic!("deliberate guest trap"),
            Some("network") => Self::network_probe(&request.payload),
            Some("spin") => {
                if request.payload.get("signalReady") == Some(&Value::Bool(true)) {
                    log(LogLevel::Info, "spin-ready")?;
                }
                let mut counter = 0_u64;
                loop {
                    counter = counter.wrapping_add(1);
                    std::hint::black_box(counter);
                }
            }
            Some("allocate") => {
                let bytes = request
                    .payload
                    .get("bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(16 * 1024 * 1024) as usize;
                let mut allocation = vec![0_u8; bytes];
                for page in allocation.chunks_mut(64 * 1024) {
                    page[0] = 1;
                }
                std::hint::black_box(&allocation);
                Ok(json!({ "allocated": allocation.len() }))
            }
            Some("output") => {
                let bytes = request
                    .payload
                    .get("bytes")
                    .and_then(Value::as_u64)
                    .unwrap_or(16 * 1024) as usize;
                Ok(json!({ "data": "x".repeat(bytes) }))
            }
            _ => Ok(json!({ "echo": request.payload })),
        }
    }

    fn event(_: Envelope) -> Result<Value> {
        Self::lifecycle("event")?;
        Ok(json!({ "accepted": true }))
    }

    fn update(_: Envelope) -> Result<Value> {
        Self::lifecycle("update")?;
        Ok(json!({ "updated": true }))
    }

    fn stop(_: Envelope) -> Result<Value> {
        Self::lifecycle("stop")?;
        MATRIX.store(false, Ordering::Relaxed);
        Ok(json!({ "stopped": true }))
    }
}

#[cfg_attr(target_arch = "wasm32", extism_pdk::plugin_fn)]
pub fn cordis_init(input: Vec<u8>) -> extism_pdk::FnResult<String> {
    tessivum_pdk::dispatch(input, Phase::Init, MinimalGuest::init)
}

#[cfg_attr(target_arch = "wasm32", extism_pdk::plugin_fn)]
pub fn cordis_call(input: Vec<u8>) -> extism_pdk::FnResult<String> {
    if let Ok(request) = serde_json::from_slice::<Envelope>(&input) {
        let response = match request.payload.get("op").and_then(Value::as_str) {
            Some("malformed") => return Ok("{not-json".into()),
            Some("unknown-field") => Some(json!({
                "requestId": request.request_id, "result": null, "ok": true
            })),
            Some("wrong-id") => Some(json!({"requestId": "wrong", "result": null})),
            Some("both-results") => Some(json!({
                "requestId": request.request_id, "result": null,
                "error": {"code": "bad", "message": "bad", "phase": "call"}
            })),
            _ => None,
        };
        if let Some(response) = response {
            return Ok(response.to_string());
        }
    }
    tessivum_pdk::dispatch(input, Phase::Call, MinimalGuest::call)
}

#[cfg_attr(target_arch = "wasm32", extism_pdk::plugin_fn)]
pub fn cordis_event(input: Vec<u8>) -> extism_pdk::FnResult<String> {
    tessivum_pdk::dispatch(input, Phase::Event, MinimalGuest::event)
}

#[cfg_attr(target_arch = "wasm32", extism_pdk::plugin_fn)]
pub fn cordis_update(input: Vec<u8>) -> extism_pdk::FnResult<String> {
    tessivum_pdk::dispatch(input, Phase::Update, MinimalGuest::update)
}

#[cfg_attr(target_arch = "wasm32", extism_pdk::plugin_fn)]
pub fn cordis_stop(input: Vec<u8>) -> extism_pdk::FnResult<String> {
    tessivum_pdk::dispatch(input, Phase::Stop, MinimalGuest::stop)
}
