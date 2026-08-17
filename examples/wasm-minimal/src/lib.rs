use serde_json::{json, Value};
use tessivum_pdk::{export_guest, Envelope, Guest, Result, ABI};

struct MinimalGuest;

impl Guest for MinimalGuest {
    fn init(_: Envelope) -> Result<Value> {
        Ok(json!({ "abi": ABI, "initialized": true }))
    }

    fn call(request: Envelope) -> Result<Value> {
        Ok(json!({ "echo": request.payload }))
    }

    fn event(_: Envelope) -> Result<Value> {
        Ok(json!({ "accepted": true }))
    }

    fn update(_: Envelope) -> Result<Value> {
        Ok(json!({ "updated": true }))
    }

    fn stop(_: Envelope) -> Result<Value> {
        Ok(json!({ "stopped": true }))
    }
}

export_guest!(MinimalGuest);
