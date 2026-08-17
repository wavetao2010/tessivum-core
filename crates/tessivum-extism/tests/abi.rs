use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tessivum_extism::{
    CapabilityRegistry, ExtismGuestEngine, InMemoryGuestEngine, PluginError, PluginManifest,
    RequestEnvelope, ResourceLimits, ResponseEnvelope, WasmPackage, WasmPluginInstance, WasmResult,
};

fn manifest(permissions: &[&str]) -> PluginManifest {
    serde_json::from_value(json!({
        "id": "example.wasm",
        "version": "1.2.3",
        "entry": "plugin.wasm",
        "abi": "cordis.plugin/v1",
        "inject": ["logger"],
        "permissions": permissions,
        "configSchema": {
            "type": "object",
            "properties": { "enabled": { "type": "boolean" } },
            "required": ["enabled"],
            "additionalProperties": false
        },
        "exports": [
            "cordis_init",
            "cordis_call",
            "cordis_event",
            "cordis_update",
            "cordis_stop"
        ]
    }))
    .expect("test manifest is syntactically valid")
}

fn response(request: &RequestEnvelope, result: Value) -> WasmResult<ResponseEnvelope> {
    Ok(ResponseEnvelope {
        request_id: request.request_id.clone(),
        result: Some(result),
        error: None,
    })
}

fn guest_error(
    request: &RequestEnvelope,
    code: &str,
    message: &str,
    phase: &str,
    details: Value,
) -> WasmResult<ResponseEnvelope> {
    Ok(ResponseEnvelope {
        request_id: request.request_id.clone(),
        result: None,
        error: Some(PluginError::new(code, message, phase).with_details(details)),
    })
}

#[test]
fn manifest_requires_the_v1_abi_complete_exports_and_valid_config_schema() {
    let package = WasmPackage::in_memory(manifest(&["cordis.log"]));
    assert!(
        package.is_ok(),
        "complete v1 manifest loads before execution"
    );

    for (field, value) in [
        ("abi", json!("cordis.plugin/v2")),
        ("id", json!("")),
        ("version", json!("not-a-version")),
        ("entry", json!("")),
        ("exports", json!(["cordis_init", "cordis_call"])),
        (
            "configSchema",
            json!({
                "type": "object",
                "required": ["missing"],
                "properties": {}
            }),
        ),
    ] {
        let mut document = serde_json::to_value(manifest(&[])).expect("manifest serializes");
        document[field] = value;
        let parsed: PluginManifest =
            serde_json::from_value(document).expect("invalid semantics still deserialize");
        let error = match WasmPackage::in_memory(parsed) {
            Ok(_) => panic!("{field} manifest unexpectedly loaded"),
            Err(error) => error,
        };
        assert!(
            !error.code.is_empty() && !error.phase.is_empty(),
            "{field} error must retain stable code and phase"
        );
    }
}

#[test]
fn request_response_and_guest_error_envelopes_preserve_the_abi_contract() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&requests);
    let engine = InMemoryGuestEngine::new(move |_export, request, _host| {
        observed
            .lock()
            .expect("test request log is available")
            .push(request.clone());
        response(&request, json!({ "echo": true }))
    });
    let instance = WasmPluginInstance::instantiate(
        WasmPackage::in_memory(manifest(&[])).expect("valid package"),
        Arc::new(engine),
        Arc::new(CapabilityRegistry::default()),
        ResourceLimits::default(),
        json!({ "enabled": true }),
    )
    .expect("in-memory guest instantiates");

    let result = instance
        .call(
            json!({ "method": "echo", "trace": "ctx" }),
            json!({ "answer": 42 }),
        )
        .expect("guest call succeeds");
    assert_eq!(result, json!({ "echo": true }));

    let captured = requests
        .lock()
        .expect("test request log is available")
        .pop()
        .expect("call reaches guest");
    assert!(!captured.request_id.is_empty());
    assert_eq!(
        captured.context,
        json!({ "method": "echo", "trace": "ctx" })
    );
    assert_eq!(captured.payload, json!({ "answer": 42 }));

    let engine = InMemoryGuestEngine::new(move |_export, request, _host| {
        guest_error(
            &request,
            "METHOD_REJECTED",
            "method is disabled",
            "call",
            json!({ "method": "echo" }),
        )
    });
    let instance = WasmPluginInstance::instantiate(
        WasmPackage::in_memory(manifest(&[])).expect("valid package"),
        Arc::new(engine),
        Arc::new(CapabilityRegistry::default()),
        ResourceLimits::default(),
        json!({ "enabled": true }),
    )
    .expect("in-memory guest instantiates");
    let error = instance
        .call(json!({}), Value::Null)
        .expect_err("guest error becomes a host error");
    assert_eq!(error.code, "METHOD_REJECTED");
    assert_eq!(error.message, "method is disabled");
    assert_eq!(error.phase, "call");
    assert_eq!(error.details, Some(json!({ "method": "echo" })));
}

#[test]
fn an_engine_without_a_wasm_module_never_reports_success() {
    let error = match WasmPluginInstance::instantiate(
        WasmPackage::in_memory(manifest(&[])).expect("manifest is valid"),
        Arc::new(ExtismGuestEngine),
        Arc::new(CapabilityRegistry::default()),
        ResourceLimits::default(),
        json!({ "enabled": true }),
    ) {
        Ok(_) => panic!("the real Extism engine accepted a package without executable wasm"),
        Err(error) => error,
    };
    assert!(!error.code.is_empty());
    assert!(!error.phase.is_empty());
}
