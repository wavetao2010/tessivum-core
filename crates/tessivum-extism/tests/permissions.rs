use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tessivum_extism::{
    Capability, CapabilityRegistry, CapabilityRequest, InMemoryGuestEngine, PluginManifest,
    ResourceLimits, ResponseEnvelope, WasmPackage, WasmPluginInstance,
};

fn capability(name: &str) -> Capability {
    serde_json::from_value(json!(name)).expect("documented capability parses")
}

fn manifest(permissions: &[&str]) -> PluginManifest {
    serde_json::from_value(json!({
        "id": "permissions.wasm",
        "version": "1.0.0",
        "entry": "plugin.wasm",
        "abi": "cordis.plugin/v1",
        "inject": [],
        "permissions": permissions,
        "configSchema": { "type": "object" },
        "exports": [
            "cordis_init",
            "cordis_call",
            "cordis_event",
            "cordis_update",
            "cordis_stop"
        ]
    }))
    .expect("test manifest parses")
}

fn instance(
    manifest: PluginManifest,
    engine: InMemoryGuestEngine,
    registry: Arc<CapabilityRegistry>,
) -> WasmPluginInstance {
    WasmPluginInstance::instantiate(
        WasmPackage::in_memory(manifest).expect("test package validates"),
        Arc::new(engine),
        registry,
        ResourceLimits::default(),
        json!({}),
    )
    .expect("test guest instantiates")
}

#[test]
fn every_v1_capability_is_available_only_when_declared_and_granted() {
    let cases = [
        ("cordis.log", json!({ "level": "info", "message": "hello" })),
        ("cordis.config.get", json!({ "key": "theme" })),
        (
            "cordis.service.call",
            json!({ "service": "echo", "method": "call" }),
        ),
        (
            "cordis.event.emit",
            json!({ "event": "notice", "payload": 1 }),
        ),
        ("cordis.event.subscribe", json!({ "event": "notice" })),
        (
            "cordis.registration.dispose",
            json!({ "registration": "r-1" }),
        ),
        ("cordis.kv.get", json!({ "key": "counter" })),
        ("cordis.kv.set", json!({ "key": "counter", "value": 2 })),
    ];

    for (name, payload) in cases {
        let capability = capability(name);
        let registry = Arc::new(CapabilityRegistry::default());
        registry.grant(capability);
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&delivered);
        let expected_payload = payload.clone();
        let expected_capability = capability;
        registry
            .register(capability, move |request: CapabilityRequest| {
                assert_eq!(request.capability, expected_capability);
                assert_eq!(request.plugin_id, "permissions.wasm");
                assert_eq!(request.payload, expected_payload);
                observed
                    .lock()
                    .expect("handler log is available")
                    .push(request.payload);
                Ok(json!({ "capability": name, "handled": true }))
            })
            .expect("capability handler registers");

        let invocation = capability;
        let input = payload.clone();
        let engine = InMemoryGuestEngine::new(move |_export, request, host| {
            let result = host.invoke(invocation, input.clone())?;
            Ok(ResponseEnvelope::success(&request, result))
        });
        let result = instance(manifest(&[name]), engine, registry)
            .call(json!({ "capability": name }), payload.clone())
            .expect("declared and granted capability reaches its handler");
        assert_eq!(result, json!({ "capability": name, "handled": true }));
        assert_eq!(
            delivered
                .lock()
                .expect("handler log is available")
                .as_slice(),
            &[payload]
        );
    }
}

#[test]
fn capability_denial_distinguishes_missing_permission_from_missing_handler() {
    let log = capability("cordis.log");

    let registry = Arc::new(CapabilityRegistry::default());
    registry
        .register(log, |_request| Ok(Value::Null))
        .expect("handler registers without granting policy");
    let denied_capability = log;
    let engine = InMemoryGuestEngine::new(move |_export, request, host| {
        let result = host.invoke(denied_capability, json!({ "message": "no grant" }))?;
        Ok(ResponseEnvelope::success(&request, result))
    });
    let error = instance(manifest(&["cordis.log"]), engine, registry)
        .call(json!({}), Value::Null)
        .expect_err("declared capability still requires host policy grant");
    assert_eq!(error.code, "PERMISSION_DENIED");
    assert_eq!(error.phase, "call");

    let registry = Arc::new(CapabilityRegistry::default());
    registry.grant(log);
    registry
        .register(log, |_request| Ok(Value::Null))
        .expect("handler registers for undeclared case");
    let undeclared_capability = log;
    let engine = InMemoryGuestEngine::new(move |_export, request, host| {
        let result = host.invoke(undeclared_capability, json!({ "message": "undeclared" }))?;
        Ok(ResponseEnvelope::success(&request, result))
    });
    let error = instance(manifest(&[]), engine, registry)
        .call(json!({}), Value::Null)
        .expect_err("undeclared capability fails closed even when policy grants it");
    assert_eq!(error.code, "PERMISSION_DENIED");
    assert_eq!(error.phase, "call");

    let registry = Arc::new(CapabilityRegistry::default());
    registry.grant(log);
    let unavailable_capability = log;
    let engine = InMemoryGuestEngine::new(move |_export, request, host| {
        let result = host.invoke(unavailable_capability, json!({ "message": "no handler" }))?;
        Ok(ResponseEnvelope::success(&request, result))
    });
    let error = instance(manifest(&["cordis.log"]), engine, registry)
        .call(json!({}), Value::Null)
        .expect_err("granted capability without a handler remains unavailable");
    assert_eq!(error.code, "CAPABILITY_UNAVAILABLE");
    assert_eq!(error.phase, "call");
}
