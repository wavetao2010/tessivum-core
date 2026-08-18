use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

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
    assert_eq!(error.code, "GUEST_REJECTED");
    assert_eq!(error.message, "guest rejected request");
    assert_eq!(error.phase, "call");
    assert_eq!(error.details, None);
}

#[test]
fn guest_response_envelopes_reject_ambiguous_and_spoofed_failures() {
    for mixed in [false, true] {
        let engine = InMemoryGuestEngine::new(move |_export, request, _host| {
            Ok(ResponseEnvelope {
                request_id: request.request_id,
                result: mixed.then(|| json!({ "result": true })),
                error: mixed.then(|| PluginError::new("TIMEOUT", "guest timeout", "call")),
            })
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
            .expect_err("mixed and empty envelopes are rejected");
        assert_eq!(error.code, "PROTOCOL_INVALID");
        assert_eq!(error.phase, "call");
        assert_eq!(error.details, None);
    }

    let engine = InMemoryGuestEngine::new(move |_export, request, _host| {
        guest_error(
            &request,
            "TIMEOUT",
            "guest secret timeout",
            "event",
            json!({ "secret": "guest details" }),
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
        .expect_err("a guest cannot spoof the invoked phase");
    assert_eq!(error.code, "PROTOCOL_INVALID");
    assert_eq!(
        error.message,
        "guest error phase does not match invoked export"
    );
    assert_eq!(error.phase, "call");
    assert_eq!(error.details, None);
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
fn wasm_u32(mut value: u32, output: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            return;
        }
    }
}

fn wasm_section(id: u8, payload: Vec<u8>, module: &mut Vec<u8>) {
    module.push(id);
    wasm_u32(payload.len() as u32, module);
    module.extend(payload);
}

fn lifecycle_wasm(wrong_export: Option<tessivum_extism::GuestExport>) -> Vec<u8> {
    let mut module = b"\0asm\x01\0\0\0".to_vec();
    wasm_section(1, vec![2, 0x60, 0, 1, 0x7f, 0x60, 0, 0], &mut module);

    let mut functions = vec![5];
    let mut code = vec![5];
    for export in tessivum_extism::GuestExport::ALL {
        let wrong = wrong_export == Some(export);
        functions.push(u8::from(wrong));
        let body = if wrong {
            vec![0, 0x0b]
        } else {
            vec![0, 0x41, 0, 0x0b]
        };
        wasm_u32(body.len() as u32, &mut code);
        code.extend(body);
    }
    wasm_section(3, functions, &mut module);

    let mut exports = vec![5];
    for (index, export) in tessivum_extism::GuestExport::ALL.into_iter().enumerate() {
        let name = export.as_str().as_bytes();
        wasm_u32(name.len() as u32, &mut exports);
        exports.extend(name);
        exports.push(0);
        wasm_u32(index as u32, &mut exports);
    }
    wasm_section(7, exports, &mut module);
    wasm_section(10, code, &mut module);
    module
}

fn wasm_i32(mut value: i32, output: &mut Vec<u8>) {
    loop {
        let mut byte = (value as u8) & 0x7f;
        value >>= 7;
        let done = (value == 0 && byte & 0x40 == 0) || (value == -1 && byte & 0x40 != 0);
        if !done {
            byte |= 0x80;
        }
        output.push(byte);
        if done {
            return;
        }
    }
}

fn wasm_name(name: &str, output: &mut Vec<u8>) {
    wasm_u32(name.len() as u32, output);
    output.extend(name.as_bytes());
}

fn tainted_trap_wasm(message: &str) -> Vec<u8> {
    let mut module = b"\0asm\x01\0\0\0".to_vec();
    wasm_section(
        1,
        vec![
            4, 0x60, 0, 1, 0x7f, 0x60, 1, 0x7e, 1, 0x7e, 0x60, 2, 0x7e, 0x7f, 0, 0x60, 1, 0x7e, 0,
        ],
        &mut module,
    );
    let mut imports = vec![3];
    for (name, type_index) in [("alloc", 1), ("store_u8", 2), ("error_set", 3)] {
        wasm_name("extism:host/env", &mut imports);
        wasm_name(name, &mut imports);
        imports.push(0);
        imports.push(type_index);
    }
    wasm_section(2, imports, &mut module);
    wasm_section(3, vec![5, 0, 0, 0, 0, 0], &mut module);

    let mut exports = vec![5];
    for (index, export) in tessivum_extism::GuestExport::ALL.into_iter().enumerate() {
        let name = export.as_str().as_bytes();
        wasm_u32(name.len() as u32, &mut exports);
        exports.extend(name);
        exports.push(0);
        wasm_u32((index + 3) as u32, &mut exports);
    }
    wasm_section(7, exports, &mut module);

    let mut code = vec![5];
    for export in tessivum_extism::GuestExport::ALL {
        let mut body = if export == tessivum_extism::GuestExport::Call {
            vec![1, 1, 0x7e, 0x42]
        } else {
            vec![0, 0x41, 0, 0x0b]
        };
        if export == tessivum_extism::GuestExport::Call {
            wasm_i32(message.len() as i32, &mut body);
            body.extend([0x10, 0, 0x21, 0]);
            for (index, byte) in message.bytes().enumerate() {
                body.extend([0x20, 0, 0x42]);
                wasm_i32(index as i32, &mut body);
                body.push(0x7c);
                body.push(0x41);
                wasm_i32(i32::from(byte), &mut body);
                body.extend([0x10, 1]);
            }
            body.extend([0x20, 0, 0x10, 2, 0, 0x0b]);
        }
        wasm_u32(body.len() as u32, &mut code);
        code.extend(body);
    }
    wasm_section(10, code, &mut module);
    module
}

struct CountingEngine(Arc<AtomicUsize>);

impl tessivum_extism::GuestEngine for CountingEngine {
    fn instantiate(
        &self,
        _package: &WasmPackage,
        _host: tessivum_extism::HostBindings,
        _limits: ResourceLimits,
    ) -> WasmResult<Box<dyn tessivum_extism::GuestInstance>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(PluginError::new(
            "UNREACHABLE",
            "engine should not run",
            "instantiate",
        ))
    }
}

#[test]
fn wrong_lifecycle_signature_is_rejected_before_engine_instantiation() {
    for export in tessivum_extism::GuestExport::ALL {
        let instantiated = Arc::new(AtomicUsize::new(0));
        let error = match WasmPluginInstance::instantiate(
            WasmPackage {
                manifest: manifest(&[]),
                wasm: Some(lifecycle_wasm(Some(export))),
            },
            Arc::new(CountingEngine(Arc::clone(&instantiated))),
            Arc::new(CapabilityRegistry::default()),
            ResourceLimits::default(),
            json!({ "enabled": true }),
        ) {
            Ok(_) => panic!("wrong ABI signature reached the engine"),
            Err(error) => error,
        };
        assert_eq!(error.code, "ABI_EXPORT_INVALID", "{}", error.message);
        assert_eq!(instantiated.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn modules_over_eight_mebibytes_reject_before_engine_instantiation() {
    let instantiated = Arc::new(AtomicUsize::new(0));
    let error = match WasmPluginInstance::instantiate(
        WasmPackage {
            manifest: manifest(&[]),
            wasm: Some(vec![0; 8 * 1024 * 1024 + 1]),
        },
        Arc::new(CountingEngine(Arc::clone(&instantiated))),
        Arc::new(CapabilityRegistry::default()),
        ResourceLimits::default(),
        json!({ "enabled": true }),
    ) {
        Ok(_) => panic!("oversized module reached the engine"),
        Err(error) => error,
    };
    assert_eq!(error.code, "MODULE_INVALID");
    assert_eq!(instantiated.load(Ordering::SeqCst), 0);
}

#[test]
fn guest_trap_text_cannot_spoof_resource_limit_errors() {
    for message in ["timeout", "fuel"] {
        let instance = WasmPluginInstance::instantiate(
            WasmPackage::from_bytes(manifest(&[]), tainted_trap_wasm(message))
                .expect("tainted trap module has the required ABI exports"),
            Arc::new(ExtismGuestEngine),
            Arc::new(CapabilityRegistry::default()),
            ResourceLimits::default(),
            json!({ "enabled": true }),
        )
        .expect("Extism instantiates the tainted trap module");
        let error = instance
            .call(json!({}), Value::Null)
            .expect_err("the guest trap must fail");
        assert_eq!(error.code, "GUEST_TRAP");
        assert_eq!(error.message, "guest execution failed");
        assert_eq!(error.phase, "call");
        assert_eq!(error.details, None);
    }
}

fn package_directory(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("tessivum-extism-{name}-{}", std::process::id()));
    fs::create_dir_all(&path).expect("test package directory creates");
    path
}

fn write_manifest(path: &std::path::Path, manifest: &PluginManifest) {
    fs::write(
        path,
        serde_json::to_vec(manifest).expect("manifest serializes"),
    )
    .expect("test manifest writes");
}

#[test]
fn manifest_entry_rejects_absolute_and_traversal_paths() {
    let package = package_directory("entry-path");
    let outside = package
        .parent()
        .expect("package has parent")
        .join(format!("outside-{}.wasm", std::process::id()));
    fs::write(&outside, lifecycle_wasm(None)).expect("outside module writes");
    for entry in [
        outside.to_string_lossy().into_owned(),
        "../outside.wasm".to_owned(),
    ] {
        let manifest_path = package.join("plugin.json");
        let mut document = manifest(&[]);
        document.entry = entry;
        write_manifest(&manifest_path, &document);
        let error = WasmPackage::from_manifest_file(&manifest_path)
            .expect_err("manifest entry must remain within its package");
        assert_eq!(error.code, "MANIFEST_INVALID");
    }
    fs::remove_file(outside).expect("outside module removes");
    fs::remove_dir_all(package).expect("package directory removes");
}

#[cfg(unix)]
#[test]
fn manifest_entry_rejects_symlink_escapes() {
    let package = package_directory("entry-symlink");
    let outside = package
        .parent()
        .expect("package has parent")
        .join(format!("outside-symlink-{}.wasm", std::process::id()));
    fs::write(&outside, lifecycle_wasm(None)).expect("outside module writes");
    std::os::unix::fs::symlink(&outside, package.join("plugin.wasm"))
        .expect("escape symlink creates");
    let manifest_path = package.join("plugin.json");
    write_manifest(&manifest_path, &manifest(&[]));
    let error = WasmPackage::from_manifest_file(&manifest_path)
        .expect_err("symlink target must remain within its package");
    assert_eq!(error.code, "MANIFEST_INVALID");
    fs::remove_file(outside).expect("outside module removes");
    fs::remove_dir_all(package).expect("package directory removes");
}

#[test]
fn manifest_loader_rejects_oversized_manifest_and_wasm_before_allocation() {
    let package = package_directory("bounded-package");
    let manifest_path = package.join("plugin.json");
    fs::write(&manifest_path, vec![b' '; 256 * 1024 + 1]).expect("oversized manifest writes");
    let manifest_error = WasmPackage::from_manifest_file(&manifest_path)
        .expect_err("oversized manifest must fail before parsing");
    assert_eq!(manifest_error.code, "PACKAGE_READ_FAILED");

    write_manifest(&manifest_path, &manifest(&[]));
    let wasm_path = package.join("plugin.wasm");
    let wasm = fs::File::create(&wasm_path).expect("oversized module creates");
    wasm.set_len(8 * 1024 * 1024 + 1)
        .expect("oversized module length sets");
    let wasm_error = WasmPackage::from_manifest_file(&manifest_path)
        .expect_err("oversized module must fail before reading");
    assert_eq!(wasm_error.code, "PACKAGE_READ_FAILED");
    fs::remove_dir_all(package).expect("package directory removes");
}

#[cfg(unix)]
#[test]
fn manifest_loader_does_not_follow_the_final_manifest_symlink() {
    let package = package_directory("manifest-symlink");
    let target = package.join("target.json");
    write_manifest(&target, &manifest(&[]));
    let link = package.join("plugin.json");
    std::os::unix::fs::symlink(&target, &link).expect("manifest symlink creates");
    let error = WasmPackage::from_manifest_file(&link)
        .expect_err("manifest final symlink must not be followed");
    assert_eq!(error.code, "PACKAGE_READ_FAILED");
    fs::remove_dir_all(package).expect("package directory removes");
}
