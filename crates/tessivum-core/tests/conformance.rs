use std::{path::PathBuf, process::Command};

use serde_json::Value;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("fixtures/conformance")
        .join(name)
}

#[test]
fn full_catalog_executes_without_not_implemented() {
    for (name, status) in [
        ("lifecycle.json", "PASS"),
        ("services.json", "PASS"),
        ("events.json", "PASS"),
        ("loader.json", "MISMATCH"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_conformance"))
            .arg(fixture(name))
            .output()
            .expect("conformance runner starts");
        assert_eq!(
            output.status.success(),
            status == "PASS",
            "{name} returned unexpected exit status: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let result: Value = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|error| panic!("{name} emitted invalid JSON: {error}"));
        assert_eq!(result["status"], status, "{name}: {result}");
        assert_ne!(result["status"], "NOT_IMPLEMENTED", "{name}: {result}");
        if name == "loader.json" {
            assert_eq!(result["error"]["code"], "TRACE_MISMATCH");
            assert!(
                result["trace"]
                    .as_array()
                    .is_some_and(|trace| !trace.is_empty()),
                "loader execution must expose its transaction trace: {result}"
            );
        }
    }
}
