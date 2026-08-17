use std::{fs, path::PathBuf, process::Command};

use serde_json::Value;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("fixtures/conformance")
        .join(name)
}

#[test]
fn full_catalog_executes_every_declared_case() {
    for name in [
        "lifecycle.json",
        "services.json",
        "events.json",
        "loader.json",
    ] {
        let fixture_document: Value = serde_json::from_slice(
            &fs::read(fixture(name)).unwrap_or_else(|error| panic!("{name} is readable: {error}")),
        )
        .unwrap_or_else(|error| panic!("{name} is valid JSON: {error}"));
        let expected_cases = Value::Array(
            fixture_document["input"]["cases"]
                .as_array()
                .unwrap_or_else(|| panic!("{name} declares cases"))
                .iter()
                .map(|case| case["id"].clone())
                .collect(),
        );
        let output = Command::new(env!("CARGO_BIN_EXE_conformance"))
            .arg(fixture(name))
            .output()
            .expect("conformance runner starts");
        assert!(
            output.status.success(),
            "{name} returned unexpected exit status: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let result: Value = serde_json::from_slice(&output.stdout)
            .unwrap_or_else(|error| panic!("{name} emitted invalid JSON: {error}"));
        assert_eq!(result["status"], "PASS", "{name}: {result}");
        assert_eq!(result["executedCases"], expected_cases, "{name}: {result}");
        assert_ne!(result["status"], "NOT_IMPLEMENTED", "{name}: {result}");
    }
}

#[test]
fn empty_oracle_baseline_is_rejected_outside_loader() {
    let mut document: Value = serde_json::from_slice(
        &fs::read(fixture("lifecycle.json")).expect("lifecycle fixture is readable"),
    )
    .expect("lifecycle fixture is valid JSON");
    document["expectedTrace"] = Value::Array(Vec::new());
    let path = std::env::temp_dir().join(format!(
        "tessivum-conformance-empty-baseline-{}.json",
        std::process::id()
    ));
    fs::write(
        &path,
        serde_json::to_vec(&document).expect("fixture serializes"),
    )
    .expect("temporary fixture writes");
    let output = Command::new(env!("CARGO_BIN_EXE_conformance"))
        .arg(&path)
        .output();
    let _ = fs::remove_file(&path);
    let output = output.expect("conformance runner starts");
    let result: Value =
        serde_json::from_slice(&output.stdout).expect("conformance runner emits JSON");
    assert!(
        !output.status.success(),
        "non-Loader empty baseline must fail validation: {result}"
    );
    assert_eq!(result["status"], "INVALID_FIXTURE", "{result}");
    assert_eq!(result["error"]["code"], "INVALID_FIXTURE", "{result}");
}
