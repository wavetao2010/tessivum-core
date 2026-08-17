use std::{env, fs, process::ExitCode};

use serde_json::json;
use tessivum_core::parse_fixture;

fn fail(status: &str, fixture: &str, error: impl std::fmt::Display) -> ExitCode {
    println!(
        "{}",
        json!({
            "fixture": fixture,
            "status": status,
            "error": {
                "code": status,
                "fixture": fixture,
                "message": error.to_string(),
            },
        })
    );
    ExitCode::from(1)
}

fn main() -> ExitCode {
    let mut paths = env::args_os().skip(1);
    let Some(path) = paths.next() else {
        return fail("USAGE", "<input>", "expected exactly one fixture path");
    };
    if paths.next().is_some() {
        return fail("USAGE", "<input>", "expected exactly one fixture path");
    }

    let display = path.to_string_lossy();
    let document = match fs::read_to_string(&path) {
        Ok(document) => document,
        Err(error) => return fail("INPUT_ERROR", &display, error),
    };
    let fixture = match parse_fixture(&document) {
        Ok(fixture) => fixture,
        Err(error) => return fail("INVALID_FIXTURE", &display, error),
    };

    let name = &fixture.name;
    println!(
        "{}",
        json!({
            "fixture": name,
            "status": "NOT_IMPLEMENTED",
            "error": {
                "code": "NOT_IMPLEMENTED",
                "fixture": name,
                "message": "Rust conformance execution is not implemented",
            },
        })
    );
    ExitCode::from(2)
}
