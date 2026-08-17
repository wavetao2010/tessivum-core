use serde_json::{json, Value};
use tessivum_core::{parse_entry_tree, Entry, EntryId, EntryOptions, RuntimeKind};

fn entry_options(id: &str) -> EntryOptions {
    EntryOptions {
        id: EntryId::new(id).expect("test entry id is valid"),
        name: Some(format!("plugin.{id}")),
        runtime: RuntimeKind::Native,
        config: json!({"enabled": true, "label": id}),
        inject: vec!["logger".into(), "metrics".into()],
        isolate: vec!["storage".into()],
        intercept: json!({"logger": {"level": "debug"}}),
        disabled: false,
        group: Some("headless".into()),
    }
}

#[test]
fn yaml_and_json_entry_trees_parse_and_validate_before_loading() {
    let json_tree = parse_entry_tree(
        r#"{
            "entries": [
                {
                    "package": "test/native",
                    "id": "base",
                    "name": "plugin.base",
                    "runtime": "native",
                    "config": {"mode": "base"},
                    "inject": ["logger"],
                    "isolate": ["storage"],
                    "intercept": {"logger": {"level": "info"}},
                    "disabled": false,
                    "group": "base"
                },
                {
                    "package": "test/wasm",
                    "id": "headless",
                    "runtime": "wasm",
                    "config": null,
                    "disabled": true
                }
            ]
        }"#,
    )
    .expect("base/headless JSON tree parses");
    json_tree
        .validate()
        .expect("parsed JSON tree is valid before any runtime starts");

    let yaml_tree = parse_entry_tree(
        r#"
entries:
  - package: test/native
    id: base
    name: plugin.base
    runtime: native
    config:
      mode: base
    inject: [logger]
    isolate: [storage]
    intercept:
      logger:
        level: info
    disabled: false
    group: base
  - package: test/wasm
    id: headless
    runtime: wasm
    config: null
    disabled: true
"#,
    )
    .expect("equivalent base/headless YAML tree parses");
    yaml_tree
        .validate()
        .expect("parsed YAML tree is valid before any runtime starts");
}

#[test]
fn stable_ids_schema_and_runtime_are_rejected_before_loading() {
    assert!(
        EntryId::new("").is_err(),
        "an entry cannot have an empty stable identifier"
    );
    assert!(
        EntryId::new("contains spaces").is_err(),
        "stable entry identifiers cannot be ambiguous display labels"
    );
    assert!(
        parse_entry_tree(
            r#"{"entries":[
                {"package":"test/one","id":"duplicate","runtime":"native"},
                {"package":"test/two","id":"duplicate","runtime":"wasm"}
            ]}"#
        )
        .is_err(),
        "duplicate entry identifiers fail while the candidate is detached"
    );
    assert!(
        parse_entry_tree(
            r#"{"entries":[{"package":"test/one","id":"bad-runtime","runtime":"python"}]}"#
        )
        .is_err(),
        "an unsupported runtime cannot survive schema parsing"
    );
    assert!(
        parse_entry_tree(r#"{"entries":[{"package":"test/one","id":"unknown-field","runtime":"native","surprise":true}]}"#)
            .is_err(),
        "entry schema is closed rather than silently accepting misspelled configuration"
    );
}

#[test]
fn entry_options_round_trip_all_runtime_context_controls() {
    let entry = Entry {
        package: "test/native".into(),
        options: entry_options("contextual"),
    };
    let serialized =
        serde_json::to_value(&entry).expect("entry serializes into configuration data");
    assert_eq!(
        serialized,
        json!({
            "package": "test/native",
            "id": "contextual",
            "name": "plugin.contextual",
            "runtime": "native",
            "config": {"enabled": true, "label": "contextual"},
            "inject": ["logger", "metrics"],
            "isolate": ["storage"],
            "intercept": {"logger": {"level": "debug"}},
            "disabled": false,
            "group": "headless"
        }),
        "declarative context controls survive the public entry representation"
    );

    let restored: Entry =
        serde_json::from_value(serialized).expect("serialized entry deserializes");
    assert_eq!(
        restored.options.inject,
        vec!["logger", "metrics"],
        "required services retain their declared order"
    );
    assert_eq!(restored.options.isolate, vec!["storage"]);
    assert_eq!(
        restored.options.intercept,
        Value::Object(
            [("logger".into(), json!({"level": "debug"}))]
                .into_iter()
                .collect(),
        )
    );
    assert!(!restored.options.disabled);
    assert_eq!(restored.options.group.as_deref(), Some("headless"));

    let disabled = Entry {
        package: "test/legacy".into(),
        options: EntryOptions {
            runtime: RuntimeKind::LegacyNode,
            disabled: true,
            ..entry_options("disabled")
        },
    };
    assert_eq!(
        serde_json::to_value(disabled).expect("disabled legacy entry serializes")["runtime"],
        "legacy-node",
        "runtime names remain stable configuration data"
    );
}
