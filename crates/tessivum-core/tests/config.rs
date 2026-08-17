use std::collections::BTreeMap;

use serde_json::{json, Value};
use tessivum_core::{evaluate_config, ConfigExpression, ConfigScope};

fn scope() -> ConfigScope {
    ConfigScope {
        environment: BTreeMap::from([("MODE".into(), "production".into())]),
        platform: "test-os".into(),
        architecture: "test-arch".into(),
        profiles: BTreeMap::from([("headless".into(), json!(true))]),
        services: BTreeMap::from([(
            "http".into(),
            json!({"endpoint": "https://example.test", "enabled": true}),
        )]),
    }
}

#[test]
fn safe_config_expressions_resolve_only_declared_inputs() {
    let scope = scope();

    assert_eq!(
        ConfigExpression::parse("${env.MODE}")
            .expect("whitelisted environment projection parses")
            .evaluate(&scope)
            .expect("whitelisted environment projection evaluates"),
        json!("production")
    );
    assert_eq!(
        ConfigExpression::parse("platform")
            .expect("platform expression parses")
            .evaluate(&scope)
            .expect("platform expression evaluates"),
        json!("test-os")
    );
    assert_eq!(
        ConfigExpression::parse("architecture")
            .expect("architecture expression parses")
            .evaluate(&scope)
            .expect("architecture expression evaluates"),
        json!("test-arch")
    );
    assert_eq!(
        ConfigExpression::parse("profile.headless")
            .expect("declared profile expression parses")
            .evaluate(&scope)
            .expect("declared profile expression evaluates"),
        json!(true)
    );
    assert_eq!(
        ConfigExpression::parse("service.http.endpoint")
            .expect("declared service projection parses")
            .evaluate(&scope)
            .expect("declared service projection evaluates"),
        json!("https://example.test")
    );

    assert!(
        ConfigExpression::parse("env.SECRET")
            .expect("well-formed but unapproved environment expression parses")
            .evaluate(&scope)
            .is_err(),
        "the evaluator must not read host environment variables outside its explicit projection"
    );
    assert!(
        ConfigExpression::parse("service.http.missing")
            .expect("well-formed missing service projection parses")
            .evaluate(&scope)
            .is_err(),
        "service configuration is a read-only declared projection rather than arbitrary lookup"
    );
}

#[test]
fn embedded_expressions_are_recursive_and_arbitrary_javascript_is_rejected() {
    let resolved = evaluate_config(
        &json!({
            "endpoint": {"$expr": "service.http.endpoint"},
            "metadata": [
                {"$expr": "${env.MODE}"},
                {"$expr": "profile.headless"},
                null,
            ],
        }),
        &scope(),
    )
    .expect("recursive safe expressions evaluate");

    assert_eq!(
        resolved,
        json!({
            "endpoint": "https://example.test",
            "metadata": ["production", true, null],
        }),
        "only expression leaves change; ordinary JSON values preserve their shape"
    );
    assert!(
        evaluate_config(&json!({"$js": "process.exit(1)"}), &scope()).is_err(),
        "general JavaScript is never evaluated in the native configuration evaluator"
    );
    assert!(
        evaluate_config(&json!({"$expr": "platform", "other": true}), &scope()).is_err(),
        "an expression node has no hidden sibling configuration"
    );
}

#[test]
fn auditable_boolean_string_and_null_operators_have_fixed_types() {
    let scope = scope();
    let expression = ConfigExpression::Concat {
        values: vec![
            ConfigExpression::Literal {
                value: Value::String("mode=".into()),
            },
            ConfigExpression::Env {
                name: "MODE".into(),
            },
        ],
    };
    assert_eq!(
        expression
            .evaluate(&scope)
            .expect("string concat evaluates declared string operands"),
        json!("mode=production")
    );
    assert_eq!(
        ConfigExpression::And {
            values: vec![
                ConfigExpression::Profile {
                    name: "headless".into(),
                },
                ConfigExpression::Not {
                    value: Box::new(ConfigExpression::Literal {
                        value: json!(false)
                    }),
                },
            ],
        }
        .evaluate(&scope)
        .expect("boolean operators evaluate boolean operands"),
        json!(true)
    );
    assert_eq!(
        ConfigExpression::Coalesce {
            values: vec![
                ConfigExpression::Literal { value: Value::Null },
                ConfigExpression::Literal {
                    value: json!("fallback"),
                },
            ],
        }
        .evaluate(&scope)
        .expect("coalesce returns the first non-null scalar"),
        json!("fallback")
    );
    assert!(
        ConfigExpression::Literal {
            value: json!({"not": "a scalar"})
        }
        .evaluate(&scope)
        .is_err(),
        "the literal language deliberately excludes object and array construction"
    );
    assert!(
        ConfigExpression::Not {
            value: Box::new(ConfigExpression::Literal {
                value: json!("not boolean")
            }),
        }
        .evaluate(&scope)
        .is_err(),
        "boolean operators cannot coerce arbitrary JSON values"
    );
}
