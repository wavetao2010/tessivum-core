use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::LoaderError;

/// The explicitly allowed inputs available to configuration expressions.
///
/// Values are supplied by the host; evaluating an expression never reads an
/// unapproved environment variable and never executes host code.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigScope {
    pub environment: BTreeMap<String, String>,
    pub platform: String,
    pub architecture: String,
    pub profiles: BTreeMap<String, Value>,
    pub services: BTreeMap<String, Value>,
}

impl Default for ConfigScope {
    fn default() -> Self {
        Self {
            environment: BTreeMap::new(),
            platform: std::env::consts::OS.to_owned(),
            architecture: std::env::consts::ARCH.to_owned(),
            profiles: BTreeMap::new(),
            services: BTreeMap::new(),
        }
    }
}

impl ConfigScope {
    /// Captures only the named environment variables from the host process.
    pub fn from_allowed_environment<I, S>(allowed: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let environment = allowed
            .into_iter()
            .filter_map(|name| {
                let name = name.as_ref();
                std::env::var(name)
                    .ok()
                    .map(|value| (name.to_owned(), value))
            })
            .collect();
        Self {
            environment,
            platform: std::env::consts::OS.to_owned(),
            architecture: std::env::consts::ARCH.to_owned(),
            profiles: BTreeMap::new(),
            services: BTreeMap::new(),
        }
    }
}

/// A small, auditable configuration language. It intentionally has no call,
/// property-evaluation, or arbitrary-language form.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ConfigExpression {
    Literal {
        value: Value,
    },
    Env {
        name: String,
    },
    Platform,
    Architecture,
    Profile {
        name: String,
    },
    Service {
        name: String,
        #[serde(default)]
        path: Vec<String>,
    },
    Concat {
        values: Vec<ConfigExpression>,
    },
    Equals {
        left: Box<ConfigExpression>,
        right: Box<ConfigExpression>,
    },
    Not {
        value: Box<ConfigExpression>,
    },
    And {
        values: Vec<ConfigExpression>,
    },
    Or {
        values: Vec<ConfigExpression>,
    },
    Coalesce {
        values: Vec<ConfigExpression>,
    },
}

impl ConfigExpression {
    /// Parses one reference expression. The grammar is deliberately limited to
    /// `env.NAME`, `platform`, `architecture`, `profile.NAME`, and
    /// `service.NAME.path`.
    pub fn parse(source: &str) -> Result<Self, LoaderError> {
        let source = source
            .strip_prefix("${")
            .and_then(|value| value.strip_suffix('}'))
            .unwrap_or(source);
        if source == "platform" {
            return Ok(Self::Platform);
        }
        if source == "architecture" {
            return Ok(Self::Architecture);
        }
        let mut parts = source.split('.');
        let Some(kind) = parts.next() else {
            return Err(LoaderError::Expression("empty expression".into()));
        };
        let parts = parts.collect::<Vec<_>>();
        if parts.iter().any(|part| !is_name(part)) {
            return Err(LoaderError::Expression(format!(
                "invalid expression {source:?}"
            )));
        }
        match (kind, parts.as_slice()) {
            ("env", [name]) => Ok(Self::Env {
                name: (*name).to_owned(),
            }),
            ("profile", [name]) => Ok(Self::Profile {
                name: (*name).to_owned(),
            }),
            ("service", [name, path @ ..]) => Ok(Self::Service {
                name: (*name).to_owned(),
                path: path.iter().map(|part| (*part).to_owned()).collect(),
            }),
            _ => Err(LoaderError::Expression(format!(
                "unsupported expression {source:?}"
            ))),
        }
    }

    pub fn evaluate(&self, scope: &ConfigScope) -> Result<Value, LoaderError> {
        match self {
            Self::Literal { value } => {
                ensure_scalar(value)?;
                Ok(value.clone())
            }
            Self::Env { name } => scope
                .environment
                .get(name)
                .cloned()
                .map(Value::String)
                .ok_or_else(|| {
                    LoaderError::Expression(format!("environment variable {name:?} is not allowed"))
                }),
            Self::Platform => Ok(Value::String(scope.platform.clone())),
            Self::Architecture => Ok(Value::String(scope.architecture.clone())),
            Self::Profile { name } => scope.profiles.get(name).cloned().ok_or_else(|| {
                LoaderError::Expression(format!("profile {name:?} is not declared"))
            }),
            Self::Service { name, path } => {
                let mut value = scope.services.get(name).ok_or_else(|| {
                    LoaderError::Expression(format!("service {name:?} is not declared"))
                })?;
                for segment in path {
                    value = value
                        .as_object()
                        .and_then(|object| object.get(segment))
                        .ok_or_else(|| {
                            LoaderError::Expression(format!(
                                "service projection {name:?}.{segment} is unavailable"
                            ))
                        })?;
                }
                Ok(value.clone())
            }
            Self::Concat { values } => {
                let mut result = String::new();
                for value in values {
                    let value = value.evaluate(scope)?;
                    let Some(value) = value.as_str() else {
                        return Err(LoaderError::Expression(
                            "concat accepts only strings".into(),
                        ));
                    };
                    result.push_str(value);
                }
                Ok(Value::String(result))
            }
            Self::Equals { left, right } => {
                Ok(Value::Bool(left.evaluate(scope)? == right.evaluate(scope)?))
            }
            Self::Not { value } => Ok(Value::Bool(!as_bool(value.evaluate(scope)?)?)),
            Self::And { values } => {
                for value in values {
                    if !as_bool(value.evaluate(scope)?)? {
                        return Ok(Value::Bool(false));
                    }
                }
                Ok(Value::Bool(true))
            }
            Self::Or { values } => {
                for value in values {
                    if as_bool(value.evaluate(scope)?)? {
                        return Ok(Value::Bool(true));
                    }
                }
                Ok(Value::Bool(false))
            }
            Self::Coalesce { values } => {
                for value in values {
                    let value = value.evaluate(scope)?;
                    if !value.is_null() {
                        return Ok(value);
                    }
                }
                Ok(Value::Null)
            }
        }
    }
}

/// Evaluates embedded `{ "$expr": "..." }` leaves in a configuration value.
/// Other JSON/YAML values are copied unchanged after recursively resolving
/// their children.
pub fn evaluate_config(value: &Value, scope: &ConfigScope) -> Result<Value, LoaderError> {
    match value {
        Value::Array(values) => values
            .iter()
            .map(|value| evaluate_config(value, scope))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(values) => {
            if let Some(expression) = values.get("$expr") {
                if values.len() != 1 {
                    return Err(LoaderError::Expression(
                        "an expression object may contain only $expr".into(),
                    ));
                }
                let expression = expression
                    .as_str()
                    .ok_or_else(|| LoaderError::Expression("$expr must be a string".into()))?;
                return ConfigExpression::parse(expression)?.evaluate(scope);
            }
            if values.keys().any(|key| key == "$js" || key == "!!js") {
                return Err(LoaderError::Expression(
                    "arbitrary JavaScript expressions are not supported".into(),
                ));
            }
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), evaluate_config(value, scope)?)))
                .collect::<Result<serde_json::Map<_, _>, LoaderError>>()
                .map(Value::Object)
        }
        _ => Ok(value.clone()),
    }
}

fn ensure_scalar(value: &Value) -> Result<(), LoaderError> {
    if value.is_string() || value.is_boolean() || value.is_null() {
        Ok(())
    } else {
        Err(LoaderError::Expression(
            "expression literals must be strings, booleans, or null".into(),
        ))
    }
}

fn as_bool(value: Value) -> Result<bool, LoaderError> {
    value
        .as_bool()
        .ok_or_else(|| LoaderError::Expression("boolean expression expected".into()))
}

fn is_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}
