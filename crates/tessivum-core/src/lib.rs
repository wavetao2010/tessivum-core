use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod config;
mod context;
mod error;
mod events;
mod ids;
mod lifecycle;
pub mod loader;
pub mod native;
mod service;

pub use config::{evaluate_config, ConfigExpression, ConfigScope};
pub use context::ContextHandle;
pub use error::{CoreError, LoaderError, PluginError};
pub use events::{
    DispatchDiagnostic, DispatchMode, DispatchOutcome, DynamicEvent, EventBus, EventFuture,
    EventKey, EventOptions, EventResult, EventValue, ListenerHandle, WaterfallNext,
};
pub use ids::{FiberId, Generation, RealmLabel, ResourceId, ScopeId, ServiceKey};
pub use lifecycle::{BoxDisposer, CancellationToken, EffectMeta, Fiber, FiberState, Scope};
pub use loader::{
    apply_entry_patches, parse_entry_tree, persist_entry_tree, ActivationState, Entry, EntryGroup,
    EntryId, EntryOptions, EntryTree, HmrDriver, Loader, LoaderFuture, LoaderResult, LoaderRuntime,
    PackageResolver, Patch, ResolvedPackage, RuntimeHandle, RuntimeKind,
};
pub use native::{
    NativeConfigError, NativeConfigKind, NativeConfigSchema, NativeFiberSnapshot, NativePlugin,
    NativePluginDescriptor, NativePluginError, NativePluginFactory, NativePluginFuture,
    NativePluginInstance, NativePluginPhase, NativePluginResult, NativePluginRuntime,
    NativePluginRuntimeSnapshot, NativePluginSnapshot,
};
pub use service::{
    Dependency, DependencySnapshot, DependencySubscription, ServiceHandle, ServiceSnapshot,
};

pub const SCHEMA_VERSION: &str = "tessivum.conformance/v1";
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Fixture {
    pub schema_version: String,
    pub name: String,
    pub domain: Domain,
    pub scenario: String,
    #[serde(default, deserialize_with = "deserialize_optional_input")]
    pub input: Option<Value>,
    pub expected_trace: Vec<TraceEvent>,
}

impl Fixture {
    pub fn validate(&self) -> Result<(), FixtureError> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(FixtureError::Invalid(format!(
                "schemaVersion must be {SCHEMA_VERSION:?}"
            )));
        }
        if !is_catalog_identifier(&self.name) {
            return Err(FixtureError::Invalid(
                "name must be a lowercase dot- or dash-separated identifier".into(),
            ));
        }
        if !is_catalog_identifier(&self.scenario) {
            return Err(FixtureError::Invalid(
                "scenario must be a lowercase dot- or dash-separated identifier".into(),
            ));
        }
        if self.input.as_ref().is_some_and(|input| !input.is_object()) {
            return Err(FixtureError::Invalid("input must be an object".into()));
        }
        if self.expected_trace.is_empty() && self.domain != Domain::Loader {
            return Err(FixtureError::Invalid(
                "expectedTrace must not be empty".into(),
            ));
        }
        for (index, event) in self.expected_trace.iter().enumerate() {
            event.validate(index)?;
        }
        Ok(())
    }
}

fn is_catalog_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() {
        return false;
    }

    let mut after_separator = false;
    for byte in bytes {
        if byte.is_ascii_lowercase() || byte.is_ascii_digit() {
            after_separator = false;
        } else if matches!(byte, b'.' | b'-') && !after_separator {
            after_separator = true;
        } else {
            return false;
        }
    }
    !after_separator
}

fn deserialize_optional_input<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Value::deserialize(deserializer).map(Some)
}

fn deserialize_optional_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    String::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Domain {
    Lifecycle,
    Service,
    Event,
    Loader,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TraceEvent {
    #[serde(rename = "event")]
    pub kind: TraceEventKind,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub subject: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub from: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub to: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub label: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub phase: Option<String>,
    pub value: Option<Value>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    pub error: Option<String>,
}

impl TraceEvent {
    fn validate(&self, index: usize) -> Result<(), FixtureError> {
        for (field, value) in [
            ("subject", self.subject.as_deref()),
            ("from", self.from.as_deref()),
            ("to", self.to.as_deref()),
            ("label", self.label.as_deref()),
            ("phase", self.phase.as_deref()),
            ("error", self.error.as_deref()),
        ] {
            if value.is_some_and(|value| value.trim().is_empty()) {
                return Err(FixtureError::Invalid(format!(
                    "expectedTrace[{index}].{field} must not be blank"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TraceEventKind {
    FiberCreated,
    FiberStateChanged,
    ServiceProvided,
    ServiceRemoved,
    ListenerAdded,
    ListenerRemoved,
    EventDispatched,
    EffectCreated,
    EffectDisposed,
    PluginError,
    ConfigCommitted,
    ConfigRolledBack,
}

#[derive(Debug)]
pub enum FixtureError {
    MalformedDocument(serde_json::Error),
    Invalid(String),
}

impl fmt::Display for FixtureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedDocument(error) => write!(formatter, "malformed JSON fixture: {error}"),
            Self::Invalid(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for FixtureError {}

pub fn parse_fixture(document: &str) -> Result<Fixture, FixtureError> {
    let fixture: Fixture =
        serde_json::from_str(document).map_err(FixtureError::MalformedDocument)?;
    fixture.validate()?;
    Ok(fixture)
}
