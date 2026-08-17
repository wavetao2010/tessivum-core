use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{FiberState, Generation};

/// A stable error payload that may cross a plugin-runtime boundary.
///
/// Consumers must branch on `code` and `phase`; `message` and `details` are
/// diagnostic data only.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginError {
    pub code: String,
    pub message: String,
    pub phase: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl PluginError {
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        phase: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            phase: phase.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }
}

impl fmt::Display for PluginError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} during {}: {}",
            self.code, self.phase, self.message
        )
    }
}

impl Error for PluginError {}
/// A lifecycle operation could not complete.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoreError {
    /// The requested operation is not valid for the resource's current state.
    InvalidState {
        operation: &'static str,
        state: FiberState,
    },
    /// A cancellation-aware operation stopped before completing.
    Cancelled,
    /// User setup failed before the fiber became active.
    SetupFailed(String),
    /// One or more disposers failed after all cleanup was attempted.
    Cleanup(Vec<CoreError>),
    /// A typed lookup found a provider with a different Rust contract type.
    ServiceTypeMismatch { key: String },
    /// A service handle was used after its provider changed or was removed.
    StaleServiceHandle { key: String, generation: Generation },
    /// A listener received a payload whose native contract did not match its event key.
    EventTypeMismatch {
        event: String,
        expected: &'static str,
    },
    /// A synchronous dispatch encountered a listener that requires asynchronous dispatch.
    AsyncEventListener {
        operation: &'static str,
        event: String,
    },
    /// A waterfall continuation was called more than once.
    WaterfallContinuationUsed,
    /// A native plugin lifecycle hook returned a structured failure.
    Plugin {
        phase: &'static str,
        message: String,
    },
}

impl CoreError {
    pub fn cleanup(errors: Vec<Self>) -> Self {
        Self::Cleanup(errors)
    }

    pub fn cleanup_errors(&self) -> Option<&[Self]> {
        match self {
            Self::Cleanup(errors) => Some(errors),
            _ => None,
        }
    }
}

impl From<String> for CoreError {
    fn from(message: String) -> Self {
        Self::SetupFailed(message)
    }
}

impl From<&str> for CoreError {
    fn from(message: &str) -> Self {
        Self::SetupFailed(message.into())
    }
}

impl fmt::Display for CoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidState { operation, state } => {
                write!(formatter, "cannot {operation} while {state:?}")
            }
            Self::Cancelled => formatter.write_str("operation cancelled"),
            Self::SetupFailed(message) => formatter.write_str(message),
            Self::ServiceTypeMismatch { key } => {
                write!(formatter, "service {key} has a different Rust type")
            }
            Self::StaleServiceHandle { key, generation } => {
                write!(
                    formatter,
                    "service handle for {key} at {generation} is stale"
                )
            }
            Self::Cleanup(errors) => write!(formatter, "{} cleanup error(s)", errors.len()),
            Self::EventTypeMismatch { event, expected } => {
                write!(formatter, "event {event} expected payload type {expected}")
            }
            Self::AsyncEventListener { operation, event } => {
                write!(formatter, "cannot {operation} event {event} synchronously")
            }
            Self::WaterfallContinuationUsed => {
                formatter.write_str("waterfall continuation already used")
            }
            Self::Plugin { phase, message } => {
                write!(formatter, "native plugin {phase} failed: {message}")
            }
        }
    }
}

impl Error for CoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Cleanup(errors) => errors.first().map(|error| error as &(dyn Error + 'static)),
            _ => None,
        }
    }
}

/// A configuration, package-resolution, or loader transaction failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LoaderError {
    Parse(String),
    Validation(String),
    Expression(String),
    Patch(String),
    MissingEntry(crate::EntryId),
    Persistence(String),
    Runtime {
        stage: &'static str,
        entry: crate::EntryId,
        name: Option<String>,
        message: String,
    },
    Aggregate(Vec<LoaderError>),
    Transaction {
        failure: Box<LoaderError>,
        rollback: Vec<LoaderError>,
    },
}

impl LoaderError {
    pub fn aggregate(errors: impl IntoIterator<Item = Self>) -> Self {
        let mut errors = errors.into_iter().collect::<Vec<_>>();
        if errors.len() == 1 {
            errors.pop().expect("one error")
        } else {
            Self::Aggregate(errors)
        }
    }

    pub fn transaction(failure: Self, rollback: Vec<Self>) -> Self {
        if rollback.is_empty() {
            failure
        } else {
            Self::Transaction {
                failure: Box::new(failure),
                rollback,
            }
        }
    }

    pub fn rollback_errors(&self) -> &[Self] {
        match self {
            Self::Transaction { rollback, .. } => rollback,
            _ => &[],
        }
    }

    pub(crate) fn in_entry(self, stage: &'static str, entry: &crate::Entry) -> Self {
        Self::Runtime {
            stage,
            entry: entry.options.id.clone(),
            name: entry.options.name.clone(),
            message: self.to_string(),
        }
    }
}

impl fmt::Display for LoaderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(message) => write!(formatter, "invalid loader document: {message}"),
            Self::Validation(message) => {
                write!(formatter, "invalid loader configuration: {message}")
            }
            Self::Expression(message) => write!(formatter, "invalid safe expression: {message}"),
            Self::Patch(message) => write!(formatter, "invalid loader patch: {message}"),
            Self::MissingEntry(id) => write!(formatter, "loader entry {id} does not exist"),
            Self::Persistence(message) => {
                write!(formatter, "cannot persist loader configuration: {message}")
            }
            Self::Runtime {
                stage,
                entry,
                name,
                message,
            } => {
                write!(formatter, "failed to {stage} loader entry {entry}")?;
                if let Some(name) = name {
                    write!(formatter, " ({name})")?;
                }
                write!(formatter, ": {message}")
            }
            Self::Aggregate(errors) => write!(formatter, "{} loader error(s)", errors.len()),
            Self::Transaction { failure, rollback } => write!(
                formatter,
                "loader transaction failed: {failure}; {} rollback error(s)",
                rollback.len()
            ),
        }
    }
}

impl Error for LoaderError {}
