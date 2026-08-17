use std::{error::Error, fmt};

use crate::FiberState;

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
            Self::Cleanup(errors) => write!(formatter, "{} cleanup error(s)", errors.len()),
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
