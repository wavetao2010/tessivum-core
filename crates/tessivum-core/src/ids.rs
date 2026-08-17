use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};

use serde::{Deserialize, Serialize};

macro_rules! numeric_id {
    ($name:ident, $display:literal) => {
        #[repr(transparent)]
        #[derive(
            Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, concat!($display, ":{}"), self.0)
            }
        }
    };
}

numeric_id!(ScopeId, "scope");
numeric_id!(FiberId, "fiber");
numeric_id!(ResourceId, "resource");
numeric_id!(Generation, "generation");

/// A versioned, stable identifier for a native service contract.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ServiceKey {
    pub name: String,
    pub contract_version: String,
}

impl ServiceKey {
    pub fn new(name: impl Into<String>, contract_version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            contract_version: contract_version.into(),
        }
    }

    /// Returns the stable diagnostic key used in snapshots and errors.
    pub fn diagnostic_key(&self) -> String {
        format!("{}@{}", self.name, self.contract_version)
    }
}

impl fmt::Display for ServiceKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.diagnostic_key())
    }
}

/// A stable name for an explicitly shared service realm.
#[repr(transparent)]
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct RealmLabel(String);

impl RealmLabel {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for RealmLabel {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for RealmLabel {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for RealmLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

static NEXT_SCOPE_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_FIBER_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_RESOURCE_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

fn next(counter: &AtomicU64) -> u64 {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .expect("tessivum lifecycle identifiers exhausted")
}

pub(crate) fn next_scope_id() -> ScopeId {
    ScopeId(next(&NEXT_SCOPE_ID))
}

pub(crate) fn next_fiber_id() -> FiberId {
    FiberId(next(&NEXT_FIBER_ID))
}

pub(crate) fn next_resource_id() -> ResourceId {
    ResourceId(next(&NEXT_RESOURCE_ID))
}

pub(crate) fn next_generation() -> Generation {
    Generation(next(&NEXT_GENERATION))
}
