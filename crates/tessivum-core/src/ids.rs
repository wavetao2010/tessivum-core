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
