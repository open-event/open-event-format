#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::type_complexity
    )
)]

pub mod artifacts;
pub mod benchmarks;
pub mod clock;
pub mod columns;
pub mod compat;
pub mod constant;
pub mod corrections;
pub mod deletes;
pub mod deletion_store;
#[allow(unsafe_code)]
pub mod encoding;
pub mod error;
pub mod events;
pub mod field_deletion_store;
pub mod file;
#[cfg(test)]
mod fuzz_smoke;
pub mod indexes;
pub mod integrity;
pub mod introspection;
pub mod invariants;
pub mod layout;
pub mod lifecycle;
pub mod object_store;
pub mod security;
pub mod typed_id;
pub mod vector;

#[cfg(feature = "write")]
pub mod writer;
