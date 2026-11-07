//! The OE conformance harness: one test binary (`conformance`, rooted at this `tests/conformance/main.rs`) over a `mod`
//! tree mirroring `tests/conformance/<capability>/<requirement_slug>.rs` (Cargo only auto-discovers top-level files and
//! `*/main.rs` under `tests/`, so the subdirectory files are reached via modules from this root). One `#[test]` per
//! spec scenario, named by the scenario slug and annotated `/// conformance: <scenario-id>`; `conformance/ledger.toml`
//! tracks status.
//!
//! Stub convention: a scenario whose THEN depends on a subsystem outside this crate is a named stub; stubs may assert
//! the in-scope fragment but are recorded as `stub` in the ledger.
// Panic-shortcut lints are exempt in the conformance suite: a panicking test is the mechanism working.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::indexing_slicing)]

mod support;

mod oe_apis;
mod oe_benchmarks_and_acceptance_gates;
mod oe_column_design;
mod oe_core_invariants;
mod oe_deletes_and_corrections;
mod oe_encodings_and_compression;
mod oe_file_layout;
mod oe_file_lifecycle;
mod oe_hardware_deployment;
mod oe_layout_and_clustering;
mod oe_logical_event_model;
mod oe_manifest_integration;
mod oe_physical_artifacts;
mod oe_query_metadata_and_indexes;
mod oe_reader_compatibility;
mod oe_security_and_isolation;
mod oe_write_path;
