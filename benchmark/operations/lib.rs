#![cfg_attr(
    test,
    allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::indexing_slicing)
)]

//! Measures the OE columnar format against Parquet and Vortex over one identical dataset: creating a file, reading
//! it back, searching it, changing rows, removing rows, and comparing two versions of it.
//!
//! All three formats are handed the same generated rows, cut into blocks of the same size, and asked the same twelve
//! questions. Every answer is checked against the others before any timing is reported, so a faster number is never a
//! shortcut. What they differ in is how the format lets them answer, which is the point of the exercise: OE prunes on
//! the granule directory in its footer, addresses a single row's payload field through a mark, and records a delete as
//! a bitmap beside an untouched file; Parquet prunes on per-row-group statistics, reaches a row by decoding the pages
//! around it, and records a delete by rewriting the file; Vortex pushes the predicate into one scan over its layout
//! tree, takes explicit row indices, and — like Parquet — rewrites the file to delete.
//!
//! OE is the baseline every ratio in a report is measured against, because this exists to tell us where OE stands.
//!
//! Two caveats decide how the results should be read. The first is the delete: this compares *file* formats, and
//! table formats layered over Parquet (Iceberg v3, Delta Lake) have deletion vectors of their own, so a delete under
//! one of those costs roughly what it costs OE here. The rewrite measured below is what a bare Parquet or Vortex file
//! costs, nothing more. The second is the Vortex version: `oe` pins `vortex-array` 0.76.0 for the arrays it
//! embeds in OE column blocks, that whole release is yanked upstream and published no `vortex-file`, so the file
//! format measured here is 0.77.0 and both versions sit in this crate's dependency graph.
//!
//! This crate is dev/bench-only and is never a dependency of the `oe` library.

#[allow(
    unsafe_code,
    reason = "the counting allocator's GlobalAlloc impl; this crate is dev/bench-only"
)]
pub mod allocation;
pub mod api;
pub mod cli;
pub mod constant;
pub mod dataset;
pub mod environment;
pub mod model;
pub mod oe_format;
pub mod parquet;
pub mod regression;
pub mod results;
pub mod run;
pub mod vortex;

pub use api::{FormatUnderTest, Measured, OperationOutcome, PhaseElapsed};
pub use model::{
    AllocationMeasurement, AllocationSample, BenchmarkFormat, BenchmarkReport, BenchmarkSpec, Environment, Operation,
    OperationReport, PersistedRun, WriterPhase,
};
pub use regression::{MemoryRegression, check_memory_regressions, memory_regressions};
pub use results::append_run;
pub use run::{BenchmarkSelection, compare, compare_selected, render_table};
