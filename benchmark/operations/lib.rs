#![cfg_attr(
    test,
    allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::indexing_slicing)
)]

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
