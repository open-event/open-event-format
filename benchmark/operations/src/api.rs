//! The one thing the runner asks of a format: answer this operation, and say what it cost.
//!
//! Both sides — the OE engine in [`oe`](crate::oe_format) and the Parquet reader and writer in
//! [`parquet`](crate::parquet) — sit behind this single interface, so the runner has no per-format branches and
//! neither side can quietly do less work than the other.

use crate::model::{Operation, WriterPhase};
use anyhow::Result;
use std::path::Path;
use std::time::Duration;

/// What one answered operation cost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperationOutcome {
    /// File bytes read or written. For a read this is what the format's own directory says the plan had to touch —
    /// the surviving groups' column blocks, not the whole file — so a format that prunes better reports less.
    pub bytes: u64,
    /// Rows produced or changed. The runner compares this across every format and fails the run when they
    /// disagree, which is what stops a faster answer from being a wrong one.
    pub matched_rows: u64,
}

/// One operation, answered and timed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Measured {
    /// How long the operation itself took, excluding the setup described on [`FormatUnderTest::run`] and any
    /// `input_conversion`.
    pub elapsed: Duration,
    /// Time the harness spent turning the generated rows into the format's own input shape before the operation
    /// could start. Harness cost, kept apart from `elapsed` so the format is charged only for its own work; `None`
    /// where the operation converts nothing.
    pub input_conversion: Option<Duration>,
    pub outcome: OperationOutcome,
}

/// One elapsed OE writer phase from a single benchmark iteration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhaseElapsed {
    pub elapsed: Duration,
    pub phase: WriterPhase,
}

/// One storage format, prepared over a dataset and ready to be measured.
pub trait FormatUnderTest {
    /// The name this format reports under.
    fn name(&self) -> &'static str;

    /// The size on disk of the file the comparison reads from.
    fn file_bytes(&self) -> u64;

    /// Answers one operation once and reports how long the operation itself took.
    ///
    /// Both implementations follow the same rule about what the clock covers: opening a reader over the file's bytes
    /// and parsing its metadata is setup and sits outside the timing, because a caller does it once and then answers
    /// many queries; everything from the first pruning decision onward is inside it. Each iteration gets a reader
    /// that has decoded nothing yet, so no iteration is subsidised by the one before. [`Operation::OpenMetadata`]
    /// measures that excluded setup on its own.
    ///
    /// Any file an operation writes goes under `scratch`, which the caller owns and clears between runs.
    fn run(&self, operation: Operation, scratch: &Path) -> Result<Measured>;

    /// Runs an operation with optional format-specific internal phase collection. Formats without internal phase
    /// instrumentation use the ordinary path and return no phases.
    fn run_with_phases(&self, operation: Operation, scratch: &Path) -> Result<(Measured, Vec<PhaseElapsed>)> {
        self.run(operation, scratch).map(|measured| (measured, Vec::new()))
    }
}
