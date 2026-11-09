
use crate::model::{Operation, WriterPhase};
use anyhow::Result;
use std::path::Path;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperationOutcome {
    pub bytes: u64,
    pub matched_rows: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Measured {
    pub elapsed: Duration,
    pub input_conversion: Option<Duration>,
    pub outcome: OperationOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhaseElapsed {
    pub elapsed: Duration,
    pub phase: WriterPhase,
}

pub trait FormatUnderTest {
    fn name(&self) -> &'static str;

    fn file_bytes(&self) -> u64;

    fn run(&self, operation: Operation, scratch: &Path) -> Result<Measured>;

    fn run_with_phases(&self, operation: Operation, scratch: &Path) -> Result<(Measured, Vec<PhaseElapsed>)> {
        self.run(operation, scratch).map(|measured| (measured, Vec::new()))
    }
}
