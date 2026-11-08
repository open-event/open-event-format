//! Runs a tenant's background maintenance in one call: plans and executes compactions, re-points the erasure
//! backlog at the files those compactions publish, and runs every sweeper.
//!
//! Without this, the application wires the pieces itself each cycle: [`plan_compaction_cycle`] and
//! [`plan_graph_compaction_cycle`] on the head generation, [`execute_compaction`] per job, then
//! [`sweep_retired_files`], [`sweep_dropped_objects`] and [`sweep_state_chunks`]. A [`Maintenance`] runner holds the
//! policies and the publisher and does all of it in [`Maintenance::run_once`]; [`Maintenance::due`] says when the next
//! run is due, on a jittered interval so a fleet of writers does not compact in lock step.
//!
//! A compaction that loses its publication race (`CasLost`, after the publisher's own rebases) is not an error: the
//! runner plans again from the new head and retries, a bounded number of times per run. Every other failure of a job
//! is recorded in the report and the run moves on, so one bad input does not hold back the sweepers.
//!
//! See: oe-file-lifecycle/spec.md

use super::build::OeBuildConfig;
use super::erasure::retarget_backlog_after_compaction;
use super::merge::execute_compaction;
use super::publish::{NoopPeerNotices, OePublisher, PeerNotices, PublishFailure};
use super::retire::{SweepPolicy, SweepReport, sweep_retired_files};
use super::state_gc::{FileStateRoots, StateChunkLedger, StateChunkSweep, sweep_state_chunks};
use super::sweep::{DroppedObjectLedger, ObjectSweepReport, sweep_dropped_objects};
use crate::clock::Clock;
use crate::error::{PublishError, StateChunkError, StorageError};
use crate::invariants::{MonotonicClock, PublishedSet};
use crate::lifecycle::{
    CompactionJob, CompactionPolicy, GraphCompactionPolicy, PartState, plan_compaction_cycle,
    plan_graph_compaction_cycle,
};
use crate::object_store::ObjectStore;
use crate::security::{FileErasureBacklog, StateChunkKey};

/// How often maintenance runs, and how hard a run tries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceSchedule {
    /// Monotonic nanoseconds between runs.
    pub interval_nanos: u64,
    /// Up to this many nanoseconds are taken off each interval at random, so writers started together drift apart.
    /// Raised to no more than `interval_nanos`.
    pub jitter_nanos: u64,
    /// How many times one run plans again after a compaction lost its publication race.
    pub max_cas_retries: u32,
}

impl Default for MaintenanceSchedule {
    fn default() -> Self {
        Self {
            interval_nanos: 60_000_000_000,
            jitter_nanos: 15_000_000_000,
            max_cas_retries: 3,
        }
    }
}

/// What a [`Maintenance`] runner does each run.
#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    /// The build configuration compactions build their outputs under; its tenant is the tenant maintained.
    pub build: OeBuildConfig,
    /// Event-file compaction scheduling.
    pub compaction: CompactionPolicy,
    /// Graph-file compaction scheduling.
    pub graph_compaction: GraphCompactionPolicy,
    pub schedule: MaintenanceSchedule,
    /// The safety windows of the file and object sweepers.
    pub sweep: SweepPolicy,
}

/// The state chunk sweep a runner performs after the file sweeps: the tenant's chunk ledger, how to read a file's
/// state roots, and the key the chunks are sealed with.
pub struct StateChunkMaintenance {
    pub key: StateChunkKey,
    pub ledger: Box<dyn StateChunkLedger + Send>,
    pub roots: Box<dyn FileStateRoots + Send>,
}

/// One compaction a run published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionOutcome {
    /// The generation that published it.
    pub generation: u64,
    /// The replaced files, now `Outdated`.
    pub inputs: Vec<u128>,
    /// The merged file, now `Active`.
    pub output: u128,
}

/// A failure one step of a run met; the run carried on with the next step.
#[derive(Debug)]
pub enum MaintenanceError {
    /// The erasure backlog could not be re-pointed at a compaction's output.
    Backlog { inputs: Vec<u128>, error: StorageError },
    /// A compaction job failed and published nothing.
    Compaction { inputs: Vec<u128>, failure: PublishFailure },
    /// The dropped-object sweep failed.
    ObjectSweep(PublishFailure),
    /// The head generation could not be read for planning or for the state chunk sweep.
    Plan(PublishFailure),
    /// The retired-file sweep failed.
    RetiredFileSweep(PublishFailure),
    /// The state chunk sweep failed.
    StateChunkSweep(StateChunkError),
}

/// What one maintenance run did.
#[derive(Debug, Default)]
pub struct MaintenanceReport {
    /// How many times a compaction lost its publication race and the run planned again.
    pub cas_retries: u32,
    pub compactions: Vec<CompactionOutcome>,
    pub errors: Vec<MaintenanceError>,
    pub objects: Option<ObjectSweepReport>,
    pub retired_files: Option<SweepReport>,
    pub state_chunks: Option<StateChunkSweep>,
}

impl MaintenanceReport {
    /// Whether every step succeeded.
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }
}

/// A tenant's maintenance runner: one per tenant and catalogue, one run at a time.
pub struct Maintenance {
    /// Re-pointed at each compaction's output, when the tenant keeps an erasure backlog.
    pub backlog: Option<FileErasureBacklog>,
    pub config: MaintenanceConfig,
    next_due: Option<u64>,
    pub notices: Box<dyn PeerNotices + Send>,
    pub publisher: OePublisher,
    /// Swept after the file sweeps, when the tenant stores graph state chunks.
    pub state_chunks: Option<StateChunkMaintenance>,
}

impl Maintenance {
    /// A runner with `config`, no backlog, no state chunk sweep, and peers not notified.
    pub fn new(config: MaintenanceConfig) -> Self {
        Self {
            backlog: None,
            config,
            next_due: None,
            notices: Box::new(NoopPeerNotices::default()),
            publisher: OePublisher::new(),
            state_chunks: None,
        }
    }

    /// Whether a run is due by `clock`'s monotonic time: the first call always is, and each `true` schedules the next
    /// run one interval later, less a random jitter drawn from `clock`.
    pub fn due(&mut self, clock: &dyn MonotonicClock) -> bool {
        let now = clock.monotonic_nanos();
        if self.next_due.is_some_and(|next| now < next) {
            return false;
        }
        let schedule = self.config.schedule;
        let jitter = clock.jitter(schedule.jitter_nanos.min(schedule.interval_nanos));
        self.next_due = Some(now.saturating_add(schedule.interval_nanos - jitter));
        true
    }

    /// Runs one maintenance cycle: every compaction the policies plan on the head generation (planning again after a
    /// lost race, up to the schedule's retry bound), the backlog re-pointing for each published compaction, then the
    /// retired-file sweep, the dropped-object sweep and, when configured, the state chunk sweep.
    ///
    /// `ledger` is the catalogue's dropped-object ledger and `oldest_live_snapshot` the oldest generation a running
    /// query reads (the head when none runs), as the sweepers take them. Failures are collected in the report rather
    /// than ending the run.
    pub fn run_once(
        &mut self,
        published: &mut dyn PublishedSet,
        objects: &dyn ObjectStore,
        ledger: &mut dyn DroppedObjectLedger,
        oldest_live_snapshot: u64,
        clock: &dyn Clock,
    ) -> MaintenanceReport {
        let mut report = MaintenanceReport::default();
        self.compact(published, objects, clock, &mut report);
        // The sweepers must not delete what a query started at the head we just advanced might read: a compaction
        // only moves files to `Outdated`, which the sweepers honour on their own.
        match sweep_retired_files(published, objects, &self.config.sweep, oldest_live_snapshot, clock) {
            Ok(sweep) => report.retired_files = Some(sweep),
            Err(failure) => report.errors.push(MaintenanceError::RetiredFileSweep(failure)),
        }
        match sweep_dropped_objects(
            published,
            objects,
            ledger,
            &self.config.sweep,
            oldest_live_snapshot,
            None,
            clock,
        ) {
            Ok(sweep) => report.objects = Some(sweep),
            Err(failure) => report.errors.push(MaintenanceError::ObjectSweep(failure)),
        }
        if let Some(chunks) = &self.state_chunks {
            match published.head() {
                Ok((_, head)) => match sweep_state_chunks(
                    &head,
                    self.config.build.tenant_id,
                    chunks.roots.as_ref(),
                    &chunks.key,
                    chunks.ledger.as_ref(),
                    objects,
                    clock,
                ) {
                    Ok(sweep) => report.state_chunks = Some(sweep),
                    Err(error) => report.errors.push(MaintenanceError::StateChunkSweep(error)),
                },
                Err(error) => report
                    .errors
                    .push(MaintenanceError::Plan(PublishFailure::Publish(error))),
            }
        }
        report
    }

    /// The jobs both planners schedule on the head generation, event jobs first.
    fn plan(&self, published: &dyn PublishedSet) -> Result<Vec<CompactionJob>, PublishFailure> {
        let (_, head) = published.head().map_err(PublishFailure::Publish)?;
        let tenant_id = self.config.build.tenant_id;
        let mut jobs = plan_compaction_cycle(&self.config.compaction, &head, tenant_id);
        jobs.extend(plan_graph_compaction_cycle(
            &self.config.graph_compaction,
            &head,
            tenant_id,
        ));
        Ok(jobs)
    }

    fn compact(
        &mut self,
        published: &mut dyn PublishedSet,
        objects: &dyn ObjectStore,
        clock: &dyn Clock,
        report: &mut MaintenanceReport,
    ) {
        // Inputs of failed jobs are not planned again this run, so a file that cannot be merged is not retried
        // forever; they are planned afresh next run.
        let mut failed: Vec<u128> = Vec::new();
        'plan: loop {
            let jobs = match self.plan(published) {
                Ok(jobs) => jobs,
                Err(failure) => {
                    report.errors.push(MaintenanceError::Plan(failure));
                    return;
                }
            };
            for job in jobs {
                if job.file_ids.iter().any(|file_id| failed.contains(file_id)) {
                    continue;
                }
                // A job planned before a lost race may name a file that is no longer active; it is planned again.
                if !self.inputs_active(published, &job) {
                    continue;
                }
                match execute_compaction(
                    &mut self.publisher,
                    &job,
                    &self.config.build,
                    published,
                    objects,
                    self.notices.as_mut(),
                    clock,
                ) {
                    Ok(done) => {
                        let output = done.entry.file_id;
                        if let Some(backlog) = &self.backlog
                            && let Err(error) = retarget_backlog_after_compaction(backlog, &job.file_ids, output)
                        {
                            report.errors.push(MaintenanceError::Backlog {
                                inputs: job.file_ids.clone(),
                                error,
                            });
                        }
                        report.compactions.push(CompactionOutcome {
                            generation: done.generation,
                            inputs: job.file_ids,
                            output,
                        });
                    }
                    Err(PublishFailure::Publish(PublishError::CasLost { .. }))
                        if report.cas_retries < self.config.schedule.max_cas_retries =>
                    {
                        report.cas_retries += 1;
                        continue 'plan;
                    }
                    Err(failure) => {
                        failed.extend_from_slice(&job.file_ids);
                        report.errors.push(MaintenanceError::Compaction {
                            inputs: job.file_ids,
                            failure,
                        });
                    }
                }
            }
            return;
        }
    }

    fn inputs_active(&self, published: &dyn PublishedSet, job: &CompactionJob) -> bool {
        published.head().is_ok_and(|(_, head)| {
            job.file_ids.iter().all(|file_id| {
                head.files
                    .iter()
                    .any(|entry| entry.file_id == *file_id && entry.part_state == PartState::Active)
            })
        })
    }
}

#[cfg(test)]
#[path = "test/maintenance.rs"]
mod tests;
