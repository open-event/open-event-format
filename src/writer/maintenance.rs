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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceSchedule {
    pub interval_nanos: u64,
    pub jitter_nanos: u64,
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

#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    pub build: OeBuildConfig,
    pub compaction: CompactionPolicy,
    pub graph_compaction: GraphCompactionPolicy,
    pub schedule: MaintenanceSchedule,
    pub sweep: SweepPolicy,
}

pub struct StateChunkMaintenance {
    pub key: StateChunkKey,
    pub ledger: Box<dyn StateChunkLedger + Send>,
    pub roots: Box<dyn FileStateRoots + Send>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionOutcome {
    pub generation: u64,
    pub inputs: Vec<u128>,
    pub output: u128,
}

#[derive(Debug)]
pub enum MaintenanceError {
    Backlog { inputs: Vec<u128>, error: StorageError },
    Compaction { inputs: Vec<u128>, failure: PublishFailure },
    ObjectSweep(PublishFailure),
    Plan(PublishFailure),
    RetiredFileSweep(PublishFailure),
    StateChunkSweep(StateChunkError),
}

#[derive(Debug, Default)]
pub struct MaintenanceReport {
    pub cas_retries: u32,
    pub compactions: Vec<CompactionOutcome>,
    pub errors: Vec<MaintenanceError>,
    pub objects: Option<ObjectSweepReport>,
    pub retired_files: Option<SweepReport>,
    pub state_chunks: Option<StateChunkSweep>,
}

impl MaintenanceReport {
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }
}

pub struct Maintenance {
    pub backlog: Option<FileErasureBacklog>,
    pub config: MaintenanceConfig,
    next_due: Option<u64>,
    pub notices: Box<dyn PeerNotices + Send>,
    pub publisher: OePublisher,
    pub state_chunks: Option<StateChunkMaintenance>,
}

impl Maintenance {
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
