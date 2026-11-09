use super::*;
use crate::error::PublishError;
use crate::events::relationships::{EventRelationships, RelationshipKind, RelationshipRef};
use crate::invariants::sim::{SimClock, SimulatedPublishedSet};
use crate::layout::Profile;
use crate::layout::graph::{EdgeKind, GraphInput, RowEdges};
use crate::layout::reader::OeFile;
use crate::lifecycle::ManifestGeneration;
use crate::object_store::oe_object_key;
use crate::object_store::sim::SimObjectStore;
use crate::writer::build::{BuildInput, BuildRow, OeBuildConfig, OeRow, build_oe_file};
use crate::writer::merge::read_build_rows;
use crate::writer::merge::tests::{build_config, publish_built, rich_row, rows_of, tenant};
use crate::writer::sweep::SimDroppedObjectLedger;

const POLICY: SweepPolicy = SweepPolicy {
    in_flight_query_horizon_nanos: 1_000,
    safety_window_nanos: 5_000,
};

fn config(build: OeBuildConfig) -> MaintenanceConfig {
    MaintenanceConfig {
        build,
        compaction: CompactionPolicy::default(),
        graph_compaction: GraphCompactionPolicy::default(),
        schedule: MaintenanceSchedule {
            interval_nanos: 1_000_000,
            jitter_nanos: 250_000,
            max_cas_retries: 3,
        },
        sweep: POLICY,
    }
}

fn lineage_build() -> OeBuildConfig {
    OeBuildConfig {
        delivery_lineage: true,
        ..build_config()
    }
}

struct RacedSet {
    fail: u32,
    inner: SimulatedPublishedSet,
}

impl PublishedSet for RacedSet {
    fn head(&self) -> Result<(u64, ManifestGeneration), PublishError> {
        self.inner.head()
    }

    fn put_generation(&mut self, generation: ManifestGeneration) -> Result<(), PublishError> {
        self.inner.put_generation(generation)
    }

    fn advance_head(&mut self, expected: u64, next: u64) -> Result<(), PublishError> {
        if self.fail > 0 {
            self.fail -= 1;
            return Err(PublishError::CasLost {
                current_generation: expected,
            });
        }
        self.inner.advance_head(expected, next)
    }

    fn generation(&self, id: u64) -> Result<ManifestGeneration, PublishError> {
        self.inner.generation(id)
    }
}

fn three_small_files(published: &mut SimulatedPublishedSet, objects: &SimObjectStore) -> Vec<u128> {
    let ranges = [(1, 3), (4, 5), (6, 8)];
    ranges
        .iter()
        .map(|&(first, last)| {
            let built = build_oe_file((first..=last).map(rich_row).collect::<Vec<_>>(), &lineage_build()).unwrap();
            publish_built(&built, first, last, published, objects);
            built.file_id
        })
        .collect()
}

fn open_entry(objects: &SimObjectStore, published: &dyn PublishedSet, file_id: u128) -> OeFile {
    let (_, head) = published.head().unwrap();
    let entry = head.files.iter().find(|entry| entry.file_id == file_id).unwrap();
    let stored = objects.get(&oe_object_key(tenant(), file_id)).unwrap().unwrap();
    OeFile::open(stored.bytes, Some(&entry.file_seal)).unwrap()
}

#[test]
fn one_run_compacts_and_later_runs_sweep_the_replaced_files() {
    let objects = SimObjectStore::new();
    let mut published = SimulatedPublishedSet::new();
    let clock = SimClock::new(7);
    let inputs = three_small_files(&mut published, &objects);
    let mut ledger = SimDroppedObjectLedger::default();
    let mut maintenance = Maintenance::new(config(lineage_build()));

    let (head_id, _) = published.head().unwrap();
    let report = maintenance.run_once(&mut published, &objects, &mut ledger, head_id, &clock);
    assert!(report.is_clean(), "{:?}", report.errors);
    assert_eq!(report.compactions.len(), 1);
    let output = report.compactions[0].output;
    assert_eq!(report.compactions[0].inputs, inputs);
    let rows = crate::writer::merge::read_event_rows(&open_entry(&objects, &published, output)).unwrap();
    assert_eq!(rows, (1..=8).map(rich_row).collect::<Vec<_>>());

    let mut deleted = Vec::new();
    for _ in 0..4 {
        clock.advance(3_000);
        let (head_id, _) = published.head().unwrap();
        let report = maintenance.run_once(&mut published, &objects, &mut ledger, head_id, &clock);
        assert!(report.is_clean(), "{:?}", report.errors);
        assert!(report.compactions.is_empty());
        deleted.extend(report.retired_files.unwrap().deleted);
    }
    deleted.sort_unstable();
    let mut expected = inputs.clone();
    expected.sort_unstable();
    assert_eq!(deleted, expected);
    for input in &inputs {
        assert!(objects.object(&oe_object_key(tenant(), *input)).is_none());
    }
    let (_, head) = published.head().unwrap();
    assert_eq!(head.files.len(), 1);
    assert_eq!(head.files[0].file_id, output);
}

#[test]
fn a_lost_publication_race_is_retried_from_the_new_head() {
    let objects = SimObjectStore::new();
    let mut inner = SimulatedPublishedSet::new();
    let inputs = three_small_files(&mut inner, &objects);
    let mut published = RacedSet {
        fail: crate::writer::publish::MAX_REBASE_ATTEMPTS + 1,
        inner,
    };
    let clock = SimClock::new(1);
    let mut maintenance = Maintenance::new(config(lineage_build()));
    let (head_id, _) = published.head().unwrap();
    let report = maintenance.run_once(
        &mut published,
        &objects,
        &mut SimDroppedObjectLedger::default(),
        head_id,
        &clock,
    );
    assert!(report.is_clean(), "{:?}", report.errors);
    assert_eq!(report.cas_retries, 1);
    assert_eq!(report.compactions.len(), 1);
    assert_eq!(report.compactions[0].inputs, inputs);

    let objects = SimObjectStore::new();
    let mut inner = SimulatedPublishedSet::new();
    three_small_files(&mut inner, &objects);
    let mut published = RacedSet { fail: u32::MAX, inner };
    let (head_id, _) = published.head().unwrap();
    let report = maintenance.run_once(
        &mut published,
        &objects,
        &mut SimDroppedObjectLedger::default(),
        head_id,
        &clock,
    );
    assert_eq!(report.cas_retries, 3);
    assert!(report.compactions.is_empty());
    assert!(matches!(
        report.errors.as_slice(),
        [MaintenanceError::Compaction {
            failure: PublishFailure::Publish(PublishError::CasLost { .. }),
            ..
        }]
    ));
}

fn graph_built(first: u64, end: u64) -> (crate::writer::build::BuiltOe, Vec<BuildRow>) {
    let mut rows = OeRow::into_build_rows(rows_of(first + 1..=end));
    let mut graph = GraphInput {
        first_sn: first,
        subject: b"!room:example.org".to_vec(),
        ..GraphInput::default()
    };
    for (sn, row) in (first..).zip(&mut rows) {
        row.external_id = Some(format!("${sn}:example.org").into_bytes());
        let mut refs = Vec::new();
        let mut edges = RowEdges::default();
        if sn > 0 {
            refs.push(RelationshipRef::to_subject_sequence(RelationshipKind::Auth, 0));
            refs.push(RelationshipRef::to_subject_sequence(RelationshipKind::Prev, sn - 1));
            edges.auth.push(0);
            edges.prev.push(sn - 1);
        }
        row.relationships = (!refs.is_empty()).then(|| EventRelationships::new(refs).unwrap());
        graph.rows.push(edges);
    }
    let config = OeBuildConfig {
        graph: Some(graph),
        profile: Profile::Graph,
        ..build_config()
    };
    (build_oe_file(rows.clone(), &config).unwrap(), rows)
}

#[test]
fn graph_files_of_one_subject_compact_with_their_graph_rebuilt() {
    let objects = SimObjectStore::new();
    let mut published = SimulatedPublishedSet::new();
    let (a, rows_a) = graph_built(0, 4);
    let (b, rows_b) = graph_built(4, 9);
    let (c, rows_c) = graph_built(9, 12);
    publish_built(&a, 1, 4, &mut published, &objects);
    publish_built(&b, 5, 9, &mut published, &objects);
    publish_built(&c, 10, 12, &mut published, &objects);
    let clock = SimClock::new(3);
    let mut maintenance = Maintenance::new(config(build_config()));
    let (head_id, _) = published.head().unwrap();
    let report = maintenance.run_once(
        &mut published,
        &objects,
        &mut SimDroppedObjectLedger::default(),
        head_id,
        &clock,
    );
    assert!(report.is_clean(), "{:?}", report.errors);
    assert_eq!(report.compactions.len(), 1);
    assert_eq!(report.compactions[0].inputs, vec![a.file_id, b.file_id, c.file_id]);

    let merged = open_entry(&objects, &published, report.compactions[0].output);
    assert_eq!(merged.header().profile, Profile::Graph);
    let expected: Vec<BuildRow> = rows_a.into_iter().chain(rows_b).chain(rows_c).collect();
    assert_eq!(read_build_rows(&merged).unwrap(), expected);
    for sn in 0..12u64 {
        assert_eq!(
            merged.graph_sn_of(format!("${sn}:example.org").as_bytes()).unwrap(),
            Some(sn)
        );
        let edges = merged.granule_edges(merged.graph_granule_of(sn).unwrap()).unwrap();
        let prev: Vec<u64> = sn.checked_sub(1).into_iter().collect();
        assert_eq!(edges.parents(sn, EdgeKind::Prev), prev.as_slice(), "prev of {sn}");
        let children: Vec<u64> = (sn + 1 < 12).then_some(sn + 1).into_iter().collect();
        assert_eq!(edges.children(sn, EdgeKind::Prev), children, "children of {sn}");
    }
    let (_, head) = published.head().unwrap();
    let summary = head
        .snapshot_files()
        .find_map(|entry| entry.graph_summary.clone())
        .unwrap();
    assert_eq!((summary.min_event_sn, summary.max_event_sn), (0, 11));
}

#[test]
fn backlog_entries_follow_their_events_into_the_compacted_file() {
    let dir = tempfile::tempdir().unwrap();
    let backlog = FileErasureBacklog::open(dir.path().join("backlog.log"));
    let objects = SimObjectStore::new();
    let mut published = SimulatedPublishedSet::new();
    let first = build_oe_file(rows_of(1..=2), &build_config()).unwrap();
    let second = build_oe_file(rows_of(3..=5), &build_config()).unwrap();
    publish_built(&first, 1, 2, &mut published, &objects);
    publish_built(&second, 3, 5, &mut published, &objects);
    let events: Vec<_> = rows_of(1..=5)
        .into_iter()
        .map(|row| row.event.envelope.event_id)
        .collect();
    for (index, event) in events.iter().enumerate() {
        let file = if index < 2 { first.file_id } else { second.file_id };
        backlog.record_unresolved(file, *event).unwrap();
    }
    let subject = <crate::deletes::SubjectId as crate::typed_id::TypedIdTestExt>::new_test_id(9);
    backlog.resolve(events[3], subject).unwrap();

    let mut maintenance = Maintenance::new(MaintenanceConfig {
        compaction: CompactionPolicy {
            min_output_over_largest: 1,
            ..CompactionPolicy::default()
        },
        ..config(build_config())
    });
    maintenance.backlog = Some(backlog.clone());
    let (head_id, _) = published.head().unwrap();
    let report = maintenance.run_once(
        &mut published,
        &objects,
        &mut SimDroppedObjectLedger::default(),
        head_id,
        &SimClock::new(5),
    );
    assert!(report.is_clean(), "{:?}", report.errors);
    let output = report.compactions[0].output;
    let pending = backlog.pending().unwrap();
    assert_eq!(pending.len(), 5);
    for entry in &pending {
        assert_eq!(entry.file_id, output, "{:?}", entry.event_id);
    }
    assert_eq!(
        pending
            .iter()
            .find(|entry| entry.event_id == events[3])
            .unwrap()
            .subject,
        Some(subject)
    );
}

#[test]
fn due_runs_on_a_jittered_interval() {
    let clock = SimClock::new(11);
    let mut maintenance = Maintenance::new(config(build_config()));
    assert!(maintenance.due(&clock), "the first run is due at once");
    assert!(!maintenance.due(&clock));
    clock.advance(749_999);
    assert!(
        !maintenance.due(&clock),
        "never due before the interval less its whole jitter"
    );
    clock.advance(250_001);
    assert!(maintenance.due(&clock), "always due once the full interval has passed");
    assert!(!maintenance.due(&clock));
}
