use super::*;
use crate::artifacts::batch::{EventInput, PayloadInput};
use crate::artifacts::state_chunk::build_state_tree;
use crate::columns::{FreetextDeclaration, PromotedColumn, PromotionPlan, column_ids};
use crate::compat::tests::{PRE_GRAPH_KNOWN as PRE_GRAPH_BITS, PRE_TIME_SERIES_REQUIRED_KNOWN as PRE_TIME_SERIES_BITS};

const PRE_GRAPH_KNOWN: u64 = PRE_GRAPH_BITS | required_features::OCCURRED_RELATIVE_INGESTED_AT;
const PRE_TIME_SERIES_REQUIRED_KNOWN: u64 = PRE_TIME_SERIES_BITS | required_features::OCCURRED_RELATIVE_INGESTED_AT;
use crate::events::variant::VariantValue;
use crate::events::{
    EventEnvelope, EventFlags, EventId, RelationshipKind, RelationshipRef, StreamId, TenantId, TimestampValue,
};
use crate::layout::footer::ColumnKind;
use crate::layout::graph::{ChainCoverInput, ChainLabel, DepthRange, EdgeKind, GraphInput, RowEdges, UnresolvedEdge};
use crate::layout::{LayoutTargets, Profile, required_features};
use crate::security::{StateChunkKey, TenantDek};
use crate::typed_id::TypedIdTestExt;
use crate::writer::build::{BuildLifecycle, BuiltOe, OeBuildConfig, OeRow, build_oe_file};
use crate::writer::source_form::tests::{plain_row, source_form_config};
use std::collections::BTreeMap;

fn row(i: u64) -> OeRow {
    let mut payload = std::collections::BTreeMap::new();
    payload.insert("amount".to_owned(), VariantValue::Int(1_000 + i as i64));
    OeRow {
        epoch: 1,
        sequence: i + 1,
        event: EventInput {
            connector_delivery_hash_high: 0,
            connector_delivery_hash_low: i,
            envelope: EventEnvelope {
                account_id: None,
                account_id_hash_low: 4,
                actor_id: None,
                actor_id_hash_low: 3,
                dedupe_hash_high: 6,
                dedupe_hash_low: 100 + i,
                entity_id: Some(format!("opp-{i}")),
                entity_id_hash_high: 1,
                entity_id_hash_low: i,
                entity_type: "opportunity".to_owned(),
                event_id: EventId::new_test_id(0xF1F0_0000 + u128::from(i)),
                event_type: "deal.updated".to_owned(),
                flags: EventFlags(0),
                ingested_at: TimestampValue::from_physical_nanos(2_000_000 + i as i64 * 1000),
                occurred_at: TimestampValue::from_physical_nanos(1_000_000 + i as i64 * 1000),
                schema_version: 2,
                source: "crm".to_owned(),
                stream_id: StreamId(2),
                stream_sequence: i,
                tenant_id: TenantId::new_test_id(7),
                trace_id_hash_low: 5,
            },
            payload: PayloadInput::Variant(VariantValue::object(payload)),
            provenance: None,
            relationships: None,
            source_delivery: None,
            source_schema: None,
        },
    }
}

fn config() -> OeBuildConfig {
    OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: 42,
        footer_dek: None,
        footer_encryption: crate::security::FooterEncryption::Plaintext,
        freetext: FreetextDeclaration::default(),
        freetext_row_offset_index: false,
        generation_id: 1,
        graph: None,
        io_alignment_bytes: 0,
        lifecycle: BuildLifecycle::FreshPublication,
        page_size_rows: 0,
        profile: crate::layout::Profile::Event,
        promotion: PromotionPlan { columns: Vec::new() },
        targets: LayoutTargets {
            index_granularity: 8,
            index_granularity_bytes: 1 << 20,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
            stripe_target_bytes: 1 << 20,
        },
        tenant_id: TenantId::new_test_id(7),
        time_series_bucket_nanos: None,
        delivery_lineage: false,
    }
}

fn built() -> BuiltOe {
    build_oe_file((0..8).map(row).collect(), &config()).unwrap()
}

fn graph_file(everything: bool) -> BuiltOe {
    let key = StateChunkKey::derive(&TenantDek::new([4; 32]));
    let root = build_state_tree(&key, &BTreeMap::new()).unwrap().root;
    let mut config = source_form_config(8);
    config.profile = Profile::Graph;
    config.graph = Some(GraphInput {
        chain_cover: everything.then(|| ChainCoverInput {
            labels: (0..32).map(|seq| ChainLabel { chain_id: 1, seq }).collect(),
            links: Vec::new(),
        }),
        depth: everything.then_some(DepthRange { max: 33, min: 2 }),
        first_sn: 0,
        inbound: Vec::new(),
        rows: (0..32u64)
            .map(|sn| RowEdges {
                auth: (0..sn.min(1)).collect(),
                prev: sn.checked_sub(1).into_iter().collect(),
            })
            .collect(),
        state_roots: if everything {
            BTreeMap::from([(0, root)])
        } else {
            BTreeMap::new()
        },
        subject: b"!fixture:example.org".to_vec(),
        unresolved: if everything {
            vec![UnresolvedEdge {
                child: 3,
                parent: RelationshipRef::to_external(RelationshipKind::Prev, b"$late:example.org").unwrap(),
            }]
        } else {
            Vec::new()
        },
    });
    let rows: Vec<_> = (0..32u64)
        .map(|i| {
            let mut row = plain_row(i);
            if everything {
                row.external_id = Some(format!("$fixture{i}:example.org").into_bytes());
            }
            row
        })
        .collect();
    build_oe_file(rows, &config).unwrap()
}

fn time_series_file(reading: impl Fn(u64) -> f64) -> BuiltOe {
    let mut config = config();
    config.profile = Profile::TimeSeries;
    config.promotion = PromotionPlan {
        columns: vec![PromotedColumn {
            kind: ColumnKind::F64,
            name: "used".to_owned(),
            path: "used".to_owned(),
            since_schema_version: 1,
            substring_searchable: false,
        }],
    };
    let rows = (0..2_048)
        .map(|i| {
            let mut row = row(i);
            let payload = BTreeMap::from([("used".to_owned(), VariantValue::Double(reading(i)))]);
            row.event.payload = PayloadInput::Variant(VariantValue::object(payload));
            row
        })
        .collect();
    build_oe_file(rows, &config).unwrap()
}

fn memory_reading(i: u64) -> f64 {
    (6_000_000_000 + (i / 2) * 40_960) as f64 / 16_000_000_000.0
}

fn decimal_reading(i: u64) -> f64 {
    (2_000 + (i * 7) % 50) as f64 / 100.0
}

fn open_as_reader_knowing(bytes: &[u8], known: u64) -> Result<OeFile, FormatError> {
    let file = OeFile::open(bytes.to_vec(), None)?;
    let unknown = file.footer().required_feature_flags & !known;
    if unknown != 0 {
        return Err(FormatError::UnknownRequiredFeature { bits: unknown });
    }
    Ok(file)
}

#[test]
fn graph_and_xor_float_fixtures_open_here_and_are_refused_by_older_readers() {
    let dir = tempfile::tempdir().unwrap();
    let graph = graph_file(true);
    let time_series = time_series_file(memory_reading);
    generate_fixture(dir.path(), "graph", &graph).unwrap();
    generate_fixture(dir.path(), "time-series-xor-floats", &time_series).unwrap();
    let sweep = read_fixture_corpus(dir.path()).unwrap();
    assert!(sweep.failures.is_empty(), "{:?}", sweep.failures);
    assert_eq!(sweep.opened, ["oe-v1.0-graph.oe", "oe-v1.0-time-series-xor-floats.oe"]);

    assert_eq!(
        open_as_reader_knowing(&graph.bytes, PRE_GRAPH_KNOWN | required_features::EXTERNAL_ID_INDEX_32).unwrap_err(),
        FormatError::UnknownRequiredFeature {
            bits: required_features::GRAPH
        }
    );
    assert!(open_as_reader_knowing(&graph.bytes, required_features::KNOWN).is_ok());
    assert_eq!(
        open_as_reader_knowing(&time_series.bytes, PRE_TIME_SERIES_REQUIRED_KNOWN).unwrap_err(),
        FormatError::UnknownRequiredFeature {
            bits: required_features::XOR_FLOAT_BLOCKS
        }
    );
    assert!(open_as_reader_knowing(&time_series.bytes, required_features::KNOWN).is_ok());
}

#[test]
fn files_with_earlier_feature_sets_still_open_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let event = built();
    let edges_only = graph_file(false);
    let decimal = time_series_file(decimal_reading);
    for (label, built, known) in [
        ("event", &event, PRE_GRAPH_KNOWN),
        ("graph-edges-only", &edges_only, PRE_TIME_SERIES_REQUIRED_KNOWN),
        ("time-series-decimal", &decimal, PRE_TIME_SERIES_REQUIRED_KNOWN),
    ] {
        assert!(open_as_reader_knowing(&built.bytes, known).is_ok(), "{label}");
        generate_fixture(dir.path(), label, built).unwrap();
    }
    assert_eq!(
        edges_only.footer.required_feature_flags & required_features::GRAPH,
        required_features::GRAPH_DENSE_IDS
            | required_features::GRAPH_CSR_FORWARD
            | required_features::GRAPH_CSR_REVERSE
    );
    let sweep = read_fixture_corpus(dir.path()).unwrap();
    assert!(sweep.failures.is_empty(), "{:?}", sweep.failures);
    assert_eq!(sweep.opened.len(), 3);

    let file = OeFile::open(edges_only.bytes.clone(), Some(&edges_only.file_seal)).unwrap();
    assert_eq!(file.granule_edges(1).unwrap().parents(9, EdgeKind::Prev), [8]);
    let file = OeFile::open(decimal.bytes.clone(), Some(&decimal.file_seal)).unwrap();
    let aggregate = file
        .time_series_aggregate(column_ids::PROMOTED_BASE, 0, 60_000_000_000, 60_000_000_000)
        .unwrap();
    assert_eq!(aggregate.count, 2_048);
}

#[test]
fn the_committed_corpus_and_an_absent_directory_both_sweep_clean() {
    let committed = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_CORPUS_DIR);
    let sweep = read_fixture_corpus(&committed).unwrap();
    assert!(
        sweep.failures.is_empty(),
        "a committed fixture no longer opens: {:?}",
        sweep.failures
    );

    let absent = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_data/no-such-corpus");
    let sweep = read_fixture_corpus(&absent).unwrap();
    assert!(sweep.opened.is_empty() && sweep.failures.is_empty());
}

#[test]
fn generated_fixtures_are_swept_and_a_broken_one_is_named() {
    let dir = tempfile::tempdir().unwrap();
    let built = built();
    let path = generate_fixture(dir.path(), "minimal", &built).unwrap();
    assert_eq!(
        path.file_name().unwrap().to_string_lossy(),
        "oe-v1.0-minimal.oe",
        "fixture names carry the frozen writer version"
    );
    let sweep = read_fixture_corpus(dir.path()).unwrap();
    assert_eq!(sweep.opened, vec!["oe-v1.0-minimal.oe".to_owned()]);
    assert!(sweep.failures.is_empty());

    let mut corrupted = built.bytes.clone();
    let middle = corrupted.len() / 2;
    corrupted[middle] ^= 0xFF;
    std::fs::write(dir.path().join("oe-v1.0-corrupted.oe"), &corrupted).unwrap();
    let sweep = read_fixture_corpus(dir.path()).unwrap();
    assert_eq!(sweep.opened, vec!["oe-v1.0-minimal.oe".to_owned()]);
    assert_eq!(sweep.failures.len(), 1);
    assert_eq!(sweep.failures[0].0, "oe-v1.0-corrupted.oe");
}
