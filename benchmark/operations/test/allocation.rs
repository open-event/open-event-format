use super::*;
use oe::artifacts::batch::{EventInput, PayloadInput};
use oe::columns::{FreetextDeclaration, PromotedColumn, PromotionPlan};
use oe::events::variant::VariantValue;
use oe::events::{EventEnvelope, EventFlags, EventId, StreamId, TenantId, TimestampValue};
use oe::invariants::io::ThreadPoolEncodeExecutor;
use oe::layout::footer::ColumnKind;
use oe::layout::{LayoutTargets, Profile};
use oe::security::FooterEncryption;
use oe::typed_id::TypedIdTestExt;
use oe::writer::build::{BuildLifecycle, OeBuildConfig, OeRow, build_oe_file_with_executor};
use std::collections::BTreeMap;
use std::time::Instant;

/// Rows per granule, the same cut the committed default benchmark spec uses.
const ROWS_PER_GRANULE: usize = 8_192;

/// The payload shapes the write path is measured over. Together they cover the six shapes IMP-014 calls out rather
/// than letting an optimization tune only for the default fixture.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    HighCardinality,
    LargeStrings,
    NarrowHomogeneous,
    Nested,
    Sparse,
    Wide,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::HighCardinality => "high-cardinality-schema",
            Shape::LargeStrings => "large-strings",
            Shape::NarrowHomogeneous => "narrow-homogeneous",
            Shape::Nested => "nested-payload",
            Shape::Sparse => "sparse",
            Shape::Wide => "wide-object",
        }
    }

    /// How many rows this shape is measured over. The text-heavy shape carries roughly fifty times the payload bytes
    /// per row, so it takes fewer rows to reach a comparable file.
    fn row_count(self) -> u64 {
        match self {
            Shape::HighCardinality | Shape::LargeStrings | Shape::Wide => 20_000,
            Shape::NarrowHomogeneous | Shape::Nested | Shape::Sparse => 100_000,
        }
    }
}

/// SplitMix64's finalizer, as in [`crate::dataset`]: a fixed, dependency-free bit mixer, so every shape is the same
/// on every host and in every process.
fn mix(value: u64) -> u64 {
    let mut z = value.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn payload(shape: Shape, index: u64) -> VariantValue {
    let mut fields: BTreeMap<String, VariantValue> = BTreeMap::new();
    fields.insert(
        "char_count".to_owned(),
        VariantValue::Int((mix(index) % 1_000_000) as i64),
    );
    fields.insert("kind".to_owned(), VariantValue::String(format!("k{}", index % 4)));
    match shape {
        Shape::NarrowHomogeneous => {
            fields.insert("note".to_owned(), VariantValue::String(format!("row {index} settled")));
        }
        Shape::Nested => {
            fields.insert(
                "note".to_owned(),
                VariantValue::String(format!("message {index} says hello")),
            );
            fields.insert(
                "details".to_owned(),
                VariantValue::Object(BTreeMap::from([
                    (
                        "actor".to_owned(),
                        VariantValue::String(format!("actor-{}", mix(index ^ 3) % 512)),
                    ),
                    ("retries".to_owned(), VariantValue::Int((index % 4) as i64)),
                    (
                        "tags".to_owned(),
                        VariantValue::Array(
                            (0..3)
                                .map(|tag| VariantValue::String(format!("tag-{}", mix(index ^ tag) % 64)))
                                .collect(),
                        ),
                    ),
                ])),
            );
        }
        Shape::Sparse => {
            fields.insert("note".to_owned(), VariantValue::String(format!("row {index} settled")));
            // One of thirty-two optional attributes per row, so every one of them is absent from all but a
            // thirty-second of the rows — the shape that fills a granule with mostly-empty typed columns.
            let attribute = mix(index ^ 11) % 32;
            fields.insert(
                format!("attribute_{attribute:02}"),
                VariantValue::Int((mix(index) % 1_000) as i64),
            );
        }
        Shape::LargeStrings => {
            fields.insert(
                "note".to_owned(),
                VariantValue::String(format!("row {index} ").repeat(64)),
            );
            fields.insert(
                "client".to_owned(),
                VariantValue::String(format!("client {} ", mix(index ^ 7) % 8).repeat(32)),
            );
        }
        Shape::Wide => {
            fields.insert("note".to_owned(), VariantValue::String(format!("row {index} settled")));
            for attribute in 0..64 {
                fields.insert(
                    format!("attribute_{attribute:02}"),
                    VariantValue::Int((mix(index ^ attribute) % 1_000) as i64),
                );
            }
        }
        Shape::HighCardinality => {
            fields.insert("note".to_owned(), VariantValue::String(format!("row {index} settled")));
            // Across the fixture this creates 2,048 distinct paths, while each individual row remains small.
            let attribute = mix(index ^ 17) % 2_048;
            fields.insert(
                format!("schema_field_{attribute:04}"),
                VariantValue::String(format!("value-{}", mix(index ^ 19))),
            );
        }
    }
    VariantValue::Object(fields)
}

fn rows(shape: Shape) -> Vec<OeRow> {
    let tenant_id = TenantId::new_test_id(7);
    (0..shape.row_count())
        .map(|index| OeRow {
            epoch: 1,
            event: EventInput {
                connector_delivery_hash_high: 0,
                connector_delivery_hash_low: mix(index),
                envelope: EventEnvelope {
                    account_id: None,
                    account_id_hash_low: 0,
                    actor_id: (!index.is_multiple_of(3)).then(|| format!("actor-{}", mix(index) % 512)),
                    actor_id_hash_low: mix(index ^ 1),
                    dedupe_hash_high: 0,
                    dedupe_hash_low: mix(index ^ 2),
                    entity_id: Some(format!("ent-{:016x}", mix(index ^ 5))),
                    entity_id_hash_high: 0,
                    entity_id_hash_low: mix(index ^ 5),
                    entity_type: "room".to_owned(),
                    event_id: EventId::new_test_id(u128::from(index) + 1),
                    event_type: ["message.sent", "message.edited"][index as usize % 2].to_owned(),
                    flags: EventFlags(0),
                    ingested_at: TimestampValue::from_physical_nanos(2_000_000 + index as i64 * 1_000),
                    occurred_at: TimestampValue::from_physical_nanos(1_000_000 + index as i64 * 1_000),
                    schema_version: 1,
                    source: ["gateway", "bot"][index as usize % 2].to_owned(),
                    stream_id: StreamId(2),
                    stream_sequence: index,
                    tenant_id,
                    trace_id_hash_low: 0,
                },
                payload: PayloadInput::Variant(payload(shape, index)),
                provenance: None,
                relationships: None,
                source_delivery: None,
                source_schema: None,
            },
            sequence: index + 1,
        })
        .collect()
}

fn config() -> OeBuildConfig {
    OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: 1_000_000,
        delivery_lineage: false,
        footer_dek: None,
        footer_encryption: FooterEncryption::Plaintext,
        freetext: FreetextDeclaration {
            fields: vec!["note".to_owned()],
        },
        freetext_row_offset_index: false,
        generation_id: 1,
        graph: None,
        io_alignment_bytes: 0,
        lifecycle: BuildLifecycle::FreshPublication,
        page_size_rows: 0,
        profile: Profile::Event,
        promotion: PromotionPlan {
            columns: vec![PromotedColumn {
                kind: ColumnKind::String,
                name: "kind".to_owned(),
                path: "kind".to_owned(),
                since_schema_version: 1,
                substring_searchable: false,
            }],
        },
        targets: LayoutTargets {
            index_granularity: ROWS_PER_GRANULE,
            // Effectively off, as in the committed default spec: the row target alone decides where a granule is cut.
            index_granularity_bytes: usize::MAX,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
            stripe_target_bytes: 4 * 1024 * 1024,
        },
        tenant_id: TenantId::new_test_id(7),
        time_series_bucket_nanos: None,
    }
}

/// Reports what one OE create/write asks the allocator for, over each payload shape.
///
/// Run it with the counting allocator installed, in release, or the totals are all zero and the timing is a debug
/// build's:
///
/// ```text
/// cargo test --release --features alloc-counting -- --ignored --nocapture writer_allocation
/// ```
#[test]
#[ignore = "release-mode IMP-014 writer allocation benchmark"]
fn benchmark_writer_allocations_by_payload_shape() {
    assert!(
        counting_enabled(),
        "run with --features alloc-counting; without it every total is zero"
    );
    let executor = ThreadPoolEncodeExecutor;
    let config = config();
    for shape in [
        Shape::NarrowHomogeneous,
        Shape::Wide,
        Shape::Nested,
        Shape::LargeStrings,
        Shape::Sparse,
        Shape::HighCardinality,
    ] {
        // Built outside the measured window: the totals are the writer's, not the generator's.
        let built_rows = rows(shape);
        let started = Instant::now();
        let (built, totals) = measure(|| build_oe_file_with_executor(built_rows, &config, &executor).unwrap());
        let elapsed = started.elapsed();
        eprintln!(
            "IMP-014 {shape}: rows={rows} allocations={allocations} reallocations={reallocations} \
             requested={requested_mib:.2}MiB peak_live={peak_mib:.2}MiB largest={largest_kib:.1}KiB \
             file={file_mib:.2}MiB elapsed={elapsed_ms:.1}ms",
            shape = shape.name(),
            rows = shape.row_count(),
            allocations = totals.allocations,
            reallocations = totals.reallocations,
            requested_mib = totals.bytes as f64 / (1024.0 * 1024.0),
            peak_mib = totals.peak_live_bytes as f64 / (1024.0 * 1024.0),
            largest_kib = totals.largest_bytes as f64 / 1024.0,
            file_mib = built.bytes.len() as f64 / (1024.0 * 1024.0),
            elapsed_ms = elapsed.as_secs_f64() * 1_000.0,
        );
    }
}

#[test]
fn allocation_benchmark_covers_every_required_payload_shape() {
    let names = [
        Shape::NarrowHomogeneous,
        Shape::Wide,
        Shape::Nested,
        Shape::LargeStrings,
        Shape::Sparse,
        Shape::HighCardinality,
    ]
    .map(Shape::name);
    assert_eq!(
        names,
        [
            "narrow-homogeneous",
            "wide-object",
            "nested-payload",
            "large-strings",
            "sparse",
            "high-cardinality-schema",
        ]
    );
}

/// The totals must report what the allocator was actually asked for, or every number the benchmark above prints is
/// meaningless. A vector reserved once and grown past that reservation is a case with a known answer: one allocation
/// for the reservation, and at least one reallocation for the growth past it.
#[test]
#[cfg(feature = "alloc-counting")]
fn measure_counts_a_reservation_and_its_growth_separately() {
    let (grown, totals) = measure(|| {
        let mut values: Vec<u64> = Vec::with_capacity(16);
        values.extend(0..1_024);
        values
    });

    assert_eq!(grown.len(), 1_024);
    assert!(totals.allocations >= 1, "{totals:?}");
    assert!(totals.reallocations >= 1, "{totals:?}");
    assert!(totals.bytes >= 1_024 * 8, "{totals:?}");
    // The test harness itself allocates on other threads, so the process-wide live-byte delta can be offset by one of
    // those threads freeing pre-window memory. A dedicated benchmark process has no such unrelated work.
    assert!(totals.peak_live_bytes > 0, "{totals:?}");
    assert!(totals.largest_bytes >= 1_024 * 8, "{totals:?}");
}

/// Counters are process-wide so allocations from writer workers must all land in the same operation sample.
#[test]
#[cfg(feature = "alloc-counting")]
fn measure_counts_allocations_from_multiple_threads() {
    const THREADS: usize = 4;
    const BYTES_PER_THREAD: usize = 256 * 1024;
    let (bytes, totals) = measure(|| {
        let ready = std::sync::Arc::new(std::sync::Barrier::new(THREADS));
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let ready = std::sync::Arc::clone(&ready);
                std::thread::spawn(move || {
                    let bytes = std::hint::black_box(vec![0u8; BYTES_PER_THREAD]);
                    ready.wait();
                    bytes
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap().len())
            .sum::<usize>()
    });

    assert_eq!(bytes, THREADS * BYTES_PER_THREAD);
    assert!(totals.allocations >= THREADS as u64, "{totals:?}");
    assert!(totals.bytes >= (THREADS * BYTES_PER_THREAD) as u64, "{totals:?}");
    assert!(totals.largest_bytes >= BYTES_PER_THREAD as u64, "{totals:?}");
    assert!(totals.peak_live_bytes > 0, "{totals:?}");
}

#[test]
#[cfg(unix)]
fn resident_high_water_mark_is_available_on_unix() {
    assert!(resident_high_water_bytes().is_some());
}

/// Without the feature the counters are never touched, and a caller that printed the zeros as measurements would be
/// reporting a fiction. `counting_enabled` is what tells the two builds apart.
#[test]
fn totals_are_zero_and_declared_unavailable_when_counting_is_off() {
    let (produced, totals) = measure(|| vec![0u8; 4_096]);

    assert_eq!(produced.len(), 4_096);
    if counting_enabled() {
        assert!(totals.allocations >= 1, "{totals:?}");
    } else {
        assert_eq!(totals, AllocationTotals::default());
    }
}
