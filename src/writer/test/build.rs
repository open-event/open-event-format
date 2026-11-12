use super::*;
use crate::columns::PromotedColumn;
use crate::encoding::{Compression, Transform, encode_block};
use crate::events::provenance::{SignatureScheme, SignedEventProvenance, hex_bytes};
use crate::events::sim::{SimulatedEventAuthor, signed_event_payload};
use crate::events::variant::VariantValue;
use crate::events::{EventEnvelope, EventFlags, EventId, StreamId, TimestampValue};
use crate::invariants::io::ThreadPoolEncodeExecutor;
use crate::layout::graph::RowEdges;
use crate::layout::reader::{OeFile, OeFooter, PayloadRead, SpeculativeTail, tail_range};
use crate::typed_id::TypedIdTestExt;

fn fields_of(fields: &BTreeMap<String, VariantValue>) -> Vec<(FieldName, VariantValue)> {
    fields
        .iter()
        .map(|(name, value)| (FieldName::from(name.as_str()), value.clone()))
        .collect()
}

fn row(i: u64) -> OeRow {
    let mut payload = std::collections::BTreeMap::new();
    payload.insert("kind".to_owned(), VariantValue::String(format!("k{}", i % 4)));
    payload.insert("amount".to_owned(), VariantValue::Int(1_000 + i as i64));
    payload.insert(
        "note".to_owned(),
        VariantValue::String(format!("free text body number {i}")),
    );
    if i.is_multiple_of(3) {
        payload.insert("rare".to_owned(), VariantValue::Bool(true));
    }
    OeRow {
        epoch: 1,
        sequence: i + 1,
        event: EventInput {
            envelope: EventEnvelope {
                event_id: EventId::new_test_id(0xBEEF_0000 + u128::from(i)),
                tenant_id: TenantId::new_test_id(7),
                stream_id: StreamId(2),
                stream_sequence: i,
                occurred_at: TimestampValue::from_physical_nanos(1_000_000 + i as i64 * 1000),
                ingested_at: TimestampValue::from_physical_nanos(2_000_000 + i as i64 * 1000),
                source: ["crm", "billing"][i as usize % 2].to_owned(),
                event_type: "deal.updated".to_owned(),
                entity_type: "opportunity".to_owned(),
                entity_id_hash_low: i,
                entity_id_hash_high: 1,
                entity_id: Some(format!("opp-{i}")),
                actor_id_hash_low: 3,
                actor_id: i.is_multiple_of(2).then(|| "actor-1".to_owned()),
                account_id_hash_low: 4,
                account_id: None,
                trace_id_hash_low: 5,
                dedupe_hash_low: 100 + i,
                dedupe_hash_high: 6,
                schema_version: 2,
                flags: EventFlags(0),
            },
            payload: PayloadInput::Variant(VariantValue::Object(payload)),
            source_schema: None,
            source_delivery: None,
            connector_delivery_hash_low: i,
            connector_delivery_hash_high: 0,
            provenance: None,
            relationships: None,
        },
    }
}

#[test]
fn normalized_field_identities_reuse_shapes_without_interning_unused_nested_keys() {
    let nested = VariantValue::Object(BTreeMap::from([("nested-only".to_owned(), VariantValue::Int(1))]));
    let first = BTreeMap::from([
        ("amount".to_owned(), VariantValue::Int(1)),
        ("details".to_owned(), nested.clone()),
    ]);
    let same_shape = BTreeMap::from([
        ("amount".to_owned(), VariantValue::Int(2)),
        ("details".to_owned(), nested),
    ]);
    let evolved = BTreeMap::from([
        ("amount".to_owned(), VariantValue::Int(3)),
        ("region".to_owned(), VariantValue::String("au".to_owned())),
    ]);

    let mut identities = FieldIdentityCatalog::default();
    let first_shape = identities.intern_object(&fields_of(&first)).unwrap();
    let repeated_shape = identities.intern_object(&fields_of(&same_shape)).unwrap();
    let evolved_shape = identities.intern_object(&fields_of(&evolved)).unwrap();

    assert_eq!(repeated_shape, first_shape);
    assert_ne!(evolved_shape, first_shape);
    assert_eq!(identities.shapes.len(), 2);
    assert_eq!(identities.field_id("amount"), Some(0));
    assert!(identities.field_id("details").is_some());
    assert!(identities.field_id("region").is_some());
    assert_eq!(identities.field_id("nested-only"), None);
}

#[test]
fn alternating_payload_shapes_keep_their_own_field_ids() {
    let wide = BTreeMap::from([
        ("amount".to_owned(), VariantValue::Int(1)),
        ("kind".to_owned(), VariantValue::String("a".to_owned())),
        ("region".to_owned(), VariantValue::String("au".to_owned())),
    ]);
    let narrow = BTreeMap::from([("amount".to_owned(), VariantValue::Int(2))]);

    let mut identities = FieldIdentityCatalog::default();
    let mut seen = Vec::new();
    for round in 0..4 {
        let fields = if round % 2 == 0 { &wide } else { &narrow };
        let shape_id = identities.intern_object(&fields_of(fields)).unwrap();
        seen.push(shape_id);
    }

    assert_eq!(seen[0], seen[2]);
    assert_eq!(seen[1], seen[3]);
    assert_ne!(seen[0], seen[1]);
    assert_eq!(identities.shapes.len(), 2);
    let named = |shape_id: u32| -> Vec<&str> {
        identities
            .shape(shape_id)
            .iter()
            .map(|&field_id| &*identities.field_names()[field_id as usize])
            .collect()
    };
    assert_eq!(named(seen[0]), ["amount", "kind", "region"]);
    assert_eq!(named(seen[1]), ["amount"]);
}

#[test]
fn recent_shapes_are_kept_most_recently_used_first() {
    let shapes: Vec<BTreeMap<String, VariantValue>> = (0..5)
        .map(|width| {
            (0..=width)
                .map(|field| (format!("f{field}"), VariantValue::Int(field as i64)))
                .collect()
        })
        .collect();
    let mut identities = FieldIdentityCatalog::default();
    let ids: Vec<u32> = shapes[..4]
        .iter()
        .map(|fields| identities.intern_object(&fields_of(fields)).unwrap())
        .collect();
    assert_eq!(ids, [0, 1, 2, 3]);
    assert_eq!(identities.recent_shapes, [3, 2, 1, 0]);

    for (round, fields) in shapes[..4].iter().enumerate().cycle().take(12) {
        assert_eq!(identities.intern_object(&fields_of(fields)).unwrap(), round as u32);
    }
    assert_eq!(identities.shapes.len(), 4);
    assert_eq!(identities.recent_shapes, [3, 2, 1, 0]);

    assert_eq!(identities.intern_object(&fields_of(&shapes[4])).unwrap(), 4);
    assert_eq!(identities.recent_shapes, [4, 3, 2, 1]);
    assert_eq!(identities.intern_object(&fields_of(&shapes[0])).unwrap(), 0);
    assert_eq!(identities.recent_shapes, [0, 4, 3, 2]);
    assert_eq!(identities.shapes.len(), 5);
}

#[test]
fn shape_routes_sit_at_each_shapes_own_field_positions() {
    let narrow = BTreeMap::from([
        ("amount".to_owned(), VariantValue::Int(1)),
        ("kind".to_owned(), VariantValue::String("renewal".to_owned())),
        ("note".to_owned(), VariantValue::String("body".to_owned())),
    ]);
    let wide = BTreeMap::from([
        ("amount".to_owned(), VariantValue::Int(2)),
        ("escalations".to_owned(), VariantValue::Int(3)),
        ("kind".to_owned(), VariantValue::String("expansion".to_owned())),
        ("note".to_owned(), VariantValue::String("body".to_owned())),
    ]);
    let unrouted = BTreeMap::from([("escalations".to_owned(), VariantValue::Int(4))]);

    let mut identities = FieldIdentityCatalog::default();
    let narrow_shape = identities.intern_object(&fields_of(&narrow)).unwrap();
    let wide_shape = identities.intern_object(&fields_of(&wide)).unwrap();
    let unrouted_shape = identities.intern_object(&fields_of(&unrouted)).unwrap();

    let promotion = PromotionPlan {
        columns: vec![PromotedColumn {
            kind: ColumnKind::String,
            name: "kind".to_owned(),
            path: "kind".to_owned(),
            since_schema_version: 1,
            substring_searchable: false,
        }],
    };
    let shred_plan = vec![ShredEntry {
        column_id: 100,
        path: "amount".to_owned(),
    }];
    let freetext = vec![FreetextEntry {
        column_id: 200,
        declared_field: "note".to_owned(),
    }];
    let field_routes = build_field_routes(&identities, &promotion, &shred_plan, &freetext);
    let shape_routes = build_shape_routes(&identities, &field_routes);

    let field_id = |path: &str| identities.field_id(path).expect("interned");
    assert_eq!(
        shape_routes.field_routes(narrow_shape),
        [field_id("amount"), field_id("kind"), field_id("note")],
        "amount, kind and note all route"
    );
    assert_eq!(
        shape_routes.field_routes(wide_shape),
        [
            field_id("amount"),
            FIELD_STAYS_RESIDUAL,
            field_id("kind"),
            field_id("note")
        ],
        "the field inserted in the middle stays, and shifts the routes after it"
    );
    assert_eq!(shape_routes.field_routes(unrouted_shape), [FIELD_STAYS_RESIDUAL]);
    assert!(shape_routes.field_routes(NON_OBJECT_FIELD_SHAPE).is_empty());
    assert!(field_routes.get(FIELD_STAYS_RESIDUAL as usize).is_none());
}

#[test]
fn payloads_that_change_shape_row_to_row_route_by_their_own_shape() {
    let rows: Vec<OeRow> = (0..64)
        .map(|i| {
            let mut shifting = row(i);
            if i.is_multiple_of(5)
                && let PayloadInput::Variant(VariantValue::Object(fields)) = &mut shifting.event.payload
            {
                fields.insert("middle".to_owned(), VariantValue::Int(500 + i as i64));
            }
            shifting
        })
        .collect();

    let built = build_oe_file(rows.clone(), &config()).unwrap();
    let amount = built
        .footer
        .shredded
        .iter()
        .find(|entry| entry.path == "amount")
        .expect("the frequent integer path shreds");
    let note = built.footer.freetext.first().expect("note moves by declaration");
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();

    let mut first_row = 0u64;
    for granule in &file.footer().granules {
        let ColumnData::I64(amounts) = file.read_column(amount.column_id, granule.granule_id).unwrap().data else {
            panic!("amount shreds into an integer column");
        };
        let ColumnData::Strings(kinds) = file
            .read_column(column_ids::PROMOTED_BASE, granule.granule_id)
            .unwrap()
            .data
        else {
            panic!("kind is promoted into a string column");
        };
        let ColumnData::Strings(notes) = file.read_column(note.column_id, granule.granule_id).unwrap().data else {
            panic!("note is a free-text string column");
        };
        for offset in 0..amounts.len() {
            let i = first_row + offset as u64;
            assert_eq!(amounts.get(offset).copied(), Some(1_000 + i as i64), "row {i} amount");
            assert_eq!(
                kinds.get(offset),
                Some(Some(format!("k{}", i % 4).as_str())),
                "row {i} kind"
            );
            assert_eq!(
                notes.get(offset),
                Some(Some(format!("free text body number {i}").as_str())),
                "row {i} note"
            );
        }
        first_row += amounts.len() as u64;
    }
    assert_eq!(first_row, 64, "every row reached the typed columns");

    for (ordinal, input_row) in rows.iter().enumerate() {
        let PayloadRead::Value(value) = file.payload(ordinal as u64).unwrap() else {
            panic!("payload present");
        };
        let PayloadInput::Variant(expected) = &input_row.event.payload else {
            panic!("variant input");
        };
        assert_eq!(&value, expected, "row {ordinal}");
    }
    assert_eq!(file.payload_path(5, "middle").unwrap(), Some(VariantValue::Int(505)));
    assert_eq!(file.payload_path(6, "middle").unwrap(), None);
    assert_eq!(file.payload_path(6, "rare").unwrap(), Some(VariantValue::Bool(true)));
}

#[test]
#[ignore = "release-mode IMP-003 phase benchmark"]
fn benchmark_string_paths_against_stable_field_ids() {
    let payloads: Vec<VariantValue> = (0..100_000)
        .map(|row| {
            VariantValue::Object(BTreeMap::from([
                ("amount".to_owned(), VariantValue::Int(row)),
                ("kind".to_owned(), VariantValue::String(format!("k{}", row % 4))),
                ("note".to_owned(), VariantValue::String(format!("event {row}"))),
            ]))
        })
        .collect();
    let field_ids = [0, 1, 2];
    let names = vec!["amount".to_owned(), "kind".to_owned(), "note".to_owned()];
    let normalized: Vec<Vec<VariantValue>> = payloads
        .iter()
        .map(|payload| match payload {
            VariantValue::Object(fields) => fields.values().cloned().collect(),
            other => vec![other.clone()],
        })
        .collect();
    let mut legacy_samples = Vec::new();
    let mut identified_samples = Vec::new();

    for iteration in 0..7 {
        let run_legacy = || {
            let started = Instant::now();
            let mut statistics = PathStatistics::new();
            for payload in &payloads {
                statistics.observe(Some(payload));
            }
            std::hint::black_box(statistics.shred_candidates(&[]));
            started.elapsed().as_nanos()
        };
        let run_identified = || {
            let started = Instant::now();
            let mut statistics = PathStatistics::for_field_ids(names.len());
            for values in &normalized {
                statistics.observe_field_ids(&field_ids, values);
            }
            std::hint::black_box(statistics.shred_candidates_by_id(&names, &HashSet::new()));
            started.elapsed().as_nanos()
        };
        if iteration % 2 == 0 {
            legacy_samples.push(run_legacy());
            identified_samples.push(run_identified());
        } else {
            identified_samples.push(run_identified());
            legacy_samples.push(run_legacy());
        }
    }
    legacy_samples.sort_unstable();
    identified_samples.sort_unstable();
    let legacy_median = legacy_samples.get(3).copied().unwrap_or_default();
    let identified_median = identified_samples.get(3).copied().unwrap_or_default();
    eprintln!(
        "IMP-003 path-statistics: legacy={legacy_samples:?} identified={identified_samples:?}; median legacy={}ns \
         identified={}ns reduction={:.1}%",
        legacy_median,
        identified_median,
        100.0 * (1.0 - identified_median as f64 / legacy_median as f64)
    );
}

fn config() -> OeBuildConfig {
    OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: 42,
        footer_dek: None,
        footer_encryption: crate::security::FooterEncryption::Plaintext,
        freetext: FreetextDeclaration {
            fields: vec!["note".to_owned()],
        },
        freetext_row_offset_index: false,
        generation_id: 1,
        graph: None,
        io_alignment_bytes: 0,
        lifecycle: BuildLifecycle::FreshPublication,
        page_size_rows: 0,
        profile: crate::layout::Profile::Event,
        promotion: PromotionPlan {
            columns: vec![PromotedColumn {
                name: "kind".to_owned(),
                path: "kind".to_owned(),
                kind: ColumnKind::String,
                since_schema_version: 1,
                substring_searchable: false,
            }],
        },
        targets: LayoutTargets {
            index_granularity: 16,
            index_granularity_bytes: 1 << 20,
            stripe_target_bytes: 4096,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
        },
        tenant_id: TenantId::new_test_id(7),
        time_series_bucket_nanos: None,
        delivery_lineage: false,
    }
}

#[test]
fn footer_first_open_matches_whole_file_open() {
    let rows: Vec<OeRow> = (0..64).map(row).collect();
    let built = build_oe_file(rows.clone(), &config()).unwrap();

    let range = tail_range(built.bytes.len() as u64, Some(built.footer_len), built.tree_len);
    assert!(range.exact);
    assert_eq!(range.len, built.footer_len);
    assert_eq!(range.start, built.bytes.len() as u64 - built.footer_len);

    let tail = &built.bytes[range.start as usize..];
    let footer_first = OeFooter::open(tail).unwrap();
    let whole_file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    assert_eq!(footer_first.footer(), whole_file.footer());
}

#[test]
fn granule_for_ordinal_returns_none_instead_of_panicking_on_an_overflowing_row_range() {
    let rows: Vec<OeRow> = (0..4).map(row).collect();
    let built = build_oe_file(rows, &config()).unwrap();
    let mut footer = built.footer.clone();
    footer.granules[0].first_row_ordinal = u64::MAX;

    assert_eq!(granule_for_ordinal(&footer, u64::MAX), None);
}

#[test]
fn update_reuses_unchanged_blocks_and_matches_a_full_rewrite() {
    let before: Vec<OeRow> = (0..64).map(row).collect();
    let source = build_oe_file(before.clone(), &config()).unwrap();
    let mut after = before.clone();
    let ordinals = [2usize, 19, 47];
    for ordinal in ordinals {
        let row = &mut after[ordinal];
        row.event.envelope.dedupe_hash_low += 10_000;
        let PayloadInput::Variant(VariantValue::Object(payload)) = &mut row.event.payload else {
            panic!("test rows carry object payloads");
        };
        payload.insert("amount".to_owned(), VariantValue::Int(90_000 + ordinal as i64));
        payload.insert(
            "note".to_owned(),
            VariantValue::String(format!("corrected note {ordinal}")),
        );
    }

    let full = build_oe_file(after.clone(), &config()).unwrap();
    assert_eq!(source.footer.columns, full.footer.columns);
    assert_eq!(source.footer.dictionaries, full.footer.dictionaries);
    assert_eq!(source.footer.freetext, full.footer.freetext);
    assert_eq!(source.footer.presence, full.footer.presence);
    assert_eq!(source.footer.shredded, full.footer.shredded);
    assert_eq!(source.footer.shared_dictionaries, full.footer.shared_dictionaries);
    assert!(!source.footer.marks.is_empty());
    assert!(
        source
            .footer
            .granules
            .iter()
            .zip(&full.footer.granules)
            .all(|(source, candidate)| {
                source.granule_id == candidate.granule_id
                    && source.first_row_ordinal == candidate.first_row_ordinal
                    && source.row_count == candidate.row_count
                    && source.stripe_id == candidate.stripe_id
            })
    );
    let changes: Vec<OeRowChange> = ordinals
        .into_iter()
        .map(|ordinal| OeRowChange {
            after: after[ordinal].clone(),
            before: before[ordinal].clone(),
            row_ordinal: ordinal as u64,
        })
        .collect();
    let updated =
        update_oe_file_from_changes_with_executor(&source, after, &changes, &config(), &SerialEncodeExecutor).unwrap();
    assert!(updated.reused_source_blocks > 0);
    assert!(updated.reused_source_block_bytes > 0);
    assert!(updated.encoded_blocks < full.encoded_blocks);
    assert!(updated.bytes.len() <= full.bytes.len() + full.bytes.len() / 100);

    let expected = OeFile::open(full.bytes, Some(&full.file_seal)).unwrap();
    let actual = OeFile::open(updated.bytes, Some(&updated.file_seal)).unwrap();
    assert_eq!(actual.footer().columns, expected.footer().columns);
    assert_eq!(actual.footer().granules, expected.footer().granules);
    for granule in &expected.footer().granules {
        for column in &expected.footer().columns {
            assert_eq!(
                actual.read_column(column.column_id, granule.granule_id).unwrap().data,
                expected.read_column(column.column_id, granule.granule_id).unwrap().data,
                "column {} granule {}",
                column.column_id,
                granule.granule_id
            );
        }
        for ordinal in granule.first_row_ordinal..granule.first_row_ordinal + u64::from(granule.row_count) {
            assert_eq!(
                actual.payload(ordinal).unwrap(),
                expected.payload(ordinal).unwrap(),
                "row {ordinal}"
            );
        }
    }
}

#[test]
fn incompatible_schema_and_row_geometry_fall_back_to_a_full_rewrite() {
    let before: Vec<OeRow> = (0..64).map(row).collect();
    let source = build_oe_file(before.clone(), &config()).unwrap();

    let mut changed_schema = config();
    changed_schema.promotion.columns.push(PromotedColumn {
        name: "amount_copy".to_owned(),
        path: "amount".to_owned(),
        kind: ColumnKind::I64,
        since_schema_version: 1,
        substring_searchable: false,
    });
    let full_schema = build_oe_file(before.clone(), &changed_schema).unwrap();
    let updated_schema =
        update_oe_file_with_executor(&source, &before, before.clone(), &changed_schema, &SerialEncodeExecutor).unwrap();
    assert_eq!(updated_schema.reused_source_blocks, 0);
    assert_eq!(updated_schema.bytes, full_schema.bytes);

    let after_delete = before[..63].to_vec();
    let full_delete = build_oe_file(after_delete.clone(), &config()).unwrap();
    let updated_delete =
        update_oe_file_with_executor(&source, &before, after_delete, &config(), &SerialEncodeExecutor).unwrap();
    assert_eq!(updated_delete.reused_source_blocks, 0);
    assert_eq!(updated_delete.bytes, full_delete.bytes);

    let dense_changes: Vec<OeRowChange> = (0..16)
        .map(|ordinal| OeRowChange {
            after: before[ordinal].clone(),
            before: before[ordinal].clone(),
            row_ordinal: ordinal as u64,
        })
        .collect();
    let dense = update_oe_file_from_changes_with_executor(
        &source,
        before.clone(),
        &dense_changes,
        &config(),
        &SerialEncodeExecutor,
    )
    .unwrap();
    assert_eq!(dense.reused_source_blocks, 0);
    assert_eq!(dense.bytes, source.bytes);
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

fn sparse_config() -> OeBuildConfig {
    let mut cfg = config();
    cfg.targets.stripe_target_bytes = 1 << 20;
    cfg
}

fn payload_of(row: &mut OeRow) -> &mut BTreeMap<String, VariantValue> {
    let PayloadInput::Variant(VariantValue::Object(payload)) = &mut row.event.payload else {
        panic!("test rows carry object payloads");
    };
    payload
}

fn mutate(row: &mut OeRow, rng: &mut Lcg, wide: bool, ordinal: usize) {
    match rng.below(if wide { 6 } else { 3 }) {
        0 => {
            payload_of(row).insert("amount".to_owned(), VariantValue::Int(rng.below(1_000_000) as i64));
        }
        1 => {
            let note = format!("rewritten note {ordinal} {}", rng.next());
            payload_of(row).insert("note".to_owned(), VariantValue::String(note));
        }
        2 => row.event.envelope.dedupe_hash_low = rng.next(),
        3 => {
            let kind = format!("k{}", rng.below(4));
            payload_of(row).insert("kind".to_owned(), VariantValue::String(kind));
        }
        4 => {
            let payload = payload_of(row);
            if payload.remove("rare").is_none() {
                payload.insert("rare".to_owned(), VariantValue::Bool(true));
            }
        }
        _ => {
            payload_of(row).insert("extra".to_owned(), VariantValue::Int(ordinal as i64));
        }
    }
}

fn changes_for(before: &[OeRow], after: &[OeRow], ordinals: &[usize]) -> Vec<OeRowChange> {
    ordinals
        .iter()
        .map(|&ordinal| OeRowChange {
            after: after[ordinal].clone(),
            before: before[ordinal].clone(),
            row_ordinal: ordinal as u64,
        })
        .collect()
}

fn plan_reuse<'a>(
    source: &'a BuiltOe,
    after: &[OeRow],
    changes: &[OeRowChange],
    cfg: &OeBuildConfig,
) -> Option<UpdateReuse<'a>> {
    plan_change_set_reuse(
        source,
        &OeRow::into_build_rows(after.to_vec()),
        &OeRow::build_changes(changes),
        cfg,
    )
}

fn ordinary_update(source: &BuiltOe, after: &[OeRow], changes: &[OeRowChange], cfg: &OeBuildConfig) -> BuiltOe {
    let reuse = plan_reuse(source, after, changes, cfg).map(|reuse| UpdateReuse { sparse: None, ..reuse });
    build_oe_file_sealed(
        OeRow::into_build_rows(after.to_vec()),
        cfg,
        &SerialEncodeExecutor,
        None,
        None,
        reuse.as_ref(),
    )
    .unwrap()
}

fn assert_same_content(expected: &BuiltOe, actual: &BuiltOe) {
    let expected = OeFile::open(expected.bytes.clone(), Some(&expected.file_seal)).unwrap();
    let actual = OeFile::open(actual.bytes.clone(), Some(&actual.file_seal)).unwrap();
    assert_eq!(actual.footer().columns, expected.footer().columns);
    assert_eq!(actual.footer().granules, expected.footer().granules);
    assert_eq!(actual.footer().page_stats, expected.footer().page_stats);
    for granule in &expected.footer().granules {
        for column in &expected.footer().columns {
            assert_eq!(
                actual.read_column(column.column_id, granule.granule_id).unwrap().data,
                expected.read_column(column.column_id, granule.granule_id).unwrap().data,
                "column {} granule {}",
                column.column_id,
                granule.granule_id
            );
        }
        for ordinal in granule.first_row_ordinal..granule.first_row_ordinal + u64::from(granule.row_count) {
            assert_eq!(
                actual.payload(ordinal).unwrap(),
                expected.payload(ordinal).unwrap(),
                "row {ordinal}"
            );
        }
    }
}

#[test]
fn sparse_update_matches_the_ordinary_update_byte_for_byte_over_random_mutations() {
    let cfg = sparse_config();
    let before: Vec<OeRow> = (0..400).map(row).collect();
    let source = build_oe_file(before.clone(), &cfg).unwrap();
    let mut rng = Lcg(0x9E37_79B9_7F4A_7C15);
    let mut sparse_rounds = 0;
    for round in 0..24 {
        let mut after = before.clone();
        let count = 1 + rng.below(40);
        let mut ordinals: Vec<usize> = (0..count).map(|_| rng.below(before.len())).collect();
        ordinals.sort_unstable();
        ordinals.dedup();
        let wide = round % 4 == 3;
        for &ordinal in &ordinals {
            mutate(&mut after[ordinal], &mut rng, wide, ordinal);
        }
        let changes = changes_for(&before, &after, &ordinals);
        let sparse = plan_reuse(&source, &after, &changes, &cfg)
            .and_then(|reuse| reuse.sparse)
            .is_some();

        let ordinary = ordinary_update(&source, &after, &changes, &cfg);
        let updated =
            update_oe_file_from_changes_with_executor(&source, after.clone(), &changes, &cfg, &SerialEncodeExecutor)
                .unwrap();
        assert_eq!(updated.bytes, ordinary.bytes, "round {round}");
        assert_eq!(
            updated.reused_source_blocks, ordinary.reused_source_blocks,
            "round {round}"
        );

        let from_source =
            update_oe_file_from_source_changes_with_executor(&source, &changes, &cfg, &SerialEncodeExecutor);
        if sparse {
            sparse_rounds += 1;
            assert_eq!(from_source.unwrap().bytes, ordinary.bytes, "round {round}");
        } else {
            assert!(from_source.is_err(), "round {round}");
        }
        if round % 6 == 0 {
            assert_same_content(&build_oe_file(after.clone(), &cfg).unwrap(), &updated);
        }
    }
    assert!(sparse_rounds >= 12, "{sparse_rounds} sparse rounds");
}

#[test]
fn a_tail_granule_change_builds_only_itself_and_its_segment_head() {
    let cfg = sparse_config();
    let before: Vec<OeRow> = (0..400).map(row).collect();
    let source = build_oe_file(before.clone(), &cfg).unwrap();
    let mut after = before.clone();
    let ordinals: Vec<usize> = (320..336).step_by(3).collect();
    for &ordinal in &ordinals {
        payload_of(&mut after[ordinal]).insert("amount".to_owned(), VariantValue::Int(-(ordinal as i64)));
    }
    let changes = changes_for(&before, &after, &ordinals);
    let plan = plan_reuse(&source, &after, &changes, &cfg)
        .and_then(|reuse| reuse.sparse)
        .expect("a payload-only change set plans a sparse update");
    let built: Vec<usize> = (0..plan.build.len())
        .filter(|&index| plan.builds(index).is_some())
        .collect();
    assert_eq!(built, [16, 20]);
    assert!(plan.builds(16).unwrap().is_subset(plan.builds(20).unwrap()));

    let ordinary = ordinary_update(&source, &after, &changes, &cfg);
    let updated =
        update_oe_file_from_changes_with_executor(&source, after.clone(), &changes, &cfg, &SerialEncodeExecutor)
            .unwrap();
    assert_eq!(updated.bytes, ordinary.bytes);
    let from_source =
        update_oe_file_from_source_changes_with_executor(&source, &changes, &cfg, &SerialEncodeExecutor).unwrap();
    assert_eq!(from_source.bytes, ordinary.bytes);
    assert!(updated.reused_source_blocks > 0);
}

#[test]
fn sparse_update_is_deterministic_across_thread_counts() {
    let cfg = sparse_config();
    let before: Vec<OeRow> = (0..400).map(row).collect();
    let source = build_oe_file(before.clone(), &cfg).unwrap();
    let mut after = before.clone();
    let mut rng = Lcg(7);
    let ordinals: Vec<usize> = (0..400).step_by(13).collect();
    for &ordinal in &ordinals {
        mutate(&mut after[ordinal], &mut rng, false, ordinal);
    }
    let changes = changes_for(&before, &after, &ordinals);
    assert!(
        plan_reuse(&source, &after, &changes, &cfg)
            .and_then(|reuse| reuse.sparse)
            .is_some()
    );

    let single_thread = rayon::ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    let serial = single_thread.install(|| {
        (
            update_oe_file_from_changes_with_executor(&source, after.clone(), &changes, &cfg, &SerialEncodeExecutor)
                .unwrap(),
            update_oe_file_from_source_changes_with_executor(&source, &changes, &cfg, &SerialEncodeExecutor).unwrap(),
        )
    });
    let pooled =
        update_oe_file_from_changes_with_executor(&source, after.clone(), &changes, &cfg, &ThreadPoolEncodeExecutor)
            .unwrap();
    let pooled_from_source =
        update_oe_file_from_source_changes_with_executor(&source, &changes, &cfg, &ThreadPoolEncodeExecutor).unwrap();
    assert_eq!(serial.0.bytes, pooled.bytes);
    assert_eq!(serial.1.bytes, pooled.bytes);
    assert_eq!(pooled_from_source.bytes, pooled.bytes);
}

#[test]
fn changes_only_update_refuses_what_needs_the_full_rows() {
    let cfg = sparse_config();
    let before: Vec<OeRow> = (0..64).map(row).collect();
    let source = build_oe_file(before.clone(), &cfg).unwrap();
    let refused = |edit: fn(&mut OeRow)| {
        let mut after = before.clone();
        edit(&mut after[5]);
        let changes = changes_for(&before, &after, &[5]);
        update_oe_file_from_source_changes_with_executor(&source, &changes, &cfg, &SerialEncodeExecutor)
            .err()
            .map(|error| error.to_string())
    };
    assert!(refused(|row| row.event.envelope.actor_id = Some("actor-9".to_owned())).is_some());
    assert!(
        refused(|row| {
            payload_of(row).insert("kind".to_owned(), VariantValue::String("k9".to_owned()));
        })
        .is_some()
    );
    assert!(
        refused(|row| {
            payload_of(row).insert("extra".to_owned(), VariantValue::Int(1));
        })
        .is_some()
    );
    let mut after = before.clone();
    after[5].event.envelope.actor_id = Some("actor-9".to_owned());
    let changes = changes_for(&before, &after, &[5]);
    let updated =
        update_oe_file_from_changes_with_executor(&source, after.clone(), &changes, &cfg, &SerialEncodeExecutor)
            .unwrap();
    assert_eq!(updated.bytes, ordinary_update(&source, &after, &changes, &cfg).bytes);
}

fn row_with_blob(i: u64) -> OeRow {
    let mut built = row(i);
    payload_of(&mut built).insert("blob".to_owned(), VariantValue::Binary(vec![i as u8; 8]));
    built
}

fn granule_bytes(built: &BuiltOe, granule: &GranuleEntry) -> usize {
    built
        .footer
        .page_stats
        .iter()
        .filter(|stats| stats.granule_id == granule.granule_id)
        .map(|stats| stats.row_count as usize * 8)
        .sum::<usize>()
        + granule.compressed_bytes_estimate as usize
}

#[test]
fn a_residual_that_moves_a_stripe_boundary_falls_back_to_the_ordinary_update() {
    let before: Vec<OeRow> = (0..96).map(row_with_blob).collect();
    let probe = build_oe_file(before.clone(), &sparse_config()).unwrap();
    assert!(probe.footer.shredded.iter().all(|entry| entry.path != "blob"));
    let mut cfg = config();
    cfg.targets.stripe_target_bytes =
        granule_bytes(&probe, &probe.footer.granules[0]) + granule_bytes(&probe, &probe.footer.granules[1]) + 16;
    let source = build_oe_file(before.clone(), &cfg).unwrap();
    assert_eq!(source.footer.granules[0].stripe_id, source.footer.granules[1].stripe_id);
    assert_ne!(source.footer.granules[1].stripe_id, source.footer.granules[2].stripe_id);

    let mut after = before.clone();
    payload_of(&mut after[3]).insert("blob".to_owned(), VariantValue::Binary(vec![7; 200]));
    let changes = changes_for(&before, &after, &[3]);
    assert!(
        plan_reuse(&source, &after, &changes, &cfg)
            .and_then(|reuse| reuse.sparse)
            .is_some()
    );
    let updated =
        update_oe_file_from_changes_with_executor(&source, after.clone(), &changes, &cfg, &SerialEncodeExecutor)
            .unwrap();
    let full = build_oe_file(after.clone(), &cfg).unwrap();
    assert_eq!(updated.reused_source_blocks, 0);
    assert_eq!(updated.bytes, full.bytes);
    assert_ne!(full.footer.granules[1].stripe_id, full.footer.granules[0].stripe_id);
    assert!(update_oe_file_from_source_changes_with_executor(&source, &changes, &cfg, &SerialEncodeExecutor).is_err());
}

#[test]
fn a_residual_that_moves_a_byte_cut_falls_back_to_the_ordinary_update() {
    let mut cfg = sparse_config();
    cfg.targets.index_granularity = 1_000;
    cfg.targets.index_granularity_bytes = 2_000;
    let before: Vec<OeRow> = (0..96).map(row_with_blob).collect();
    let source = build_oe_file(before.clone(), &cfg).unwrap();
    assert!(source.footer.granules.len() > 2);
    assert!(source.footer.granules.iter().all(|granule| granule.row_count < 1_000));

    let mut after = before.clone();
    payload_of(&mut after[1]).insert("blob".to_owned(), VariantValue::Binary(vec![7; 1_500]));
    let changes = changes_for(&before, &after, &[1]);
    assert!(
        plan_reuse(&source, &after, &changes, &cfg)
            .and_then(|reuse| reuse.sparse)
            .is_some()
    );
    let updated =
        update_oe_file_from_changes_with_executor(&source, after.clone(), &changes, &cfg, &SerialEncodeExecutor)
            .unwrap();
    let full = build_oe_file(after.clone(), &cfg).unwrap();
    assert_eq!(updated.bytes, full.bytes);
    assert_ne!(full.footer.granules[0].row_count, source.footer.granules[0].row_count);
    assert!(update_oe_file_from_source_changes_with_executor(&source, &changes, &cfg, &SerialEncodeExecutor).is_err());

    let mut after = before.clone();
    payload_of(&mut after[1]).insert("blob".to_owned(), VariantValue::Binary(vec![7; 9]));
    let changes = changes_for(&before, &after, &[1]);
    let updated =
        update_oe_file_from_changes_with_executor(&source, after.clone(), &changes, &cfg, &SerialEncodeExecutor)
            .unwrap();
    assert!(updated.reused_source_blocks > 0);
    assert_eq!(updated.bytes, ordinary_update(&source, &after, &changes, &cfg).bytes);
    let from_source =
        update_oe_file_from_source_changes_with_executor(&source, &changes, &cfg, &SerialEncodeExecutor).unwrap();
    assert_eq!(from_source.bytes, updated.bytes);
}

#[test]
fn authenticated_footer_proves_a_remote_stripe_range() {
    let rows: Vec<OeRow> = (0..64)
        .map(|i| {
            let mut built = row(i);
            let mut state = i.wrapping_add(1);
            let note: String = (0..32 * 1024)
                .map(|_| {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    char::from(33 + ((state >> 32) % 94) as u8)
                })
                .collect();
            let PayloadInput::Variant(VariantValue::Object(ref mut payload)) = built.event.payload else {
                unreachable!()
            };
            payload.insert("note".to_owned(), VariantValue::String(note));
            built
        })
        .collect();
    let mut cfg = config();
    cfg.page_size_rows = 16;
    cfg.targets.index_granularity = 64;
    cfg.targets.index_granularity_bytes = 64 * 1024 * 1024;
    cfg.targets.stripe_target_bytes = 64 * 1024 * 1024;
    let built = build_oe_file(rows, &cfg).unwrap();
    assert!(built.tree_len.is_some(), "a multi-megabyte stripe emits proof nodes");
    OeFile::open(built.bytes.clone(), Some(&built.file_seal)).expect("the eager reader accepts generated proof trees");

    let tail_range = tail_range(built.total_len, Some(built.footer_len), built.tree_len);
    let tail = &built.bytes[tail_range.start as usize..];
    let authenticated = OeFooter::open_authenticated(
        &built.bytes[..HEADER_BLOCK_LEN],
        tail,
        built.total_len,
        &built.file_seal,
        None,
    )
    .unwrap();
    assert!(authenticated.is_authenticated());
    let mut wrong_seal = built.file_seal;
    wrong_seal[0] ^= 1;
    assert!(
        OeFooter::open_authenticated(
            &built.bytes[..HEADER_BLOCK_LEN],
            tail,
            built.total_len,
            &wrong_seal,
            None,
        )
        .is_err(),
        "proof roots must be bound to the manifest's segment seal"
    );

    let stripe = built
        .footer
        .stripes
        .iter()
        .find(|stripe| stripe.byte_len > crate::file::constant::CHUNK_GROUP_BYTES as u64)
        .expect("at least one stripe spans multiple proof groups");
    let requested_offset = crate::file::constant::CHUNK_GROUP_BYTES as u64 - 31;
    let request = authenticated
        .plan_verified_stripe_read(stripe.stripe_id, requested_offset, 97)
        .unwrap();
    assert!(
        request.length < stripe.byte_len,
        "the GET is range-native, not a whole-stripe fallback"
    );
    let fetched = &built.bytes[request.file_offset as usize..(request.file_offset + request.length) as usize];
    let verified = authenticated.verify_stripe_range(&request, fetched).unwrap();
    assert_eq!(
        verified,
        &built.bytes
            [(stripe.file_offset + requested_offset) as usize..(stripe.file_offset + requested_offset + 97) as usize]
    );

    let mut corrupt = fetched.to_vec();
    corrupt[crate::file::constant::CHUNK_GROUP_BYTES - 1] ^= 1;
    assert!(authenticated.verify_stripe_range(&request, &corrupt).is_err());

    let planning_only = OeFooter::open(tail).unwrap();
    assert!(
        planning_only
            .plan_verified_stripe_read(stripe.stripe_id, requested_offset, 97)
            .is_err(),
        "an unauthenticated footer can plan metadata but must never vouch for content bytes"
    );
}

#[test]
fn speculative_open_underflows_then_hits_exact_length() {
    let rows: Vec<OeRow> = (0..64).map(row).collect();
    let built = build_oe_file(rows.clone(), &config()).unwrap();

    match OeFooter::open_speculative(&built.bytes).unwrap() {
        SpeculativeTail::Opened(footer) => assert_eq!(footer.footer().granules.len(), built.footer.granules.len()),
        SpeculativeTail::NeedsExactTail { tail_len } => panic!("whole file should not underflow, asked for {tail_len}"),
    }

    let short_start = built.bytes.len() - (built.footer_len as usize - 5);
    match OeFooter::open_speculative(&built.bytes[short_start..]).unwrap() {
        SpeculativeTail::NeedsExactTail { tail_len } => {
            assert_eq!(tail_len, built.footer_len);
            let exact = &built.bytes[built.bytes.len() - tail_len as usize..];
            assert!(OeFooter::open(exact).is_ok(), "exact retry opens the footer");
        }
        SpeculativeTail::Opened(_) => panic!("a short tail must underflow"),
    }
}

#[test]
fn build_and_read_round_trip() {
    let rows: Vec<OeRow> = (0..64).map(row).collect();
    let built = build_oe_file(rows.clone(), &config()).unwrap();
    assert!(built.footer.granules.len() >= 4);
    assert!(built.footer.shredded.iter().any(|entry| entry.path == "amount"));
    assert!(!built.footer.shredded.iter().any(|entry| entry.path == "note"));
    assert_eq!(built.footer.freetext.len(), 1);

    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    assert_eq!(file.header().row_count, 64);
    for granule in &file.footer().granules {
        let sequences = file.read_column(column_ids::SEQUENCE, granule.granule_id).unwrap();
        let ColumnData::U64(values) = sequences.data else {
            panic!("sequence is u64");
        };
        assert_eq!(values.first().copied().unwrap(), granule.first_sequence);
        assert_eq!(values.last().copied().unwrap(), granule.last_sequence);
    }
    for (ordinal, input_row) in rows.iter().enumerate() {
        let PayloadRead::Value(value) = file.payload(ordinal as u64).unwrap() else {
            panic!("payload present");
        };
        let PayloadInput::Variant(expected) = &input_row.event.payload else {
            panic!("variant input");
        };
        assert_eq!(&value, expected, "row {ordinal}");
    }
    assert_eq!(file.payload_path(5, "amount").unwrap(), Some(VariantValue::Int(1005)));
    assert_eq!(file.payload_path(6, "rare").unwrap(), Some(VariantValue::Bool(true)));
    assert_eq!(file.payload_path(5, "missing").unwrap(), None);
    let hits = file.granules_for_sequence(1, 1, 16);
    assert!(!hits.is_empty() && hits.len() < file.footer().granules.len());
    let none = file.granules_for_sequence(2, 1, 16);
    assert!(none.is_empty());
}

#[test]
fn a_build_without_trailing_stages_reads_back_the_same_rows() {
    let rows: Vec<OeRow> = (0..64).map(row).collect();
    let published = build_oe_file(rows.clone(), &config()).unwrap();
    let control = build_oe_file(
        rows,
        &OeBuildConfig {
            lifecycle: BuildLifecycle::FreshPublicationWithoutTrailing,
            ..config()
        },
    )
    .unwrap();
    let published = OeFile::open(published.bytes, None).unwrap();
    let control = OeFile::open(control.bytes, None).unwrap();
    assert_eq!(published.header().row_count, control.header().row_count);
    for granule in &control.footer().granules {
        for column in &control.footer().columns {
            let mark = control.mark(column.column_id, 0, granule.granule_id).unwrap().unwrap();
            assert_eq!(
                mark.codec_pipeline_id.compression().unwrap(),
                Compression::None,
                "column {} granule {}",
                column.column_id,
                granule.granule_id
            );
            assert_eq!(
                control.read_column(column.column_id, granule.granule_id).unwrap().data,
                published
                    .read_column(column.column_id, granule.granule_id)
                    .unwrap()
                    .data,
                "column {} granule {}",
                column.column_id,
                granule.granule_id
            );
        }
    }
    for ordinal in 0..64 {
        assert_eq!(control.payload(ordinal).unwrap(), published.payload(ordinal).unwrap());
    }
}

#[test]
fn the_freetext_row_offset_index_is_opt_in() {
    let rows: Vec<OeRow> = (0..64).map(row).collect();

    let default_build = build_oe_file(rows.clone(), &config()).unwrap();
    assert_eq!(
        default_build.footer.optional_feature_flags & optional_features::TYPED_COLUMN_ROW_OFFSETS,
        0,
        "a default build declares no per-row offset index"
    );
    assert!(
        default_build.footer.freetext_row_offsets.is_empty(),
        "a default build stores no per-row offset index"
    );

    let mut opted_in = config();
    opted_in.freetext_row_offset_index = true;
    let indexed_build = build_oe_file(rows, &opted_in).unwrap();
    assert_ne!(
        indexed_build.footer.optional_feature_flags & optional_features::TYPED_COLUMN_ROW_OFFSETS,
        0,
        "a build that opts in declares the per-row offset index"
    );
    assert_eq!(
        indexed_build.footer.freetext_row_offsets.len(),
        indexed_build.footer.granules.len(),
        "one row-offset index entry per granule for the one declared free-text column"
    );
    assert!(
        indexed_build.bytes.len() > default_build.bytes.len(),
        "the index costs bytes the default build does not spend"
    );
}

#[test]
fn string_column_blocks_carry_a_text_token_filter() {
    let rows: Vec<OeRow> = (0..64).map(row).collect();
    let built = build_oe_file(rows.clone(), &config()).unwrap();

    assert!(
        built.footer.text_token_indexes.is_empty(),
        "no filter bytes ride the footer"
    );
    assert!(
        built.footer.optional_feature_flags & crate::layout::optional_features::TEXT_TOKEN_FILTER_OFFSETS != 0,
        "the relocation is declared as an optional feature"
    );

    let kind_id = built
        .footer
        .columns
        .iter()
        .find(|descriptor| descriptor.name == "kind")
        .expect("promoted kind column present")
        .column_id;
    let kind_entries: Vec<_> = built
        .footer
        .text_token_offsets
        .iter()
        .filter(|entry| entry.column_id == kind_id)
        .collect();
    assert_eq!(
        kind_entries.len(),
        built.footer.granules.len(),
        "one unpaged filter entry per granule for the kind column"
    );

    let file = OeFile::open(built.bytes, None).unwrap();
    for granule in &file.footer().granules {
        let index = file
            .text_token_filter(kind_id, granule.granule_id, 0)
            .unwrap()
            .expect("the kind column's filter resolves from the data area");
        assert!(index.might_contain_token("k1"));
        assert!(!index.might_contain_token("no-such-kind"));

        assert!(
            file.text_token_filter(column_ids::SEQUENCE, granule.granule_id, 0)
                .unwrap()
                .is_none()
        );
        assert!(
            file.text_token_filter(column_ids::ENTITY_ID, granule.granule_id, 0)
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn a_column_declared_substring_searchable_carries_trigrams() {
    let rows: Vec<OeRow> = (0..64)
        .map(|i| {
            let mut r = row(i);
            let PayloadInput::Variant(VariantValue::Object(ref mut map)) = r.event.payload else {
                unreachable!("row payload is a variant object");
            };
            map.insert(
                "title".to_owned(),
                VariantValue::String(format!("quarterly revenue report {i}")),
            );
            r
        })
        .collect();

    let title_id = column_ids::PROMOTED_BASE + 1;
    let region_id = column_ids::CONTEXT_BASE;
    let build_with = |substring_searchable: bool| {
        let mut cfg = config();
        cfg.promotion.columns.push(PromotedColumn {
            kind: ColumnKind::String,
            name: "title".to_owned(),
            path: "title".to_owned(),
            since_schema_version: 1,
            substring_searchable,
        });
        cfg.analytical_columns = vec![AnalyticalColumn {
            column_id: region_id,
            data: ColumnData::Strings(
                (0..rows.len())
                    .map(|i| Some(format!("northwest district {i}")))
                    .collect(),
            ),
            internal_only: false,
            kind: ColumnKind::String,
            name: "region".to_owned(),
            substring_searchable,
        }];
        let built = build_oe_file(rows.clone(), &cfg).unwrap();
        OeFile::open(built.bytes, None).unwrap()
    };

    let columns = [(title_id, "revenue"), (region_id, "district")];

    let undeclared = build_with(false);
    for granule in &undeclared.footer().granules {
        for (column_id, present) in columns {
            let index = undeclared
                .text_token_filter(column_id, granule.granule_id, 0)
                .unwrap()
                .expect("a searchable string column carries a token filter either way");
            assert!(!index.has_ngrams(), "no trigrams without the declaration");
            assert!(
                index.might_contain_token(present),
                "whole-value tokens are still answered"
            );
            assert!(
                index.might_contain_substring("zeppelin"),
                "without trigrams a substring probe cannot prune, so the granule is kept",
            );
        }
    }

    let declared = build_with(true);
    for granule in &declared.footer().granules {
        for (column_id, present) in columns {
            let index = declared
                .text_token_filter(column_id, granule.granule_id, 0)
                .unwrap()
                .expect("a searchable string column carries a token filter either way");
            assert!(index.has_ngrams(), "the declaration puts trigrams on the filter");
            assert!(
                index.might_contain_token(present),
                "whole-value tokens are still answered"
            );
            assert!(index.might_contain_substring(present));
            assert!(
                !index.might_contain_substring("zeppelin"),
                "a needle whose trigrams the block never saw is proved absent",
            );
        }
    }
}

#[test]
fn substring_search_declared_where_no_filter_carries_it_is_rejected() {
    let rows: Vec<OeRow> = (0..8).map(row).collect();

    let mut promoted = config();
    promoted.promotion.columns.push(PromotedColumn {
        kind: ColumnKind::I64,
        name: "amount_promoted".to_owned(),
        path: "amount".to_owned(),
        since_schema_version: 1,
        substring_searchable: true,
    });
    let err = build_oe_file(rows.clone(), &promoted).unwrap_err();
    assert!(
        matches!(
            err,
            FormatError::Structural { rule } if rule == "only a string promoted column may be declared substring-searchable"
        ),
        "{err:?}",
    );

    let analytical = |kind, data, internal_only| AnalyticalColumn {
        column_id: column_ids::CONTEXT_BASE,
        data,
        internal_only,
        kind,
        name: "ctx".to_owned(),
        substring_searchable: true,
    };
    let text = || ColumnData::Strings((0..rows.len()).map(|i| Some(format!("value {i}"))).collect());
    for column in [
        analytical(ColumnKind::I64, ColumnData::I64(vec![0i64; rows.len()]), false),
        analytical(ColumnKind::String, text(), true),
    ] {
        let mut cfg = config();
        cfg.analytical_columns = vec![column];
        let err = build_oe_file(rows.clone(), &cfg).unwrap_err();
        assert!(
            matches!(
                err,
                FormatError::Structural { rule }
                    if rule == "only a public string analytical column may be declared substring-searchable"
            ),
            "{err:?}",
        );
    }
}

#[test]
fn stripe_marks_pages_are_colocated_with_the_stripes_filter_bytes() {
    let mut cfg = config();
    cfg.page_size_rows = 8;
    let rows: Vec<OeRow> = (0..64).map(row).collect();
    let built = build_oe_file(rows, &cfg).unwrap();

    assert!(
        built.footer.required_feature_flags & crate::layout::required_features::STRIPE_MARKS_PAGES != 0,
        "the placement is declared as a required feature"
    );
    assert!(built.footer.stripes.len() > 1, "the fixture spans several stripes");

    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    for stripe in &built.footer.stripes {
        let granules_of_stripe: hashbrown::HashSet<u32> = built
            .footer
            .granules
            .iter()
            .filter(|granule| granule.stripe_id == stripe.stripe_id)
            .map(|granule| granule.granule_id)
            .collect();

        for mark in built
            .footer
            .marks
            .iter()
            .filter(|mark| granules_of_stripe.contains(&mark.granule_id))
        {
            assert_eq!(
                file.mark(mark.column_id, mark.projection_id, mark.granule_id).unwrap(),
                Some(*mark),
                "mark for column {} granule {}",
                mark.column_id,
                mark.granule_id
            );
        }
        let mut by_block: std::collections::BTreeMap<(u32, u32, u32), Vec<_>> = std::collections::BTreeMap::new();
        for page in built
            .footer
            .page_directory
            .iter()
            .filter(|page| granules_of_stripe.contains(&page.granule_id))
        {
            by_block
                .entry((page.column_id, page.projection_id, page.granule_id))
                .or_default()
                .push(*page);
        }
        for ((column_id, projection_id, granule_id), expected_pages) in by_block {
            assert_eq!(
                file.page_directory_for(column_id, projection_id, granule_id).unwrap(),
                expected_pages,
                "page directory for column {column_id} granule {granule_id}"
            );
        }

        let mut extents: Vec<(u64, u64, bool)> = Vec::new();
        for entry in built
            .footer
            .text_token_offsets
            .iter()
            .filter(|entry| granules_of_stripe.contains(&entry.granule_id))
        {
            extents.push((stripe.file_offset + entry.index_offset, entry.index_len, false));
        }
        for entry in built
            .footer
            .entity_hash_filters
            .iter()
            .filter(|entry| granules_of_stripe.contains(&entry.granule_id))
        {
            extents.push((stripe.file_offset + entry.index_offset, entry.index_len, false));
        }
        for entry in built
            .footer
            .marks_page_offsets
            .iter()
            .filter(|entry| entry.stripe_id == stripe.stripe_id)
        {
            extents.push((stripe.file_offset + entry.page_offset, entry.page_len, true));
        }
        extents.sort_by_key(|&(start, ..)| start);
        let first_marks_page = extents
            .iter()
            .position(|&(_, _, is_marks_page)| is_marks_page)
            .expect("the stripe has marks pages");
        assert!(first_marks_page > 0, "the stripe has filter bytes ahead of its pages");
        assert!(
            extents
                .get(first_marks_page..)
                .unwrap_or_default()
                .iter()
                .all(|&(_, _, is_marks_page)| is_marks_page),
            "every marks page follows the filters"
        );
        for pair in extents.windows(2) {
            let [(a_start, a_len, _), (b_start, ..)] = pair else {
                continue;
            };
            let gap = b_start - (a_start + a_len);
            assert!(gap < 64, "gap of {gap} bytes exceeds block-alignment padding");
        }
        let (last_start, last_len, _) = *extents.last().unwrap();
        assert_eq!(
            last_start + last_len,
            stripe.file_offset + stripe.byte_len,
            "the co-located tail ends exactly at the stripe end, inside its checksummed range"
        );
    }
}

#[test]
fn a_file_with_an_out_of_order_occurred_at_is_flagged_as_carrying_late_events() {
    let ordered: Vec<OeRow> = (0..32).map(row).collect();
    let built = build_oe_file(ordered.clone(), &config()).unwrap();
    assert_eq!(
        built.footer.optional_feature_flags & optional_features::OE_LATE_EVENTS,
        0,
        "a file with occurred_at in ingest order must not declare late events"
    );

    let mut late = ordered;
    if let Some(last) = late.last_mut() {
        last.event.envelope.occurred_at = TimestampValue::from_physical_nanos(1);
    }
    let built = build_oe_file(late, &config()).unwrap();
    assert_ne!(
        built.footer.optional_feature_flags & optional_features::OE_LATE_EVENTS,
        0,
        "a row whose occurred_at predates an earlier-ingested row must declare late events"
    );
}

#[test]
fn paged_columns_round_trip_with_non_byte_aligned_pages() {
    let rows: Vec<OeRow> = (0..17).map(row).collect();
    let mut cfg = config();
    cfg.page_size_rows = 5;
    cfg.targets.index_granularity = 64;
    cfg.targets.index_granularity_bytes = 1 << 30;

    let built = build_oe_file(rows.clone(), &cfg).unwrap();
    assert_eq!(built.footer.granules.len(), 1, "test expects one paged granule");
    assert_ne!(
        built.footer.optional_feature_flags & optional_features::PER_PAGE_MARKS,
        0,
        "paged files must declare the page directory feature",
    );

    let granule_id = built.footer.granules[0].granule_id;
    let sequence_mark = built
        .footer
        .marks
        .iter()
        .find(|mark| mark.column_id == column_ids::SEQUENCE && mark.granule_id == granule_id)
        .expect("sequence mark");
    assert_eq!(sequence_mark.page_count, 4);
    let mut sequence_pages: Vec<_> = built
        .footer
        .page_directory
        .iter()
        .filter(|entry| entry.column_id == column_ids::SEQUENCE && entry.granule_id == granule_id)
        .collect();
    sequence_pages.sort_by_key(|entry| entry.page_index);
    assert_eq!(
        sequence_pages
            .iter()
            .map(|entry| (entry.page_index, entry.first_row_ordinal, entry.row_count))
            .collect::<Vec<_>>(),
        vec![(0, 0, 5), (1, 5, 5), (2, 10, 5), (3, 15, 2)]
    );
    assert!(
        sequence_pages
            .windows(2)
            .all(|window| { window[0].compressed_offset + window[0].compressed_len == window[1].compressed_offset }),
        "page directory entries must point at each contiguous encoded page"
    );

    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    for (page_index, sequence_page) in sequence_pages.iter().enumerate() {
        let page = file
            .read_page(column_ids::SEQUENCE, granule_id, page_index as u32)
            .unwrap();
        assert_eq!(
            page.data,
            ColumnData::U64(
                (sequence_page.first_row_ordinal + 1
                    ..=sequence_page.first_row_ordinal + u64::from(sequence_page.row_count))
                    .collect()
            ),
            "plain page {page_index} must read its own [start, end) range"
        );
    }

    let sequences = file.read_column(column_ids::SEQUENCE, granule_id).unwrap();
    assert_eq!(
        sequences.data,
        ColumnData::U64((1..=17).collect()),
        "plain paged columns must concatenate every page range"
    );

    let promoted = file.read_column(column_ids::PROMOTED_BASE, granule_id).unwrap();
    let ColumnData::Strings(promoted_values) = &promoted.data else {
        panic!("promoted kind is string data");
    };
    assert_eq!(promoted_values.len(), rows.len());

    let second_promoted_page = file.read_page(column_ids::PROMOTED_BASE, granule_id, 1).unwrap();
    let ColumnData::Strings(second_promoted_values) = &second_promoted_page.data else {
        panic!("promoted kind is string data");
    };
    assert_eq!(
        second_promoted_values.iter().collect::<Vec<_>>(),
        vec![Some("k1"), Some("k2"), Some("k3"), Some("k0"), Some("k1")],
        "presence-gated page 1 must slice dense values by value offset plus present count"
    );
    assert_eq!(
        second_promoted_page.presence[0] & 0b0001_1111,
        0b0001_1111,
        "each page presence bitmap is repacked from bit 0"
    );

    for row_index in 0..rows.len() {
        assert_ne!(
            promoted.presence[row_index / 8] & (1 << (row_index % 8)),
            0,
            "presence bit {row_index} must survive bit-level page append"
        );
    }

    for (ordinal, input_row) in rows.iter().enumerate() {
        let PayloadRead::Value(value) = file.payload(ordinal as u64).unwrap() else {
            panic!("payload present for row {ordinal}");
        };
        let PayloadInput::Variant(expected) = &input_row.event.payload else {
            panic!("variant input");
        };
        assert_eq!(&value, expected, "row {ordinal}");
    }
}

#[test]
fn page_minmax_emitted_for_hot_column_and_empty_for_cold_column() {
    let rows: Vec<OeRow> = (0..17).map(row).collect();
    let mut cfg = config();
    cfg.promotion.columns.push(PromotedColumn {
        name: "amount_promoted".to_owned(),
        path: "amount".to_owned(),
        kind: ColumnKind::I64,
        since_schema_version: 1,
        substring_searchable: false,
    });
    cfg.page_size_rows = 5;
    cfg.targets.index_granularity = 64;
    cfg.targets.index_granularity_bytes = 1 << 30;

    let built = build_oe_file(rows.clone(), &cfg).unwrap();
    let granule_id = built.footer.granules[0].granule_id;
    let amount_column_id = column_ids::PROMOTED_BASE + 1;

    let mut amount_pages: Vec<_> = built
        .footer
        .page_minmax
        .iter()
        .filter(|entry| entry.column_id == amount_column_id && entry.granule_id == granule_id)
        .collect();
    amount_pages.sort_by_key(|entry| entry.page_index);
    assert_eq!(
        amount_pages
            .iter()
            .map(|entry| (entry.page_index, entry.min_i128, entry.max_i128, entry.row_count))
            .collect::<Vec<_>>(),
        vec![
            (0, Some(1000), Some(1004), 5),
            (1, Some(1005), Some(1009), 5),
            (2, Some(1010), Some(1014), 5),
            (3, Some(1015), Some(1016), 2),
        ],
        "each hot page's bounds must match its own slice of values, not the granule-wide range"
    );

    assert!(
        built
            .footer
            .page_minmax
            .iter()
            .all(|entry| entry.column_id != column_ids::SEQUENCE),
        "a required column the promotion plan does not mention must carry no page-level minmax entries, even though \
         it is split into the same physical pages as the hot column"
    );
}

#[test]
fn read_page_rejects_out_of_range_index_on_single_page_column() {
    let rows: Vec<OeRow> = (0..8).map(row).collect();
    let built = build_oe_file(rows.clone(), &config()).unwrap();
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    let granule_id = built.footer.granules[0].granule_id;

    assert!(
        file.read_page(column_ids::SEQUENCE, granule_id, 0).is_ok(),
        "page 0 of a single-page column reads as the whole column",
    );
    assert!(
        matches!(
            file.read_page(column_ids::SEQUENCE, granule_id, 7),
            Err(FormatError::RefOutOfRange { .. }),
        ),
        "an index past the only page must be rejected, not read as page 0",
    );
}

#[test]
fn granules_never_span_more_than_one_epoch() {
    let mut rows: Vec<OeRow> = Vec::new();
    for epoch in 1..=2u64 {
        for sequence in 1..=6u64 {
            let mut r = row(sequence);
            r.epoch = epoch;
            r.sequence = sequence;
            rows.push(r);
        }
    }
    let mut cfg = config();
    cfg.targets.index_granularity = 64;
    cfg.targets.index_granularity_bytes = 1 << 30;

    let built = build_oe_file(rows.clone(), &cfg).unwrap();
    assert_eq!(
        built.footer.granules.len(),
        2,
        "the epoch change forces a granule boundary even though one granule would otherwise fit every row",
    );
    for granule in &built.footer.granules {
        assert_eq!(
            granule.first_epoch, granule.last_epoch,
            "each granule covers exactly one epoch, so its sequence range is epoch-scoped",
        );
    }

    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    let epoch_one = file.granules_for_sequence(1, 1, 6);
    assert_eq!(epoch_one.len(), 1);
    assert_eq!(epoch_one[0].first_epoch, 1);
    let epoch_two = file.granules_for_sequence(2, 1, 6);
    assert_eq!(epoch_two.len(), 1);
    assert_eq!(epoch_two[0].first_epoch, 2);
}

#[test]
fn mixed_scale_decimals_normalize_to_the_largest_scale() {
    let prices = [(15i128, 1u8), (150, 2), (2567, 3)];
    let rows: Vec<OeRow> = prices
        .iter()
        .enumerate()
        .map(|(i, &(unscaled, scale))| {
            let mut r = row(i as u64);
            let PayloadInput::Variant(VariantValue::Object(ref mut map)) = r.event.payload else {
                unreachable!("row payload is a variant object");
            };
            map.insert("price".to_owned(), VariantValue::Decimal { scale, unscaled });
            r
        })
        .collect();
    let mut cfg = config();
    cfg.promotion.columns.push(PromotedColumn {
        kind: ColumnKind::Decimal,
        name: "price".to_owned(),
        path: "price".to_owned(),
        since_schema_version: 1,
        substring_searchable: false,
    });

    let built = build_oe_file(rows.clone(), &cfg).unwrap();
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    let granule_id = built.footer.granules[0].granule_id;
    let column = file.read_column(column_ids::PROMOTED_BASE + 1, granule_id).unwrap();
    assert_eq!(
        column.data,
        ColumnData::Decimal {
            scale: 3,
            values: vec![1500, 1500, 2567],
        },
        "each value is lifted to scale 3: 1.5 and 1.50 become 1500, 2.567 stays 2567",
    );
}

#[test]
fn marks_are_stripe_relative_and_bounded_by_their_stripe() {
    use crate::layout::{HEADER_BLOCK_LEN, required_features};
    use hashbrown::HashMap;

    let rows: Vec<OeRow> = (0..64).map(row).collect();
    let built = build_oe_file(rows.clone(), &config()).unwrap();

    assert_ne!(
        built.footer.required_feature_flags & required_features::STRIPE_RELATIVE_MARKS,
        0,
        "footer must declare STRIPE_RELATIVE_MARKS",
    );
    assert_ne!(
        built.header.feature_flags & required_features::STRIPE_RELATIVE_MARKS,
        0,
        "header must mirror the required feature",
    );
    assert!(
        built.footer.stripes.len() >= 2,
        "test needs multiple stripes to be meaningful"
    );

    let stripe: HashMap<u32, (u64, u64)> = built
        .footer
        .stripes
        .iter()
        .map(|s| (s.stripe_id, (s.file_offset, s.byte_len)))
        .collect();
    let stripe_of_granule: HashMap<u32, u32> = built
        .footer
        .granules
        .iter()
        .map(|g| (g.granule_id, g.stripe_id))
        .collect();

    let mut min_offset_in_stripe: HashMap<u32, u64> = HashMap::new();
    for mark in &built.footer.marks {
        let stripe_id = stripe_of_granule[&mark.granule_id];
        let (base, byte_len) = stripe[&stripe_id];
        assert!(
            mark.compressed_offset + mark.compressed_size <= byte_len,
            "mark ({}, {}) offset {} + size {} escapes stripe {} (len {})",
            mark.column_id,
            mark.granule_id,
            mark.compressed_offset,
            mark.compressed_size,
            stripe_id,
            byte_len,
        );
        let absolute = base + mark.compressed_offset;
        assert!(absolute >= base && absolute + mark.compressed_size <= base + byte_len);
        let entry = min_offset_in_stripe.entry(stripe_id).or_insert(u64::MAX);
        *entry = (*entry).min(mark.compressed_offset);
    }
    assert!(
        min_offset_in_stripe.values().all(|&m| m == 0),
        "each stripe's first block is at its base (relative offset 0)",
    );
    assert!(
        stripe.values().any(|&(base, _)| base > HEADER_BLOCK_LEN as u64),
        "a later stripe must start past the header",
    );

    assert!(
        !built.footer.payload_granules.is_empty(),
        "test needs payload granules to be meaningful",
    );
    for payload in &built.footer.payload_granules {
        let stripe_id = stripe_of_granule[&payload.granule_id];
        let (base, byte_len) = stripe[&stripe_id];
        for (what, offset, len) in [
            ("dictionary", payload.dictionary_offset, payload.dictionary_len),
            ("offsets", payload.offsets_offset, payload.offsets_len),
            ("residual", payload.residual_offset, payload.residual_len),
        ] {
            assert!(
                offset + len <= byte_len,
                "payload {what} for granule {} offset {offset} + len {len} escapes stripe {stripe_id} (len {byte_len})",
                payload.granule_id,
            );
            let absolute = base + offset;
            assert!(absolute >= base && absolute + len <= base + byte_len);
        }
    }

    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    for granule in &file.footer().granules {
        let read = file.read_column(column_ids::SEQUENCE, granule.granule_id).unwrap();
        let ColumnData::U64(values) = read.data else {
            panic!("sequence is u64");
        };
        assert_eq!(values.len(), granule.row_count as usize);
        assert_eq!(values[0], granule.first_sequence);
    }
    let mut saw_value = false;
    for row in 0..rows.len() as u64 {
        if let PayloadRead::Value(_) = file.payload(row).expect("payload read resolves") {
            saw_value = true;
        }
    }
    assert!(
        saw_value,
        "at least one row's payload resolves through the stripe-relative arena",
    );
}

#[test]
fn residuals_larger_than_the_arena_estimate_still_round_trip() {
    let rows: Vec<OeRow> = (0..48)
        .map(|index| {
            let mut built = row(index);
            let PayloadInput::Variant(VariantValue::Object(fields)) = &mut built.event.payload else {
                panic!("the generated payload is an object");
            };
            fields.insert("essay".to_owned(), VariantValue::String("x".repeat(4096)));
            fields.insert(
                "details".to_owned(),
                VariantValue::Object(BTreeMap::from([
                    ("depth".to_owned(), VariantValue::Int(index as i64)),
                    (
                        "tags".to_owned(),
                        VariantValue::Array(vec![VariantValue::String("t".repeat(64))]),
                    ),
                ])),
            );
            built
        })
        .collect();

    let built = build_oe_file(rows.clone(), &config()).unwrap();
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();

    for (ordinal, input) in rows.iter().enumerate() {
        let PayloadRead::Value(value) = file.payload(ordinal as u64).unwrap() else {
            panic!("payload present");
        };
        let PayloadInput::Variant(expected) = &input.event.payload else {
            panic!("variant input");
        };
        assert_eq!(&value, expected, "row {ordinal}");
    }
}

#[test]
fn repeated_residuals_share_one_copy_and_still_round_trip() {
    let with_details = |index: u64, group: u64| {
        let mut built = row(index);
        let PayloadInput::Variant(VariantValue::Object(fields)) = &mut built.event.payload else {
            panic!("the generated payload is an object");
        };
        fields.clear();
        fields.insert(
            "details".to_owned(),
            VariantValue::Object(BTreeMap::from([
                ("group".to_owned(), VariantValue::Int(group as i64)),
                (
                    "tags".to_owned(),
                    VariantValue::Array(vec![VariantValue::String("t".repeat(64))]),
                ),
            ])),
        );
        built
    };
    let uniform: Vec<OeRow> = (0..64).map(|index| with_details(index, 0)).collect();
    let runs: Vec<OeRow> = (0..64).map(|index| with_details(index, index / 3)).collect();
    for (name, rows, uniform_slots) in [("uniform", uniform, true), ("runs", runs, false)] {
        let built = build_oe_file(rows.clone(), &config()).unwrap();
        let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
        for (ordinal, input) in rows.iter().enumerate() {
            let PayloadRead::Value(value) = file.payload(ordinal as u64).unwrap() else {
                panic!("{name}: payload present");
            };
            let PayloadInput::Variant(expected) = &input.event.payload else {
                panic!("variant input");
            };
            assert_eq!(&value, expected, "{name}: row {ordinal}");
        }
        let residual: u64 = built
            .footer
            .payload_granules
            .iter()
            .map(|payload| payload.residual_len)
            .sum();
        let distinct = if uniform_slots { 1 } else { rows.len().div_ceil(3) };
        assert!(
            residual <= (distinct + built.footer.payload_granules.len()) as u64 * 128,
            "{name}: {residual} residual bytes for {distinct} distinct residuals"
        );
        if uniform_slots {
            for payload in &built.footer.payload_granules {
                assert_eq!(payload.offsets_len, 8, "{name}: one shared entry per granule");
            }
        }
    }
}

#[test]
fn identical_input_builds_identical_files() {
    let rows: Vec<OeRow> = (0..32).map(row).collect();
    let first = build_oe_file(rows.clone(), &config()).unwrap();
    let second = build_oe_file(rows.clone(), &config()).unwrap();
    assert_eq!(first.bytes, second.bytes);
    assert_eq!(first.file_id, second.file_id);
    assert_eq!(first.file_seal, second.file_seal);
}

#[test]
fn an_fsst_column_replays_its_heads_table_across_granules_and_round_trips() {
    let values: Vec<Option<String>> = (0..5_000u64)
        .map(|i| Some(format!("user-{}@example-{}.test", i * 7919 % 100_003, i % 13)))
        .collect();
    let mut cfg = config();
    cfg.targets.index_granularity = 256;
    cfg.analytical_columns = vec![AnalyticalColumn {
        column_id: column_ids::CONTEXT_BASE,
        internal_only: false,
        kind: ColumnKind::String,
        name: "ctx".to_owned(),
        data: ColumnData::Strings(values.clone().into()),
        substring_searchable: false,
    }];
    let rows: Vec<OeRow> = (0..5_000).map(row).collect();
    let built = build_oe_file(rows.clone(), &cfg).unwrap();
    assert!(built.footer.granules.len() > REPLAY_SEGMENT_GRANULES);
    let marks: Vec<_> = built
        .footer
        .marks
        .iter()
        .filter(|mark| mark.column_id == column_ids::CONTEXT_BASE)
        .collect();
    assert!(!marks.is_empty());
    assert!(
        marks.iter().all(|mark| mark
            .codec_pipeline_id
            .transform()
            .is_ok_and(|transform| transform == Transform::FsstString)),
        "every granule of the column stores an FSST block"
    );

    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    let mut first_row = 0usize;
    for granule in &file.footer().granules {
        let read = file.read_column(column_ids::CONTEXT_BASE, granule.granule_id).unwrap();
        let expected = values
            .get(first_row..first_row + granule.row_count as usize)
            .unwrap()
            .to_vec();
        assert_eq!(
            read.data,
            ColumnData::Strings(expected.into()),
            "granule {}",
            granule.granule_id
        );
        first_row += granule.row_count as usize;
    }
    assert_eq!(first_row, values.len());
    assert_eq!(build_oe_file(rows, &cfg).unwrap().bytes, built.bytes);
}

#[test]
fn file_dictionaries_are_the_sorted_distinct_values_at_every_cardinality() {
    let row_count = 3_000u64;
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move |bound: u64| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) % bound
    };
    for cardinality in [1, 2, 37, 500, 2_999, row_count] {
        let mut draw = |i: u64| if cardinality == row_count { i } else { next(cardinality) };
        let rows: Vec<OeRow> = (0..row_count)
            .map(|i| {
                let mut row = row(i);
                let envelope = &mut row.event.envelope;
                envelope.source = format!("source-{}", draw(i));
                envelope.event_type = format!("type-{}", draw(i));
                envelope.entity_type = format!("entity-{}", draw(i));
                row
            })
            .collect();
        let expected = |value: fn(&EventEnvelope) -> &str| -> Vec<String> {
            let distinct: std::collections::BTreeSet<&str> = rows.iter().map(|r| value(&r.event.envelope)).collect();
            distinct.into_iter().map(str::to_owned).collect()
        };
        let expected_source = expected(|e| &e.source);
        let expected_event_type = expected(|e| &e.event_type);
        let expected_entity_type = expected(|e| &e.entity_type);
        let built = build_oe_file(rows, &config()).unwrap();
        assert_eq!(
            built.footer.dictionaries.source, expected_source,
            "cardinality {cardinality}"
        );
        assert_eq!(
            built.footer.dictionaries.event_type, expected_event_type,
            "cardinality {cardinality}"
        );
        assert_eq!(
            built.footer.dictionaries.entity_type, expected_entity_type,
            "cardinality {cardinality}"
        );
        if cardinality == row_count {
            assert_eq!(built.footer.dictionaries.source.len(), row_count as usize);
        }
    }
}

#[test]
fn thread_pool_encode_builds_the_same_bytes_as_the_serial_executor() {
    use crate::invariants::io::ThreadPoolEncodeExecutor;
    use crate::invariants::sim::SerialEncodeExecutor;
    let rows: Vec<OeRow> = (0..256).map(row).collect();
    let serial = build_oe_file_with_executor(rows.clone(), &config(), &SerialEncodeExecutor).unwrap();
    for thread_count in [1, 2, 4] {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .build()
            .expect("test pool");
        let pooled =
            pool.install(|| build_oe_file_with_executor(rows.clone(), &config(), &ThreadPoolEncodeExecutor).unwrap());
        assert_eq!(serial.bytes, pooled.bytes, "{thread_count} workers");
        assert_eq!(serial.file_id, pooled.file_id, "{thread_count} workers");
        assert_eq!(serial.file_seal, pooled.file_seal, "{thread_count} workers");
    }
}

#[test]
fn footer_metadata_groups_tiny_granule_jobs_per_worker() {
    assert_eq!(metadata_job_count(0, 8), 0);
    assert_eq!(metadata_job_count(3, 8), 1);
    assert_eq!(metadata_job_count(13, 8), 4);
    assert_eq!(metadata_job_count(100, 6), 6);
    assert_eq!(metadata_job_count(100, 1), 1);
}

#[test]
fn profiled_build_preserves_bytes_and_reports_each_phase_once() {
    use crate::invariants::sim::SerialEncodeExecutor;
    let rows: Vec<OeRow> = (0..256).map(row).collect();
    let ordinary = build_oe_file_with_executor(rows.clone(), &config(), &SerialEncodeExecutor).unwrap();
    let (profiled, profile) = build_oe_file_profiled_with_executor(rows, &config(), &SerialEncodeExecutor).unwrap();
    assert_eq!(profiled.bytes, ordinary.bytes);
    assert_eq!(profile.phases.len(), 10);
    for phase in [
        BuildPhase::Normalization,
        BuildPhase::DictionaryConstruction,
        BuildPhase::PathStatistics,
        BuildPhase::GranuleConstruction,
        BuildPhase::BlockEncoding,
        BuildPhase::Layout,
        BuildPhase::FooterConstruction,
        BuildPhase::Integrity,
        BuildPhase::FileAssembly,
        BuildPhase::RowDestruction,
    ] {
        assert_eq!(
            profile.phases.iter().filter(|(reported, _)| *reported == phase).count(),
            1,
            "{phase:?}"
        );
    }
}

#[test]
fn corrupted_file_refuses() {
    let rows: Vec<OeRow> = (0..8).map(row).collect();
    let built = build_oe_file(rows.clone(), &config()).unwrap();
    let mut corrupted = built.bytes.to_vec();
    let stripe_offset = built.footer.stripes[0].file_offset as usize;
    corrupted[stripe_offset] ^= 0xFF;
    assert!(OeFile::open(corrupted, None).is_err());
    let mut tampered = built.bytes.to_vec();
    let last = tampered.len() - 20;
    tampered[last] ^= 0x01;
    assert!(OeFile::open(tampered, Some(&built.file_seal)).is_err());
}

#[test]
fn unknown_required_feature_refuses_unknown_optional_ignored() {
    use crate::compat::check_features;
    use crate::error::FormatError;
    let unknown_required = required_features::ALL | (1 << 60);
    assert!(matches!(
        check_features(unknown_required, 0),
        Err(FormatError::UnknownRequiredFeature { .. })
    ));
    let usable = check_features(
        required_features::ALL,
        optional_features::VARIANT_SHREDDED_FIELD_BLOCKS | (1 << 61),
    )
    .unwrap();
    assert_eq!(usable, optional_features::VARIANT_SHREDDED_FIELD_BLOCKS);
}

#[test]
fn promoted_column_presence_map_gates_by_schema_version() {
    let mut rows: Vec<OeRow> = (0..8).map(row).collect();
    for row in rows.iter_mut().take(4) {
        row.event.envelope.schema_version = 0;
    }
    let mut gated = config();
    gated.promotion.columns[0].since_schema_version = 1;
    let built = build_oe_file(rows.clone(), &gated).unwrap();
    let file = OeFile::open(built.bytes, None).unwrap();
    let promoted = file
        .read_column(column_ids::PROMOTED_BASE, file.footer().granules[0].granule_id)
        .unwrap();
    for index in 0..4usize {
        assert_eq!(promoted.presence[index / 8] & (1 << (index % 8)), 0);
    }
    for index in 4..8usize {
        assert_ne!(promoted.presence[index / 8] & (1 << (index % 8)), 0);
    }
    assert!(matches!(file.payload(0).unwrap(), PayloadRead::Value(_)));
}

#[test]
fn encoded_block_uncompressed_len_is_the_decoded_length() {
    let data = ColumnData::Strings(vec![Some("compressible payload value ".repeat(20)); 2_000].into());
    let encoded = encode_block(&data, false);
    assert!(
        encoded.uncompressed_len >= encoded.bytes.len() as u64,
        "decoded length is never smaller than the stored length",
    );
    assert!(
        encoded.uncompressed_len > encoded.bytes.len() as u64,
        "a compressible block decodes to strictly more bytes than it stores (uncompressed_len={}, bytes={})",
        encoded.uncompressed_len,
        encoded.bytes.len(),
    );
}

#[test]
fn column_marks_record_the_uncompressed_not_the_compressed_size() {
    let mut cfg = config();
    cfg.targets.index_granularity = 4_096;
    cfg.analytical_columns = vec![AnalyticalColumn {
        column_id: column_ids::CONTEXT_BASE,
        internal_only: false,
        kind: ColumnKind::String,
        name: "ctx".to_owned(),
        data: ColumnData::Strings(vec![Some("compressible payload value ".repeat(20)); 2_000].into()),
        substring_searchable: false,
    }];
    let rows: Vec<OeRow> = (0..2_000).map(row).collect();
    let built = build_oe_file(rows.clone(), &cfg).unwrap();

    assert!(!built.footer.marks.is_empty());
    for mark in &built.footer.marks {
        assert!(
            mark.uncompressed_size >= mark.compressed_size,
            "decoded size must be at least the stored size (column {}, granule {})",
            mark.column_id,
            mark.granule_id,
        );
    }
    assert!(
        built
            .footer
            .marks
            .iter()
            .any(|mark| mark.uncompressed_size > mark.compressed_size),
        "at least one compressed block must decode to more bytes than it stores",
    );
}

#[test]
fn analytical_column_with_wrong_value_count_is_rejected() {
    let rows: Vec<OeRow> = (0..8).map(row).collect();
    let mut cfg = config();
    cfg.analytical_columns = vec![AnalyticalColumn {
        column_id: column_ids::CONTEXT_BASE,
        internal_only: false,
        kind: ColumnKind::I64,
        name: "short".to_owned(),
        data: ColumnData::I64(vec![0i64; rows.len() - 1]),
        substring_searchable: false,
    }];
    assert!(build_oe_file(rows.clone(), &cfg).is_err());
}

#[test]
fn an_analytical_column_id_that_collides_with_another_column_is_rejected() {
    let rows: Vec<OeRow> = (0..8).map(row).collect();
    let analytical = |column_id| AnalyticalColumn {
        column_id,
        internal_only: false,
        kind: ColumnKind::I64,
        name: format!("analytical-{column_id}"),
        data: ColumnData::I64(vec![0i64; rows.len()]),
        substring_searchable: false,
    };

    let mut cfg = config();
    cfg.analytical_columns = vec![analytical(column_ids::PAYLOAD_FLAGS)];
    assert!(build_oe_file(rows.clone(), &cfg).is_err());

    let mut cfg = config();
    cfg.freetext = FreetextDeclaration {
        fields: vec!["note".to_owned()],
    };
    cfg.analytical_columns = vec![analytical(column_ids::FREETEXT_BASE)];
    assert!(build_oe_file(rows.clone(), &cfg).is_err());

    let mut cfg = config();
    cfg.analytical_columns = vec![
        analytical(column_ids::CONTEXT_BASE),
        analytical(column_ids::CONTEXT_BASE),
    ];
    assert!(build_oe_file(rows.clone(), &cfg).is_err());

    let mut cfg = config();
    cfg.analytical_columns = vec![
        analytical(column_ids::CONTEXT_BASE),
        analytical(column_ids::EMBEDDING_BASE),
    ];
    assert!(build_oe_file(rows.clone(), &cfg).is_ok());
}

#[test]
fn io_alignment_is_recorded_and_padded_when_configured() {
    let rows: Vec<OeRow> = (0..64).map(row).collect();

    let unaligned = build_oe_file(rows.clone(), &config()).unwrap();
    assert_eq!(unaligned.footer.io_alignment_bytes, 0);

    let mut aligned_config = config();
    aligned_config.io_alignment_bytes = 4_096;
    let aligned = build_oe_file(rows.clone(), &aligned_config).unwrap();
    assert_eq!(aligned.footer.io_alignment_bytes, 4_096);
    for mark in &aligned.footer.marks {
        assert_eq!(
            mark.compressed_offset % 4_096,
            0,
            "block offset must be congruent to the recorded IO alignment (column {}, granule {})",
            mark.column_id,
            mark.granule_id,
        );
    }

    let buffered = OeFile::open(unaligned.bytes.clone(), Some(&unaligned.file_seal)).unwrap();
    let direct = OeFile::open(aligned.bytes.clone(), Some(&aligned.file_seal)).unwrap();
    assert_eq!(buffered.header().row_count, direct.header().row_count);
    for granule in &direct.footer().granules {
        let a = direct.read_column(column_ids::SEQUENCE, granule.granule_id).unwrap();
        let b = buffered.read_column(column_ids::SEQUENCE, granule.granule_id).unwrap();
        assert_eq!(a.data, b.data, "decoded column identical regardless of alignment");
    }
}

#[test]
fn a_non_power_of_two_or_oversized_io_alignment_is_rejected() {
    let rows: Vec<OeRow> = (0..8).map(row).collect();

    for bad in [3_u32, 100, 6_000, 8_192] {
        let mut cfg = config();
        cfg.io_alignment_bytes = bad;
        assert!(
            matches!(
                build_oe_file(rows.clone(), &cfg),
                Err(FormatError::Structural { rule })
                    if rule == "io_alignment_bytes must be a power of two no larger than the header block"
            ),
            "io_alignment_bytes = {bad} must be rejected (non-power-of-two or larger than the header block)"
        );
    }

    let mut ok = config();
    ok.io_alignment_bytes = 512;
    assert!(build_oe_file(rows.clone(), &ok).is_ok());
}

#[test]
fn built_column_blocks_stay_under_the_reader_max_page_bound() {
    let rows: Vec<OeRow> = (0..512).map(row).collect();
    let built = build_oe_file(rows.clone(), &config()).unwrap();

    for mark in &built.footer.marks {
        if mark.page_count <= 1 {
            assert!(
                mark.compressed_size <= crate::layout::MAX_PAGE_BYTES,
                "single-page block exceeds MAX_PAGE_BYTES (column {}, granule {}, {} bytes)",
                mark.column_id,
                mark.granule_id,
                mark.compressed_size,
            );
        }
    }
    for page in &built.footer.page_directory {
        assert!(
            page.compressed_len <= crate::layout::MAX_PAGE_BYTES,
            "page block exceeds MAX_PAGE_BYTES (column {}, granule {}, page {}, {} bytes)",
            page.column_id,
            page.granule_id,
            page.page_index,
            page.compressed_len,
        );
    }
    OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
}

#[test]
fn payload_reconstruction_over_many_shredded_rows_stays_correct() {
    let rows: Vec<OeRow> = (0..2_048).map(row).collect();
    let mut config = config();
    config.targets.index_granularity = 4_096;
    config.targets.index_granularity_bytes = 1 << 30;
    let built = build_oe_file(rows.clone(), &config).unwrap();
    assert_eq!(
        built.footer.granules.len(),
        1,
        "the whole batch must land in one granule"
    );
    assert!(built.footer.shredded.iter().any(|entry| entry.path == "amount"));

    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    for (ordinal, input_row) in rows.iter().enumerate() {
        let PayloadRead::Value(value) = file.payload(ordinal as u64).unwrap() else {
            panic!("payload present for row {ordinal}");
        };
        let PayloadInput::Variant(expected) = &input_row.event.payload else {
            panic!("variant input");
        };
        assert_eq!(&value, expected, "row {ordinal}");
    }
    assert_eq!(
        file.payload_path(1_000, "amount").unwrap(),
        Some(VariantValue::Int(2_000))
    );
    assert_eq!(
        file.payload_path(2_047, "amount").unwrap(),
        Some(VariantValue::Int(1_000 + 2_047))
    );
    assert_eq!(file.payload_path(1_000, "missing").unwrap(), None);
}

#[test]
fn rewrite_planner_reuses_real_stripes_and_preserves_file_seal() {
    use crate::layout::HEADER_BLOCK_LEN;
    use crate::writer::compaction::{StripeChange, StripeDisposition, plan_stripe_reuse};

    let rows: Vec<OeRow> = (0..2_048).map(row).collect();
    let built = build_oe_file(rows.clone(), &config()).unwrap();
    assert!(
        built.footer.stripes.len() >= 2,
        "need multiple stripes to exercise reuse"
    );

    let unchanged: Vec<StripeChange> = built
        .footer
        .stripes
        .iter()
        .map(|&stripe| StripeChange {
            changed: false,
            stripe,
            uncertain: false,
        })
        .collect();
    let plan = plan_stripe_reuse(&unchanged, HEADER_BLOCK_LEN as u64);
    assert!(plan.reuses_every_stripe());
    OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();

    let mut with_change = unchanged.clone();
    let last = with_change.len() - 1;
    with_change[last].changed = true;
    let plan = plan_stripe_reuse(&with_change, HEADER_BLOCK_LEN as u64);
    assert!(matches!(plan.dispositions[last], StripeDisposition::Rebuild { .. }));
}

#[test]
fn provider_checksum_is_computed_once_alongside_the_segment_seal() {
    let rows: Vec<OeRow> = (0..64).map(row).collect();
    let built = build_oe_file(rows.clone(), &config()).unwrap();
    assert_eq!(built.file_crc64_nvme, crate::file::integrity::crc64_nvme(&built.bytes));

    let rebuilt = build_oe_file(rows.clone(), &config()).unwrap();
    assert_eq!(built.file_seal, rebuilt.file_seal);
    assert_eq!(built.file_crc64_nvme, rebuilt.file_crc64_nvme);
}

#[test]
fn mixed_scale_shredded_decimals_round_trip_exactly() {
    let prices = [(15i128, 1u8), (150, 2), (2567, 3), (7, 1), (99, 2), (1, 0)];
    let rows: Vec<OeRow> = prices
        .iter()
        .enumerate()
        .map(|(i, &(unscaled, scale))| {
            let mut r = row(i as u64);
            let PayloadInput::Variant(VariantValue::Object(ref mut map)) = r.event.payload else {
                unreachable!("row payload is a variant object");
            };
            map.insert("price".to_owned(), VariantValue::Decimal { scale, unscaled });
            r
        })
        .collect();

    let built = build_oe_file(rows.clone(), &config()).unwrap();
    assert!(
        built.footer.shredded.iter().any(|entry| entry.path == "price"),
        "the fully-present decimal path is shredded",
    );

    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    for (ordinal, input_row) in rows.iter().enumerate() {
        let PayloadRead::Value(value) = file.payload(ordinal as u64).unwrap() else {
            panic!("payload present");
        };
        let PayloadInput::Variant(expected) = &input_row.event.payload else {
            panic!("variant input");
        };
        assert_eq!(&value, expected, "row {ordinal} round-trips exactly");
    }
    assert_eq!(
        file.payload_path(0, "price").unwrap(),
        Some(VariantValue::Decimal { unscaled: 15, scale: 1 }),
    );
    assert_eq!(
        file.payload_path(1, "price").unwrap(),
        Some(VariantValue::Decimal {
            unscaled: 150,
            scale: 2
        }),
    );
}

#[test]
fn large_mantissa_decimal_round_trips_without_saturating() {
    let huge = i128::MAX - 1;
    let mut rows: Vec<OeRow> = Vec::new();
    for (i, (unscaled, scale)) in [(2567i128, 3u8), (huge, 1)].into_iter().enumerate() {
        let mut r = row(i as u64);
        let PayloadInput::Variant(VariantValue::Object(ref mut map)) = r.event.payload else {
            unreachable!("row payload is a variant object");
        };
        map.insert("price".to_owned(), VariantValue::Decimal { scale, unscaled });
        rows.push(r);
    }
    for i in 2..8u64 {
        let mut r = row(i);
        let PayloadInput::Variant(VariantValue::Object(ref mut map)) = r.event.payload else {
            unreachable!("row payload is a variant object");
        };
        map.insert(
            "price".to_owned(),
            VariantValue::Decimal {
                scale: 3,
                unscaled: i128::from(i),
            },
        );
        rows.push(r);
    }

    let built = build_oe_file(rows.clone(), &config()).unwrap();
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    assert_eq!(
        file.payload_path(1, "price").unwrap(),
        Some(VariantValue::Decimal {
            unscaled: huge,
            scale: 1
        }),
        "the large mantissa survives intact instead of clamping to i128::MAX",
    );
    assert_eq!(
        file.payload_path(0, "price").unwrap(),
        Some(VariantValue::Decimal {
            unscaled: 2567,
            scale: 3
        }),
    );
}

#[test]
fn timestamp_field_round_trips_as_timestamp_including_mixed_int_path() {
    let rows: Vec<OeRow> = (0..16)
        .map(|i| {
            let mut r = row(i);
            let PayloadInput::Variant(VariantValue::Object(ref mut map)) = r.event.payload else {
                unreachable!("row payload is a variant object");
            };
            let value = if i.is_multiple_of(8) {
                VariantValue::Timestamp(TimestampValue::from_physical_nanos(9_000 + i as i64))
            } else {
                VariantValue::Int(500 + i as i64)
            };
            map.insert("when".to_owned(), value);
            r
        })
        .collect();

    let built = build_oe_file(rows.clone(), &config()).unwrap();
    assert!(
        built.footer.shredded.iter().any(|entry| entry.path == "when"),
        "the Int-consensus path shreds as I64",
    );

    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    for (ordinal, input_row) in rows.iter().enumerate() {
        let PayloadRead::Value(value) = file.payload(ordinal as u64).unwrap() else {
            panic!("payload present");
        };
        let PayloadInput::Variant(expected) = &input_row.event.payload else {
            panic!("variant input");
        };
        assert_eq!(&value, expected, "row {ordinal} round-trips exactly");
    }
    assert_eq!(
        file.payload_path(0, "when").unwrap(),
        Some(VariantValue::Timestamp(TimestampValue::from_physical_nanos(9_000))),
    );
    assert_eq!(file.payload_path(1, "when").unwrap(), Some(VariantValue::Int(501)));
}

#[test]
fn float_stays_residual_while_double_and_int_shred() {
    let rows: Vec<OeRow> = (0..16)
        .map(|i| {
            let mut r = row(i);
            let PayloadInput::Variant(VariantValue::Object(ref mut map)) = r.event.payload else {
                unreachable!("row payload is a variant object");
            };
            map.insert("ratio".to_owned(), VariantValue::Float(1.5 + i as f32));
            map.insert("score".to_owned(), VariantValue::Double(2.5 + i as f64));
            r
        })
        .collect();

    let built = build_oe_file(rows.clone(), &config()).unwrap();
    assert!(
        !built.footer.shredded.iter().any(|entry| entry.path == "ratio"),
        "a pure-Float path is never shredded",
    );
    assert!(built.footer.shredded.iter().any(|entry| entry.path == "score"));
    assert!(built.footer.shredded.iter().any(|entry| entry.path == "amount"));

    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    for (ordinal, input_row) in rows.iter().enumerate() {
        let PayloadRead::Value(value) = file.payload(ordinal as u64).unwrap() else {
            panic!("payload present");
        };
        let PayloadInput::Variant(expected) = &input_row.event.payload else {
            panic!("variant input");
        };
        assert_eq!(&value, expected, "row {ordinal} round-trips exactly");
    }
    assert_eq!(file.payload_path(3, "ratio").unwrap(), Some(VariantValue::Float(4.5)));
    assert_eq!(file.payload_path(3, "score").unwrap(), Some(VariantValue::Double(5.5)));
    assert_eq!(file.payload_path(3, "amount").unwrap(), Some(VariantValue::Int(1003)));
}

#[test]
fn promoted_decimal_that_overflows_on_rescale_is_rejected() {
    let huge = i128::MAX - 1;
    let prices = [(huge, 0u8), (150, 2)];
    let rows: Vec<OeRow> = prices
        .iter()
        .enumerate()
        .map(|(i, &(unscaled, scale))| {
            let mut r = row(i as u64);
            let PayloadInput::Variant(VariantValue::Object(ref mut map)) = r.event.payload else {
                unreachable!("row payload is a variant object");
            };
            map.insert("price".to_owned(), VariantValue::Decimal { scale, unscaled });
            r
        })
        .collect();
    let mut cfg = config();
    cfg.promotion.columns.push(PromotedColumn {
        kind: ColumnKind::Decimal,
        name: "price".to_owned(),
        path: "price".to_owned(),
        since_schema_version: 1,
        substring_searchable: false,
    });

    let err = build_oe_file(rows.clone(), &cfg).unwrap_err();
    assert!(
        matches!(
            err,
            FormatError::Structural { rule } if rule == "decimal value overflows i128 when rescaled to the column scale"
        ),
        "overflow on promoted rescale must error, not clamp: {err:?}",
    );
}

#[test]
fn payload_heavy_rows_cut_granules_on_bytes_instead_of_tripping_the_stripe_clamp() {
    let body = "x".repeat(32 * 1024);
    let rows: Vec<OeRow> = (0..32)
        .map(|i| {
            let mut r = row(i);
            let PayloadInput::Variant(VariantValue::Object(ref mut map)) = r.event.payload else {
                unreachable!("row payload is a variant object");
            };
            map.insert(format!("body_{i}"), VariantValue::String(body.clone()));
            r
        })
        .collect();
    let mut cfg = config();
    cfg.targets = LayoutTargets {
        index_granularity: 1024,
        index_granularity_bytes: 64 * 1024,
        max_stripe_bytes: 256 * 1024,
        min_bytes_for_wide: 10 * 1024 * 1024,
        stripe_target_bytes: 128 * 1024,
    };

    let built = build_oe_file(rows.clone(), &cfg).expect("the byte cut must fire before the stripe clamp");
    assert!(
        built.footer.granules.len() > 1,
        "the payload value bytes force byte-target granule cuts"
    );
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    assert_eq!(file.header().row_count, 32);
}

#[test]
fn oversized_freetext_arena_is_caught_by_the_stripe_clamp() {
    let rows: Vec<OeRow> = (0..8)
        .map(|i| {
            let mut r = row(i);
            let PayloadInput::Variant(VariantValue::Object(ref mut map)) = r.event.payload else {
                unreachable!("row payload is a variant object");
            };
            map.insert("note".to_owned(), VariantValue::String("y".repeat(64 * 1024)));
            r
        })
        .collect();
    let mut cfg = config();
    cfg.freetext_row_offset_index = true;
    cfg.targets = LayoutTargets {
        index_granularity: 1024,
        index_granularity_bytes: 1 << 30,
        max_stripe_bytes: 256 * 1024,
        min_bytes_for_wide: 10 * 1024 * 1024,
        stripe_target_bytes: 128 * 1024,
    };

    let err = build_oe_file(rows.clone(), &cfg).unwrap_err();
    assert!(
        matches!(
            err,
            FormatError::Structural { rule } if rule == "stripe clamp: a single granule exceeds the maximum stripe size"
        ),
        "the clamp must see the free-text arena: {err:?}",
    );
}

#[test]
fn default_build_leaves_the_footer_plaintext() {
    let rows: Vec<OeRow> = (0..8).map(row).collect();
    let built = build_oe_file(rows.clone(), &config()).unwrap();
    assert_eq!(built.header.feature_flags & required_features::FOOTER_ENCRYPTED, 0);
    let end = built.bytes.len() - 12;
    let start = end - (built.footer_len as usize - 12);
    assert_eq!(
        &built.bytes[start..end],
        crate::layout::footer::encode_footer(&built.footer).as_slice(),
        "a plaintext build writes the encode_footer output verbatim",
    );
    OeFile::open(built.bytes.clone(), Some(&built.file_seal)).expect("plaintext file opens via plain open");
}

#[test]
fn encrypted_footer_without_a_key_is_rejected() {
    let rows: Vec<OeRow> = (0..8).map(row).collect();
    let mut cfg = config();
    cfg.footer_encryption = crate::security::FooterEncryption::Encrypted;
    cfg.footer_dek = None;
    assert!(
        build_oe_file(rows.clone(), &cfg).is_err(),
        "an encrypted footer with no key must fail rather than write plaintext",
    );
}

#[test]
fn authenticated_footer_open_decrypts_after_binding_the_stored_tail() {
    let dek = [0x5au8; 32];
    let mut cfg = config();
    cfg.footer_encryption = crate::security::FooterEncryption::Encrypted;
    cfg.footer_dek = Some(dek);
    let built = build_oe_file((0..8).map(row).collect(), &cfg).unwrap();
    let range = tail_range(built.total_len, Some(built.footer_len), built.tree_len);
    let tail = &built.bytes[range.start as usize..];

    let opened = OeFooter::open_authenticated(
        &built.bytes[..HEADER_BLOCK_LEN],
        tail,
        built.total_len,
        &built.file_seal,
        Some(&dek),
    )
    .unwrap();
    assert!(opened.is_authenticated());
    assert_eq!(opened.footer().stripe_checksums, built.footer.stripe_checksums);
    assert_eq!(opened.footer().stripe_proofs, built.footer.stripe_proofs);
    assert_eq!(opened.footer().integrity_gaps, built.footer.integrity_gaps);
    assert!(
        OeFooter::open_authenticated(
            &built.bytes[..HEADER_BLOCK_LEN],
            tail,
            built.total_len,
            &built.file_seal,
            None,
        )
        .is_err()
    );
    let wrong_dek = [0xa5u8; 32];
    assert!(
        OeFooter::open_authenticated(
            &built.bytes[..HEADER_BLOCK_LEN],
            tail,
            built.total_len,
            &built.file_seal,
            Some(&wrong_dek),
        )
        .is_err()
    );
}

fn signed_row(author: &SimulatedEventAuthor, i: u64) -> OeRow {
    let payload = signed_event_payload(&format!("message {i}"), &[&["e", "root"], &["p", "peer"]]);
    let provenance = author
        .sign(1, 1_700_000_000 + i as i64, &payload)
        .expect("the payload is well-shaped");
    let mut row = row(i);
    row.event.payload = PayloadInput::Variant(payload);
    row.event.provenance = Some(provenance);
    row
}

#[test]
fn an_archived_signed_event_re_verifies_offline_from_the_file_alone() {
    let author = SimulatedEventAuthor::from_seed(6);
    let rows: Vec<OeRow> = (0..8).map(|i| signed_row(&author, i)).collect();
    let built = build_oe_file(rows.clone(), &config()).expect("a OE of signed rows builds");
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).expect("the sealed file opens");

    let granule_id = file.footer().granules.first().expect("one granule").granule_id;
    let pubkeys = file
        .read_column(column_ids::AUTHOR_PUBKEY, granule_id)
        .expect("author_pubkey is materialized");
    let signatures = file
        .read_column(column_ids::SIGNATURE, granule_id)
        .expect("signature is materialized");
    let ids = file
        .read_column(column_ids::PROTOCOL_EVENT_ID, granule_id)
        .expect("protocol_event_id is materialized");
    let schemes = file
        .read_column(column_ids::SIGNATURE_SCHEME, granule_id)
        .expect("signature_scheme is materialized");
    let kinds = file
        .read_column(column_ids::PROTOCOL_KIND, granule_id)
        .expect("protocol_kind is materialized");
    let claimed = file
        .read_column(column_ids::CLAIMED_AT, granule_id)
        .expect("claimed_at is materialized");

    let (
        ColumnData::Strings(pubkeys),
        ColumnData::Strings(signatures),
        ColumnData::Strings(ids),
        ColumnData::Strings(schemes),
        ColumnData::I64(kinds),
        ColumnData::I64(claimed),
    ) = (
        pubkeys.data,
        signatures.data,
        ids.data,
        schemes.data,
        kinds.data,
        claimed.data,
    )
    else {
        panic!("provenance columns keep their declared types");
    };

    for (index, row) in rows.iter().enumerate() {
        let scheme = SignatureScheme::from_str(schemes.get(index).flatten().expect("a stored scheme tag"))
            .expect("the tag is in the registry");
        let stored = SignedEventProvenance {
            author_pubkey: hex_bytes(pubkeys.get(index).flatten().expect("a pubkey")).expect("lowercase hex"),
            claimed_at: TimestampValue::from_physical_nanos(*claimed.get(index).expect("a claimed_at")),
            protocol_event_id: hex_bytes(ids.get(index).flatten().expect("an id")).expect("lowercase hex"),
            protocol_kind: u32::try_from(*kinds.get(index).expect("a kind")).expect("kinds are small"),
            scheme,
            signature: hex_bytes(signatures.get(index).flatten().expect("a signature")).expect("lowercase hex"),
        };
        assert_eq!(Some(&stored), row.event.provenance.as_ref(), "row {index} round-trips");

        let PayloadRead::Value(payload) = file.payload(index as u64).expect("the payload reads back") else {
            panic!("a signed event carries an inline payload");
        };
        stored
            .verify(&payload)
            .unwrap_or_else(|error| panic!("archived row {index} must re-verify offline: {error}"));
    }
}

#[test]
fn a_file_of_unsigned_events_materializes_no_provenance_columns() {
    let built = build_oe_file((0..4).map(row).collect::<Vec<_>>(), &config()).expect("a OE builds");
    assert!(
        !built
            .footer
            .columns
            .iter()
            .any(|column| column.column_id >= column_ids::PROVENANCE_BASE),
        "a stream without signatures pays nothing for the provenance family"
    );
}

#[test]
fn a_point_read_into_a_seekable_residual_arena_inflates_one_frame_not_the_whole_arena() {
    let rows: Vec<OeRow> = (0..2048)
        .map(|index| {
            let mut row = row(index);
            if let PayloadInput::Variant(VariantValue::Object(payload)) = &mut row.event.payload {
                payload.insert(
                    "detail".to_owned(),
                    VariantValue::Object(
                        (0..4)
                            .map(|field| {
                                (
                                    format!("field{field}"),
                                    VariantValue::String(format!("row {index} field {field} of the payload")),
                                )
                            })
                            .collect(),
                    ),
                );
            }
            row
        })
        .collect();
    let one_granule = OeBuildConfig {
        targets: LayoutTargets {
            index_granularity: 2048,
            ..config().targets
        },
        ..config()
    };
    let fresh = build_oe_file(rows.clone(), &one_granule).unwrap();
    let rewritten = build_oe_file(
        rows,
        &OeBuildConfig {
            lifecycle: BuildLifecycle::RewriteOrCompaction,
            ..one_granule
        },
    )
    .unwrap();

    let arena_bytes = fresh.footer.payload_granules.first().unwrap().residual_len;
    assert!(
        arena_bytes > 2 * u64::from(seekable_zstd::FRAME_BYTES),
        "the arena must span several frames for this test to mean anything: {arena_bytes} bytes"
    );
    assert_eq!(
        rewritten.footer.payload_granules.first().unwrap().residual_compression,
        crate::layout::footer::ResidualCompression::ZstdSeekable
    );

    let fresh_file = OeFile::open(fresh.bytes, None).unwrap();
    let rewritten_file = OeFile::open(rewritten.bytes.clone(), None).unwrap();
    for ordinal in [0u64, 1, 1000, 2047] {
        assert_eq!(
            fresh_file.payload(ordinal).unwrap(),
            rewritten_file.payload(ordinal).unwrap(),
            "row {ordinal} must reconstruct identically from the seekable arena"
        );
    }

    let point_reader = OeFile::open(rewritten.bytes, None).unwrap();
    assert_eq!(point_reader.payload(1000).unwrap(), fresh_file.payload(1000).unwrap());
    let held = point_reader.residual_cache_bytes();
    assert!(held > 0, "the read must have inflated the frame it needed");
    assert!(
        held <= u64::from(seekable_zstd::FRAME_BYTES),
        "a point read held {held} bytes of a {arena_bytes}-byte arena; it must hold at most one frame"
    );
}

#[test]
fn rewrite_lifecycle_compresses_residual_arenas_and_payloads_read_back_identically() {
    let rows: Vec<OeRow> = (0..64).map(row).collect();
    let fresh = build_oe_file(
        rows.clone(),
        &OeBuildConfig {
            lifecycle: BuildLifecycle::FreshPublication,
            ..config()
        },
    )
    .unwrap();
    let rewritten = build_oe_file(
        rows,
        &OeBuildConfig {
            lifecycle: BuildLifecycle::RewriteOrCompaction,
            ..config()
        },
    )
    .unwrap();

    assert!(
        fresh
            .footer
            .payload_granules
            .iter()
            .all(|payload| payload.residual_compression == crate::layout::footer::ResidualCompression::None),
        "fresh publications keep residual arenas uncompressed"
    );
    assert!(
        rewritten
            .footer
            .payload_granules
            .iter()
            .any(|payload| payload.residual_compression == crate::layout::footer::ResidualCompression::ZstdSeekable),
        "the rewrite lifecycle compresses residual arenas that shrink"
    );
    let fresh_residual: u64 = fresh.footer.payload_granules.iter().map(|p| p.residual_len).sum();
    let rewritten_residual: u64 = rewritten.footer.payload_granules.iter().map(|p| p.residual_len).sum();
    assert!(rewritten_residual < fresh_residual);

    let fresh_file = OeFile::open(fresh.bytes, None).unwrap();
    let rewritten_file = OeFile::open(rewritten.bytes, None).unwrap();
    for ordinal in 0..64u64 {
        assert_eq!(
            fresh_file.payload(ordinal).unwrap(),
            rewritten_file.payload(ordinal).unwrap(),
            "row {ordinal} must reconstruct identically from the compressed arena"
        );
    }
    assert_eq!(
        fresh_file.payload_path(3, "amount").unwrap(),
        rewritten_file.payload_path(3, "amount").unwrap()
    );
}

#[test]
fn byte_identical_blocks_within_a_stripe_share_one_extent() {
    let rows: Vec<OeRow> = (0..8).map(row).collect();
    let mut config = config();
    config.analytical_columns = vec![
        AnalyticalColumn {
            column_id: crate::columns::column_ids::CONTEXT_BASE,
            internal_only: false,
            kind: ColumnKind::I64,
            name: "left".to_owned(),
            data: ColumnData::I64((0..8).map(|i| 5_000 + i).collect()),
            substring_searchable: false,
        },
        AnalyticalColumn {
            column_id: crate::columns::column_ids::CONTEXT_BASE + 1,
            internal_only: false,
            kind: ColumnKind::I64,
            name: "right".to_owned(),
            data: ColumnData::I64((0..8).map(|i| 5_000 + i).collect()),
            substring_searchable: false,
        },
    ];
    let built = build_oe_file(rows.clone(), &config).unwrap();
    assert!(built.aliased_block_bytes > 0, "identical blocks must be aliased");

    let left_mark = built
        .footer
        .marks
        .iter()
        .find(|mark| mark.column_id == crate::columns::column_ids::CONTEXT_BASE)
        .unwrap();
    let right_mark = built
        .footer
        .marks
        .iter()
        .find(|mark| mark.column_id == crate::columns::column_ids::CONTEXT_BASE + 1)
        .unwrap();
    assert_eq!(left_mark.compressed_offset, right_mark.compressed_offset);
    assert_eq!(left_mark.compressed_size, right_mark.compressed_size);

    let file = crate::layout::reader::OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    for column in [
        crate::columns::column_ids::CONTEXT_BASE,
        crate::columns::column_ids::CONTEXT_BASE + 1,
    ] {
        let read = file.read_column(column, 0).unwrap();
        assert_eq!(read.data, ColumnData::I64((0..8).map(|i| 5_000 + i).collect()));
    }
    assert_eq!(
        file.column_block_reads(),
        1,
        "an aliased extent must be fetched and decoded once per scan"
    );

    let again = build_oe_file(rows, &config).unwrap();
    assert_eq!(built.bytes, again.bytes);
}

#[test]
fn aliased_file_scope_dictionary_blocks_decode_each_columns_own_values() {
    let rows: Vec<OeRow> = (0..64).map(row).collect();
    let mut config = config();
    config.analytical_columns = vec![
        AnalyticalColumn {
            column_id: crate::columns::column_ids::CONTEXT_BASE,
            internal_only: false,
            kind: ColumnKind::String,
            name: "left".to_owned(),
            data: ColumnData::Strings((0..64).map(|_| Some("aaaa")).collect()),
            substring_searchable: false,
        },
        AnalyticalColumn {
            column_id: crate::columns::column_ids::CONTEXT_BASE + 1,
            internal_only: false,
            kind: ColumnKind::String,
            name: "right".to_owned(),
            data: ColumnData::Strings((0..64).map(|_| Some("bbbb")).collect()),
            substring_searchable: false,
        },
    ];
    let built = build_oe_file(rows, &config).unwrap();
    let left_col = crate::columns::column_ids::CONTEXT_BASE;
    let right_col = crate::columns::column_ids::CONTEXT_BASE + 1;
    for (column, mark_value) in [(left_col, "aaaa"), (right_col, "bbbb")] {
        assert!(
            built
                .footer
                .shared_dictionaries
                .iter()
                .any(|entry| { entry.column_id == column && entry.values == vec![mark_value.to_owned()] }),
            "column {column} must carry its own one-value file-scope alphabet"
        );
    }
    let left_mark = built
        .footer
        .marks
        .iter()
        .find(|mark| mark.column_id == left_col)
        .unwrap();
    let right_mark = built
        .footer
        .marks
        .iter()
        .find(|mark| mark.column_id == right_col)
        .unwrap();
    assert_eq!(
        left_mark.codec_pipeline_id.side_stream(),
        Ok(crate::encoding::SideStream::FileScopeDictionary),
        "the test must exercise file-scope code streams"
    );
    assert_eq!(left_mark.compressed_offset, right_mark.compressed_offset);
    assert_eq!(left_mark.compressed_size, right_mark.compressed_size);

    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    for (column, expected) in [(left_col, "aaaa"), (right_col, "bbbb")] {
        let read = file.read_column(column, 0).unwrap();
        assert_eq!(
            read.data,
            ColumnData::Strings((0..16).map(|_| Some(expected)).collect()),
            "column {column} must decode through its own alphabet, not a cached alias"
        );
    }
}

#[test]
fn per_stripe_distinct_counts_are_recorded_for_promoted_columns() {
    let rows: Vec<OeRow> = (0..24).map(row).collect();
    let built = build_oe_file(rows, &config()).unwrap();
    assert!(
        !built.footer.stripe_ndv.is_empty(),
        "the promoted column must carry NDV entries"
    );
    for entry in &built.footer.stripe_ndv {
        assert!(entry.exact, "four distinct kinds count exactly");
        assert!(
            (1..=4).contains(&entry.distinct_count),
            "kind takes four values, stripe {} claims {}",
            entry.stripe_id,
            entry.distinct_count
        );
    }
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    assert_eq!(
        file.footer().stripe_ndv,
        built.footer.stripe_ndv,
        "footer bytes round-trip the entries"
    );
}

#[test]
fn constant_integer_blocks_store_no_bytes_and_rebuild_from_stats() {
    let constant_col = crate::columns::column_ids::CONTEXT_BASE;
    let rows: Vec<OeRow> = (0..8).map(row).collect();
    let mut config = config();
    config.analytical_columns = vec![AnalyticalColumn {
        column_id: constant_col,
        internal_only: false,
        kind: ColumnKind::I64,
        name: "constant".to_owned(),
        data: ColumnData::I64(vec![777; 8]),
        substring_searchable: false,
    }];
    let built = build_oe_file(rows, &config).unwrap();
    assert_ne!(
        built.footer.required_feature_flags & crate::layout::required_features::ELIDED_CONSTANT_BLOCKS,
        0,
        "elision is governed by its required bit"
    );
    let mark = *built
        .footer
        .marks
        .iter()
        .find(|mark| mark.column_id == constant_col && mark.granule_id == 0)
        .expect("the constant column has a mark");
    assert_eq!(mark.compressed_size, 0, "a provable constant block must elide its body");

    let file = crate::layout::reader::OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    let expected = ColumnData::I64(vec![777; mark.row_count as usize]);
    let read = file.read_column(constant_col, 0).unwrap();
    assert_eq!(read.data, expected);
    assert!(read.presence.is_empty());
    assert_eq!(
        file.column_block_reads(),
        0,
        "rebuilding from stats must not count as a block fetch"
    );
    assert!(
        file.read_column_raw(constant_col, 0).is_err(),
        "an elided block has no raw bytes"
    );
    assert_eq!(file.read_page(constant_col, 0, 0).unwrap().data, expected);
}

#[test]
fn a_fully_present_promoted_column_stores_the_zero_byte_presence_form() {
    let rows: Vec<OeRow> = (0..24).map(row).collect();
    let built = build_oe_file(rows, &config()).unwrap();
    assert_ne!(
        built.footer.required_feature_flags & crate::layout::required_features::COMPRESSED_PRESENCE,
        0,
        "the encoded presence frame is governed by its required bit"
    );
    let kind_col = built
        .footer
        .shredded
        .iter()
        .find(|entry| entry.path == "amount")
        .expect("amount auto-shreds")
        .column_id;
    let mark = *built
        .footer
        .marks
        .iter()
        .find(|mark| mark.column_id == kind_col && mark.granule_id == 0)
        .expect("kind has a mark");
    assert!(mark.page_count <= 1 && mark.compressed_size > 0);
    let stripe_id = built.footer.granules[0].stripe_id;
    let stripe_base = built
        .footer
        .stripes
        .iter()
        .find(|stripe| stripe.stripe_id == stripe_id)
        .unwrap()
        .file_offset;
    let frame_start = (stripe_base + mark.compressed_offset) as usize;
    assert_eq!(
        built.bytes[frame_start],
        crate::layout::presence_forms::ALL_PRESENT,
        "a fully present block stores the zero-byte form"
    );

    let file = crate::layout::reader::OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    let read = file.read_column(kind_col, 0).unwrap();
    assert_eq!(read.data.row_count() as u32, mark.row_count);
    let all_set = (0..mark.row_count as usize).all(|row| read.presence[row / 8] & (1 << (row % 8)) != 0);
    assert!(all_set, "the rebuilt bitmap marks every row present");
}

#[test]
fn hot_but_sparse_paths_store_as_sparse_shredded_columns() {
    let rows: Vec<OeRow> = (0..64)
        .map(|i| {
            let mut sparse_row = row(i);
            if i % 4 == 0
                && let PayloadInput::Variant(VariantValue::Object(fields)) = &mut sparse_row.event.payload
            {
                fields.insert("burst".to_owned(), VariantValue::Int(9_000 + i as i64));
            }
            sparse_row
        })
        .collect();
    let built = build_oe_file(rows.clone(), &config()).unwrap();
    assert_ne!(
        built.footer.required_feature_flags & crate::layout::required_features::SPARSE_SHREDDED_COLUMNS,
        0,
        "the sparse tier is governed by its required bit"
    );
    let sparse = built
        .footer
        .sparse_keys
        .iter()
        .find(|entry| entry.path == "burst")
        .expect("burst qualifies for the sparse tier");
    assert!(
        built
            .footer
            .shredded
            .iter()
            .any(|entry| entry.column_id == sparse.column_id && entry.path == "burst"),
        "sparse keys ride the shredded machinery"
    );
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    for i in 0..64u64 {
        assert_eq!(
            file.payload_path(i, "burst").unwrap(),
            (i % 4 == 0).then(|| VariantValue::Int(9_000 + i as i64)),
            "row {i}"
        );
    }
    let again = build_oe_file(rows, &config()).unwrap();
    assert_eq!(built.bytes, again.bytes);
}

#[test]
fn the_sparse_key_budget_leaves_overflow_paths_residual() {
    let rows: Vec<OeRow> = (0..64)
        .map(|i| {
            let mut sparse_row = row(i);
            if let PayloadInput::Variant(VariantValue::Object(fields)) = &mut sparse_row.event.payload {
                for key in 0..20u64 {
                    if i % 4 == key % 4 {
                        fields.insert(format!("sparse-{key:02}"), VariantValue::Int((i * 100 + key) as i64));
                    }
                }
            }
            sparse_row
        })
        .collect();
    let built = build_oe_file(rows, &config()).unwrap();
    assert_eq!(
        built.footer.sparse_keys.len(),
        crate::columns::SPARSE_KEYS_PER_FILE_MAX,
        "the pinned budget bounds the sparse key set"
    );
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    for i in 0..64u64 {
        for key in 0..20u64 {
            assert_eq!(
                file.payload_path(i, &format!("sparse-{key:02}")).unwrap(),
                (i % 4 == key % 4).then(|| VariantValue::Int((i * 100 + key) as i64)),
                "row {i} key {key}"
            );
        }
    }
}

#[test]
fn a_status_like_column_shares_one_file_scope_alphabet() {
    let rows: Vec<OeRow> = (0..64).map(row).collect();
    let built = build_oe_file(rows.clone(), &config()).unwrap();
    assert_ne!(
        built.footer.required_feature_flags & crate::layout::required_features::SHARED_DICTIONARIES,
        0,
        "file-scope alphabets are governed by their required bit"
    );
    let kind_col = crate::columns::column_ids::PROMOTED_BASE;
    let entry = built
        .footer
        .shared_dictionaries
        .iter()
        .find(|entry| entry.column_id == kind_col)
        .expect("the promoted kind column repeats one value set per granule, so it shares");
    assert_eq!(entry.values, vec!["k0", "k1", "k2", "k3"]);
    assert!(
        built.footer.marks.iter().any(|mark| {
            mark.column_id == kind_col
                && mark.codec_pipeline_id.side_stream() == Ok(crate::encoding::SideStream::FileScopeDictionary)
        }),
        "kind blocks record the file dictionary scope"
    );
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    for (ordinal, input_row) in rows.iter().enumerate() {
        let expected = match &input_row.event.payload {
            PayloadInput::Variant(VariantValue::Object(fields)) => fields.get("kind").cloned(),
            _ => None,
        };
        assert_eq!(
            file.payload_path(ordinal as u64, "kind").unwrap(),
            expected,
            "row {ordinal}"
        );
    }
}

fn kind_alphabet_for_granules(granule_kinds: &[&[&str]]) -> Option<Vec<String>> {
    let rows: Vec<OeRow> = granule_kinds
        .iter()
        .enumerate()
        .flat_map(|(granule, kinds)| {
            (0..16u64).map(move |offset| {
                let i = granule as u64 * 16 + offset;
                let mut kind_row = row(i);
                if let PayloadInput::Variant(VariantValue::Object(fields)) = &mut kind_row.event.payload {
                    fields.insert(
                        "kind".to_owned(),
                        VariantValue::String(kinds[offset as usize % kinds.len()].to_owned()),
                    );
                }
                kind_row
            })
        })
        .collect();
    let built = build_oe_file(rows, &config()).unwrap();
    built
        .footer
        .shared_dictionaries
        .iter()
        .find(|entry| entry.column_id == crate::columns::column_ids::PROMOTED_BASE)
        .map(|entry| entry.values.clone())
}

#[test]
fn shared_alphabet_discovery_counts_matches_before_the_first_divergence() {
    let abc: &[&str] = &["a", "b", "c"];
    let cba: &[&str] = &["c", "b", "a"];
    let other: &[&str] = &["a", "b", "z"];
    let expected = Some(vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]);
    assert_eq!(
        kind_alphabet_for_granules(&[abc, cba]),
        expected,
        "row order does not matter"
    );
    assert_eq!(kind_alphabet_for_granules(&[abc, cba, abc, abc]), expected);
    assert_eq!(
        kind_alphabet_for_granules(&[abc, abc, other]),
        expected,
        "a late divergence keeps the earlier matches"
    );
    assert_eq!(
        kind_alphabet_for_granules(&[abc, other, abc]),
        None,
        "an early divergence leaves one match"
    );
    assert_eq!(
        kind_alphabet_for_granules(&[abc]),
        None,
        "a single granule never shares"
    );
}

#[test]
fn a_sealed_file_gains_a_membership_artifact_without_changing_a_byte() {
    use crate::indexes::artifact::{ArtifactKind, IndexArtifact, build_membership_artifact, plan_artifact_builds};
    use crate::indexes::pruning::{FilterClause, PredicateKind, StatVerdict};
    use crate::indexes::stable_hash;

    let rows: Vec<OeRow> = (0..64).map(row).collect();
    let built = build_oe_file(rows, &config()).unwrap();
    let before = built.bytes.clone();
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();

    let plans = plan_artifact_builds(&file);
    assert!(
        !plans.is_empty(),
        "promoted columns are the heat signal that gates building"
    );
    let (kind, column_id) = plans[0];
    assert_eq!(kind, ArtifactKind::BinaryFuse);

    let artifact = build_membership_artifact(&file, column_id, 3).unwrap();
    assert_eq!(
        built.bytes, before,
        "building an artifact reads the file, never rewrites it"
    );
    let encoded = artifact.encode();
    assert_eq!(IndexArtifact::decode(&encoded).unwrap(), artifact);
    assert!(artifact.usable_for(file.header().file_id, &file.footer().schema_fingerprint, 3));
    assert!(
        artifact.usable_for(file.header().file_id, &file.footer().schema_fingerprint, 9),
        "an inexact membership filter survives newer deletion-vector generations"
    );

    let clause = |value: u64| FilterClause {
        column_id,
        predicate: PredicateKind::Equality,
        value_hi: value as i128,
        value_lo: value as i128,
    };
    for granule in &file.footer().granules {
        let present = artifact
            .term_for(granule.granule_id, &clause(stable_hash(b"k1")))
            .expect("covered granule");
        assert_eq!(
            present.verdict(),
            StatVerdict::CouldMatch,
            "granule {}",
            granule.granule_id
        );
        let absent = artifact
            .term_for(granule.granule_id, &clause(stable_hash(b"never-a-kind")))
            .expect("covered granule");
        assert_eq!(
            absent.verdict(),
            StatVerdict::ProvenAbsent,
            "granule {}",
            granule.granule_id
        );
    }
    let past_the_file = file.footer().granules.last().unwrap().granule_id + 100;
    assert!(
        artifact.term_for(past_the_file, &clause(stable_hash(b"k1"))).is_none(),
        "an uncovered granule keeps its footer-tier terms"
    );
}

#[test]
fn a_streamed_build_is_byte_identical_to_the_materialized_one() {
    let rows: Vec<OeRow> = (0..64).map(row).collect();
    let materialized = build_oe_file(rows.clone(), &config()).unwrap();

    let mut streamed_bytes = vec![0u8; materialized.bytes.len()];
    let mut ranges: Vec<(u64, usize)> = Vec::new();
    let streamed = build_oe_file_streamed(rows, &config(), &SerialEncodeExecutor, &mut |offset, bytes| {
        ranges.push((offset, bytes.len()));
        let at = offset as usize;
        streamed_bytes[at..at + bytes.len()].copy_from_slice(&bytes);
    })
    .unwrap();

    assert!(
        ranges.len() >= 4,
        "stripes, footer region, trailer, and header arrive as separate ranges"
    );
    assert_eq!(
        ranges.last(),
        Some(&(0, HEADER_BLOCK_LEN)),
        "the header is the last range out"
    );
    let stripe_ranges: Vec<(u64, usize)> = materialized
        .footer
        .stripes
        .iter()
        .map(|stripe| (stripe.file_offset, stripe.byte_len as usize))
        .collect();
    assert!(
        stripe_ranges
            .iter()
            .all(|(offset, len)| ranges.iter().any(|(at, span)| at == offset && span >= len)),
        "every stripe arrives as its own range"
    );
    let mut sorted = ranges.clone();
    sorted.sort_unstable();
    let mut expected_start = 0u64;
    for (offset, len) in sorted {
        assert_eq!(
            offset, expected_start,
            "the ranges partition the file without gaps or overlaps"
        );
        expected_start = offset + len as u64;
    }
    assert_eq!(expected_start, streamed.total_len);
    assert_eq!(streamed_bytes, *materialized.bytes);
    assert_eq!(streamed.file_id, materialized.file_id);
    assert_eq!(streamed.file_seal, materialized.file_seal);
    assert_eq!(streamed.file_crc64_nvme, materialized.file_crc64_nvme);
    assert_eq!(streamed.total_len, materialized.bytes.len() as u64);
    assert_eq!(streamed.footer.stripe_checksums, materialized.footer.stripe_checksums);

    let reopened = OeFile::open(streamed_bytes, Some(&streamed.file_seal)).unwrap();
    assert_eq!(reopened.header().row_count, 64);
}

#[test]
#[ignore = "release-mode IMP-007 phase benchmark"]
fn benchmark_granule_construction_over_mostly_residual_payloads() {
    use crate::invariants::sim::SerialEncodeExecutor;
    let rows: Vec<OeRow> = (0..50_000)
        .map(|i| {
            let mut wide = row(i);
            let PayloadInput::Variant(VariantValue::Object(fields)) = &mut wide.event.payload else {
                unreachable!("row payload is a variant object");
            };
            fields.remove("rare");
            fields.insert("ratio".to_owned(), VariantValue::Float(1.5 + i as f32));
            fields.insert("weight".to_owned(), VariantValue::Float(0.25 * i as f32));
            fields.insert(
                "detail".to_owned(),
                VariantValue::Object(BTreeMap::from([
                    ("channel".to_owned(), VariantValue::String("web".to_owned())),
                    ("retries".to_owned(), VariantValue::Int(i as i64 % 3)),
                ])),
            );
            fields.insert(
                "meta".to_owned(),
                VariantValue::Object(BTreeMap::from([(
                    "locale".to_owned(),
                    VariantValue::String("en-gb".to_owned()),
                )])),
            );
            fields.insert(
                "tags".to_owned(),
                VariantValue::Array(vec![
                    VariantValue::String("a".to_owned()),
                    VariantValue::String("b".to_owned()),
                ]),
            );
            fields.insert(
                "history".to_owned(),
                VariantValue::Array(vec![VariantValue::Int(i as i64)]),
            );
            wide
        })
        .collect();
    let mut cfg = config();
    cfg.targets.index_granularity = 8_192;
    cfg.targets.index_granularity_bytes = usize::MAX;
    cfg.targets.stripe_target_bytes = 8 * 1024 * 1024;

    let mut samples = Vec::new();
    for _ in 0..7 {
        let (built, profile) = build_oe_file_profiled_with_executor(rows.clone(), &cfg, &SerialEncodeExecutor).unwrap();
        assert_eq!(
            built.footer.shredded.len(),
            1,
            "only amount shreds; the rest of the payload stays residual: {:?}",
            built.footer.shredded,
        );
        samples.push(
            profile
                .phases
                .iter()
                .find(|(phase, _)| *phase == BuildPhase::GranuleConstruction)
                .map(|(_, nanos)| *nanos)
                .unwrap_or_default(),
        );
        std::hint::black_box(built.bytes.len());
    }
    samples.sort_unstable();
    eprintln!(
        "IMP-007 granule construction over 50,000 mostly-residual rows: {samples:?}; median={}ns",
        samples.get(3).copied().unwrap_or_default(),
    );
}

#[test]
fn normalization_lays_payload_values_out_in_shape_order() {
    let mut rows = OeRow::into_build_rows(vec![row(0), row(1), row(2), row(3)]);
    rows[0].payload = BuildPayload::Object(fields_of(&BTreeMap::from([
        ("kind".to_owned(), VariantValue::String("renewal".to_owned())),
        ("amount".to_owned(), VariantValue::Int(1)),
    ])));
    rows[1].payload = BuildPayload::Whole(VariantValue::Int(9));
    rows[2].payload = BuildPayload::ExternalRef("blob://payload/2".to_owned());
    rows[3].payload = BuildPayload::None;

    let all_rows = [(0, rows.len())];
    let payloads = NormalizedPayloads::normalize(&mut rows, &all_rows).unwrap();

    let object = payloads.payload(0);
    assert_eq!(payloads.field_names(object).collect::<Vec<_>>(), ["amount", "kind"]);
    assert_eq!(
        payloads.values(object),
        [VariantValue::Int(1), VariantValue::String("renewal".to_owned())].as_slice()
    );
    assert_eq!(payloads.payload(1), NormalizedPayload::Whole { batch: 0, start: 2 });
    assert_eq!(payloads.values(payloads.payload(1)), [VariantValue::Int(9)].as_slice());
    assert_eq!(payloads.payload(2), NormalizedPayload::ExternalRef);
    assert!(payloads.values(payloads.payload(2)).is_empty());
    assert_eq!(payloads.payload(3), NormalizedPayload::None);
    assert!(payloads.values(payloads.payload(3)).is_empty());

    assert_eq!(rows[0].payload, BuildPayload::None);
    assert_eq!(rows[1].payload, BuildPayload::None);
    assert_eq!(
        rows[2].payload,
        BuildPayload::ExternalRef("blob://payload/2".to_owned())
    );
    assert_eq!(rows[3].payload, BuildPayload::None);
}

#[test]
fn event_rows_convert_to_build_rows_sharing_their_strings() {
    let rows: Vec<OeRow> = (0..8).map(row).collect();
    let mut shared = SharedStrings::default();
    let first = shared.build_row(rows[0].clone());
    let third = shared.build_row(rows[2].clone());
    assert!(Arc::ptr_eq(&first.envelope.source, &third.envelope.source));
    assert!(Arc::ptr_eq(&first.envelope.event_type, &third.envelope.event_type));
    let BuildPayload::Object(first_fields) = &first.payload else {
        panic!("test rows carry object payloads");
    };
    let BuildPayload::Object(third_fields) = &third.payload else {
        panic!("test rows carry object payloads");
    };
    assert_eq!(
        first_fields.iter().map(|(name, _)| &**name).collect::<Vec<_>>(),
        ["amount", "kind", "note", "rare"]
    );
    assert!(Arc::ptr_eq(&first_fields[0].0, &third_fields[0].0));
    assert_eq!(first.envelope.entity_id.as_deref(), Some("opp-0"));

    let converted = OeRow::into_build_rows(rows.clone());
    assert_eq!(converted[0], first);
    assert_eq!(converted[2], third);
    let from_events = build_oe_file(rows, &config()).unwrap();
    let from_build_rows = build_oe_file(converted, &config()).unwrap();
    assert_eq!(from_build_rows.bytes, from_events.bytes);
}

#[test]
fn a_payload_field_list_out_of_key_order_is_refused() {
    let mut rows = OeRow::into_build_rows(vec![row(0), row(1)]);
    rows[1].payload = BuildPayload::Object(vec![
        (FieldName::from("kind"), VariantValue::String("k1".to_owned())),
        (FieldName::from("amount"), VariantValue::Int(1)),
    ]);
    let unsorted = build_oe_file(rows.clone(), &config()).unwrap_err();
    assert!(matches!(unsorted, FormatError::Structural { rule } if rule.contains("ascending key order")));

    rows[1].payload = BuildPayload::Object(vec![
        (FieldName::from("amount"), VariantValue::Int(1)),
        (FieldName::from("amount"), VariantValue::Int(2)),
    ]);
    let repeated = build_oe_file(rows, &config()).unwrap_err();
    assert!(matches!(repeated, FormatError::Structural { rule } if rule.contains("ascending key order")));
}

#[test]
fn every_variant_kind_round_trips_through_the_normalized_representation() {
    let nested = VariantValue::Object(BTreeMap::from([
        ("depth".to_owned(), VariantValue::Int(2)),
        (
            "tags".to_owned(),
            VariantValue::Array(vec![VariantValue::String("a".to_owned()), VariantValue::Null]),
        ),
    ]));
    let every_kind = VariantValue::Object(BTreeMap::from([
        (
            "array".to_owned(),
            VariantValue::Array(vec![VariantValue::Int(1), VariantValue::Bool(false)]),
        ),
        ("binary".to_owned(), VariantValue::Binary(vec![0, 1, 254, 255])),
        ("bool".to_owned(), VariantValue::Bool(true)),
        (
            "decimal".to_owned(),
            VariantValue::Decimal {
                unscaled: -12_345,
                scale: 3,
            },
        ),
        ("double".to_owned(), VariantValue::Double(-1.5)),
        ("float".to_owned(), VariantValue::Float(0.25)),
        ("int".to_owned(), VariantValue::Int(-9_007_199_254_740_993)),
        ("null".to_owned(), VariantValue::Null),
        ("object".to_owned(), nested),
        (
            "string".to_owned(),
            VariantValue::String("a string long enough to leave the short-string form behind".to_owned()),
        ),
        (
            "timestamp".to_owned(),
            VariantValue::Timestamp(TimestampValue::from_physical_nanos(1_700_000_000_000_000_000)),
        ),
        (
            "uuid".to_owned(),
            VariantValue::Uuid(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef),
        ),
    ]));
    let payloads = [
        PayloadInput::Variant(every_kind),
        PayloadInput::Variant(VariantValue::Array(vec![VariantValue::Int(7)])),
        PayloadInput::Variant(VariantValue::String("bare".to_owned())),
        PayloadInput::ExternalRef("blob://payload/1".to_owned()),
        PayloadInput::None,
        PayloadInput::Variant(VariantValue::Object(BTreeMap::from([(
            "int".to_owned(),
            VariantValue::Int(5),
        )]))),
    ];
    let rows: Vec<OeRow> = payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| {
            let mut row = row(index as u64);
            row.event.payload = payload.clone();
            row
        })
        .collect();

    let built = build_oe_file(rows.clone(), &config()).unwrap();
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    for (ordinal, payload) in payloads.iter().enumerate() {
        let read = file.payload(ordinal as u64).unwrap();
        match payload {
            PayloadInput::Variant(expected) => {
                let PayloadRead::Value(value) = read else {
                    panic!("row {ordinal} reads back as a value");
                };
                assert_eq!(&value, expected, "row {ordinal}");
            }
            PayloadInput::ExternalRef(expected) => {
                assert_eq!(read, PayloadRead::External(expected.clone()), "row {ordinal}");
            }
            PayloadInput::None => assert_eq!(read, PayloadRead::None, "row {ordinal}"),
            PayloadInput::Encoded(_) => unreachable!("the fixture carries no encoded payloads"),
        }
    }

    let again = build_oe_file(rows, &config()).unwrap();
    assert_eq!(again.bytes, built.bytes);
    assert_eq!(again.file_id, built.file_id);
}

#[test]
fn entity_hash_filters_never_rule_out_the_granule_holding_an_id() {
    let rows: Vec<OeRow> = (0..128).map(row).collect();
    let mut cfg = config();
    cfg.targets.index_granularity = 16;
    let built = build_oe_file(rows.clone(), &cfg).unwrap();
    assert_ne!(
        built.footer.optional_feature_flags & optional_features::ENTITY_HASH_POINT_FILTERS,
        0,
        "a file whose filters fit the budget declares the feature",
    );
    assert_eq!(
        built.footer.entity_hash_filters.len(),
        built.footer.granules.len(),
        "one filter per granule",
    );
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();

    let mut candidates_seen = 0usize;
    for (ordinal, source) in rows.iter().enumerate() {
        let holder = file
            .footer()
            .granules
            .iter()
            .find(|granule| {
                let first = granule.first_row_ordinal;
                (first..first + u64::from(granule.row_count)).contains(&(ordinal as u64))
            })
            .expect("every row sits in a granule");
        let candidates = file.granules_for_entity_hash(source.event.envelope.entity_id_hash_low);
        assert!(
            candidates.iter().any(|granule| granule.granule_id == holder.granule_id),
            "row {ordinal} lives in granule {} but its hash was ruled out of it",
            holder.granule_id,
        );
        candidates_seen += candidates.len();
    }
    assert!(
        candidates_seen < rows.len() * 2,
        "a present hash should usually name its own granule and no other, not {candidates_seen} across {} lookups",
        rows.len(),
    );
}

#[test]
fn entity_hash_filters_rule_out_granules_for_ids_the_file_does_not_hold() {
    let rows: Vec<OeRow> = (0..128).map(row).collect();
    let mut cfg = config();
    cfg.targets.index_granularity = 16;
    let built = build_oe_file(rows, &cfg).unwrap();
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();

    let absent: Vec<u64> = (1_000..1_512).collect();
    let fully_pruned = absent
        .iter()
        .filter(|hash| file.granules_for_entity_hash(**hash).is_empty())
        .count();
    assert!(
        fully_pruned * 100 >= absent.len() * 95,
        "only {fully_pruned} of {} absent hashes read nothing",
        absent.len(),
    );
}

#[test]
fn entity_hash_filters_keep_every_granule_sharing_a_colliding_hash() {
    const COLLIDING: u64 = 0x00c0_11ee;
    let mut rows: Vec<OeRow> = (0..64).map(row).collect();
    for (index, ordinal) in [3usize, 40].into_iter().enumerate() {
        let envelope = &mut rows[ordinal].event.envelope;
        envelope.entity_id_hash_low = COLLIDING;
        envelope.entity_id = Some(format!("collides-{index}"));
    }
    let mut cfg = config();
    cfg.targets.index_granularity = 16;
    let built = build_oe_file(rows, &cfg).unwrap();
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();

    let candidates = file.granules_for_entity_hash(COLLIDING);
    let ids: Vec<u32> = candidates.iter().map(|granule| granule.granule_id).collect();
    assert!(
        ids.contains(&0) && ids.contains(&2),
        "both granules carrying the colliding hash must survive pruning, got {ids:?}",
    );
}

#[test]
fn repeated_entity_ids_size_the_filters_by_entity_not_by_row() {
    let mut cfg = config();
    cfg.targets.index_granularity = 256;
    let distinct: Vec<OeRow> = (0..1024).map(row).collect();
    let repeated: Vec<OeRow> = (0..1024)
        .map(|index| {
            let mut built = row(index);
            built.event.envelope.entity_id_hash_low = index % 4;
            built.event.envelope.entity_id = Some(format!("opp-{}", index % 4));
            built
        })
        .collect();

    let filter_bytes = |rows: Vec<OeRow>| -> usize {
        let built = build_oe_file(rows, &cfg).unwrap();
        built
            .footer
            .entity_hash_filters
            .iter()
            .map(|entry| entry.index_len as usize)
            .sum()
    };
    let distinct_bytes = filter_bytes(distinct);
    let repeated_bytes = filter_bytes(repeated);
    assert!(
        repeated_bytes * 4 < distinct_bytes,
        "repeated ids cost {repeated_bytes} bytes against {distinct_bytes} for all-distinct ids",
    );
}

#[test]
fn an_encrypted_footer_still_prunes_on_the_entity_hash_filters() {
    let dek = [0x27u8; 32];
    let mut cfg = config();
    cfg.targets.index_granularity = 16;
    cfg.footer_encryption = crate::security::FooterEncryption::Encrypted;
    cfg.footer_dek = Some(dek);
    let rows: Vec<OeRow> = (0..128).map(row).collect();
    let built = build_oe_file(rows, &cfg).unwrap();
    let file = OeFile::open_with_keys(built.bytes.clone(), Some(&built.file_seal), Some(&dek)).unwrap();

    assert_eq!(file.footer().entity_hash_filters.len(), file.footer().granules.len());
    let held = file.granules_for_entity_hash(70);
    assert_eq!(
        held.iter().map(|granule| granule.granule_id).collect::<Vec<_>>(),
        vec![4],
        "row 70 sits in granule 4 and no other granule's filter admits its hash",
    );
    assert!(file.granules_for_entity_hash(1_000).is_empty());
}

fn graph_chain(rows: u64) -> GraphInput {
    GraphInput {
        chain_cover: None,
        depth: None,
        first_sn: 0,
        inbound: Vec::new(),
        rows: (0..rows)
            .map(|sn| RowEdges {
                auth: Vec::new(),
                prev: sn.checked_sub(1).into_iter().collect(),
            })
            .collect(),
        state_roots: BTreeMap::new(),
        subject: b"room".to_vec(),
        unresolved: Vec::new(),
    }
}

#[test]
fn graph_edges_are_built_only_into_a_whole_graph_profile_file_in_sequence_order() {
    let rows: Vec<OeRow> = (0..4).map(row).collect();
    let mut graph = config();
    graph.profile = Profile::Graph;
    graph.graph = Some(graph_chain(4));
    let built = build_oe_file(rows.clone(), &graph).unwrap();
    assert_eq!(
        built.footer.required_feature_flags & required_features::GRAPH,
        required_features::GRAPH_DENSE_IDS
            | required_features::GRAPH_CSR_FORWARD
            | required_features::GRAPH_CSR_REVERSE
    );

    let mut without_edges = graph.clone();
    without_edges.graph = None;
    let built = build_oe_file(rows.clone(), &without_edges).unwrap();
    assert_eq!(built.footer.required_feature_flags & required_features::GRAPH, 0);
    assert!(built.footer.graph.is_none());

    let mut event_profile = graph.clone();
    event_profile.profile = Profile::Event;
    let mut short = graph.clone();
    short.graph = Some(graph_chain(3));
    for refused in [event_profile, short] {
        assert!(matches!(
            build_oe_file(rows.clone(), &refused),
            Err(FormatError::Structural { .. })
        ));
    }
    let entity_order = build_oe_file_in_order(
        OeRow::into_build_rows(rows),
        &graph,
        &SerialEncodeExecutor,
        RowOrder::Entity,
    );
    assert!(matches!(entity_order, Err(FormatError::Structural { .. })));
}

fn snapshot_rows(keys: &[Option<&str>]) -> Vec<OeRow> {
    keys.iter()
        .enumerate()
        .map(|(i, key)| {
            let mut row = row(i as u64);
            row.event.envelope.entity_id = key.map(str::to_owned);
            row
        })
        .collect()
}

#[test]
fn a_snapshot_build_indexes_its_keys_and_refuses_rows_it_cannot_hold() {
    let mut cfg = config();
    cfg.profile = Profile::Snapshot;
    cfg.targets.stripe_target_bytes = usize::MAX;
    let keys: Vec<String> = (0..100).map(|i| format!("opp-{i:04}")).collect();
    let sorted: Vec<Option<&str>> = keys.iter().map(|key| Some(key.as_str())).collect();
    let built = build_oe_file(snapshot_rows(&sorted), &cfg).unwrap();
    assert!(built.footer.granules.len() > 1);
    assert_eq!(built.footer.stripes.len(), built.footer.granules.len());
    assert_ne!(
        built.footer.optional_feature_flags & optional_features::SNAPSHOT_KEY_INDEX,
        0
    );
    let index = built.footer.snapshot_keys.as_ref().unwrap();
    assert_eq!(index.granule_first_keys.first().map(String::as_str), Some("opp-0000"));

    let mut event = cfg.clone();
    event.profile = Profile::Event;
    let built = build_oe_file(snapshot_rows(&sorted), &event).unwrap();
    assert!(built.footer.snapshot_keys.is_none());
    assert_eq!(
        built.footer.optional_feature_flags & optional_features::SNAPSHOT_KEY_INDEX,
        0
    );

    for keys in [
        vec![Some("b"), Some("a")],
        vec![Some("a"), Some("a")],
        vec![Some("a"), None],
    ] {
        assert!(build_oe_file(snapshot_rows(&keys), &cfg).is_err(), "{keys:?}");
    }
    let mut two_epochs = snapshot_rows(&[Some("a"), Some("b")]);
    two_epochs[1].epoch = 2;
    two_epochs[1].sequence = 1;
    assert!(build_oe_file(two_epochs, &cfg).is_err());
}

#[test]
fn a_time_series_build_refuses_rows_that_go_back_in_time_and_widths_it_cannot_use() {
    let mut cfg = config();
    cfg.profile = Profile::TimeSeries;
    cfg.promotion.columns.push(PromotedColumn {
        kind: ColumnKind::F64,
        name: "load".to_owned(),
        path: "load".to_owned(),
        since_schema_version: 1,
        substring_searchable: false,
    });
    let built = build_oe_file((0..40).map(row).collect(), &cfg).unwrap();
    let rollups = built.footer.time_series_rollups.as_ref().unwrap();
    assert_eq!(rollups.bucket_nanos, DEFAULT_TIME_SERIES_BUCKET_NANOS);
    assert_eq!(rollups.column_ids, vec![column_ids::PROMOTED_BASE + 1]);
    assert_eq!(rollups.row_count(), 40);

    let mut late: Vec<OeRow> = (0..40).map(row).collect();
    late[20].event.envelope.occurred_at = TimestampValue::from_physical_nanos(0);
    assert_eq!(
        build_oe_file(late, &cfg).unwrap_err(),
        FormatError::Structural {
            rule: "a time-series file's rows never go back in occurred_at"
        }
    );
    for width in [0, -5] {
        cfg.time_series_bucket_nanos = Some(width);
        assert!(build_oe_file((0..4).map(row).collect(), &cfg).is_err());
    }
    let mut event = config();
    event.time_series_bucket_nanos = Some(1_000);
    assert_eq!(
        build_oe_file((0..4).map(row).collect(), &event).unwrap_err(),
        FormatError::Structural {
            rule: "only a time-series file takes a rollup bucket width"
        }
    );
}

#[test]
fn built_files_summarize_the_entities_they_hold() {
    use crate::indexes::entity_summary::EntityKey;
    let cfg = sparse_config();
    let before: Vec<OeRow> = (0..400).map(row).collect();
    let source = build_oe_file(before.clone(), &cfg).unwrap();
    assert!(!source.entity_summary.is_exact());
    assert_eq!(source.entity_summary.entity_count(), 400);
    assert!((0..400).all(|i| source.entity_summary.may_hold(EntityKey::new(1, i))));
    let strangers = (0..10_000u64)
        .filter(|i| source.entity_summary.may_hold(EntityKey::new(2, *i)))
        .count();
    assert!(strangers < 500, "{strangers} false positives");

    let mut after = before.clone();
    let mut rng = Lcg(7);
    mutate(&mut after[3], &mut rng, false, 3);
    let changes = changes_for(&before, &after, &[3]);
    let updated =
        update_oe_file_from_changes_with_executor(&source, after.clone(), &changes, &cfg, &SerialEncodeExecutor)
            .unwrap();
    assert_eq!(updated.entity_summary, source.entity_summary);

    let small = build_oe_file((0..8).map(row).collect(), &config()).unwrap();
    let keys: Vec<EntityKey> = (0..8).map(|i| EntityKey::new(1, i)).collect();
    assert_eq!(small.entity_summary.exact_keys(), Some(keys.as_slice()));
    assert!(!small.entity_summary.may_hold(EntityKey::new(1, 8)));
}

#[test]
fn granule_bounds_come_from_one_pass_that_agrees_with_separate_folds() {
    assert_eq!(min_max(std::iter::empty::<i64>()), None);
    assert_eq!(min_max([7i64]), Some((7, 7)));
    let values = [5i64, -3, i64::MAX, 0, i64::MIN, 9];
    for len in 1..=values.len() {
        let slice = &values[..len];
        assert_eq!(
            min_max(slice.iter().copied()),
            Some((*slice.iter().min().unwrap(), *slice.iter().max().unwrap()))
        );
    }
    let keys = [(2u64, 1u64), (1, 9), (2, 0), (1, 3)];
    assert_eq!(min_max(keys), Some(((1, 3), (2, 1))));

    let built = build_oe_file((0..40).map(row).collect(), &config()).unwrap();
    for granule in &built.footer.granules {
        assert!(granule.min_occurred_at_physical <= granule.max_occurred_at_physical);
        assert!(granule.min_ingested_at_physical <= granule.max_ingested_at_physical);
        assert!((granule.first_epoch, granule.first_sequence) <= (granule.last_epoch, granule.last_sequence));
    }
}

#[test]
fn a_built_file_carries_its_entity_summary_for_a_reader_holding_only_the_file() {
    use crate::writer::merge::tests::{build_config, rows_of};
    let built = build_oe_file(rows_of(1..=20), &build_config()).unwrap();
    assert_ne!(
        built.footer.optional_feature_flags & crate::layout::optional_features::ENTITY_SUMMARY,
        0
    );
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    assert_eq!(file.entity_summary(), Some(&built.entity_summary));
}

fn rows_with_shared_entities(count: u64, distinct: u64) -> Vec<OeRow> {
    (0..count)
        .map(|i| {
            let mut entity_row = row(i);
            entity_row.event.envelope.entity_id = Some(format!("entity-{:04}", (i * 37) % distinct));
            entity_row
        })
        .collect()
}

#[test]
fn recurring_entity_ids_share_one_compressed_union_alphabet() {
    use crate::encoding::predicate::{StringPredicate, count_string_block_shared, filter_string_block_shared};
    use crate::encoding::{decode_block_range_shared, take_block_rows_shared};
    let config = || {
        let mut config = config();
        config.targets.index_granularity = 1024;
        config
    };
    let rows = rows_with_shared_entities(4096, 1500);
    let built = build_oe_file(rows.clone(), &config()).unwrap();
    let entity_col = crate::columns::column_ids::ENTITY_ID;
    assert_ne!(
        built.footer.required_feature_flags & crate::layout::required_features::PACKED_SHARED_DICTIONARIES,
        0,
        "a compressed alphabet is governed by its required bit"
    );
    assert!(
        built
            .footer
            .shared_dictionaries
            .iter()
            .all(|entry| entry.column_id != entity_col)
    );
    let packed = built
        .footer
        .packed_shared_dictionaries
        .iter()
        .find(|entry| entry.column_id == entity_col)
        .expect("recurring entity ids share a union alphabet");
    let mut expected_alphabet: Vec<String> = (0..1500).map(|id| format!("entity-{id:04}")).collect();
    expected_alphabet.sort();
    assert_eq!(packed.values().unwrap(), expected_alphabet);
    assert!(packed.compressed.len() < expected_alphabet.iter().map(String::len).sum::<usize>());
    let entity_marks: Vec<_> = built
        .footer
        .marks
        .iter()
        .filter(|mark| mark.column_id == entity_col)
        .collect();
    assert!(entity_marks.len() > 1);
    assert!(
        entity_marks
            .iter()
            .all(|mark| mark.codec_pipeline_id.side_stream() == Ok(crate::encoding::SideStream::FileScopeDictionary)),
        "every entity block stores codes against the file alphabet"
    );

    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    let wanted = "entity-0042".to_owned();
    let predicate = StringPredicate::Equals(wanted.clone());
    let translated = file.shared_dictionary_predicate(entity_col, &predicate).unwrap();
    let mut found = 0usize;
    for granule in &file.footer().granules {
        let first = granule.first_row_ordinal as usize;
        let ids: Vec<&str> = rows[first..first + granule.row_count as usize]
            .iter()
            .map(|r| r.event.envelope.entity_id.as_deref().unwrap())
            .collect();
        let read = file.read_column(entity_col, granule.granule_id).unwrap();
        let ColumnData::Strings(values) = &read.data else {
            panic!("entity ids decode as strings")
        };
        assert_eq!(values.iter().map(Option::unwrap).collect::<Vec<_>>(), ids);
        let (_, views) = file
            .read_column_string_views(entity_col, granule.granule_id)
            .unwrap()
            .unwrap();
        assert_eq!(views.iter().map(Option::unwrap).collect::<Vec<_>>(), ids);

        let raw = file.read_column_raw(entity_col, granule.granule_id).unwrap();
        let alphabet = file.shared_alphabet(entity_col);
        let ColumnData::Strings(range) = decode_block_range_shared(raw.pipeline, &raw.body, alphabet, 3, 9).unwrap()
        else {
            panic!("a range of entity ids decodes as strings")
        };
        assert_eq!(range.iter().map(Option::unwrap).collect::<Vec<_>>(), ids[3..9]);
        let ColumnData::Strings(taken) =
            take_block_rows_shared(raw.pipeline, &raw.body, alphabet, &[0, 5, 15]).unwrap()
        else {
            panic!("taken entity ids are strings")
        };
        assert_eq!(
            taken.iter().map(Option::unwrap).collect::<Vec<_>>(),
            [ids[0], ids[5], ids[15]]
        );

        let mask = filter_string_block_shared(raw.pipeline, &raw.body, &predicate, Some(&translated))
            .unwrap()
            .expect("a shared-scope block answers equality from its codes");
        assert_eq!(mask, ids.iter().map(|id| *id == wanted).collect::<Vec<_>>());
        let count = count_string_block_shared(raw.pipeline, &raw.body, &predicate, Some(&translated))
            .unwrap()
            .unwrap();
        assert_eq!(count, mask.iter().filter(|hit| **hit).count());
        found += count;
    }
    assert_eq!(
        found,
        rows.iter()
            .filter(|r| r.event.envelope.entity_id.as_deref() == Some("entity-0042"))
            .count()
    );
    let absent = StringPredicate::Equals("entity-9999".to_owned());
    let translated = file.shared_dictionary_predicate(entity_col, &absent).unwrap();
    let raw = file.read_column_raw(entity_col, 0).unwrap();
    let mask = filter_string_block_shared(raw.pipeline, &raw.body, &absent, Some(&translated))
        .unwrap()
        .unwrap();
    assert!(mask.iter().all(|hit| !hit));

    assert_eq!(build_oe_file(rows, &config()).unwrap().bytes, built.bytes);
}

#[test]
fn entity_ids_that_never_recur_keep_block_local_encodings() {
    let rows: Vec<OeRow> = (0..512).map(row).collect();
    let built = build_oe_file(rows, &config()).unwrap();
    assert!(built.footer.packed_shared_dictionaries.is_empty());
    assert_eq!(
        built.footer.required_feature_flags & crate::layout::required_features::PACKED_SHARED_DICTIONARIES,
        0
    );
}

#[test]
fn updates_over_a_compressed_entity_alphabet_reuse_only_while_it_is_unchanged() {
    let config = || {
        let mut config = config();
        config.targets.index_granularity = 1024;
        config
    };
    let before = rows_with_shared_entities(4096, 1500);
    let source = build_oe_file(before.clone(), &config()).unwrap();
    assert!(!source.footer.packed_shared_dictionaries.is_empty());
    let entity_col = crate::columns::column_ids::ENTITY_ID;
    let check = |updated: &BuiltOe, after: &[OeRow]| {
        let full = build_oe_file(after.to_vec(), &config()).unwrap();
        assert_eq!(
            updated.footer.packed_shared_dictionaries,
            full.footer.packed_shared_dictionaries
        );
        let actual = OeFile::open(updated.bytes.clone(), Some(&updated.file_seal)).unwrap();
        for granule in &actual.footer().granules {
            let ColumnData::Strings(values) = actual.read_column(entity_col, granule.granule_id).unwrap().data else {
                panic!("entity ids decode as strings")
            };
            let first = granule.first_row_ordinal as usize;
            let expected: Vec<&str> = after[first..first + granule.row_count as usize]
                .iter()
                .map(|r| r.event.envelope.entity_id.as_deref().unwrap())
                .collect();
            assert_eq!(values.iter().map(Option::unwrap).collect::<Vec<_>>(), expected);
        }
    };

    let mut after = before.clone();
    after[5].event.envelope.entity_id = before[6].event.envelope.entity_id.clone();
    let updated =
        update_oe_file_with_executor(&source, &before, after.clone(), &config(), &SerialEncodeExecutor).unwrap();
    assert!(updated.reused_source_blocks > 0);
    check(&updated, &after);

    let mut after = before.clone();
    after[5].event.envelope.entity_id = Some("entity-zzzz".to_owned());
    let updated =
        update_oe_file_with_executor(&source, &before, after.clone(), &config(), &SerialEncodeExecutor).unwrap();
    check(&updated, &after);
}

fn with_payload_edit(
    before: &[OeRow],
    ordinal: usize,
    edit: impl FnOnce(&mut BTreeMap<String, VariantValue>),
) -> Vec<OeRow> {
    let mut after = before.to_vec();
    edit(payload_of(&mut after[ordinal]));
    after
}

fn assert_update_matches_a_fresh_build(before: Vec<OeRow>, after: Vec<OeRow>, cfg: &OeBuildConfig) {
    let source = build_oe_file(before.clone(), cfg).unwrap();
    let full = build_oe_file(after.clone(), cfg).unwrap();
    assert_eq!(source.footer.shredded, full.footer.shredded);
    let updated = update_oe_file_with_executor(&source, &before, after, cfg, &SerialEncodeExecutor).unwrap();
    assert!(updated.reused_source_blocks > 0);
    assert_same_content(&full, &updated);
}

#[test]
fn an_update_that_changes_a_shredded_values_kind_rebuilds_the_residual() {
    let cfg = sparse_config();
    let ints: Vec<OeRow> = (0..64).map(row).collect();
    let source = build_oe_file(ints.clone(), &cfg).unwrap();
    assert!(source.footer.shredded.iter().any(|entry| entry.path == "amount"));
    let text = with_payload_edit(&ints, 21, |payload| {
        payload.insert("amount".to_owned(), VariantValue::String("unknown".to_owned()));
    });

    assert_update_matches_a_fresh_build(ints.clone(), text.clone(), &cfg);
    assert_update_matches_a_fresh_build(text.clone(), ints, &cfg);
    let removed = with_payload_edit(&text, 21, |payload| {
        payload.remove("amount");
    });
    assert_update_matches_a_fresh_build(text, removed, &cfg);
}

#[test]
fn an_update_that_makes_a_free_text_field_a_non_string_rebuilds_the_residual() {
    let cfg = sparse_config();
    let strings: Vec<OeRow> = (0..64).map(row).collect();
    let number = with_payload_edit(&strings, 40, |payload| {
        payload.insert("note".to_owned(), VariantValue::Int(7));
    });

    assert_update_matches_a_fresh_build(strings.clone(), number.clone(), &cfg);
    assert_update_matches_a_fresh_build(number, strings, &cfg);
}

#[test]
fn an_update_that_changes_a_shredded_decimals_scale_rebuilds_the_residual() {
    let cfg = sparse_config();
    let priced: Vec<OeRow> = (0..64)
        .map(|i| {
            let mut priced = row(i);
            payload_of(&mut priced).insert(
                "price".to_owned(),
                VariantValue::Decimal {
                    scale: 2,
                    unscaled: 100 + i128::from(i),
                },
            );
            priced
        })
        .collect();
    let source = build_oe_file(priced.clone(), &cfg).unwrap();
    assert!(source.footer.shredded.iter().any(|entry| entry.path == "price"));
    let rescale = |payload: &mut BTreeMap<String, VariantValue>| {
        payload.insert(
            "price".to_owned(),
            VariantValue::Decimal {
                scale: 3,
                unscaled: 4_321,
            },
        );
    };

    for ordinal in [20, 32] {
        let rescaled = with_payload_edit(&priced, ordinal, rescale);
        assert_update_matches_a_fresh_build(priced.clone(), rescaled.clone(), &cfg);
        assert_update_matches_a_fresh_build(rescaled, priced.clone(), &cfg);
    }
}

#[test]
fn an_update_that_changes_only_delivery_lineage_rebuilds_the_lineage_blocks() {
    let cfg = OeBuildConfig {
        delivery_lineage: true,
        ..sparse_config()
    };
    let before: Vec<OeRow> = (0..64)
        .map(|i| {
            let mut delivered = row(i);
            delivered.event.source_delivery = Some(format!("delivery-{}", i % 3));
            delivered
        })
        .collect();
    let source = build_oe_file(before.clone(), &cfg).unwrap();
    for column_id in [column_ids::SOURCE_DELIVERY, column_ids::CONNECTOR_DELIVERY_HASH] {
        assert!(source.footer.columns.iter().any(|column| column.column_id == column_id));
    }
    let mut after = before.clone();
    after[21].event.source_delivery = Some("redelivered".to_owned());
    after[40].event.connector_delivery_hash_low = 999_999;

    assert_update_matches_a_fresh_build(before, after, &cfg);
}

fn inbox_rows() -> Vec<OeRow> {
    (0..64)
        .map(|i| {
            let mut inbox_row = row(i);
            inbox_row.event.envelope.entity_id_hash_low = (i * 37) % 64;
            inbox_row
        })
        .collect()
}

#[test]
fn an_inbox_update_maps_changes_to_granules_by_the_files_entity_order() {
    let cfg = OeBuildConfig {
        profile: crate::layout::Profile::Inbox,
        ..sparse_config()
    };
    let before = inbox_rows();
    let source = build_oe_file(before.clone(), &cfg).unwrap();
    assert!(source.footer.granules.len() > 1);
    let changed = 5usize;
    let file_ordinal = before[changed].event.envelope.entity_id_hash_low;
    assert_ne!(
        granule_for_ordinal(&source.footer, changed as u64),
        granule_for_ordinal(&source.footer, file_ordinal)
    );
    let mut after = before.clone();
    after[changed].event.envelope.dedupe_hash_low += 10_000;
    payload_of(&mut after[changed]).insert("amount".to_owned(), VariantValue::Int(77_777));
    let full = build_oe_file(after.clone(), &cfg).unwrap();

    let updated = update_oe_file_with_executor(&source, &before, after.clone(), &cfg, &SerialEncodeExecutor).unwrap();
    assert!(updated.reused_source_blocks > 0);
    assert_same_content(&full, &updated);

    let by_file_ordinal = [OeRowChange {
        after: after[changed].clone(),
        before: before[changed].clone(),
        row_ordinal: file_ordinal,
    }];
    let updated = update_oe_file_from_changes_with_executor(
        &source,
        after.clone(),
        &by_file_ordinal,
        &cfg,
        &SerialEncodeExecutor,
    )
    .unwrap();
    assert!(updated.reused_source_blocks > 0);
    assert_same_content(&full, &updated);

    let by_handed_position = changes_for(&before, &after, &[changed]);
    let updated =
        update_oe_file_from_changes_with_executor(&source, after, &by_handed_position, &cfg, &SerialEncodeExecutor)
            .unwrap();
    assert_eq!(updated.reused_source_blocks, 0);
    assert_same_content(&full, &updated);
}

#[test]
fn an_update_across_the_inbox_row_order_borrows_nothing() {
    let cfg = sparse_config();
    let inbox = OeBuildConfig {
        profile: crate::layout::Profile::Inbox,
        ..sparse_config()
    };
    let rows = inbox_rows();
    let source = build_oe_file(rows.clone(), &inbox).unwrap();
    let full = build_oe_file(rows.clone(), &cfg).unwrap();

    let updated = update_oe_file_with_executor(&source, &rows, rows.clone(), &cfg, &SerialEncodeExecutor).unwrap();
    assert_eq!(updated.reused_source_blocks, 0);
    assert_eq!(updated.bytes, full.bytes);
}

#[test]
fn a_payload_wider_than_the_shredded_id_range_keeps_the_overflow_in_the_residual() {
    let rows: Vec<OeRow> = (0..8)
        .map(|i| {
            let mut wide = row(i);
            let payload = payload_of(&mut wide);
            for field in 0..1_100i64 {
                payload.insert(format!("f{field:04}"), VariantValue::Int(field + i as i64));
            }
            wide
        })
        .collect();

    let built = build_oe_file(rows.clone(), &config()).unwrap();
    assert_eq!(built.footer.shredded.len(), 1_000);
    assert!(
        built
            .footer
            .shredded
            .iter()
            .all(|entry| (column_ids::SHREDDED_BASE..column_ids::FREETEXT_BASE).contains(&entry.column_id))
    );
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    for (ordinal, input_row) in rows.iter().enumerate() {
        let PayloadInput::Variant(expected) = &input_row.event.payload else {
            panic!("variant input");
        };
        assert_eq!(
            file.payload(ordinal as u64).unwrap(),
            PayloadRead::Value(expected.clone()),
            "row {ordinal}"
        );
    }
}

#[test]
fn a_column_family_larger_than_its_id_range_is_refused() {
    let refused = |cfg: &OeBuildConfig| {
        matches!(
            build_oe_file((0..4).map(row).collect::<Vec<OeRow>>(), cfg),
            Err(FormatError::Structural { rule }) if rule.contains("column id range")
        )
    };
    let mut promoted = config();
    promoted.promotion.columns = (0..1_001)
        .map(|index| PromotedColumn {
            name: format!("promoted{index}"),
            path: format!("path{index}"),
            kind: ColumnKind::I64,
            since_schema_version: 1,
            substring_searchable: false,
        })
        .collect();
    assert!(refused(&promoted));

    let mut freetext = config();
    freetext.freetext.fields = (0..1_001).map(|index| format!("text{index}")).collect();
    assert!(refused(&freetext));
}

#[test]
fn residual_keys_are_gathered_across_rows_of_differing_field_sets() {
    let rows: Vec<OeRow> = (0..48)
        .map(|i| {
            let mut varied = row(i);
            let payload = payload_of(&mut varied);
            match i % 4 {
                0 => {
                    payload.insert("only_some".to_owned(), VariantValue::Bool(i % 8 == 0));
                }
                1 => {
                    payload.insert(
                        "nested".to_owned(),
                        VariantValue::Object(BTreeMap::from([(format!("inner{}", i % 3), VariantValue::Int(1))])),
                    );
                }
                _ => {}
            }
            varied
        })
        .collect();

    let built = build_oe_file(rows.clone(), &config()).unwrap();
    let file = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    for (ordinal, input_row) in rows.iter().enumerate() {
        let PayloadInput::Variant(expected) = &input_row.event.payload else {
            panic!("variant input");
        };
        assert_eq!(
            file.payload(ordinal as u64).unwrap(),
            PayloadRead::Value(expected.clone()),
            "row {ordinal}"
        );
    }
}
