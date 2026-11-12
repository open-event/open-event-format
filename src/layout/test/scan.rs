use super::*;
use crate::artifacts::batch::{EventInput, PayloadInput};
use crate::columns::{FreetextDeclaration, PromotionPlan};
use crate::events::{EventEnvelope, EventFlags, EventId, StreamId, TenantId, TimestampValue};
use crate::layout::LayoutTargets;
use crate::typed_id::TypedIdTestExt;
use crate::writer::build::{BuildLifecycle, OeBuildConfig, OeRow, build_oe_file};

fn row(i: u64) -> OeRow {
    let mut payload = std::collections::BTreeMap::new();
    if !i.is_multiple_of(5) {
        payload.insert(
            "note".to_owned(),
            VariantValue::String(format!(
                "row {i} carries a note of its own, {}",
                "x".repeat((i % 7) as usize)
            )),
        );
    }
    payload.insert("amount".to_owned(), VariantValue::Int(i as i64 * 37 - 500));
    payload.insert(
        "region".to_owned(),
        VariantValue::String(["north", "south", "east", "west"][(i % 4) as usize].to_owned()),
    );
    OeRow {
        epoch: 1,
        sequence: i + 1,
        event: EventInput {
            envelope: EventEnvelope {
                event_id: EventId::new_test_id(0x5CA7_0000 + u128::from(i)),
                tenant_id: TenantId::new_test_id(3),
                stream_id: StreamId(1),
                stream_sequence: i,
                occurred_at: TimestampValue::from_physical_nanos(1_000 + i as i64 * 3),
                ingested_at: TimestampValue::from_physical_nanos(2_000 + i as i64 * 3),
                source: "crm".to_owned(),
                event_type: if i.is_multiple_of(3) {
                    "deal.updated"
                } else {
                    "deal.created"
                }
                .to_owned(),
                entity_type: "opportunity".to_owned(),
                entity_id_hash_low: i * 7919,
                entity_id_hash_high: 1,
                entity_id: None,
                actor_id_hash_low: 3,
                actor_id: None,
                account_id_hash_low: 4,
                account_id: None,
                trace_id_hash_low: 5,
                dedupe_hash_low: i,
                dedupe_hash_high: 6,
                schema_version: 1,
                flags: EventFlags(0),
            },
            payload: PayloadInput::Variant(VariantValue::object(payload)),
            source_schema: None,
            source_delivery: None,
            connector_delivery_hash_low: i,
            connector_delivery_hash_high: 0,
            provenance: None,
            relationships: None,
        },
    }
}

fn file(rows: u64, rows_per_granule: usize) -> OeFile {
    let config = OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: 0,
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
        promotion: PromotionPlan::default(),
        targets: LayoutTargets {
            index_granularity: rows_per_granule,
            index_granularity_bytes: 64 << 20,
            stripe_target_bytes: 4096,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
        },
        tenant_id: TenantId::new_test_id(3),
        time_series_bucket_nanos: None,
        delivery_lineage: false,
    };
    let built = build_oe_file((0..rows).map(row).collect(), &config).unwrap();
    OeFile::open(built.bytes, Some(&built.file_seal)).unwrap()
}

fn all_columns(file: &OeFile) -> Vec<u32> {
    file.footer().columns.iter().map(|column| column.column_id).collect()
}

fn all_granules(file: &OeFile) -> Vec<u32> {
    file.footer()
        .granules
        .iter()
        .map(|granule| granule.granule_id)
        .collect()
}

fn collect(
    file: &OeFile,
    column_ids: &[u32],
    granule_ids: &[u32],
    fan_out: bool,
    window_bytes: u64,
) -> Result<Vec<ScanBatch>, FormatError> {
    let mut batches = Vec::new();
    file.scan_projected_inner(column_ids, granule_ids, fan_out, window_bytes, |batch| {
        batches.push(batch);
        Ok(())
    })?;
    Ok(batches)
}

fn flatten(column: &ScanColumn) -> (Vec<u8>, Vec<Option<String>>) {
    match column {
        ScanColumn::Materialized(read) => {
            let values = match &read.data {
                ColumnData::Strings(values) => values.iter().map(|value| value.map(str::to_owned)).collect(),
                other => (0..other.row_count()).map(|_| None).collect(),
            };
            (read.presence.clone(), values)
        }
        ScanColumn::Views { presence, views } => (
            presence.clone(),
            views.iter().map(|value| value.map(str::to_owned)).collect(),
        ),
    }
}

#[test]
fn projected_scan_agrees_with_per_column_reads_in_order() {
    let file = file(40, 6);
    let columns = all_columns(&file);
    let granules = all_granules(&file);
    assert!(granules.len() >= 6, "{} granules", granules.len());
    let mut asked_columns = columns.clone();
    asked_columns.reverse();
    let mut asked_granules = granules.clone();
    asked_granules.reverse();

    let batches = collect(&file, &asked_columns, &asked_granules, false, SCAN_WINDOW_BYTES).unwrap();

    assert_eq!(
        batches.iter().map(|batch| batch.granule_id).collect::<Vec<_>>(),
        asked_granules
    );
    let mut string_blocks_as_views = 0;
    for batch in &batches {
        let granule = file
            .footer()
            .granules
            .iter()
            .find(|granule| granule.granule_id == batch.granule_id)
            .unwrap();
        assert_eq!(batch.row_count, granule.row_count);
        assert_eq!(batch.columns.len(), asked_columns.len());
        let mut expected_bytes = 0;
        for (column_id, column) in asked_columns.iter().zip(&batch.columns) {
            let mark = file.mark(*column_id, 0, batch.granule_id).unwrap().unwrap();
            expected_bytes += mark.compressed_size;
            let read = file.read_column(*column_id, batch.granule_id).unwrap();
            let (presence, values) = flatten(column);
            assert_eq!(
                presence, read.presence,
                "column {column_id} granule {}",
                batch.granule_id
            );
            match column {
                ScanColumn::Materialized(scanned) => assert_eq!(scanned.data, read.data),
                ScanColumn::Views { .. } => {
                    string_blocks_as_views += 1;
                    let ColumnData::Strings(expected) = &read.data else {
                        panic!("views for a non-string column {column_id}");
                    };
                    let expected: Vec<Option<String>> = expected.iter().map(|value| value.map(str::to_owned)).collect();
                    assert_eq!(values, expected, "column {column_id} granule {}", batch.granule_id);
                    let (view_presence, views) = file
                        .read_column_string_views(*column_id, batch.granule_id)
                        .unwrap()
                        .unwrap();
                    assert_eq!(view_presence, presence);
                    assert_eq!(
                        views.iter().map(|value| value.map(str::to_owned)).collect::<Vec<_>>(),
                        values
                    );
                }
            }
        }
        assert_eq!(batch.bytes, expected_bytes);
    }
    assert!(string_blocks_as_views > 0, "no string block took the view path");
}

#[test]
fn projected_scan_windows_and_fan_out_do_not_change_the_batches() {
    let file = file(4 * 8192, 8192);
    let columns = all_columns(&file);
    let granules = all_granules(&file);
    assert_eq!(granules.len(), 4);
    let inflated: u64 = granules
        .iter()
        .flat_map(|granule_id| columns.iter().map(move |column_id| (*column_id, *granule_id)))
        .map(|(column_id, granule_id)| file.mark(column_id, 0, granule_id).unwrap().unwrap().uncompressed_size)
        .sum();
    assert!(
        scan_in_parallel(granules.len(), inflated / granules.len() as u64),
        "{inflated} inflated bytes"
    );
    let reference: Vec<_> = collect(&file, &columns, &granules, false, SCAN_WINDOW_BYTES)
        .unwrap()
        .iter()
        .map(|batch| {
            (
                batch.granule_id,
                batch.row_count,
                batch.bytes,
                batch.columns.iter().map(flatten).collect::<Vec<_>>(),
            )
        })
        .collect();
    for (fan_out, window_bytes) in [(false, 1), (true, 1), (true, SCAN_WINDOW_BYTES)] {
        let batches = collect(&file, &columns, &granules, fan_out, window_bytes).unwrap();
        let shaped: Vec<_> = batches
            .iter()
            .map(|batch| {
                (
                    batch.granule_id,
                    batch.row_count,
                    batch.bytes,
                    batch.columns.iter().map(flatten).collect::<Vec<_>>(),
                )
            })
            .collect();
        assert!(shaped == reference, "fan_out {fan_out} window {window_bytes}");
    }
}

#[test]
fn projected_scan_refuses_unknown_columns_and_granules_and_stops_on_sink_error() {
    let file = file(20, 6);
    let columns = all_columns(&file);
    let granules = all_granules(&file);

    assert!(matches!(
        collect(&file, &[u32::MAX], &granules, false, SCAN_WINDOW_BYTES),
        Err(FormatError::RefOutOfRange { .. })
    ));
    assert!(matches!(
        collect(&file, &columns, &[u32::MAX], false, SCAN_WINDOW_BYTES),
        Err(FormatError::RefOutOfRange { .. })
    ));

    let mut delivered = 0;
    let stopped = file.scan_projected_inner(&columns, &granules, false, 1, |_| {
        delivered += 1;
        Err(FormatError::Structural { rule: "sink gave up" })
    });
    assert!(matches!(stopped, Err(FormatError::Structural { rule: "sink gave up" })));
    assert_eq!(delivered, 1);
}

#[test]
fn projected_scan_of_nothing_is_empty_but_well_formed() {
    let file = file(20, 6);
    let granules = all_granules(&file);
    let batches = file.scan_projected(&[], &granules, |_| Ok(()));
    assert!(batches.is_ok());
    let mut rows = 0;
    file.scan_projected(&[], &granules, |batch| {
        assert!(batch.columns.is_empty());
        assert_eq!(batch.bytes, 0);
        rows += u64::from(batch.row_count);
        Ok(())
    })
    .unwrap();
    assert_eq!(rows, 20);
    let mut delivered = 0;
    file.scan_projected_serial(&all_columns(&file), &[], |_| {
        delivered += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(delivered, 0);
}

#[test]
fn projected_scan_repeats_duplicate_ids_across_many_windows() {
    let file = file(40, 6);
    let columns = all_columns(&file);
    let first = *columns.first().unwrap();
    let asked_columns = vec![first, first];
    let granules = all_granules(&file);
    let asked_granules: Vec<u32> = granules.iter().chain(&granules).copied().collect();
    for window_bytes in [1, SCAN_WINDOW_BYTES] {
        let batches = collect(&file, &asked_columns, &asked_granules, false, window_bytes).unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.granule_id).collect::<Vec<_>>(),
            asked_granules
        );
        for batch in &batches {
            assert_eq!(batch.columns.len(), 2);
            assert_eq!(flatten(&batch.columns[0]), flatten(&batch.columns[1]));
        }
    }
}

#[test]
fn a_live_scan_skips_fully_deleted_granules_and_marks_untouched_ones() {
    let file = file(40, 8);
    let columns = all_columns(&file);
    let granules = all_granules(&file);
    assert!(granules.len() >= 4);
    let entry = |id: u32| *file.granule_entry(id).unwrap();
    let emptied = entry(granules[1]);
    let partial = entry(granules[2]);
    let mut deleted = DeletionVector::new();
    for ordinal in emptied.first_row_ordinal..emptied.first_row_ordinal + u64::from(emptied.row_count) {
        deleted.mark_deleted(ordinal);
    }
    deleted.mark_deleted(partial.first_row_ordinal + 1);

    let reference = collect(&file, &columns, &granules, true, SCAN_WINDOW_BYTES).unwrap();
    let mut seen = Vec::new();
    file.scan_projected_live(&columns, &granules, &deleted, |batch, selection| {
        seen.push((batch, selection));
        Ok(())
    })
    .unwrap();
    let ids: Vec<u32> = seen.iter().map(|(batch, _)| batch.granule_id).collect();
    let expected: Vec<u32> = granules.iter().copied().filter(|id| *id != granules[1]).collect();
    assert_eq!(ids, expected, "the emptied granule is never scanned");
    for (batch, selection) in &seen {
        let same = reference
            .iter()
            .find(|other| other.granule_id == batch.granule_id)
            .unwrap();
        assert_eq!(batch.row_count, same.row_count);
        for (column, other) in batch.columns.iter().zip(&same.columns) {
            assert_eq!(flatten(column), flatten(other));
        }
        if batch.granule_id == granules[2] {
            let mut keep = vec![true; partial.row_count as usize];
            keep[1] = false;
            assert_eq!(*selection, RowSelection::Some(keep));
        } else {
            assert_eq!(*selection, RowSelection::All);
        }
    }
}

fn string_forms(values: &[Option<&str>]) -> (ScanColumn, ScanColumn) {
    let mut presence = vec![0u8; values.len().div_ceil(8)];
    let mut strings = crate::encoding::StringColumn::new();
    for (row, value) in values.iter().enumerate() {
        if value.is_some() {
            presence[row / 8] |= 1 << (row % 8);
            strings.push(*value);
        }
    }
    let presence = if values.iter().all(Option::is_some) {
        Vec::new()
    } else {
        presence
    };
    let views: StringViewArray = values.iter().flatten().map(|value| Some(*value)).collect();
    (
        ScanColumn::Views {
            presence: presence.clone(),
            views,
        },
        ScanColumn::Materialized(ColumnRead {
            data: ColumnData::Strings(strings),
            presence,
        }),
    )
}

#[test]
fn masking_makes_a_row_absent_in_both_string_forms() {
    let words: Vec<String> = (0..21).map(|row| format!("value {row}")).collect();
    let sparse: Vec<Option<&str>> = words
        .iter()
        .enumerate()
        .map(|(row, word)| (row % 3 != 1).then_some(word.as_str()))
        .collect();
    let dense: Vec<Option<&str>> = words.iter().map(|word| Some(word.as_str())).collect();
    for values in [sparse, dense] {
        let masked_rows = [0u32, 1, 9, 10, 11, 20];
        let (mut views, mut materialized) = string_forms(&values);
        mask_column(&mut views, values.len() as u32, &masked_rows).unwrap();
        mask_column(&mut materialized, values.len() as u32, &masked_rows).unwrap();
        let (presence, kept) = flatten(&views);
        assert_eq!((presence.clone(), kept.clone()), flatten(&materialized));
        let expected: Vec<Option<&str>> = values
            .iter()
            .enumerate()
            .map(|(row, value)| value.filter(|_| !masked_rows.contains(&(row as u32))))
            .collect();
        let read: Vec<Option<&str>> = (0..values.len() as u32)
            .map(|row| {
                value_indexes(&presence, &[row])
                    .first()
                    .and_then(|&index| kept[index].as_deref())
            })
            .collect();
        assert_eq!(read, expected);
        assert_eq!(kept.len(), expected.iter().flatten().count());
    }
}

#[test]
fn value_indexes_count_present_rows_before_each_row() {
    let presence = [0b1011_0101u8, 0b0000_0000, 0b1111_1111, 0b0000_0110];
    let bit = |row: u32| presence[row as usize / 8] & (1 << (row % 8)) != 0;
    let rows: Vec<u32> = (0..32).filter(|row| row % 5 != 3).collect();
    let expected: Vec<usize> = rows
        .iter()
        .filter(|&&row| bit(row))
        .map(|&row| (0..row).filter(|&before| bit(before)).count())
        .collect();
    assert_eq!(value_indexes(&presence, &rows), expected);
    assert_eq!(value_indexes(&presence, &[26, 31]), [14]);
    assert_eq!(value_indexes(&[], &[4, 9]), [4, 9]);
}
