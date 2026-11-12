use super::*;
use crate::artifacts::batch::{EventInput, PayloadInput};
use crate::columns::{FreetextDeclaration, PromotedColumn, PromotionPlan};
use crate::encoding::encode_block;
use crate::events::{EventEnvelope, EventFlags, EventId, StreamId, TenantId, TimestampValue};
use crate::file::bytes::Writer;
use crate::indexes::bitmap::RoaringRangeBitmap;
use crate::indexes::rank_select::RankSelect;
use crate::layout::footer::{ColumnKind, ColumnMark, EmbeddingRowOffsets, ExactCounts, Footer, GranuleEntry};
use crate::layout::{LayoutClass, LayoutTargets, optional_features};
use crate::typed_id::TypedIdTestExt;
use crate::writer::build::{AnalyticalColumn, BuildLifecycle, OeBuildConfig, OeRow, build_oe_file};

fn minimal_header() -> OeHeader {
    OeHeader {
        created_at_physical: 0,
        feature_flags: 0,
        file_id: 0,
        footer_pointer_hint: 0,
        generation_id: 0,
        layout_class: LayoutClass::Compact,
        max_epoch: 1,
        max_ingested_at_physical: 0,
        max_occurred_at_physical: 0,
        max_sequence: 1,
        min_epoch: 1,
        min_ingested_at_physical: 0,
        min_occurred_at_physical: 0,
        min_sequence: 1,
        profile: crate::layout::Profile::Event,
        projection_count: 1,
        row_count: 1,
        tenant_id: TenantId::new_test_id(1),
        version_major: 1,
        version_minor: 0,
    }
}

fn minimal_footer(mark: ColumnMark) -> Footer {
    Footer {
        audit: None,
        clustering: Vec::new(),
        columns: Vec::new(),
        dictionaries: Default::default(),
        embedding_row_offsets: Vec::new(),
        entity_hash_filters: Vec::new(),
        entity_summary: None,
        escape_hatches: Vec::new(),
        exact_counts: ExactCounts {
            row_count: 1,
            ..ExactCounts::default()
        },
        external_ids: Vec::new(),
        format_version: (1, 0),
        freetext: Vec::new(),
        freetext_row_offsets: Vec::new(),
        granules: vec![GranuleEntry {
            compressed_bytes_estimate: 0,
            first_epoch: 1,
            first_row_ordinal: 0,
            first_sequence: 1,
            granule_id: mark.granule_id,
            last_epoch: 1,
            last_sequence: 1,
            max_ingested_at_physical: 0,
            max_occurred_at_physical: 0,
            min_ingested_at_physical: 0,
            min_occurred_at_physical: 0,
            row_count: mark.row_count,
            stripe_id: 0,
        }],
        graph: None,
        inbox_recipients: Vec::new(),
        integrity_gaps: Vec::new(),
        io_alignment_bytes: 0,
        marks: vec![mark],
        marks_directory: Vec::new(),
        marks_page_offsets: Vec::new(),
        marks_pages: Vec::new(),
        optional_feature_flags: 0,
        page_directory: Vec::new(),
        page_minmax: Vec::new(),
        page_stats: Vec::new(),
        payload_granules: Vec::new(),
        presence: Vec::new(),
        profile: crate::layout::Profile::Event,
        reference_filters: None,
        required_feature_flags: required_features::ALL,
        schema_fingerprint: [0u8; 32],
        shared_dictionaries: Vec::new(),
        packed_shared_dictionaries: Vec::new(),
        shredded: Vec::new(),
        snapshot_keys: None,
        sparse_keys: Vec::new(),
        state_roots: Vec::new(),
        stripe_checksums: Vec::new(),
        stripe_proofs: Vec::new(),
        stripe_ndv: Vec::new(),
        stripes: Vec::new(),
        text_token_indexes: Vec::new(),
        text_token_offsets: Vec::new(),
        time_series_rollups: None,
        vector_columns: Vec::new(),
    }
}

fn file_with_mark(mark: ColumnMark, bytes: Vec<u8>, usable_optional_features: u64) -> OeFile {
    let footer = minimal_footer(mark);
    OeFile {
        aliased_extent_cache: DashMap::default(),
        aliased_extent_cache_bytes: AtomicU64::new(0),
        aliased_extents: DashSet::default(),
        block_reads: AtomicU64::new(0),
        bytes: FileBytes::Local(Arc::new(bytes)),
        cache_access_counter: AtomicU64::new(0),
        column_cache: DashMap::default(),
        column_cache_bytes: AtomicU64::new(0),
        decoded_cache_budget: DECODED_CACHE_BUDGET_BYTES,
        dictionary_decodes: AtomicU64::new(0),
        envelope_columns: std::sync::Mutex::new(None),
        event_ids: std::sync::Mutex::new(None),
        footer: Arc::new(footer),
        granule_dictionaries: DashMap::default(),
        granule_dictionaries_bytes: AtomicU64::new(0),
        graph_edges: DashMap::default(),
        graph_edges_bytes: AtomicU64::new(0),
        header: minimal_header(),
        index: Arc::new(FileIndex {
            usable_optional_features,
            ..FileIndex::default()
        }),
        inflated_residuals: DashMap::default(),
        inflated_residuals_bytes: AtomicU64::new(0),
        lazy_stripes: None,
        marks: Marks::Eager(Arc::new(
            [((mark.column_id, mark.projection_id, mark.granule_id), mark)].into(),
        )),
        point_probe_counts: DashMap::default(),
        residual_seek_tables: DashMap::default(),
        packed_shared_alphabets: HashMap::new(),
        shared_string_views: DashMap::new(),
        snapshot_first_keys_in_order: false,
        stripe_relative: false,
        text_token_cache: DashMap::default(),
        usable_optional_features,
    }
}

#[test]
fn rank_matches_naive_presence_popcount() {
    let presence: Vec<u8> = vec![0b1011_0010, 0b0000_0000, 0b1111_0001, 0b0100_0000, 0b1010_1010];
    let rows = (presence.len() * 8) as u64;
    let bitmap = RoaringRangeBitmap::from_rows(presence_set_rows(&presence));
    let rank = RankSelect::from_bitmap(&bitmap);
    for row in 0..rows {
        let set = presence[row as usize / 8] & (1 << (row % 8)) != 0;
        let before = (0..row)
            .filter(|r| presence[*r as usize / 8] & (1 << (r % 8)) != 0)
            .count() as u64;
        assert_eq!(rank.rank(row), before, "dense position before row {row}");
        assert_eq!(rank.rank(row + 1) > rank.rank(row), set, "presence of row {row}");
    }
}

#[test]
fn cached_column_rank_from_packed_bits_matches_raw_path_on_a_real_file() {
    let rows: Vec<_> = (0..40)
        .map(|i| freetext_row(i, (i % 2 == 0).then_some("free text body")))
        .collect();
    let built = build_oe_file(rows, &freetext_config()).unwrap();
    let note_column_id = built.footer.freetext.first().expect("note is declared").column_id;
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    for granule in &file.footer().granules {
        let read = file.read_column(note_column_id, granule.granule_id).unwrap();
        assert!(!read.presence.is_empty(), "a free-text column stores sparse presence");
        let compressed = RankSelect::from_packed_bits(&read.presence);
        let raw = RankSelect::from_bitmap(&RoaringRangeBitmap::from_rows(presence_set_rows(&read.presence)));
        assert_eq!(compressed.total(), raw.total());
        for row in 0..=(read.presence.len() as u64 * 8) {
            assert_eq!(compressed.rank(row), raw.rank(row), "rank({row})");
        }
        for k in 0..=raw.total() {
            assert_eq!(compressed.select(k), raw.select(k), "select({k})");
        }
    }
}

#[test]
fn presence_set_rows_lists_set_bits_in_order() {
    let presence: Vec<u8> = vec![0b0000_0001, 0b1000_0000, 0b0000_0000, 0b0000_0010];
    let got: Vec<u64> = presence_set_rows(&presence).collect();
    assert_eq!(got, vec![0, 15, 25]);
}

#[test]
fn empty_presence_has_no_present_rows() {
    let presence = vec![0u8; 4];
    let bitmap = RoaringRangeBitmap::from_rows(presence_set_rows(&presence));
    let rank = RankSelect::from_bitmap(&bitmap);
    assert_eq!(rank.total(), 0);
    for row in 0..(presence.len() * 8) as u64 {
        assert_eq!(rank.rank(row + 1), rank.rank(row), "row {row} must be absent");
    }
}

#[test]
fn strip_outboard_tree_removes_the_trailer() {
    let content: Vec<u8> = [&1000u64.to_le_bytes()[..], b"OEF1"].concat();
    let tree = vec![0xABu8; 40];
    let mut object = content.clone();
    object.extend_from_slice(&tree);
    object.extend_from_slice(&(tree.len() as u64).to_le_bytes());
    object.extend_from_slice(&TREE_TRAILER_MAGIC);
    assert_eq!(strip_outboard_tree(&object).unwrap(), content.as_slice());
    assert_eq!(strip_outboard_tree(&content).unwrap(), content.as_slice());
}

#[test]
fn footer_blob_len_reads_the_length_word() {
    let content: Vec<u8> = [&4096u64.to_le_bytes()[..], b"OEF1"].concat();
    assert_eq!(footer_blob_len(&content).unwrap(), 4096);
    let bad: Vec<u8> = [&4096u64.to_le_bytes()[..], b"XXXX"].concat();
    assert!(matches!(footer_blob_len(&bad), Err(FormatError::BadMagic { .. })));
}

#[test]
fn tail_range_sizes_exact_and_speculative_fetches() {
    let exact = tail_range(10_000, Some(300), Some(64));
    assert_eq!((exact.exact, exact.len, exact.start), (true, 364, 9_636));

    let no_tree = tail_range(10_000, Some(300), None);
    assert_eq!((no_tree.exact, no_tree.len, no_tree.start), (true, 300, 9_700));

    let big = tail_range(10 * SPECULATIVE_TAIL_BYTES, None, None);
    assert!(!big.exact);
    assert_eq!(big.len, SPECULATIVE_TAIL_BYTES);

    let small = tail_range(500, None, None);
    assert_eq!((small.len, small.start), (500, 0));
}

#[test]
fn bulk_read_family_returns_the_first_unreadable_granule_error() {
    let mark = ColumnMark {
        codec_pipeline_id: PipelineId(0),
        column_id: 7,
        compressed_offset: 64,
        compressed_size: 8,
        first_value_offset: None,
        granule_id: 3,
        page_count: 1,
        projection_id: 0,
        row_count: 1,
        uncompressed_offset: 0,
        uncompressed_size: 8,
    };
    let file = file_with_mark(mark, Vec::new(), 0);

    assert!(matches!(
        file.bulk_read_family(7),
        Err(FormatError::Truncated { what: "column block" })
    ));
}

#[test]
fn read_page_on_multi_page_mark_without_directory_fails_instead_of_recursing() {
    let mark = ColumnMark {
        codec_pipeline_id: PipelineId(0),
        column_id: 7,
        compressed_offset: 0,
        compressed_size: 0,
        first_value_offset: None,
        granule_id: 3,
        page_count: 2,
        projection_id: 0,
        row_count: 2,
        uncompressed_offset: 0,
        uncompressed_size: 0,
    };
    let file = file_with_mark(mark, Vec::new(), optional_features::PER_PAGE_MARKS);

    assert!(matches!(
        file.read_page(7, 3, 1),
        Err(FormatError::RefOutOfRange {
            what: "no page directory entry for multi-page mark"
        })
    ));
    assert!(matches!(
        file.read_column(7, 3),
        Err(FormatError::RefOutOfRange {
            what: "no page directory entry for multi-page mark"
        })
    ));
}

#[test]
fn speculative_open_reports_exact_retry_length() {
    let short: Vec<u8> = [&1000u64.to_le_bytes()[..], b"OEF1"].concat();
    match OeFooter::open_speculative(&short).unwrap() {
        SpeculativeTail::NeedsExactTail { tail_len } => assert_eq!(tail_len, 1012),
        other => panic!("expected an exact-retry request, got {other:?}"),
    }

    let tree = vec![0u8; 20];
    let mut object = short.clone();
    object.extend_from_slice(&tree);
    object.extend_from_slice(&(tree.len() as u64).to_le_bytes());
    object.extend_from_slice(&TREE_TRAILER_MAGIC);
    match OeFooter::open_speculative(&object).unwrap() {
        SpeculativeTail::NeedsExactTail { tail_len } => assert_eq!(tail_len, 1012 + 20 + TREE_TRAILER_LEN as u64),
        other => panic!("expected an exact-retry request, got {other:?}"),
    }
}

#[test]
fn speculative_open_asks_for_a_retry_when_the_tree_alone_exceeds_the_window() {
    let footer_region: Vec<u8> = [&1000u64.to_le_bytes()[..], b"OEF1"].concat();
    let tree = vec![0u8; 5_000];
    let mut object = footer_region;
    object.extend_from_slice(&tree);
    object.extend_from_slice(&(tree.len() as u64).to_le_bytes());
    object.extend_from_slice(&TREE_TRAILER_MAGIC);

    let short_tail = &object[object.len() - 200..];
    match OeFooter::open_speculative(short_tail).unwrap() {
        SpeculativeTail::NeedsExactTail { tail_len } => {
            assert_eq!(
                tail_len,
                tree.len() as u64 + TREE_TRAILER_LEN as u64 + SPECULATIVE_TAIL_BYTES
            );
        }
        other => panic!("expected an exact-retry request, got {other:?}"),
    }

    match OeFooter::open_speculative(&object).unwrap() {
        SpeculativeTail::NeedsExactTail { tail_len } => {
            assert_eq!(tail_len, 1012 + tree.len() as u64 + TREE_TRAILER_LEN as u64)
        }
        other => panic!("expected an exact-retry request, got {other:?}"),
    }
}

#[test]
fn forged_footer_length_near_u64_max_refuses() {
    let tail: Vec<u8> = [&u64::MAX.to_le_bytes()[..], b"OEF1"].concat();
    assert!(matches!(
        OeFooter::open(&tail),
        Err(FormatError::Truncated { what: "footer" })
    ));
    match OeFooter::open_speculative(&tail).unwrap() {
        SpeculativeTail::NeedsExactTail { tail_len } => assert_eq!(tail_len, u64::MAX),
        other => panic!("expected an exact-retry request, got {other:?}"),
    }
    assert!(matches!(
        decrypt_footer_tail(&tail, &[0u8; 32], 0),
        Err(FormatError::Truncated {
            what: "encrypted footer"
        })
    ));
}

#[test]
fn forged_stripe_relative_offset_near_u64_max_refuses() {
    let mark = ColumnMark {
        codec_pipeline_id: PipelineId(0),
        column_id: 7,
        compressed_offset: u64::MAX,
        compressed_size: 8,
        first_value_offset: None,
        granule_id: 3,
        page_count: 1,
        projection_id: 0,
        row_count: 1,
        uncompressed_offset: 0,
        uncompressed_size: 8,
    };
    let mut file = file_with_mark(mark, vec![0u8; 64], 0);
    file.stripe_relative = true;
    Arc::make_mut(&mut file.index).granule_stripe_base.insert(3, 1);
    assert!(matches!(
        file.read_column_raw(7, 3),
        Err(FormatError::RefOutOfRange {
            what: "offset beyond file range"
        })
    ));
}

#[test]
fn validate_block_counts_requires_exact_dense_row_count() {
    assert!(validate_block_counts(&[], 1000, 1000).is_ok());
    assert!(
        validate_block_counts(&[], 900, 1000).is_err(),
        "a dense block that decodes fewer values than it declares must be rejected"
    );
    assert!(
        validate_block_counts(&[], 1001, 1000).is_err(),
        "a dense block that decodes more values than it declares must be rejected"
    );
}

#[test]
fn validate_block_counts_matches_sparse_values_to_set_bits() {
    let presence = [0b0000_1011u8];
    assert!(validate_block_counts(&presence, 3, 4).is_ok());
    assert!(
        validate_block_counts(&presence, 2, 4).is_err(),
        "fewer decoded values than set bits must be rejected"
    );
    assert!(
        validate_block_counts(&presence, 4, 4).is_err(),
        "more decoded values than set bits must be rejected"
    );
}

#[test]
fn validate_block_counts_rejects_a_short_or_overlong_sparse_bitmap() {
    assert!(
        validate_block_counts(&[0xFFu8], 8, 9).is_err(),
        "a one-byte bitmap cannot cover nine declared rows"
    );
    assert!(
        validate_block_counts(&[0b0001_0001u8], 2, 4).is_err(),
        "a present bit at or beyond the declared row count must be rejected"
    );
}

fn freetext_row(i: u64, note: Option<&str>) -> OeRow {
    let mut payload = std::collections::BTreeMap::new();
    if let Some(note) = note {
        payload.insert("note".to_owned(), VariantValue::String(note.to_owned()));
    }
    payload.insert("amount".to_owned(), VariantValue::Int(i as i64));
    OeRow {
        epoch: 1,
        sequence: i + 1,
        event: EventInput {
            envelope: EventEnvelope {
                event_id: EventId::new_test_id(0xF00D_0000 + u128::from(i)),
                tenant_id: TenantId::new_test_id(3),
                stream_id: StreamId(1),
                stream_sequence: i,
                occurred_at: TimestampValue::from_physical_nanos(1_000 + i as i64),
                ingested_at: TimestampValue::from_physical_nanos(2_000 + i as i64),
                source: "crm".to_owned(),
                event_type: "deal.updated".to_owned(),
                entity_type: "opportunity".to_owned(),
                entity_id_hash_low: i,
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

#[test]
fn freetext_point_lookup_matches_whole_granule_decode_and_falls_back_identically() {
    let rows: Vec<_> = (0..20)
        .map(|i| freetext_row(i, (!i.is_multiple_of(5)).then_some("free text body")))
        .collect();
    let config = OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: 0,
        footer_dek: None,
        footer_encryption: crate::security::FooterEncryption::Plaintext,
        freetext: FreetextDeclaration {
            fields: vec!["note".to_owned()],
        },
        freetext_row_offset_index: true,
        generation_id: 1,
        graph: None,
        io_alignment_bytes: 0,
        lifecycle: BuildLifecycle::FreshPublication,
        page_size_rows: 0,
        profile: crate::layout::Profile::Event,
        promotion: PromotionPlan::default(),
        targets: LayoutTargets {
            index_granularity: 6,
            index_granularity_bytes: 1 << 20,
            stripe_target_bytes: 4096,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
        },
        tenant_id: TenantId::new_test_id(3),
        time_series_bucket_nanos: None,
        delivery_lineage: false,
    };
    let built = build_oe_file(rows.clone(), &config).unwrap();
    assert_ne!(
        built.footer.optional_feature_flags & optional_features::TYPED_COLUMN_ROW_OFFSETS,
        0
    );
    assert!(!built.footer.freetext_row_offsets.is_empty());

    let with_index = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    let mut without_index = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    without_index.usable_optional_features &= !optional_features::TYPED_COLUMN_ROW_OFFSETS;
    assert_eq!(
        without_index.usable_optional_features & optional_features::TYPED_COLUMN_ROW_OFFSETS,
        0
    );

    for (ordinal, input_row) in rows.iter().enumerate() {
        let PayloadInput::Variant(VariantValue::Object(expected)) = &input_row.event.payload else {
            panic!("variant object input");
        };
        let expected_note = expected.get("note").cloned();

        let PayloadRead::Value(VariantValue::Object(via_index)) = with_index.payload(ordinal as u64).unwrap() else {
            panic!("payload present");
        };
        let PayloadRead::Value(VariantValue::Object(via_fallback)) = without_index.payload(ordinal as u64).unwrap()
        else {
            panic!("payload present");
        };
        assert_eq!(via_index.get("note").cloned(), expected_note, "row {ordinal} via index");
        assert_eq!(
            via_fallback.get("note").cloned(),
            expected_note,
            "row {ordinal} via whole-granule fallback"
        );
    }
}

#[test]
fn payload_path_and_payload_agree_on_a_freetext_declared_field() {
    let rows: Vec<_> = (0..12)
        .map(|i| freetext_row(i, (!i.is_multiple_of(4)).then_some("free text body")))
        .collect();
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
            index_granularity: 6,
            index_granularity_bytes: 1 << 20,
            stripe_target_bytes: 4096,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
        },
        tenant_id: TenantId::new_test_id(3),
        time_series_bucket_nanos: None,
        delivery_lineage: false,
    };
    let built = build_oe_file(rows.clone(), &config).unwrap();
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();

    for (ordinal, input_row) in rows.iter().enumerate() {
        let PayloadInput::Variant(VariantValue::Object(expected)) = &input_row.event.payload else {
            panic!("variant object input");
        };
        let expected_note = expected.get("note").cloned();

        let via_path = file.payload_path(ordinal as u64, "note").unwrap();
        assert_eq!(via_path, expected_note, "payload_path row {ordinal}");

        let PayloadRead::Value(VariantValue::Object(via_payload)) = file.payload(ordinal as u64).unwrap() else {
            panic!("payload present");
        };
        assert_eq!(
            via_payload.get("note").cloned(),
            via_path,
            "payload vs payload_path row {ordinal}"
        );
    }
}

fn freetext_config() -> OeBuildConfig {
    OeBuildConfig {
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
            index_granularity: 6,
            index_granularity_bytes: 1 << 20,
            stripe_target_bytes: 4096,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
        },
        tenant_id: TenantId::new_test_id(3),
        time_series_bucket_nanos: None,
        delivery_lineage: false,
    }
}

#[test]
fn a_present_but_empty_freetext_field_is_not_reconstructed_as_missing() {
    let notes = [Some(""), Some("free text body"), None];
    let rows: Vec<_> = (0..12)
        .map(|i| freetext_row(i, notes[i as usize % notes.len()]))
        .collect();
    let mut config = freetext_config();
    config.freetext_row_offset_index = true;
    let built = build_oe_file(rows.clone(), &config).unwrap();
    assert!(!built.footer.freetext_row_offsets.is_empty());

    let with_index = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    let mut without_index = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    without_index.usable_optional_features &= !optional_features::TYPED_COLUMN_ROW_OFFSETS;

    for (ordinal, note) in (0..rows.len()).zip(notes.iter().cycle()) {
        let expected = note.map(|text| VariantValue::String(text.to_owned()));
        assert_eq!(
            with_index.payload_path(ordinal as u64, "note").unwrap(),
            expected,
            "row {ordinal} through the per-row index"
        );
        assert_eq!(
            without_index.payload_path(ordinal as u64, "note").unwrap(),
            expected,
            "row {ordinal} through the whole-granule decode"
        );
        let PayloadRead::Value(VariantValue::Object(reconstructed)) = with_index.payload(ordinal as u64).unwrap()
        else {
            panic!("payload present");
        };
        assert_eq!(
            reconstructed.get("note").cloned(),
            expected,
            "row {ordinal} payload reconstruction"
        );
    }
}

#[test]
fn payload_path_reports_no_inline_path_for_an_external_reference_row() {
    let mut rows: Vec<_> = (0..12).map(|i| freetext_row(i, Some("free text body"))).collect();
    rows[2].event.payload = PayloadInput::ExternalRef("oej://tenant/3/blob/2".to_owned());
    let built = build_oe_file(rows.clone(), &freetext_config()).unwrap();
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();

    assert_eq!(
        file.payload(2).unwrap(),
        PayloadRead::External("oej://tenant/3/blob/2".to_owned())
    );
    assert_eq!(
        file.payload_path(2, "amount").unwrap(),
        None,
        "an external-reference row carries no inline path"
    );
    assert_eq!(file.payload_path(2, "note").unwrap(), None);
    assert_eq!(file.payload_path(1, "amount").unwrap(), Some(VariantValue::Int(1)));
}

fn mixed_payload_rows(count: u64) -> Vec<OeRow> {
    let mut rows: Vec<_> = (0..count)
        .map(|i| freetext_row(i, (!i.is_multiple_of(4)).then_some("free text body")))
        .collect();
    for (ordinal, row) in rows.iter_mut().enumerate() {
        if ordinal.is_multiple_of(3)
            && let PayloadInput::Variant(VariantValue::Object(fields)) = &mut row.event.payload
        {
            fields.insert(
                "extra".to_owned().into(),
                VariantValue::String(format!("extra-{ordinal}")),
            );
        }
    }
    rows[7].event.payload = PayloadInput::ExternalRef("oej://tenant/3/blob/7".to_owned());
    rows[13].event.payload = PayloadInput::None;
    rows
}

#[test]
fn read_payload_paths_agrees_with_the_per_row_path_read() {
    let built = build_oe_file(mixed_payload_rows(24), &freetext_config()).unwrap();
    assert!(
        built.footer.shredded.iter().any(|entry| entry.path == "amount"),
        "`amount` must auto-shred for this to cover the shredded path"
    );
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    let refs: Vec<PayloadRef> = [21, 3, 13, 7, 3, 16, 0, 9, 23, 6]
        .into_iter()
        .map(|row_ordinal| PayloadRef { row_ordinal })
        .collect();

    for path in ["amount", "note", "extra", "absent-everywhere"] {
        let batched = file.read_payload_paths(&refs, path).unwrap();
        assert_eq!(batched.len(), refs.len(), "one value per reference for `{path}`");
        for (reference, value) in refs.iter().zip(&batched) {
            assert_eq!(
                *value,
                file.payload_path(reference.row_ordinal, path).unwrap(),
                "`{path}` of row {} in the caller's position",
                reference.row_ordinal
            );
        }
    }
    let amounts = file.read_payload_paths(&refs, "amount").unwrap();
    assert_eq!(amounts[0], Some(VariantValue::Int(21)));
    assert_eq!(amounts[2], None, "the row storing no payload has no value for the path");
    assert_eq!(amounts[3], None, "an external-reference row carries no inline path");
    assert_eq!(
        file.read_payload_paths(&refs, "extra").unwrap()[6],
        Some(VariantValue::String("extra-0".to_owned())),
        "a residual-only path comes back from the residual"
    );
}

#[test]
fn batched_reads_over_long_text_agree_and_fsst_plaintext_size_matches_decode() {
    let notes: Vec<String> = (0..48u64)
        .map(|i| format!("free text body {i} about deal {} in region {}", i * 7919, i % 5))
        .collect();
    let mut rows: Vec<_> = (0..48u64)
        .map(|i| freetext_row(i, (i % 5 != 2).then(|| notes[i as usize].as_str())))
        .collect();
    for (ordinal, row) in rows.iter_mut().enumerate() {
        if let PayloadInput::Variant(VariantValue::Object(fields)) = &mut row.event.payload {
            fields.insert(
                "label".to_owned().into(),
                VariantValue::String(format!("label-{ordinal}-{}", "x".repeat(ordinal % 40))),
            );
        }
    }
    let mut config = freetext_config();
    config.targets.index_granularity = 24;
    config.lifecycle = BuildLifecycle::FreshPublicationWithoutTrailing;
    let built = build_oe_file(rows, &config).unwrap();
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    let refs: Vec<PayloadRef> = [40, 1, 2, 3, 5, 3, 47, 12, 13, 14, 15, 0, 30, 31]
        .into_iter()
        .map(|row_ordinal| PayloadRef { row_ordinal })
        .collect();
    for path in ["label", "note", "amount"] {
        let batched = file.read_payload_paths(&refs, path).unwrap();
        for (reference, value) in refs.iter().zip(&batched) {
            assert_eq!(
                *value,
                file.payload_path(reference.row_ordinal, path).unwrap(),
                "`{path}`"
            );
        }
    }
    let batch = file.read_payloads(&refs).unwrap();
    for (reference, payload) in refs.iter().zip(&batch.payloads) {
        assert_eq!(*payload, file.payload(reference.row_ordinal).unwrap());
    }
    let mut fsst_blocks = 0;
    for column in &file.footer.columns {
        for granule in &file.footer.granules {
            let Ok(Some(bytes)) = file.string_block_plaintext_bytes(column.column_id, granule.granule_id) else {
                continue;
            };
            let ColumnData::Strings(values) = file.read_column(column.column_id, granule.granule_id).unwrap().data
            else {
                panic!("an FSST block decodes to strings");
            };
            assert_eq!(bytes, values.text_len() as u64);
            fsst_blocks += 1;
        }
    }
    assert!(fsst_blocks > 0, "the long text must store at least one FSST block");
}

#[test]
fn read_payload_paths_reads_each_touched_granules_block_once() {
    let rows: Vec<_> = (0..24).map(|i| freetext_row(i, Some("free text body"))).collect();
    let built = build_oe_file(rows, &freetext_config()).unwrap();
    assert!(built.footer.shredded.iter().any(|entry| entry.path == "amount"));
    let refs: Vec<PayloadRef> = [14, 1, 6, 3, 17, 8, 4, 12, 10, 0, 15, 7]
        .into_iter()
        .map(|row_ordinal| PayloadRef { row_ordinal })
        .collect();

    let batched = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    let before = batched.column_block_reads();
    batched.read_payload_paths(&refs, "amount").unwrap();
    assert_eq!(
        batched.column_block_reads() - before,
        3,
        "one block read per granule the references touch, not one per reference"
    );

    let per_row = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    let before = per_row.column_block_reads();
    for reference in &refs {
        per_row.payload_path(reference.row_ordinal, "amount").unwrap();
    }
    assert!(
        per_row.column_block_reads() - before > 3,
        "the per-row read re-reads the block per row — this is the cost the batch removes"
    );
}

#[test]
fn cold_scan_never_initializes_the_search_cache_a_point_lookup_pays_for() {
    let rows: Vec<_> = (0..20).map(|i| freetext_row(i, Some("free text body"))).collect();
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
            index_granularity: 6,
            index_granularity_bytes: 1 << 20,
            stripe_target_bytes: 4096,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
        },
        tenant_id: TenantId::new_test_id(3),
        time_series_bucket_nanos: None,
        delivery_lineage: false,
    };
    let built = build_oe_file(rows.clone(), &config).unwrap();
    let shredded_column_id = built
        .footer
        .shredded
        .iter()
        .find(|entry| entry.path == "amount")
        .expect("amount auto-shreds")
        .column_id;

    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    file.bulk_read_family(shredded_column_id).unwrap();
    assert!(
        file.column_cache.is_empty(),
        "a cold scan must not initialize the search cache"
    );

    file.payload(0).unwrap();
    assert!(
        !file.column_cache.is_empty(),
        "a point lookup must initialize the search cache"
    );
}

fn small_stripe_config(stripe_target_bytes: usize) -> OeBuildConfig {
    OeBuildConfig {
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
            index_granularity: 4,
            index_granularity_bytes: 1 << 20,
            stripe_target_bytes,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
        },
        tenant_id: TenantId::new_test_id(3),
        time_series_bucket_nanos: None,
        delivery_lineage: false,
    }
}

fn assert_footers_match_modulo_marks_form(decoded: &Footer, built: &Footer) {
    let mut decoded = decoded.clone();
    let mut built = built.clone();
    for footer in [&mut decoded, &mut built] {
        footer.marks = Vec::new();
        footer.marks_directory = Vec::new();
        footer.marks_pages = Vec::new();
        footer.page_directory = Vec::new();
    }
    assert_eq!(decoded, built);
}

#[test]
fn columnar_marks_pruned_stripe_never_decodes_its_marks_page() {
    let rows: Vec<_> = (0..40).map(|i| freetext_row(i, Some("free text body"))).collect();
    let built = build_oe_file(rows.clone(), &small_stripe_config(1)).unwrap();
    assert!(
        built.footer.stripes.len() > 1,
        "test needs multiple stripes to prove pruning skips one"
    );
    let shredded_column_id = built
        .footer
        .shredded
        .iter()
        .find(|entry| entry.path == "amount")
        .expect("amount auto-shreds")
        .column_id;

    let columnar = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    let mut row_oriented = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    row_oriented.marks = Marks::Eager(Arc::new(
        built
            .footer
            .marks
            .iter()
            .map(|mark| ((mark.column_id, mark.projection_id, mark.granule_id), *mark))
            .collect(),
    ));
    Arc::make_mut(&mut row_oriented.index).page_marks = built
        .footer
        .page_directory
        .iter()
        .map(|entry| {
            (
                (entry.column_id, entry.projection_id, entry.granule_id, entry.page_index),
                *entry,
            )
        })
        .collect();

    let surviving_granule = columnar.footer.granules[0];
    let pruned_stripe_id = columnar
        .footer
        .granules
        .iter()
        .map(|granule| granule.stripe_id)
        .find(|stripe_id| *stripe_id != surviving_granule.stripe_id)
        .expect("test built more than one stripe");
    let pruned_granule = columnar
        .footer
        .granules
        .iter()
        .find(|granule| granule.stripe_id == pruned_stripe_id)
        .expect("a granule sits in the pruned stripe")
        .granule_id;

    let got = columnar
        .read_column(shredded_column_id, surviving_granule.granule_id)
        .expect("surviving stripe reads");
    let expected = row_oriented
        .read_column(shredded_column_id, surviving_granule.granule_id)
        .expect("row-oriented reads");
    assert_eq!(got.data, expected.data, "columnar and row-oriented marks must agree");
    assert_eq!(got.presence, expected.presence);

    let got_page = columnar
        .read_page(shredded_column_id, surviving_granule.granule_id, 0)
        .expect("surviving stripe's page reads");
    let expected_page = row_oriented
        .read_page(shredded_column_id, surviving_granule.granule_id, 0)
        .expect("row-oriented page reads");
    assert_eq!(
        got_page.data, expected_page.data,
        "columnar and row-oriented per-page directories must agree"
    );
    assert_eq!(got_page.presence, expected_page.presence);

    let Marks::Columnar(state) = &columnar.marks else {
        panic!("expected columnar marks");
    };
    let guard = state.read().unwrap();
    assert_eq!(
        guard.shared_extents_found,
        HashSet::from([surviving_granule.stripe_id]),
        "only the surviving stripe may be touched at all"
    );
    assert!(
        !guard.shared_extents_found.contains(&pruned_stripe_id),
        "a pruned stripe's marks pages must never be fetched or decoded"
    );
    assert_eq!(
        guard.decoded_pages,
        HashSet::from([(shredded_column_id, 0, surviving_granule.stripe_id)]),
        "reads of one column decode that column's marks page, not every column's in the stripe"
    );
    assert!(
        !guard
            .page_marks
            .keys()
            .any(|(_, _, granule_id, _)| *granule_id == pruned_granule),
        "a pruned stripe's per-page directory must never be fetched or decoded"
    );
}

#[test]
fn open_refuses_on_a_required_feature_beyond_what_this_reader_knows() {
    let rows: Vec<_> = (0..4).map(|i| freetext_row(i, Some("free text body"))).collect();
    let built = build_oe_file(rows.clone(), &small_stripe_config(4096)).unwrap();

    let mut footer = built.footer.clone();
    let unknown_bit = 1u64 << 40;
    footer.required_feature_flags |= unknown_bit;
    let new_footer_blob = crate::layout::footer::encode_footer(&footer);

    assert_eq!(&built.bytes[built.bytes.len() - 4..], b"OEF1");
    let old_footer_len = u64::from_le_bytes(
        built.bytes[built.bytes.len() - 12..built.bytes.len() - 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let stripe_region_end = built.bytes.len() - (old_footer_len + 12);
    let mut tampered = built.bytes[..stripe_region_end].to_vec();
    tampered.extend_from_slice(&new_footer_blob);
    tampered.extend_from_slice(&(new_footer_blob.len() as u64).to_le_bytes());
    tampered.extend_from_slice(b"OEF1");

    let err = OeFile::open(tampered, None).expect_err("an unknown required feature must refuse");
    assert_eq!(err, FormatError::UnknownRequiredFeature { bits: unknown_bit });
}

#[test]
fn open_rejects_a_granule_directory_entry_whose_row_range_overflows_instead_of_panicking() {
    let rows: Vec<_> = (0..12).map(|i| freetext_row(i, Some("free text body"))).collect();
    let built = build_oe_file(rows, &small_stripe_config(1)).unwrap();
    assert!(
        built.footer.granules.len() >= 2,
        "test needs at least two granules to exercise the pairwise ascending check"
    );

    let mut footer = built.footer.clone();
    footer.granules[0].first_row_ordinal = u64::MAX;
    let new_footer_blob = crate::layout::footer::encode_footer(&footer);

    let old_footer_len = u64::from_le_bytes(
        built.bytes[built.bytes.len() - 12..built.bytes.len() - 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let stripe_region_end = built.bytes.len() - (old_footer_len + 12);
    let mut tampered = built.bytes[..stripe_region_end].to_vec();
    tampered.extend_from_slice(&new_footer_blob);
    tampered.extend_from_slice(&(new_footer_blob.len() as u64).to_le_bytes());
    tampered.extend_from_slice(b"OEF1");

    let err = OeFile::open(tampered, None).expect_err("a granule row range that overflows u64 must refuse, not panic");
    assert_eq!(
        err,
        FormatError::Structural {
            rule: "granule directory must be strictly ascending by first_row_ordinal with no overlap",
        }
    );
}

fn granule_at(granule_id: u32, first_row_ordinal: u64, row_count: u32) -> GranuleEntry {
    GranuleEntry {
        compressed_bytes_estimate: 0,
        first_epoch: 1,
        first_row_ordinal,
        first_sequence: 0,
        granule_id,
        last_epoch: 1,
        last_sequence: 0,
        max_ingested_at_physical: 0,
        max_occurred_at_physical: 0,
        min_ingested_at_physical: 0,
        min_occurred_at_physical: 0,
        row_count,
        stripe_id: 0,
    }
}

#[test]
fn granules_overlapping_rows_matches_a_full_directory_scan() {
    let directories = [
        Vec::new(),
        vec![granule_at(0, 5, 10)],
        vec![granule_at(0, 0, 0)],
        vec![
            granule_at(0, 0, 4),
            granule_at(1, 4, 0),
            granule_at(2, 4, 4),
            granule_at(3, 10, 0),
            granule_at(4, 10, 5),
            granule_at(5, 20, 1),
            granule_at(6, 21, 0),
            granule_at(7, u64::MAX - 3, 3),
        ],
    ];
    for directory in &directories {
        let mut bounds = vec![0, u64::MAX];
        for granule in directory {
            let end = granule.first_row_ordinal + u64::from(granule.row_count);
            for bound in [granule.first_row_ordinal, end] {
                bounds.extend([bound.saturating_sub(1), bound, bound.saturating_add(1)]);
            }
        }
        for &start in &bounds {
            for &end in &bounds {
                let rows = start..end;
                let scanned: Vec<u32> = directory
                    .iter()
                    .filter(|granule| {
                        granule.first_row_ordinal < rows.end
                            && rows.start < granule.first_row_ordinal + u64::from(granule.row_count)
                    })
                    .map(|granule| granule.granule_id)
                    .collect();
                let searched: Vec<u32> = granules_overlapping_rows(directory, &rows)
                    .iter()
                    .map(|granule| granule.granule_id)
                    .collect();
                assert_eq!(searched, scanned, "rows {rows:?} over {directory:?}");
            }
        }
    }
}

#[test]
fn aliased_extents_are_found_without_decoding_every_columns_marks() {
    let rows: Vec<_> = (0..8).map(|i| freetext_row(i, Some("free text body"))).collect();
    let mut config = freetext_config();
    let twin = |column_id: u32, name: &str| AnalyticalColumn {
        column_id,
        data: ColumnData::I64((0..8).map(|i| 5_000 + i).collect()),
        internal_only: false,
        kind: ColumnKind::I64,
        name: name.to_owned(),
        substring_searchable: false,
    };
    config.analytical_columns = vec![
        twin(column_ids::CONTEXT_BASE, "left"),
        twin(column_ids::CONTEXT_BASE + 1, "right"),
    ];
    let built = build_oe_file(rows, &config).unwrap();
    assert!(built.aliased_block_bytes > 0, "identical blocks must be aliased");

    let mut columnar_footer = built.footer.clone();
    columnar_footer.required_feature_flags |= required_features::COLUMNAR_MARKS;
    let blob = crate::layout::footer::encode_footer(&columnar_footer);
    let old_footer_len = u64::from_le_bytes(
        built.bytes[built.bytes.len() - 12..built.bytes.len() - 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let mut columnar_bytes = built.bytes[..built.bytes.len() - (old_footer_len + 12)].to_vec();
    columnar_bytes.extend_from_slice(&blob);
    columnar_bytes.extend_from_slice(&(blob.len() as u64).to_le_bytes());
    columnar_bytes.extend_from_slice(b"OEF1");

    let file = OeFile::open(columnar_bytes, None).unwrap();
    let expected = ColumnData::I64((0..6).map(|i| 5_000 + i).collect());
    for column_id in [column_ids::CONTEXT_BASE, column_ids::CONTEXT_BASE + 1] {
        assert_eq!(file.read_column(column_id, 0).unwrap().data, expected);
    }
    assert_eq!(
        file.column_block_reads(),
        1,
        "an aliased extent must be fetched and decoded once, found from the stripe's other columns"
    );

    let Marks::Columnar(state) = &file.marks else {
        panic!("expected columnar marks");
    };
    let decoded = state.read().unwrap().decoded_pages.clone();
    assert_eq!(
        decoded.len(),
        2,
        "only the two columns read have their marks decoded, not the whole stripe's"
    );
}

#[test]
fn aliased_extent_cache_hit_is_revalidated_against_the_asking_mark() {
    let data = ColumnData::I64((0..8).collect());
    let encoded = encode_block(&data, false);
    let mut body = Writer::with_capacity(4 + encoded.bytes.len());
    crate::layout::encode_presence(&[], 8, &mut body);
    body.put_slice(&encoded.bytes);
    let block = body.into_bytes();
    let block_len = block.len() as u64;

    let mark_a = ColumnMark {
        codec_pipeline_id: encoded.pipeline,
        column_id: 5000,
        compressed_offset: 0,
        compressed_size: block_len,
        first_value_offset: None,
        granule_id: 0,
        page_count: 1,
        projection_id: 0,
        row_count: 8,
        uncompressed_offset: 0,
        uncompressed_size: 0,
    };
    let mark_b = ColumnMark {
        column_id: 5001,
        row_count: 4096,
        ..mark_a
    };

    let mut file = file_with_mark(mark_a, block, 0);
    file.marks = Marks::Eager(Arc::new(
        [((mark_a.column_id, 0, 0), mark_a), ((mark_b.column_id, 0, 0), mark_b)].into(),
    ));
    file.aliased_extents = [(0u64, block_len)].into_iter().collect();

    let first = file.read_column(mark_a.column_id, 0).unwrap();
    assert_eq!(first.data, data);

    let second = file.read_column(mark_b.column_id, 0);
    assert!(
        second.is_err(),
        "a cache hit for column {} must be re-validated against its own row count instead of reusing column {}'s cached decode",
        mark_b.column_id,
        mark_a.column_id
    );
}

#[test]
fn columnar_only_file_is_served_when_understood_and_refuses_when_not() {
    let rows: Vec<_> = (0..12).map(|i| freetext_row(i, Some("free text body"))).collect();
    let built = build_oe_file(rows.clone(), &small_stripe_config(1)).unwrap();
    assert!(
        built.footer.stripes.len() > 1,
        "test needs multiple stripes to prove pruning skips one"
    );
    let shredded_column_id = built
        .footer
        .shredded
        .iter()
        .find(|entry| entry.path == "amount")
        .expect("amount auto-shreds")
        .column_id;

    let reframe = |footer: &Footer| -> Vec<u8> {
        let blob = crate::layout::footer::encode_footer(footer);
        let old_footer_len = u64::from_le_bytes(
            built.bytes[built.bytes.len() - 12..built.bytes.len() - 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let stripe_region_end = built.bytes.len() - (old_footer_len + 12);
        let mut out = built.bytes[..stripe_region_end].to_vec();
        out.extend_from_slice(&blob);
        out.extend_from_slice(&(blob.len() as u64).to_le_bytes());
        out.extend_from_slice(b"OEF1");
        out
    };

    let mut columnar_footer = built.footer.clone();
    columnar_footer.required_feature_flags |= required_features::COLUMNAR_MARKS;
    let columnar_bytes = reframe(&columnar_footer);

    let columnar = OeFile::open(columnar_bytes, None).expect("this reader understands columnar_marks");
    assert_ne!(
        columnar.footer().required_feature_flags & required_features::COLUMNAR_MARKS,
        0
    );
    assert!(
        columnar.footer().marks.is_empty() && columnar.footer().page_directory.is_empty(),
        "a columnar-only file carries no row-oriented marks or page directory"
    );

    let row_oriented = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    let surviving = columnar.footer().granules[0];
    let got = columnar
        .read_column(shredded_column_id, surviving.granule_id)
        .expect("surviving stripe reads through the columnar path");
    let expected = row_oriented
        .read_column(shredded_column_id, surviving.granule_id)
        .expect("row-oriented reads");
    assert_eq!(got.data, expected.data, "columnar and row-oriented reads must agree");
    assert_eq!(got.presence, expected.presence);

    let pruned_stripe_id = columnar
        .footer()
        .granules
        .iter()
        .map(|granule| granule.stripe_id)
        .find(|stripe_id| *stripe_id != surviving.stripe_id)
        .expect("more than one stripe");
    let Marks::Columnar(state) = &columnar.marks else {
        panic!("expected columnar marks");
    };
    let guard = state.read().unwrap();
    assert_eq!(
        guard.shared_extents_found,
        HashSet::from([surviving.stripe_id]),
        "only the surviving stripe may be touched at all"
    );
    assert!(
        !guard.shared_extents_found.contains(&pruned_stripe_id),
        "a pruned stripe's marks pages must never be fetched or decoded"
    );
    assert_eq!(
        guard.decoded_pages,
        HashSet::from([(shredded_column_id, 0, surviving.stripe_id)]),
        "reading one column decodes that column's marks page, not every column's in the stripe"
    );
    drop(guard);

    let unknown_bit = 1u64 << 40;
    let mut unknown_footer = columnar_footer.clone();
    unknown_footer.required_feature_flags |= unknown_bit;
    let err = OeFile::open(reframe(&unknown_footer), None).expect_err("an unknown required feature must refuse");
    assert_eq!(err, FormatError::UnknownRequiredFeature { bits: unknown_bit });

    assert!(crate::compat::check_features(required_features::COLUMNAR_MARKS, 0).is_ok());
    assert_eq!(
        crate::compat::check_features(required_features::COLUMNAR_MARKS | unknown_bit, 0),
        Err(FormatError::UnknownRequiredFeature { bits: unknown_bit })
    );
}

#[test]
fn embedding_row_slot_resolves_offset_and_len_from_the_index() {
    let mark = ColumnMark {
        codec_pipeline_id: PipelineId(0),
        column_id: 5000,
        compressed_offset: 0,
        compressed_size: 0,
        first_value_offset: None,
        granule_id: 0,
        page_count: 1,
        projection_id: 0,
        row_count: 2,
        uncompressed_offset: 0,
        uncompressed_size: 0,
    };
    let mut bytes = vec![0u8; 220];
    bytes[100..108].copy_from_slice(&[0, 0, 0, 0, 4, 0, 0, 0]);
    bytes[108..116].copy_from_slice(&[4, 0, 0, 0, 6, 0, 0, 0]);
    bytes[200..204].copy_from_slice(&[1, 2, 3, 4]);
    bytes[204..210].copy_from_slice(&[5, 6, 7, 8, 9, 10]);

    let mut file = file_with_mark(mark, bytes, optional_features::TYPED_COLUMN_ROW_OFFSETS);
    Arc::make_mut(&mut file.index).embedding_row_offsets = [(
        (5000, 0),
        EmbeddingRowOffsets {
            bytes_len: 10,
            bytes_offset: 200,
            column_id: 5000,
            granule_id: 0,
            offsets_len: 16,
            offsets_offset: 100,
        },
    )]
    .into();

    let (offset, len) = file.embedding_row_slot(5000, 0, 0).unwrap().expect("row 0 has a value");
    assert_eq!((offset, len), (0, 4));
    assert_eq!(
        file.embedding_row_bytes(5000, 0, offset, len).unwrap(),
        vec![1, 2, 3, 4]
    );

    let (offset, len) = file.embedding_row_slot(5000, 0, 1).unwrap().expect("row 1 has a value");
    assert_eq!((offset, len), (4, 6));
    assert_eq!(
        file.embedding_row_bytes(5000, 0, offset, len).unwrap(),
        vec![5, 6, 7, 8, 9, 10]
    );
}

#[test]
fn embedding_row_slot_is_none_without_an_index_for_the_column_or_granule() {
    let mark = ColumnMark {
        codec_pipeline_id: PipelineId(0),
        column_id: 5000,
        compressed_offset: 0,
        compressed_size: 0,
        first_value_offset: None,
        granule_id: 0,
        page_count: 1,
        projection_id: 0,
        row_count: 1,
        uncompressed_offset: 0,
        uncompressed_size: 0,
    };
    let file = file_with_mark(mark, Vec::new(), 0);
    assert_eq!(file.embedding_row_slot(5000, 0, 0).unwrap(), None);
}

#[test]
fn embedding_value_for_row_reads_via_the_index_in_bounded_iops() {
    let mark = ColumnMark {
        codec_pipeline_id: PipelineId(0),
        column_id: 5000,
        compressed_offset: 0,
        compressed_size: 0,
        first_value_offset: None,
        granule_id: 0,
        page_count: 1,
        projection_id: 0,
        row_count: 2,
        uncompressed_offset: 0,
        uncompressed_size: 0,
    };
    let mut bytes = vec![0u8; 220];
    bytes[100..108].copy_from_slice(&[0, 0, 0, 0, 4, 0, 0, 0]);
    bytes[108..116].copy_from_slice(&[4, 0, 0, 0, 6, 0, 0, 0]);
    bytes[200..204].copy_from_slice(&[1, 2, 3, 4]);
    bytes[204..210].copy_from_slice(&[5, 6, 7, 8, 9, 10]);

    let mut file = file_with_mark(mark, bytes, optional_features::TYPED_COLUMN_ROW_OFFSETS);
    Arc::make_mut(&mut file.index).embedding_row_offsets = [(
        (5000, 0),
        EmbeddingRowOffsets {
            bytes_len: 10,
            bytes_offset: 200,
            column_id: 5000,
            granule_id: 0,
            offsets_len: 16,
            offsets_offset: 100,
        },
    )]
    .into();

    assert_eq!(file.embedding_value_for_row(5000, 0).unwrap(), Some(vec![1, 2, 3, 4]));
    assert_eq!(
        file.embedding_value_for_row(5000, 1).unwrap(),
        Some(vec![5, 6, 7, 8, 9, 10])
    );
}

#[test]
fn embedding_value_for_row_falls_back_to_whole_block_decode_without_the_index() {
    let data = ColumnData::Strings(vec![Some("row-zero-vector".to_owned()), Some("row-one-vector".to_owned())].into());
    let encoded = encode_block(&data, false);
    let mut body = Writer::with_capacity(4 + encoded.bytes.len());
    crate::layout::encode_presence(&[], 2, &mut body);
    body.put_slice(&encoded.bytes);
    let block = body.into_bytes();

    let mark = ColumnMark {
        codec_pipeline_id: encoded.pipeline,
        column_id: 5000,
        compressed_offset: 0,
        compressed_size: block.len() as u64,
        first_value_offset: None,
        granule_id: 0,
        page_count: 1,
        projection_id: 0,
        row_count: 2,
        uncompressed_offset: 0,
        uncompressed_size: 0,
    };
    let file = file_with_mark(mark, block, 0);

    assert_eq!(
        file.embedding_value_for_row(5000, 0).unwrap(),
        Some(b"row-zero-vector".to_vec())
    );
    assert_eq!(
        file.embedding_value_for_row(5000, 1).unwrap(),
        Some(b"row-one-vector".to_vec())
    );
}

#[test]
fn a_file_without_compressed_presence_reads_the_legacy_presence_frame() {
    let data = ColumnData::Strings(vec![Some("first".to_owned()), Some("second".to_owned())].into());
    let encoded = encode_block(&data, false);
    let mut body = Writer::with_capacity(4 + encoded.bytes.len());
    body.put_u32(0);
    body.put_slice(&encoded.bytes);
    let block = body.into_bytes();

    let mark = ColumnMark {
        codec_pipeline_id: encoded.pipeline,
        column_id: 5000,
        compressed_offset: 0,
        compressed_size: block.len() as u64,
        first_value_offset: None,
        granule_id: 0,
        page_count: 1,
        projection_id: 0,
        row_count: 2,
        uncompressed_offset: 0,
        uncompressed_size: 0,
    };
    let mut file = file_with_mark(mark, block, 0);
    Arc::make_mut(&mut file.footer).required_feature_flags &= !required_features::COMPRESSED_PRESENCE;

    let read = file.read_column(5000, 0).unwrap();
    assert_eq!(read.data, data);
    assert!(read.presence.is_empty(), "a dense legacy block has no presence bitmap");
}

#[test]
fn concat_rejects_decimal_pages_with_differing_scales() {
    let err = concat_column_data(
        ColumnData::Decimal {
            values: vec![15, 20],
            scale: 1,
        },
        ColumnData::Decimal {
            values: vec![150],
            scale: 2,
        },
    )
    .unwrap_err();
    assert!(matches!(
        err,
        FormatError::Structural { rule } if rule == "multi-page mark pages must have the same decimal scale"
    ));

    let merged = concat_column_data(
        ColumnData::Decimal {
            values: vec![15, 20],
            scale: 1,
        },
        ColumnData::Decimal {
            values: vec![7],
            scale: 1,
        },
    )
    .unwrap();
    assert_eq!(
        merged,
        ColumnData::Decimal {
            values: vec![15, 20, 7],
            scale: 1
        }
    );
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && haystack.windows(needle.len()).any(|window| window == needle)
}

#[test]
fn encrypted_footer_round_trips_and_is_opaque_at_rest() {
    let rows: Vec<_> = (0..20).map(|i| freetext_row(i, Some("free text body"))).collect();
    let dek = [7u8; 32];
    let plain = build_oe_file(rows.clone(), &small_stripe_config(4096)).unwrap();
    let mut enc_config = small_stripe_config(4096);
    enc_config.footer_encryption = crate::security::FooterEncryption::Encrypted;
    enc_config.footer_dek = Some(dek);
    let enc = build_oe_file(rows.clone(), &enc_config).unwrap();

    assert_ne!(enc.header.feature_flags & required_features::FOOTER_ENCRYPTED, 0);
    let end = enc.bytes.len() - 12;
    let start = end - (enc.footer_len as usize - 12);
    let enc_region = &enc.bytes[start..end];
    let plaintext_blob = crate::layout::footer::encode_footer(&enc.footer);
    assert_ne!(
        enc_region,
        plaintext_blob.as_slice(),
        "the sealed footer must not be the plaintext blob"
    );
    assert!(
        !contains_subslice(&enc.bytes, b"epoch"),
        "a sealed footer must not leak column names at rest",
    );

    assert!(OeFile::open(enc.bytes.clone(), Some(&enc.file_seal)).is_err());
    assert!(OeFile::open_with_keys(enc.bytes.clone(), Some(&enc.file_seal), None).is_err());

    let opened = OeFile::open_with_keys(enc.bytes.clone(), Some(&enc.file_seal), Some(&dek)).unwrap();
    assert_footers_match_modulo_marks_form(opened.footer(), &plain.footer);
    let plain_file = OeFile::open(plain.bytes.clone(), Some(&plain.file_seal)).unwrap();
    for row in 0..rows.len() as u64 {
        assert_eq!(
            opened.payload(row).unwrap(),
            plain_file.payload(row).unwrap(),
            "row {row}"
        );
    }
}

fn multi_stripe_file_with_gaps() -> (Vec<u8>, [u8; 32], Footer) {
    let rows: Vec<_> = (0..40).map(|i| freetext_row(i, Some("free text body"))).collect();
    let mut config = small_stripe_config(1);
    config.io_alignment_bytes = 4096;
    let built = build_oe_file(rows, &config).unwrap();
    assert!(built.footer.stripes.len() > 1, "test needs several stripes");
    assert!(
        built.footer.integrity_gaps.iter().any(|gap| gap.byte_len > 0),
        "test needs an alignment gap to corrupt"
    );
    (built.bytes.to_vec(), built.file_seal, built.footer)
}

fn read_shape(file: &OeFile, column_id: u32, granule_id: u32) -> Result<(ColumnData, Vec<u8>), FormatError> {
    file.read_column(column_id, granule_id)
        .map(|read| (read.data, read.presence))
}

#[test]
fn eager_and_lazy_opens_refuse_a_corrupt_byte_in_every_stripe_and_gap() {
    let (bytes, seal, footer) = multi_stripe_file_with_gaps();
    let amount = footer
        .shredded
        .iter()
        .find(|entry| entry.path == "amount")
        .expect("amount auto-shreds")
        .column_id;
    let granule_in = |stripe_id: u32| {
        footer
            .granules
            .iter()
            .find(|granule| granule.stripe_id == stripe_id)
            .expect("every stripe holds a granule")
            .granule_id
    };

    for stripe in &footer.stripes {
        let mut corrupt = bytes.clone();
        corrupt[(stripe.file_offset + stripe.byte_len / 2) as usize] ^= 0x5A;
        assert!(
            matches!(
                OeFile::open(corrupt.clone(), Some(&seal)),
                Err(FormatError::Blake3Mismatch { scope: "stripe" })
            ),
            "eager open must refuse stripe {}",
            stripe.stripe_id
        );
        let lazy = OeFile::open_lazy(corrupt, Some(&seal), None).expect("lazy open defers stripe hashing");
        assert!(
            matches!(
                read_shape(&lazy, amount, granule_in(stripe.stripe_id)),
                Err(FormatError::Blake3Mismatch { scope: "stripe" })
            ),
            "lazy open must refuse the first read into stripe {}",
            stripe.stripe_id
        );
        let intact = footer
            .stripes
            .iter()
            .find(|other| other.stripe_id != stripe.stripe_id)
            .expect("several stripes");
        read_shape(&lazy, amount, granule_in(intact.stripe_id)).expect("an intact stripe still reads");
    }

    for gap in footer.integrity_gaps.iter().filter(|gap| gap.byte_len > 0) {
        let mut corrupt = bytes.clone();
        corrupt[(gap.file_offset + gap.byte_len / 2) as usize] ^= 0x5A;
        for opened in [
            OeFile::open(corrupt.clone(), Some(&seal)),
            OeFile::open_lazy(corrupt, Some(&seal), None),
        ] {
            assert!(
                matches!(opened, Err(FormatError::Blake3Mismatch { scope: "data gap" })),
                "both opens must refuse the gap at {}",
                gap.file_offset
            );
        }
    }
}

#[test]
fn lazy_open_and_fresh_reader_read_what_an_eager_open_reads() {
    let (bytes, seal, footer) = multi_stripe_file_with_gaps();
    let eager = OeFile::open(bytes.clone(), Some(&seal)).unwrap();
    let lazy = OeFile::open_lazy(bytes, Some(&seal), None).unwrap();
    let fresh = lazy.fresh_reader().unwrap();
    for granule in &footer.granules {
        for column in &footer.columns {
            let expected = read_shape(&eager, column.column_id, granule.granule_id).ok();
            assert_eq!(read_shape(&lazy, column.column_id, granule.granule_id).ok(), expected);
            assert_eq!(read_shape(&fresh, column.column_id, granule.granule_id).ok(), expected);
        }
    }
    assert!(eager.column_block_reads() > 0);
}

#[test]
fn fresh_reader_starts_with_nothing_decoded() {
    let (bytes, seal, _) = multi_stripe_file_with_gaps();
    let template = OeFile::open(bytes, Some(&seal)).unwrap();
    let expected = template.payload(0).unwrap();
    assert!(template.column_block_reads() > 0);

    let fresh = template.fresh_reader().unwrap();
    assert_eq!(fresh.column_block_reads(), 0);
    assert_eq!(fresh.decoded_cache_bytes(), 0);
    assert_eq!(fresh.granule_dictionary_decodes(), 0);
    assert_eq!(fresh.footer(), template.footer());
    assert_eq!(fresh.payload(0).unwrap(), expected);
}

#[test]
fn whole_file_open_rejects_an_undeclared_tree_appendix() {
    let rows: Vec<_> = (0..4).map(|i| freetext_row(i, Some("free text body"))).collect();
    let built = build_oe_file(rows.clone(), &small_stripe_config(4096)).unwrap();
    let expected = built.file_seal;

    let tree = vec![0xABu8; 40];
    let mut object = built.bytes.to_vec();
    object.extend_from_slice(&tree);
    object.extend_from_slice(&(tree.len() as u64).to_le_bytes());
    object.extend_from_slice(&TREE_TRAILER_MAGIC);

    assert!(OeFile::open(object, Some(&expected)).is_err());
}

#[test]
fn encrypted_whole_file_open_rejects_an_undeclared_tree_appendix() {
    let rows: Vec<_> = (0..4).map(|i| freetext_row(i, Some("free text body"))).collect();
    let dek = [7u8; 32];
    let mut enc_config = small_stripe_config(4096);
    enc_config.footer_encryption = crate::security::FooterEncryption::Encrypted;
    enc_config.footer_dek = Some(dek);
    let enc = build_oe_file(rows.clone(), &enc_config).unwrap();
    let expected = enc.file_seal;

    let tree = vec![0xCDu8; 24];
    let mut object = enc.bytes.to_vec();
    object.extend_from_slice(&tree);
    object.extend_from_slice(&(tree.len() as u64).to_le_bytes());
    object.extend_from_slice(&TREE_TRAILER_MAGIC);

    assert!(OeFile::open_with_keys(object, Some(&expected), Some(&dek)).is_err());
}

#[test]
fn encrypted_footer_rejects_the_wrong_key() {
    let rows: Vec<_> = (0..8).map(|i| freetext_row(i, Some("free text body"))).collect();
    let mut enc_config = small_stripe_config(4096);
    enc_config.footer_encryption = crate::security::FooterEncryption::Encrypted;
    enc_config.footer_dek = Some([7u8; 32]);
    let enc = build_oe_file(rows.clone(), &enc_config).unwrap();

    let wrong = [9u8; 32];
    assert!(OeFile::open_with_keys(enc.bytes, Some(&enc.file_seal), Some(&wrong)).is_err());
}

#[test]
fn open_refuses_on_a_required_header_feature_beyond_what_this_reader_knows() {
    let rows: Vec<_> = (0..4).map(|i| freetext_row(i, Some("free text body"))).collect();
    let built = build_oe_file(rows.clone(), &small_stripe_config(4096)).unwrap();

    let mut header = built.header.clone();
    let unknown_bit = 1u64 << 40;
    header.feature_flags |= unknown_bit;
    let new_header = crate::layout::encode_header(&header);
    let mut tampered = built.bytes.to_vec();
    tampered[..HEADER_BLOCK_LEN].copy_from_slice(&new_header);

    let err = OeFile::open(tampered, None).expect_err("an unknown required header feature must refuse");
    assert_eq!(err, FormatError::UnknownRequiredFeature { bits: unknown_bit });
}

#[test]
fn two_footers_sealed_under_one_file_dek_use_different_nonces() {
    let dek = [7u8; 32];
    let mut enc_config = small_stripe_config(4096);
    enc_config.footer_encryption = crate::security::FooterEncryption::Encrypted;
    enc_config.footer_dek = Some(dek);

    let sealed_footer = |rows: &[OeRow]| {
        let built = build_oe_file(rows.to_vec(), &enc_config).unwrap();
        let end = built.bytes.len() - 12;
        let start = end - (built.footer_len as usize - 12);
        built.bytes[start..end].to_vec()
    };

    let first = sealed_footer(&(0..8).map(|i| freetext_row(i, Some("first"))).collect::<Vec<_>>());
    let second = sealed_footer(&(0..8).map(|i| freetext_row(i, Some("second"))).collect::<Vec<_>>());

    assert_ne!(first[..12], second[..12], "distinct footers never share a nonce");
    let repeat_a = sealed_footer(&(0..8).map(|i| freetext_row(i, Some("same"))).collect::<Vec<_>>());
    let repeat_b = sealed_footer(&(0..8).map(|i| freetext_row(i, Some("same"))).collect::<Vec<_>>());
    assert_eq!(repeat_a, repeat_b, "rebuilding an identical file is deterministic");
}

#[test]
fn read_column_string_views_matches_the_materializing_read() {
    let notes = ["free text body", "another note entirely"];
    let rows: Vec<OeRow> = (0..24)
        .map(|i| freetext_row(i, (i % 3 != 0).then(|| notes[i as usize % 2])))
        .collect();
    let built = build_oe_file(rows, &freetext_config()).unwrap();
    let file = OeFile::open(built.bytes, None).unwrap();

    let freetext_column = crate::columns::column_ids::FREETEXT_BASE;
    for granule in &file.footer().granules {
        let materialized = file.read_column(freetext_column, granule.granule_id).unwrap();
        let (presence, views) = file
            .read_column_string_views(freetext_column, granule.granule_id)
            .unwrap()
            .expect("a string column has a view read");
        assert_eq!(presence, materialized.presence);
        let ColumnData::Strings(values) = &materialized.data else {
            panic!("free-text columns decode as strings");
        };
        assert_eq!(views.len(), values.len());
        for (index, value) in values.iter().enumerate() {
            match value {
                Some(text) => assert_eq!(views.value(index), text, "row {index}"),
                None => assert!(views.is_null(index), "row {index} must be null"),
            }
        }

        assert!(
            file.read_column_string_views(crate::columns::column_ids::SEQUENCE, granule.granule_id)
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn multi_page_string_views_match_the_materializing_read() {
    let rows: Vec<OeRow> = (0..24)
        .map(|i| {
            let note = match i % 6 {
                0 | 1 => Some("a long repeated note that stays outside an inline Arrow view"),
                2 => None,
                3 => Some(""),
                4 | 5 => Some("another long repeated note that exercises the next page"),
                _ => unreachable!(),
            };
            freetext_row(i, note)
        })
        .collect();
    let mut config = freetext_config();
    config.page_size_rows = 2;
    let built = build_oe_file(rows, &config).unwrap();
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    let column_id = crate::columns::column_ids::FREETEXT_BASE;

    assert_eq!(file.prepared_string_view_dictionary_count(), 0);
    for granule in &file.footer().granules {
        let mark = file.mark(column_id, 0, granule.granule_id).unwrap().unwrap();
        assert!(mark.page_count > 1, "the fixture must exercise multi-page assembly");
        let materialized = file.read_column(column_id, granule.granule_id).unwrap();
        let (presence, views) = file
            .read_column_string_views(column_id, granule.granule_id)
            .unwrap()
            .expect("every string page is view-decodable");
        assert_eq!(presence, materialized.presence);
        let ColumnData::Strings(values) = materialized.data else {
            panic!("free-text columns decode as strings");
        };
        assert_eq!(views.len(), values.len());
        for (index, expected) in values.iter().enumerate() {
            assert_eq!(views.is_null(index), expected.is_none(), "row {index} nullness");
            if let Some(expected) = expected {
                assert_eq!(views.value(index), expected, "row {index}");
            }
        }
    }
    if file.shared_alphabet(column_id).is_some() {
        assert_eq!(
            file.prepared_string_view_dictionary_count(),
            1,
            "a used shared dictionary is prepared once and then reused"
        );
    }
}

#[test]
fn shared_string_view_dictionaries_are_prepared_lazily() {
    let rows: Vec<OeRow> = (0..64)
        .map(|i| {
            let mut row = freetext_row(i, Some("note"));
            let PayloadInput::Variant(VariantValue::Object(payload)) = &mut row.event.payload else {
                unreachable!("the fixture carries an object payload")
            };
            payload.insert("kind".to_owned().into(), VariantValue::String(format!("k{}", i % 4)));
            row
        })
        .collect();
    let column_id = crate::columns::column_ids::PROMOTED_BASE;
    let mut config = freetext_config();
    config.targets.index_granularity = 16;
    config.promotion.columns.push(PromotedColumn {
        kind: crate::layout::footer::ColumnKind::String,
        name: "kind".to_owned(),
        path: "kind".to_owned(),
        since_schema_version: 1,
        substring_searchable: false,
    });
    let built = build_oe_file(rows, &config).unwrap();
    assert!(
        built
            .footer
            .shared_dictionaries
            .iter()
            .any(|entry| entry.column_id == column_id),
        "the fixture must use a file-scope dictionary"
    );
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    assert_eq!(file.prepared_string_view_dictionary_count(), 0);

    let first = file.footer().granules[0].granule_id;
    file.read_column(crate::columns::column_ids::SEQUENCE, first).unwrap();
    file.read_column(column_id, first).unwrap();
    assert_eq!(
        file.prepared_string_view_dictionary_count(),
        0,
        "owned and unrelated reads must not prepare Arrow dictionaries"
    );
    file.read_column_string_views(column_id, first)
        .unwrap()
        .expect("the shared dictionary block is view-decodable");
    assert_eq!(file.prepared_string_view_dictionary_count(), 1);
    let second = file.footer().granules[1].granule_id;
    file.read_column_string_views(column_id, second)
        .unwrap()
        .expect("later blocks reuse the prepared dictionary");
    assert_eq!(file.prepared_string_view_dictionary_count(), 1);
}

#[test]
fn relationship_lookups_filter_threads_and_resolve_references() {
    use crate::events::relationships::{EventRelationships, RelationshipKind, RelationshipRef, TargetIdSpace};

    let root_id: u128 = 0xF00D_0000;
    let rows: Vec<_> = (0..20)
        .map(|i| {
            let mut row = freetext_row(i, None);
            if i > 0 {
                row.event.relationships = Some(
                    EventRelationships::new(vec![
                        RelationshipRef::to_event(RelationshipKind::Parent, root_id + u128::from(i) - 1),
                        RelationshipRef::to_event(RelationshipKind::Root, root_id),
                    ])
                    .unwrap(),
                );
            }
            row
        })
        .collect();
    let config = OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: 0,
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
        promotion: PromotionPlan::default(),
        targets: LayoutTargets {
            index_granularity: 6,
            index_granularity_bytes: 1 << 20,
            stripe_target_bytes: 4096,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
        },
        tenant_id: TenantId::new_test_id(3),
        time_series_bucket_nanos: None,
        delivery_lineage: false,
    };
    let built = build_oe_file(rows, &config).unwrap();
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    assert!(file.footer().granules.len() > 1, "the thread spans granules");

    let root_bytes = root_id.to_be_bytes();
    assert_eq!(
        file.thread_rows(TargetIdSpace::EventId, &root_bytes).unwrap(),
        (1..20).collect::<Vec<u64>>()
    );
    assert_eq!(
        file.rows_referencing(RelationshipKind::Parent, TargetIdSpace::EventId, &root_bytes)
            .unwrap(),
        vec![1]
    );
    let to_row_seven = RelationshipRef::to_event(RelationshipKind::Parent, root_id + 7);
    assert_eq!(file.resolve_reference(&to_row_seven).unwrap(), vec![7]);
    let dangling = RelationshipRef::to_event(RelationshipKind::Parent, 0xDEAD_BEEF);
    assert_eq!(file.resolve_reference(&dangling).unwrap(), Vec::<u64>::new());
}

#[test]
fn link_lookups_match_whole_references_only() {
    use crate::events::relationships::{EventRelationships, RelationshipKind, RelationshipRef, TargetIdSpace};

    let target: u128 = 0xABCD_0000_0000_0000_0000_0000_0000_0001;
    let other: u128 = 0x1234_0000_0000_0000_0000_0000_0000_0002;
    let mut decoy_bytes = target.to_be_bytes().to_vec();
    decoy_bytes.extend_from_slice(&[0u8; 16]);
    let decoy: [u8; 32] = decoy_bytes.try_into().unwrap();

    let declared = [
        Vec::new(),
        vec![
            RelationshipRef::to_event(RelationshipKind::Link, other),
            RelationshipRef::to_event(RelationshipKind::Link, target),
        ],
        vec![RelationshipRef::to_event(RelationshipKind::Link, target)],
        vec![RelationshipRef::to_protocol(RelationshipKind::Link, decoy)],
        vec![
            RelationshipRef::to_event(RelationshipKind::Link, target),
            RelationshipRef::to_event(RelationshipKind::Link, other),
        ],
    ];
    let rows: Vec<_> = declared
        .into_iter()
        .enumerate()
        .map(|(i, refs)| {
            let mut row = freetext_row(i as u64, None);
            if !refs.is_empty() {
                row.event.relationships = Some(EventRelationships::new(refs).unwrap());
            }
            row
        })
        .collect();
    let config = OeBuildConfig {
        analytical_columns: Vec::new(),
        audit: None,
        created_at_physical: 0,
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
        promotion: PromotionPlan::default(),
        targets: LayoutTargets {
            index_granularity: 6,
            index_granularity_bytes: 1 << 20,
            stripe_target_bytes: 4096,
            max_stripe_bytes: 512 * 1024 * 1024,
            min_bytes_for_wide: 10 * 1024 * 1024,
        },
        tenant_id: TenantId::new_test_id(3),
        time_series_bucket_nanos: None,
        delivery_lineage: false,
    };
    let built = build_oe_file(rows, &config).unwrap();
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();

    assert_eq!(
        file.rows_referencing(RelationshipKind::Link, TargetIdSpace::EventId, &target.to_be_bytes())
            .unwrap(),
        vec![1, 2, 4]
    );
    assert_eq!(
        file.rows_referencing(RelationshipKind::Link, TargetIdSpace::ProtocolEventId, &decoy)
            .unwrap(),
        vec![3]
    );
}

#[test]
fn decoded_block_cache_stays_within_its_budget_and_reads_stay_correct() {
    let notes = [Some("a body of some length"), Some("another note"), None];
    let rows: Vec<_> = (0..36)
        .map(|i| freetext_row(i, notes[i as usize % notes.len()]))
        .collect();
    let built = build_oe_file(rows.clone(), &freetext_config()).unwrap();
    let budget = 256u64;
    let file = OeFile::open(built.bytes, Some(&built.file_seal))
        .unwrap()
        .with_decoded_cache_budget(budget);

    for pass in 0..2 {
        for (ordinal, note) in (0..rows.len() as u64).zip(notes.iter().cycle()) {
            let mut expected = std::collections::BTreeMap::new();
            expected.insert("amount".to_owned(), VariantValue::Int(ordinal as i64));
            if let Some(note) = note {
                expected.insert("note".to_owned(), VariantValue::String((*note).to_owned()));
            }
            let value = file.payload(ordinal).unwrap();
            assert_eq!(
                value,
                PayloadRead::Value(VariantValue::object(expected)),
                "pass {pass} row {ordinal}"
            );
            assert!(
                file.decoded_cache_bytes() <= budget,
                "cache exceeded its budget at pass {pass} row {ordinal}: {} bytes",
                file.decoded_cache_bytes()
            );
        }
    }
}

#[test]
fn a_granules_payload_dictionary_is_decoded_once_for_repeated_point_reads() {
    let rows: Vec<_> = (0..12).map(|i| freetext_row(i, Some("a note"))).collect();
    let built = build_oe_file(rows, &freetext_config()).unwrap();
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();

    for row in 0..6 {
        assert!(matches!(file.payload(row).unwrap(), PayloadRead::Value(_)), "row {row}");
    }
    assert_eq!(
        file.granule_dictionary_decodes(),
        1,
        "one decode for the granule, not one per row"
    );

    assert_eq!(
        file.payload_path(3, "amount").unwrap(),
        Some(VariantValue::Int(3)),
        "the held dictionary resolves the same value a cold decode would"
    );
    assert_eq!(file.granule_dictionary_decodes(), 1);

    assert!(matches!(file.payload(7).unwrap(), PayloadRead::Value(_)));
    assert!(matches!(file.payload(8).unwrap(), PayloadRead::Value(_)));
    assert_eq!(file.granule_dictionary_decodes(), 2);
}

#[test]
fn a_block_recording_an_out_of_window_pipeline_is_refused() {
    let mut mark = ColumnMark {
        column_id: 1,
        projection_id: 0,
        granule_id: 0,
        compressed_offset: 0,
        compressed_size: 8,
        uncompressed_offset: 0,
        uncompressed_size: 8,
        row_count: 1,
        page_count: 1,
        first_value_offset: None,
        codec_pipeline_id: PipelineId(11),
    };
    let file = file_with_mark(mark, vec![0u8; 64], 0);
    assert!(file.read_column_raw(1, 0).is_err(), "retired transform id 11 refuses");

    mark.codec_pipeline_id = PipelineId(200);
    let file = file_with_mark(mark, vec![0u8; 64], 0);
    assert!(file.read_column_raw(1, 0).is_err(), "an unknown family has no window");
}

#[test]
fn a_reader_not_declaring_the_entity_hash_feature_keeps_every_granule() {
    let rows: Vec<_> = (0..24).map(|i| freetext_row(i, Some("note"))).collect();
    let built = build_oe_file(rows, &freetext_config()).unwrap();
    assert!(!built.footer.entity_hash_filters.is_empty());

    let with_filters = OeFile::open(built.bytes.clone(), Some(&built.file_seal)).unwrap();
    let mut without_filters = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    without_filters.usable_optional_features &= !optional_features::ENTITY_HASH_POINT_FILTERS;

    let all_granules = without_filters.footer().granules.len();
    assert!(all_granules > 1, "the fixture must span several granules");
    assert_eq!(without_filters.granules_for_entity_hash(9).len(), all_granules);
    assert_eq!(without_filters.granules_for_entity_hash(9_999).len(), all_granules);
    assert!(with_filters.granules_for_entity_hash(9).len() < all_granules);
    assert!(with_filters.granules_for_entity_hash(9_999).is_empty());
}

fn streaming_built() -> crate::writer::build::BuiltOe {
    let rows: Vec<_> = (0..64u64)
        .map(|i| {
            let mut state = i + 1;
            let note: String = (0..32 * 1024)
                .map(|_| {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    char::from(33 + ((state >> 32) % 94) as u8)
                })
                .collect();
            freetext_row(i, Some(&note))
        })
        .collect();
    let built = build_oe_file(rows, &small_stripe_config(64 << 20)).unwrap();
    assert!(built.tree_len.is_some(), "a multi-megabyte stripe carries proof nodes");
    built
}

fn tail_of(built: &crate::writer::build::BuiltOe) -> &[u8] {
    &built.bytes[built.bytes.len() - (built.footer_len + built.tree_len.unwrap_or(0)) as usize..]
}

fn note_column(footer: &Footer) -> u32 {
    footer
        .freetext
        .iter()
        .find(|entry| entry.declared_field == "note")
        .expect("note is declared free text")
        .column_id
}

#[test]
fn open_authenticated_tail_binds_the_seal_without_the_header() {
    let built = streaming_built();
    let tail = tail_of(&built);
    let header = &built.bytes[..HEADER_BLOCK_LEN];
    let commitment = HeaderCommitment::from_header_block(header).unwrap();
    let with_header = OeFooter::open_authenticated(header, tail, built.total_len, &built.file_seal, None).unwrap();
    let tail_only =
        OeFooter::open_authenticated_tail(tail, built.total_len, &built.file_seal, &commitment, None).unwrap();
    assert!(tail_only.is_authenticated());
    assert_eq!(tail_only.footer(), with_header.footer());

    let mut other_header = commitment;
    other_header.blake3[0] ^= 1;
    assert!(matches!(
        OeFooter::open_authenticated_tail(tail, built.total_len, &built.file_seal, &other_header, None),
        Err(FormatError::Blake3Mismatch { scope: "oe file seal" })
    ));
    let mut other_seal = built.file_seal;
    other_seal[0] ^= 1;
    assert!(OeFooter::open_authenticated_tail(tail, built.total_len, &other_seal, &commitment, None).is_err());

    let stripe = &built.footer.stripes[0];
    let offset = crate::file::constant::CHUNK_GROUP_BYTES as u64 - 31;
    let read = tail_only
        .plan_verified_stripe_read(stripe.stripe_id, offset, 97)
        .unwrap();
    assert_eq!(
        read,
        with_header
            .plan_verified_stripe_read(stripe.stripe_id, offset, 97)
            .unwrap()
    );
    let fetched = &built.bytes[read.file_offset as usize..][..read.length as usize];
    assert_eq!(
        tail_only.verify_stripe_range(&read, fetched).unwrap(),
        &built.bytes[(stripe.file_offset + offset) as usize..][..97]
    );
}

#[test]
fn verify_stripe_range_in_serves_ranges_out_of_one_fetch_and_names_the_fault() {
    let built = streaming_built();
    let header = &built.bytes[..HEADER_BLOCK_LEN];
    let footer =
        OeFooter::open_authenticated(header, tail_of(&built), built.total_len, &built.file_seal, None).unwrap();
    let stripe = &built.footer.stripes[0];
    let group = crate::file::constant::CHUNK_GROUP_BYTES as u64;
    let expected = |offset: u64| &built.bytes[(stripe.file_offset + offset) as usize..][..64];

    let a = footer.plan_verified_stripe_read(stripe.stripe_id, 100, 64).unwrap();
    let b = footer
        .plan_verified_stripe_read(stripe.stripe_id, group + 100, 64)
        .unwrap();
    let fetch_start = a.file_offset;
    let fetched = &built.bytes[fetch_start as usize..(b.file_offset + b.length) as usize];
    assert_eq!(
        footer.verify_stripe_range_in(&a, fetched, fetch_start).unwrap(),
        expected(100)
    );
    assert_eq!(
        footer.verify_stripe_range_in(&b, fetched, fetch_start).unwrap(),
        expected(group + 100)
    );
    assert_eq!(
        footer.verify_stripe_range_in(&b, &fetched[..group as usize], fetch_start),
        Err(RangeFault::Corrupt),
        "a fetch that does not cover the planned leaves is never trusted"
    );
    let mut corrupt = fetched.to_vec();
    corrupt[10] ^= 1;
    assert_eq!(
        footer.verify_stripe_range_in(&a, &corrupt, fetch_start),
        Err(RangeFault::Corrupt)
    );

    let mut damaged = built.bytes.to_vec();
    let tree_start = damaged.len() - built.tree_len.unwrap() as usize;
    damaged[tree_start] ^= 1;
    let damaged_tail = &damaged[damaged.len() - tail_of(&built).len()..];
    let damaged_footer =
        OeFooter::open_authenticated(header, damaged_tail, built.total_len, &built.file_seal, None).unwrap();
    assert_eq!(
        damaged_footer.verify_stripe_range_in(&a, fetched, fetch_start),
        Err(RangeFault::TreeUnusable)
    );
    let whole = damaged_footer
        .plan_whole_stripe_read(stripe.stripe_id, 100, 64)
        .unwrap();
    assert_eq!((whole.file_offset, whole.length), (stripe.file_offset, stripe.byte_len));
    let stripe_bytes = &built.bytes[stripe.file_offset as usize..][..stripe.byte_len as usize];
    assert_eq!(
        damaged_footer
            .verify_stripe_range_in(&whole, stripe_bytes, whole.file_offset)
            .unwrap(),
        expected(100)
    );
    let mut corrupt_stripe = stripe_bytes.to_vec();
    corrupt_stripe[5_000] ^= 1;
    assert_eq!(
        damaged_footer.verify_stripe_range_in(&whole, &corrupt_stripe, whole.file_offset),
        Err(RangeFault::Corrupt)
    );
}

#[test]
fn absent_proof_appendix_opens_and_plans_whole_stripe_reads() {
    let built = streaming_built();
    let content_len = built.bytes.len() - built.tree_len.unwrap() as usize;
    let stripped = built.bytes[..content_len].to_vec();
    let note = note_column(&built.footer);
    let eager = OeFile::open(stripped.clone(), Some(&built.file_seal)).unwrap();
    let lazy = OeFile::open_lazy(stripped.clone(), Some(&built.file_seal), None).unwrap();
    assert_eq!(
        read_shape(&lazy, note, 0).unwrap(),
        read_shape(&eager, note, 0).unwrap()
    );

    let tail = &stripped[content_len - built.footer_len as usize..];
    let footer = OeFooter::open_authenticated(
        &stripped[..HEADER_BLOCK_LEN],
        tail,
        content_len as u64,
        &built.file_seal,
        None,
    )
    .unwrap();
    assert!(!footer.proof_trees_present());
    let stripe = &built.footer.stripes[0];
    let read = footer.plan_verified_stripe_read(stripe.stripe_id, 100, 64).unwrap();
    assert_eq!((read.file_offset, read.length), (stripe.file_offset, stripe.byte_len));
    let fetched = &stripped[read.file_offset as usize..][..read.length as usize];
    assert_eq!(
        footer.verify_stripe_range(&read, fetched).unwrap(),
        &stripped[(stripe.file_offset + 100) as usize..][..64]
    );
    let mut corrupt = fetched.to_vec();
    corrupt[7] ^= 1;
    assert!(footer.verify_stripe_range(&read, &corrupt).is_err());
}

#[test]
fn eager_and_lazy_opens_refuse_a_corrupt_footer_byte_and_tolerate_a_corrupt_tree() {
    let built = streaming_built();
    let content_len = built.bytes.len() - built.tree_len.unwrap() as usize;
    let footer_start = content_len - built.footer_len as usize;
    let mut corrupt = built.bytes.to_vec();
    corrupt[footer_start + built.footer_len as usize / 2] ^= 0x5A;
    assert!(OeFile::open(corrupt.clone(), Some(&built.file_seal)).is_err());
    assert!(OeFile::open_lazy(corrupt, Some(&built.file_seal), None).is_err());

    let mut damaged = built.bytes.to_vec();
    damaged[content_len + 3] ^= 0x5A;
    let note = note_column(&built.footer);
    let eager = OeFile::open(damaged.clone(), Some(&built.file_seal)).unwrap();
    let lazy = OeFile::open_lazy(damaged, Some(&built.file_seal), None).unwrap();
    for granule in &built.footer.granules {
        assert_eq!(
            read_shape(&lazy, note, granule.granule_id).unwrap(),
            read_shape(&eager, note, granule.granule_id).unwrap()
        );
    }
}

#[test]
fn cold_footer_locates_the_same_blocks_as_the_whole_file_reader() {
    let (bytes, seal, built_footer) = multi_stripe_file_with_gaps();
    let file = OeFile::open(bytes.clone(), Some(&seal)).unwrap();
    let SpeculativeTail::Opened(cold) = OeFooter::open_speculative(&bytes).unwrap() else {
        panic!("the whole file is a long enough tail");
    };
    for column in &built_footer.columns {
        for granule in &built_footer.granules {
            let stripe = built_footer
                .stripes
                .iter()
                .find(|stripe| stripe.stripe_id == granule.stripe_id)
                .unwrap();
            let page = cold
                .marks_page_range(column.column_id, 0, stripe.stripe_id)
                .map(|range| {
                    bytes[(stripe.file_offset + range.stripe_offset) as usize..][..range.length as usize].to_vec()
                });
            let marks = cold
                .stripe_marks(column.column_id, 0, stripe.stripe_id, page.as_deref())
                .unwrap();
            let found = marks.iter().find(|mark| mark.granule_id == granule.granule_id).copied();
            assert_eq!(
                found,
                file.mark(column.column_id, 0, granule.granule_id).unwrap(),
                "column {} granule {}",
                column.column_id,
                granule.granule_id
            );
            if let Some(mark) = found {
                let range = cold.block_range(&mark).unwrap();
                let via_range = &bytes[(stripe.file_offset + range.stripe_offset) as usize..][..range.length as usize];
                let position = file.file_position(granule.granule_id, mark.compressed_offset).unwrap();
                assert_eq!(via_range, &bytes[position..][..mark.compressed_size as usize]);
            }
        }
    }
}

#[test]
fn a_graph_profile_build_opens_with_its_profile_in_header_and_footer() {
    let rows: Vec<_> = (0..10).map(|i| freetext_row(i, None)).collect();
    let config = OeBuildConfig {
        profile: crate::layout::Profile::Graph,
        ..freetext_config()
    };
    let built = build_oe_file(rows, &config).unwrap();
    assert_eq!(built.header.profile, crate::layout::Profile::Graph);
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    assert_eq!(file.header().profile, crate::layout::Profile::Graph);
    assert_eq!(file.footer().profile, crate::layout::Profile::Graph);
}

fn single_block_file(data: &ColumnData, presence: &[u8], row_count: u32) -> OeFile {
    let encoded = encode_block(data, false);
    let mut body = Writer::with_capacity(4 + encoded.bytes.len());
    crate::layout::encode_presence(presence, row_count, &mut body);
    body.put_slice(&encoded.bytes);
    let block = body.into_bytes();
    let mark = ColumnMark {
        codec_pipeline_id: encoded.pipeline,
        column_id: 5000,
        compressed_offset: 0,
        compressed_size: block.len() as u64,
        first_value_offset: None,
        granule_id: 0,
        page_count: 1,
        projection_id: 0,
        row_count,
        uncompressed_offset: 0,
        uncompressed_size: 0,
    };
    file_with_mark(mark, block, 0)
}

#[test]
fn embedding_value_for_row_fallback_maps_rows_through_the_presence_bitmap() {
    let data = ColumnData::Strings(vec![Some("a".to_owned()), Some("b".to_owned())].into());
    let file = single_block_file(&data, &[0b0000_0110], 3);
    assert_eq!(file.embedding_value_for_row(5000, 0).unwrap(), None);
    assert_eq!(file.embedding_value_for_row(5000, 1).unwrap(), Some(b"a".to_vec()));
    assert_eq!(file.embedding_value_for_row(5000, 2).unwrap(), Some(b"b".to_vec()));
}

#[test]
fn a_dense_block_keeps_answering_point_reads_once_it_is_cached() {
    let values = [10i64, 20, 30];
    let file = single_block_file(&ColumnData::I64(values.to_vec()), &[], 3);
    let granule = file.footer.granules[0];
    for probe in 0..=POINT_READS_BEFORE_WHOLE_GRANULE_DECODE as usize + 8 {
        let row = probe % values.len();
        assert_eq!(
            file.shredded_value_for_row(5000, &granule, row as u64).unwrap(),
            Some(VariantValue::Int(values[row])),
            "probe {probe}"
        );
    }
    let cached = file.cached_column(5000, 0).unwrap();
    assert_eq!(cached.value_at(1), Some(VariantValue::Int(20)));
    assert_eq!(cached.value_at(3), None);
}

#[test]
fn a_tree_trailer_leaving_less_than_a_header_of_content_is_refused() {
    let mut header = minimal_header();
    header.file_id = u128::from(u32::from_le_bytes(*b"OEF1")) << 96;
    let mut bytes = crate::layout::encode_header(&header);
    assert_eq!(bytes.len(), HEADER_BLOCK_LEN);
    assert_eq!(&bytes[16..28], b"\0\0\0\0\0\0\0\0OEF1");
    let tree_len = (bytes.len() - 28) as u64;
    bytes.extend_from_slice(&tree_len.to_le_bytes());
    bytes.extend_from_slice(b"OEFT");
    assert!(matches!(
        OeFile::open(bytes, None),
        Err(FormatError::Truncated { what: "oe body tail" })
    ));
}

fn assemble_minimal(mut footer: Footer) -> Result<OeFile, FormatError> {
    footer.required_feature_flags &= !required_features::STRIPE_RELATIVE_MARKS;
    OeFile::assemble(
        FileBytes::Local(Arc::new(Vec::new())),
        minimal_header(),
        footer,
        0,
        None,
        DECODED_CACHE_BUDGET_BYTES,
    )
}

fn empty_mark(row_count: u32) -> ColumnMark {
    ColumnMark {
        codec_pipeline_id: PipelineId(0),
        column_id: 5000,
        compressed_offset: 0,
        compressed_size: 0,
        first_value_offset: None,
        granule_id: 0,
        page_count: 1,
        projection_id: 0,
        row_count,
        uncompressed_offset: 0,
        uncompressed_size: 0,
    }
}

#[test]
fn a_last_granule_whose_row_range_overflows_is_refused() {
    assemble_minimal(minimal_footer(empty_mark(1))).expect("the unmodified footer assembles");
    let mut footer = minimal_footer(empty_mark(1));
    footer.granules[0].first_row_ordinal = u64::MAX;
    assert!(matches!(
        assemble_minimal(footer),
        Err(FormatError::Structural {
            rule: "granule row range overflows"
        })
    ));
}

#[test]
fn a_mark_declaring_more_rows_than_a_block_can_hold_is_refused() {
    let mut footer = minimal_footer(empty_mark(1));
    footer.marks[0].row_count = u32::MAX;
    assert!(matches!(
        assemble_minimal(footer),
        Err(FormatError::Structural {
            rule: BLOCK_ROWS_BEYOND_BOUND
        })
    ));

    let rows: Vec<_> = (0..8).map(|i| freetext_row(i, Some("free text body"))).collect();
    let built = build_oe_file(rows, &freetext_config()).unwrap();
    let mut columnar_footer = built.footer.clone();
    columnar_footer.required_feature_flags &= !required_features::STRIPE_MARKS_PAGES;
    columnar_footer.required_feature_flags |= required_features::COLUMNAR_MARKS;
    let forged = columnar_footer.marks[0];
    columnar_footer.marks[0].row_count = u32::MAX;
    let blob = crate::layout::footer::encode_footer(&columnar_footer);
    let old_footer_len = u64::from_le_bytes(
        built.bytes[built.bytes.len() - 12..built.bytes.len() - 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let mut columnar_bytes = built.bytes[..built.bytes.len() - (old_footer_len + 12)].to_vec();
    columnar_bytes.extend_from_slice(&blob);
    columnar_bytes.extend_from_slice(&(blob.len() as u64).to_le_bytes());
    columnar_bytes.extend_from_slice(b"OEF1");
    let file = OeFile::open(columnar_bytes, None).unwrap();
    assert!(matches!(
        file.read_column(forged.column_id, forged.granule_id),
        Err(FormatError::Structural {
            rule: BLOCK_ROWS_BEYOND_BOUND
        })
    ));
}

#[test]
fn fresh_reader_shares_the_footer_and_index_with_its_template() {
    let (bytes, seal, footer) = multi_stripe_file_with_gaps();
    let template = OeFile::open(bytes, Some(&seal)).unwrap();
    let fresh = template.fresh_reader().unwrap();
    assert!(Arc::ptr_eq(&template.footer, &fresh.footer));
    assert!(Arc::ptr_eq(&template.index, &fresh.index));
    assert_eq!(fresh.column_block_reads(), 0);
    for granule in &footer.granules {
        for column in &footer.columns {
            assert_eq!(
                read_shape(&fresh, column.column_id, granule.granule_id).ok(),
                read_shape(&template, column.column_id, granule.granule_id).ok()
            );
        }
    }
}
