use crate::support;
use oe::artifacts::batch::{PayloadInput, build_batch, decode_batch};
use oe::columns::column_ids;
use oe::encoding::ColumnData;
use oe::events::transcode::{SourceFormat, transcode};
use oe::events::variant::{VariantRef, VariantValue};
use oe::layout::reader::{OeFile, PayloadRead};

#[test]
fn source_body_transcoded_source_bytes_discarded() {
    let body = br#"{"amount": 12, "kind": "won", "nested": {"x": [1, 2]}}"#;
    let value = transcode(SourceFormat::Json, body).unwrap();
    let mut input = support::event(0);
    input.payload = PayloadInput::Variant(value.clone());
    input.source_schema = Some("json:sha256:abc".to_owned());
    let payload = build_batch(std::slice::from_ref(&input), 1, 0).unwrap();
    assert!(
        !payload.windows(body.len()).any(|window| window == body),
        "source bytes must not be stored"
    );
    let decoded = decode_batch(&payload, 1).unwrap();
    let stored = decoded.events[0].payload.unwrap();
    let round = VariantRef::new(stored).decode(&decoded.dictionary).unwrap();
    assert_eq!(round, value);
    assert_eq!(decoded.events[0].source_schema, Some("json:sha256:abc"));
    let raw = transcode(SourceFormat::RawBytes, &[0xFF, 0x00, 0x01]).unwrap();
    assert_eq!(raw, VariantValue::Binary(vec![0xFF, 0x00, 0x01]));
}

#[test]
fn query_satisfiable_from_promoted_columns() {
    let built = support::built_file(32);
    assert!(
        built.footer.shredded.iter().any(|e| e.path == "amount"),
        "amount must be shredded — it is present in every event"
    );

    let file = OeFile::open(built.bytes, None).unwrap();

    let val0 = file.payload_path(0, "amount").unwrap();
    assert_eq!(
        val0,
        Some(VariantValue::Int(1_000)),
        "row 0 amount from shredded column"
    );
    let val10 = file.payload_path(10, "amount").unwrap();
    assert_eq!(
        val10,
        Some(VariantValue::Int(1_010)),
        "row 10 amount from shredded column"
    );

    assert!(!built.footer.shredded.iter().any(|e| e.path == "rare"));
}

#[test]
fn payload_read_only_for_final_matching_rows() {
    let built = support::built_file(32);
    let file = OeFile::open(built.bytes, None).unwrap();

    let surviving = file.granules_for_sequence(1, 5, 5);
    assert!(!surviving.is_empty(), "sequence 5 must survive granule pruning");

    let mut matching_row: Option<u64> = None;
    for granule in &surviving {
        let col = file.read_column(column_ids::SEQUENCE, granule.granule_id).unwrap();
        let ColumnData::U64(sequences) = &col.data else {
            continue;
        };
        for (offset, &seq) in sequences.iter().enumerate() {
            if seq == 5 {
                matching_row = Some(granule.first_row_ordinal + offset as u64);
                break;
            }
        }
    }
    let row = matching_row.expect("sequence 5 must be found within a surviving granule");

    let PayloadRead::Value(value) = file.payload(row).unwrap() else {
        panic!("payload must be present for the matching row");
    };
    let VariantValue::Object(fields) = value else {
        panic!("event payload must be an object");
    };
    assert_eq!(
        fields.get("amount"),
        Some(&VariantValue::Int(1_004)),
        "payload read for the matching row must carry the correct amount"
    );
}
