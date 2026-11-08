//! Checks that a file whose entity hashes derive from `entity_id` omits the stored hash columns, declares bit 35, and
//! reads every envelope back with the hashes it was written with.
use crate::support::{build_config, event};
use oe::columns::column_ids;
use oe::compat::check_features;
use oe::events::derived_entity_id_hash;
use oe::layout::reader::OeFile;
use oe::layout::required_features;
use oe::writer::build::{OeRow, build_oe_file};

/// conformance:
/// oe-reader-compatibility/entity-hash-columns-may-be-elided-behind-a-required-feature/elided-entity-hashes-read-back-as-written
#[test]
fn elided_entity_hashes_read_back_as_written() {
    let rows: Vec<OeRow> = (0..40u64)
        .map(|i| {
            let mut event = event(i);
            let (low, high) = derived_entity_id_hash(event.envelope.entity_id.as_deref());
            event.envelope.entity_id_hash_low = low;
            event.envelope.entity_id_hash_high = high;
            OeRow {
                epoch: 1,
                sequence: i + 1,
                event,
            }
        })
        .collect();
    let expected: Vec<_> = rows.iter().map(|row| row.event.envelope.clone()).collect();
    let built = build_oe_file(rows, &build_config()).unwrap();
    let flags = built.footer.required_feature_flags;
    assert_ne!(flags & required_features::ENTITY_HASH_COLUMNS_ELIDED, 0);
    assert!(built.footer.columns.iter().all(|column| {
        column.column_id != column_ids::ENTITY_ID_HASH_LOW && column.column_id != column_ids::ENTITY_ID_HASH_HIGH
    }));
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    for (ordinal, envelope) in expected.iter().enumerate() {
        let read = file.envelope(ordinal as u64).unwrap();
        assert_eq!(
            (read.entity_id_hash_low, read.entity_id_hash_high, &read.entity_id),
            (
                envelope.entity_id_hash_low,
                envelope.entity_id_hash_high,
                &envelope.entity_id
            )
        );
    }
    // This reader knows the bit; one whose known set stops below bit 35 does not, and refuses the file.
    assert!(check_features(flags, 0).is_ok());
    assert_ne!(flags & !((1u64 << 35) - 1), 0);
}
