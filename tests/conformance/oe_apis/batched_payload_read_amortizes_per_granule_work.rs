use crate::support;
use oe::artifacts::batch::PayloadInput;
use oe::layout::reader::{OeFile, PayloadRead, PayloadRef};
use oe::writer::build::{OeRow, build_oe_file};

fn file() -> OeFile {
    let mut rows: Vec<OeRow> = (0..48)
        .map(|i| OeRow {
            epoch: 1,
            sequence: i + 1,
            event: support::event(i),
        })
        .collect();
    rows[10].event.payload = PayloadInput::None;
    let built = build_oe_file(rows, &support::build_config()).unwrap();
    OeFile::open(built.bytes, None).unwrap()
}

#[test]
fn batched_and_per_row_reads_agree_on_values_and_order() {
    let file = file();
    let refs: Vec<PayloadRef> = [40, 3, 17, 47, 3, 33, 0, 25]
        .into_iter()
        .map(|row_ordinal| PayloadRef { row_ordinal })
        .collect();

    let batch = file.read_payloads(&refs).unwrap();
    assert_eq!(batch.payloads.len(), refs.len(), "one result per reference");
    for (reference, read) in refs.iter().zip(&batch.payloads) {
        assert_eq!(
            *read,
            file.payload(reference.row_ordinal).unwrap(),
            "row {} must match the per-row oracle in the caller's position",
            reference.row_ordinal
        );
        assert!(
            matches!(read, PayloadRead::Value(_)),
            "every referenced row here stores a payload"
        );
    }
}

#[test]
fn one_granules_dictionary_is_decoded_once_per_call() {
    let file = file();
    let refs: Vec<PayloadRef> = [1, 5, 3, 12, 7, 2, 15, 9]
        .into_iter()
        .map(|row_ordinal| PayloadRef { row_ordinal })
        .collect();

    let before = file.granule_dictionary_decodes();
    let batch = file.read_payloads(&refs).unwrap();
    assert_eq!(batch.payloads.len(), refs.len());
    assert_eq!(
        file.granule_dictionary_decodes() - before,
        1,
        "a batch confined to one granule decodes that granule's dictionary once for the call, not once per reference"
    );
}

#[test]
fn an_absent_payload_reports_absent_in_place() {
    let file = file();
    let refs = [
        PayloadRef { row_ordinal: 4 },
        PayloadRef { row_ordinal: 10 },
        PayloadRef { row_ordinal: 20 },
    ];

    let batch = file.read_payloads(&refs).unwrap();
    assert!(matches!(batch.payloads[0], PayloadRead::Value(_)));
    assert_eq!(
        batch.payloads[1],
        PayloadRead::None,
        "the row that stores no payload reports absent in its own position"
    );
    assert!(
        matches!(batch.payloads[2], PayloadRead::Value(_)),
        "an absent payload does not fail the rest of the batch"
    );
    assert_eq!(file.payload(10).unwrap(), PayloadRead::None);
}
