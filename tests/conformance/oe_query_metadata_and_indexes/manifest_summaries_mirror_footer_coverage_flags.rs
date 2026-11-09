use crate::support;
use oe::artifacts::batch::EventInput;
use oe::events::TimestampValue;
use oe::indexes::summary::summary_from_footer;
use oe::writer::build::{OeRow, build_oe_file};

fn row_with_occurred(i: u64, occurred_nanos: i64) -> OeRow {
    let mut event: EventInput = support::event(i);
    event.envelope.occurred_at = TimestampValue::from_physical_nanos(occurred_nanos);
    OeRow {
        epoch: 1,
        event,
        sequence: i + 1,
    }
}

#[test]
fn late_event_file_is_flagged_in_its_manifest_summary() {
    let in_order: Vec<OeRow> = (0..8)
        .map(|i| row_with_occurred(i, 1_000_000 + i as i64 * 1_000))
        .collect();
    let ordered_file = build_oe_file(in_order.clone(), &support::build_config()).unwrap();
    assert!(
        !summary_from_footer(&ordered_file.footer).has_late_events,
        "a file with occurred_at in ingest order must not be flagged as carrying late events"
    );

    let mut late = in_order;
    late.pop();
    late.push(row_with_occurred(7, 1));
    let late_file = build_oe_file(late, &support::build_config()).unwrap();
    assert!(
        summary_from_footer(&late_file.footer).has_late_events,
        "a row whose occurred_at predates an earlier-ingested row must flag the file as carrying late events"
    );
}
