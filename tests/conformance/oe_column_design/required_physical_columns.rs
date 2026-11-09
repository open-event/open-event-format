use crate::support;
use oe::columns::REQUIRED_COLUMNS;
use oe::events::families::{Caller, authorize_columns, column_allowed};
use oe::layout::reader::OeFile;

#[test]
fn internal_scan_columns_withheld() {
    let built = support::built_file(8);
    let file = OeFile::open(built.bytes, None).unwrap();

    for spec in REQUIRED_COLUMNS {
        assert!(
            file.footer().columns.iter().any(|c| c.column_id == spec.column_id),
            "required column {} missing",
            spec.name
        );
        let granule = file.footer().granules[0].granule_id;
        file.read_column(spec.column_id, granule).unwrap();
    }

    for internal in ["epoch", "sequence", "payload_ref"] {
        let descriptor = file
            .footer()
            .columns
            .iter()
            .find(|c| c.name == internal)
            .unwrap_or_else(|| panic!("column {internal} not in footer"));
        assert!(
            descriptor.internal_only,
            "{internal} must be marked internal_only in the column directory"
        );
        assert!(
            !column_allowed(internal, Caller::Public),
            "{internal} must be blocked at the public scan boundary"
        );
        assert!(
            column_allowed(internal, Caller::Internal),
            "{internal} must remain readable by internal callers"
        );
    }

    let (allowed, dropped) = authorize_columns(
        &["occurred_at", "event_type_id", "epoch", "sequence", "payload_ref"],
        Caller::Public,
    );
    assert_eq!(allowed, vec!["occurred_at", "event_type_id"]);
    assert_eq!(dropped, vec!["epoch", "sequence", "payload_ref"]);
}
