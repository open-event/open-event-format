use crate::support;
use oe::error::FormatError;
use oe::events::TenantId;
use oe::typed_id::TypedIdTestExt;
use oe::writer::build::{OeRow, build_oe_file};

#[test]
fn cross_tenant_file_rejected() {
    let row_0 = OeRow {
        epoch: 1,
        sequence: 1,
        event: support::event(0),
    };
    let mut row_1 = OeRow {
        epoch: 1,
        sequence: 2,
        event: support::event(1),
    };
    row_1.event.envelope.tenant_id = TenantId::new_test_id(support::tenant().uuid().as_u128() + 1);

    let result = build_oe_file(vec![row_0, row_1], &support::build_config());
    assert_eq!(
        result.unwrap_err(),
        FormatError::Structural {
            rule: "a OE file carries one tenant"
        },
        "cross-tenant batch must be rejected before any bytes are written"
    );
}
