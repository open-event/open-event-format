//! Checks that events from unrelated tenants are never packed into the same OE file. A OE file carries exactly one
//! tenant; the builder enforces this at construction time, so a cross-tenant mix is rejected before any bytes are
//! written to storage.

use crate::support;
use oe::error::FormatError;
use oe::events::TenantId;
use oe::typed_id::TypedIdTestExt;
use oe::writer::build::{OeRow, build_oe_file};

/// conformance: oe-security-and-isolation/tenant-isolation/cross-tenant-file-rejected
#[test]
fn cross_tenant_file_rejected() {
    // Build a pair of rows where the second row carries a different tenant_id than the one declared in the build
    // configuration.
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

    // build_oe_file detects the tenant mismatch and rejects the batch. The file is never written; no partial
    // ciphertext is produced.
    let result = build_oe_file(vec![row_0, row_1], &support::build_config());
    assert_eq!(
        result.unwrap_err(),
        FormatError::Structural {
            rule: "a OE file carries one tenant"
        },
        "cross-tenant batch must be rejected before any bytes are written"
    );
}
