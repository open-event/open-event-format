use oe::events::TenantId;
use oe::introspection::*;
use oe::typed_id::TypedIdTestExt;

fn record_with_secrets(tenant: TenantId) -> OeFileRecord {
    OeFileRecord {
        created_at: 1,
        embedding_sample: vec![0.5; 8],
        file_seal: [9; 32],
        file_id: 0xA,
        file_size: 8_192,
        generation_id: 3,
        granule_count: 5,
        local_path: "/var/lib/open-event/0xA.oe".to_owned(),
        object_store_credential: "AKIA-super-secret".to_owned(),
        payload_bytes: vec![0xDE, 0xAD, 0xBE, 0xEF],
        row_count: 1_000,
        tenant_id: tenant,
    }
}

#[test]
fn system_table_withholds_secrets() {
    let records = [record_with_secrets(TenantId::new_test_id(7))];

    assert_eq!(
        system_oe_files(IntrospectionCaller::Public, &records),
        Err(IntrospectionError::Unauthorized)
    );

    let rows = system_oe_files(IntrospectionCaller::Admin, &records).unwrap();
    assert_eq!(rows.len(), 1);
    for withheld in WITHHELD_FROM_INTROSPECTION {
        assert!(
            !SystemOeFilesRow::COLUMN_NAMES.contains(&withheld),
            "system.oe_files must not expose `{withheld}`"
        );
    }
}
