use oe::deletes::{
    BusinessRecord, EntityDisposition, ErasureDisposition, SubjectId, de_identify_business_record,
    resolve_conflicting_dispositions,
};
use oe::typed_id::TypedIdTestExt;
use std::collections::BTreeMap;

#[test]
fn transaction_de_identified_not_deleted() {
    let mut personal_fields = BTreeMap::new();
    personal_fields.insert("email".to_owned(), "jane@example.com".to_owned());
    personal_fields.insert("name".to_owned(), "Jane Doe".to_owned());
    let mut invoice = BusinessRecord {
        amount: 4_200,
        personal_fields,
        subject_id: Some(SubjectId::new_test_id(1)),
    };

    de_identify_business_record(&mut invoice);

    assert_eq!(invoice.subject_id, None);
    assert_eq!(invoice.personal_fields.get("email").unwrap(), "[redacted]");
    assert_eq!(invoice.personal_fields.get("name").unwrap(), "[redacted]");

    assert_eq!(invoice.amount, 4_200);
}

#[test]
fn conflicting_jurisdictions_resolved() {
    let broad_erasure = EntityDisposition {
        disposition: ErasureDisposition::CryptoShred,
        retention_carve_out: false,
    };
    let tax_retention = EntityDisposition {
        disposition: ErasureDisposition::Retain,
        retention_carve_out: true,
    };

    let financial_record_disposition = resolve_conflicting_dispositions(&[broad_erasure, tax_retention]);
    assert_eq!(financial_record_disposition, Some(tax_retention));

    let other_regime = EntityDisposition {
        disposition: ErasureDisposition::RestrictSuppress,
        retention_carve_out: false,
    };
    let everything_else_disposition = resolve_conflicting_dispositions(&[broad_erasure, other_regime]);
    assert_eq!(everything_else_disposition, Some(broad_erasure));
}
