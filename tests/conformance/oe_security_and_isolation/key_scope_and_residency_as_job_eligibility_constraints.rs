use oe::events::TenantId;
use oe::security::{KeyLease, KeyResidencyRegion, KeyScope, is_eligible_for_job};
use oe::typed_id::TypedIdTestExt;

#[test]
fn job_without_key_lease_is_ineligible() {
    let tenant = TenantId::new_test_id(42);
    let other_tenant = TenantId::new_test_id(99);
    let region = KeyResidencyRegion("eu-west".to_owned());

    assert!(
        !is_eligible_for_job(tenant, KeyScope::TenantDek, &region, &[]),
        "a node with no leases must be ineligible for a TenantDek-scoped job"
    );
    assert!(
        !is_eligible_for_job(tenant, KeyScope::SubjectKeys, &region, &[]),
        "a node with no leases must be ineligible for a SubjectKeys-scoped job"
    );

    let wrong_tenant_lease = KeyLease {
        residency: region.clone(),
        scope: KeyScope::TenantDek,
        tenant_id: other_tenant,
    };
    assert!(
        !is_eligible_for_job(tenant, KeyScope::TenantDek, &region, &[wrong_tenant_lease]),
        "a lease for a different tenant must not satisfy the job's key requirement"
    );

    let correct_lease = KeyLease {
        residency: region.clone(),
        scope: KeyScope::TenantDek,
        tenant_id: tenant,
    };
    assert!(
        is_eligible_for_job(tenant, KeyScope::TenantDek, &region, &[correct_lease]),
        "a node with the correct tenant-DEK lease must be eligible for the job"
    );

    let dek_lease = KeyLease {
        residency: region.clone(),
        scope: KeyScope::TenantDek,
        tenant_id: tenant,
    };
    assert!(
        !is_eligible_for_job(tenant, KeyScope::SubjectKeys, &region, &[dek_lease]),
        "a TenantDek lease must not satisfy a SubjectKeys job requirement"
    );

    assert!(
        is_eligible_for_job(tenant, KeyScope::None, &region, &[]),
        "a KeyScope::None job must be eligible on a node with no leases"
    );
}

#[test]
fn job_in_the_wrong_region_is_ineligible() {
    let tenant = TenantId::new_test_id(42);
    let eu_west = KeyResidencyRegion("eu-west".to_owned());
    let us_east = KeyResidencyRegion("us-east".to_owned());

    let dek_lease = KeyLease {
        residency: eu_west.clone(),
        scope: KeyScope::TenantDek,
        tenant_id: tenant,
    };
    let subject_lease = KeyLease {
        residency: eu_west.clone(),
        scope: KeyScope::SubjectKeys,
        tenant_id: tenant,
    };
    let node_leases = [dek_lease, subject_lease];

    assert!(
        is_eligible_for_job(tenant, KeyScope::TenantDek, &eu_west, &node_leases),
        "the node holds the eu-west tenant-DEK lease and is eligible for a eu-west job"
    );
    assert!(
        is_eligible_for_job(tenant, KeyScope::SubjectKeys, &eu_west, &node_leases),
        "the node holds both eu-west leases and is eligible for a eu-west SubjectKeys job"
    );

    assert!(
        !is_eligible_for_job(tenant, KeyScope::TenantDek, &us_east, &node_leases),
        "a eu-west tenant-DEK lease must not satisfy a us-east job"
    );
    assert!(
        !is_eligible_for_job(tenant, KeyScope::SubjectKeys, &us_east, &node_leases),
        "eu-west leases must not satisfy a us-east SubjectKeys job"
    );
}
