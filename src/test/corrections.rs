use super::*;
use crate::typed_id::TypedIdTestExt;

fn id(n: u128) -> EventId {
    EventId::new_test_id(n)
}

fn record(
    correcting: u128,
    corrects: u128,
    kind: CorrectionType,
    (epoch, sequence, generation): (u64, u64, u64),
    changes: &[(&str, FieldChange)],
) -> CorrectionRecord {
    CorrectionRecord {
        correcting_event_id: id(correcting),
        field_changes: changes.iter().map(|(f, c)| ((*f).to_owned(), *c)).collect(),
        metadata: CorrectionMetadata {
            correction_epoch: epoch,
            correction_generation: generation,
            correction_sequence: sequence,
            correction_type: kind,
            corrects_event_id: id(corrects),
        },
    }
}

#[test]
fn an_uncorrected_event_resolves_to_itself() {
    let resolved = CorrectionResolver::new([]).resolve(id(1));
    assert_eq!(resolved.current, CurrentVersion::Event(id(1)));
    assert!(resolved.chain.is_empty() && resolved.winner.is_none());
}

#[test]
fn a_chain_resolves_from_any_member_to_its_last_replacement() {
    let resolver = CorrectionResolver::new([
        record(3, 2, CorrectionType::Replacement, (1, 30, 1), &[]),
        record(2, 1, CorrectionType::Replacement, (1, 20, 1), &[]),
    ]);
    for member in [1, 2, 3] {
        let resolved = resolver.resolve(id(member));
        assert_eq!(resolved.root_event_id, id(1));
        assert_eq!(resolved.current, CurrentVersion::Event(id(3)));
        assert_eq!(resolved.chain, vec![id(2), id(3)]);
        assert_eq!(resolved.winner.unwrap().correction_sequence, 30);
    }
    let events = [(0, id(1)), (1, id(2)), (2, id(3)), (3, id(4))];
    assert_eq!(
        resolver.superseded_deletion_vector(&events).iter().collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert!(resolver.is_correction(id(3)) && !resolver.is_correction(id(1)));
    assert_eq!(resolver.latest_correction(id(1)).unwrap().correction_sequence, 20);
}

#[test]
fn amendments_redact_and_replace_fields_with_the_later_change_winning() {
    let resolver = CorrectionResolver::new([
        record(
            2,
            1,
            CorrectionType::Amendment,
            (1, 10, 1),
            &[("body", FieldChange::Replaced), ("email", FieldChange::Redacted)],
        ),
        record(
            3,
            2,
            CorrectionType::Amendment,
            (1, 11, 1),
            &[("body", FieldChange::Redacted), ("email", FieldChange::Replaced)],
        ),
    ]);
    let resolved = resolver.resolve(id(1));
    assert_eq!(resolved.current, CurrentVersion::Event(id(1)));
    assert!(resolved.withholds("body"));
    assert!(!resolved.withholds("email"));
    assert_eq!(resolved.replaced_fields.get("email"), Some(&id(3)));
    let fields = resolver.field_deletion_vectors(&[(5, id(1)), (6, id(2)), (7, id(3))]);
    assert_eq!(fields.len(), 1);
    assert!(fields[0].column_redacted("body", 5));
    assert!(!fields[0].column_redacted("body", 6));
}

#[test]
fn a_replacement_clears_earlier_field_changes_and_a_retraction_nullifies() {
    let resolver = CorrectionResolver::new([
        record(
            2,
            1,
            CorrectionType::Amendment,
            (1, 10, 1),
            &[("body", FieldChange::Redacted)],
        ),
        record(3, 1, CorrectionType::Replacement, (1, 12, 1), &[]),
    ]);
    let resolved = resolver.resolve(id(1));
    assert_eq!(resolved.current, CurrentVersion::Event(id(3)));
    assert!(resolved.withheld_fields.is_empty());

    let retracted = CorrectionResolver::new([
        record(3, 1, CorrectionType::Replacement, (1, 12, 1), &[]),
        record(4, 3, CorrectionType::Retraction, (1, 13, 1), &[]),
        record(
            5,
            1,
            CorrectionType::Amendment,
            (1, 14, 1),
            &[("body", FieldChange::Replaced)],
        ),
    ]);
    let resolved = retracted.resolve(id(5));
    assert_eq!(resolved.current, CurrentVersion::Retracted);
    assert!(resolved.replaced_fields.is_empty());
    assert_eq!(resolved.superseded_events().count(), 4);
}

#[test]
fn ties_break_by_epoch_then_sequence_then_generation_regardless_of_input_order() {
    let records = [
        record(2, 1, CorrectionType::Replacement, (2, 1, 1), &[]),
        record(3, 1, CorrectionType::Replacement, (1, 99, 9), &[]),
        record(4, 1, CorrectionType::Replacement, (2, 1, 0), &[]),
    ];
    let forward = CorrectionResolver::new(records.clone()).resolve(id(1));
    let mut reversed = records.to_vec();
    reversed.reverse();
    let backward = CorrectionResolver::new(reversed).resolve(id(1));
    assert_eq!(forward, backward);
    assert_eq!(forward.chain, vec![id(3), id(4), id(2)]);
    assert_eq!(forward.current, CurrentVersion::Event(id(2)));
}

#[test]
fn a_cyclic_chain_terminates() {
    let resolver = CorrectionResolver::new([
        record(2, 1, CorrectionType::Replacement, (1, 1, 1), &[]),
        record(1, 2, CorrectionType::Replacement, (1, 2, 1), &[]),
    ]);
    let resolved = resolver.resolve(id(1));
    assert_eq!(resolved.chain, vec![id(2)]);
}
