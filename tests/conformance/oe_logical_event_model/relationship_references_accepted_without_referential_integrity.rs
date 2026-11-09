use crate::support;
use oe::artifacts::batch::{build_batch, decode_batch};
use oe::events::EventId;
use oe::events::relationships::{EventRelationships, RelationshipKind, RelationshipRef};
use oe::layout::reader::OeFile;
use oe::typed_id::TypedIdTestExt;
use oe::writer::build::{OeRow, build_oe_file};

#[test]
fn reply_arrives_before_its_parent() {
    let parent_id: u128 = 0xDEAD_BEEF;
    let reply = {
        let mut event = support::event(1);
        event.relationships = Some(
            EventRelationships::new(vec![RelationshipRef::to_event(RelationshipKind::Parent, parent_id)]).unwrap(),
        );
        event
    };
    let payload = build_batch(std::slice::from_ref(&reply), 1, 0).unwrap();
    let decoded = decode_batch(&payload, 1).unwrap();
    assert_eq!(
        decoded.events.first().and_then(|event| event.relationships.clone()),
        reply.relationships,
        "the declared reference commits unchanged, target existence never checked"
    );

    let built = build_oe_file(
        vec![OeRow {
            epoch: 1,
            sequence: 1,
            event: reply,
        }],
        &support::build_config(),
    )
    .unwrap();
    let file = OeFile::open(built.bytes, None).unwrap();
    let reference = RelationshipRef::to_event(RelationshipKind::Parent, parent_id);
    assert_eq!(file.resolve_reference(&reference).unwrap(), Vec::<u64>::new());

    let parent = {
        let mut event = support::event(0);
        event.envelope.event_id = EventId::new_test_id(parent_id);
        event
    };
    let later = build_oe_file(
        vec![OeRow {
            epoch: 1,
            sequence: 2,
            event: parent,
        }],
        &support::build_config(),
    )
    .unwrap();
    let later = OeFile::open(later.bytes, None).unwrap();
    assert_eq!(later.resolve_reference(&reference).unwrap(), vec![0]);
}

#[test]
fn target_erased_after_commit() {
    let target: u128 = 0xC0FFEE;
    let referencing = |i: u64| {
        let mut event = support::event(i);
        event.relationships =
            Some(EventRelationships::new(vec![RelationshipRef::to_event(RelationshipKind::Link, target)]).unwrap());
        event
    };
    let built = build_oe_file(
        vec![
            OeRow {
                epoch: 1,
                sequence: 1,
                event: referencing(1),
            },
            OeRow {
                epoch: 1,
                sequence: 2,
                event: referencing(2),
            },
        ],
        &support::build_config(),
    )
    .unwrap();
    let file = OeFile::open(built.bytes, None).unwrap();
    let reference = RelationshipRef::to_event(RelationshipKind::Link, target);
    assert_eq!(
        file.rows_referencing(RelationshipKind::Link, reference.space, &reference.target_ref)
            .unwrap(),
        vec![0, 1]
    );
    assert_eq!(file.resolve_reference(&reference).unwrap(), Vec::<u64>::new());
}
