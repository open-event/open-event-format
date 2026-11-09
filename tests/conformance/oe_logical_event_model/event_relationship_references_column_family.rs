use crate::support;
use oe::columns::column_ids;
use oe::events::relationships::{EventRelationships, RelationshipKind, RelationshipRef, TargetIdSpace};
use oe::layout::reader::OeFile;
use oe::writer::build::{OeRow, build_oe_file};

fn built_with_relationships(
    count: u64,
    declare: impl Fn(u64) -> Option<EventRelationships>,
) -> oe::writer::build::BuiltOe {
    let rows: Vec<OeRow> = (0..count)
        .map(|i| {
            let mut event = support::event(i);
            event.relationships = declare(i);
            OeRow {
                epoch: 1,
                sequence: i + 1,
                event,
            }
        })
        .collect();
    build_oe_file(rows, &support::build_config()).unwrap()
}

fn event_target(i: u64) -> u128 {
    0xC0FFEE + u128::from(i)
}

#[test]
fn unrelated_stream_pays_nothing() {
    let built = built_with_relationships(12, |_| None);
    let file = OeFile::open(built.bytes, None).unwrap();
    assert!(
        !file.footer().columns.iter().any(
            |column| (column_ids::RELATIONSHIP_BASE..column_ids::RELATIONSHIP_BASE + 6).contains(&column.column_id)
        ),
        "a stream without relationships materializes no relationship columns"
    );
    assert_eq!(
        file.rows_referencing(
            RelationshipKind::Parent,
            TargetIdSpace::EventId,
            &event_target(0).to_be_bytes()
        )
        .unwrap(),
        Vec::<u64>::new()
    );
}

#[test]
fn children_of_an_event_resolve_by_filter() {
    let built = built_with_relationships(10, |i| match i {
        3 | 7 => Some(
            EventRelationships::new(vec![RelationshipRef::to_event(
                RelationshipKind::Parent,
                event_target(0),
            )])
            .unwrap(),
        ),
        5 => Some(
            EventRelationships::new(vec![RelationshipRef::to_event(
                RelationshipKind::Parent,
                event_target(1),
            )])
            .unwrap(),
        ),
        _ => None,
    });
    let file = OeFile::open(built.bytes, None).unwrap();
    let children = file
        .rows_referencing(
            RelationshipKind::Parent,
            TargetIdSpace::EventId,
            &event_target(0).to_be_bytes(),
        )
        .unwrap();
    assert_eq!(children, vec![3, 7], "exactly the declaring rows, in file order");
    let parent_column = file.read_column(column_ids::PARENT_REF, 0).unwrap();
    assert!(
        parent_column.presence.first().is_none_or(|byte| byte & 1 == 0),
        "row 0 (the parent) carries no relationship value of its own"
    );
}

#[test]
fn relationship_never_licenses_causal_claims() {
    let structural: Vec<&str> = RelationshipKind::ALL.iter().map(|kind| kind.as_str()).collect();
    assert_eq!(structural, vec!["auth", "link", "parent", "prev", "related", "root"]);
    for causal in ["caused_by", "causes", "driver_of"] {
        assert_eq!(
            RelationshipKind::from_str(causal),
            None,
            "{causal} is not in the registry"
        );
    }
}
