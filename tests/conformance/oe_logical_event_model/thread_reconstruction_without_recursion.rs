use crate::support;
use oe::events::relationships::{EventRelationships, RelationshipKind, RelationshipRef, TargetIdSpace};
use oe::layout::reader::OeFile;
use oe::writer::build::{OeRow, build_oe_file};

fn conversation(messages: u64) -> OeFile {
    let root_id: u128 = 0xC0FFEE;
    let rows: Vec<OeRow> = (0..messages)
        .map(|i| {
            let mut event = support::event(i);
            if i > 0 {
                event.relationships = Some(
                    EventRelationships::new(vec![
                        RelationshipRef::to_event(RelationshipKind::Parent, root_id + u128::from(i) - 1),
                        RelationshipRef::to_event(RelationshipKind::Root, root_id),
                    ])
                    .unwrap(),
                );
            }
            OeRow {
                epoch: 1,
                sequence: i + 1,
                event,
            }
        })
        .collect();
    OeFile::open(build_oe_file(rows, &support::build_config()).unwrap().bytes, None).unwrap()
}

#[test]
fn whole_conversation_in_one_filter() {
    let file = conversation(24);
    let thread = file
        .thread_rows(TargetIdSpace::EventId, &0xC0FFEEu128.to_be_bytes())
        .unwrap();
    assert_eq!(thread, (1..24).collect::<Vec<u64>>());
    let children_of_root = file
        .rows_referencing(
            RelationshipKind::Parent,
            TargetIdSpace::EventId,
            &0xC0FFEEu128.to_be_bytes(),
        )
        .unwrap();
    assert_eq!(children_of_root, vec![1], "direct replies to the root");
}

#[test]
fn root_marked_by_absence() {
    let file = conversation(4);
    let thread = file
        .thread_rows(TargetIdSpace::EventId, &0xC0FFEEu128.to_be_bytes())
        .unwrap();
    assert!(
        !thread.contains(&0),
        "the root is fetched by its own identity, not by a self-reference"
    );
    let root_column = file.read_column(oe::columns::column_ids::ROOT_REF, 0).unwrap();
    assert!(
        root_column.presence.first().is_none_or(|byte| byte & 1 == 0),
        "row 0 carries no root reference"
    );
}
