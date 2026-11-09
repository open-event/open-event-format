use super::*;
use crate::layout::Profile;
use crate::writer::build::{BuildInput, OeBuildConfig, OeRow, build_oe_file};
use crate::writer::merge::read_build_rows;
use crate::writer::merge::tests::{build_config, rows_of};

fn source(deleted: &[u64]) -> RenumberSource {
    let rows = OeRow::into_build_rows(rows_of(1..=6));
    let mut graph = GraphInput {
        subject: b"!s".to_vec(),
        ..GraphInput::default()
    };
    for sn in 0..6u64 {
        let mut edges = RowEdges::default();
        if sn > 0 {
            edges.prev.push(sn - 1);
        }
        if sn >= 2 {
            edges.auth.extend([sn - 1, 0]);
        }
        graph.rows.push(edges);
    }
    let config = OeBuildConfig {
        graph: Some(graph),
        profile: Profile::Graph,
        ..build_config()
    };
    let built = build_oe_file(rows, &config).unwrap();
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    let rows = read_build_rows(&file).unwrap();
    let mut vector = DeletionVector::new();
    for &ordinal in deleted {
        vector.mark_deleted(ordinal);
    }
    RenumberSource {
        deleted: Some(vector),
        file,
        rows,
    }
}

#[test]
fn consecutive_deleted_events_re_parent_onto_their_nearest_surviving_ancestors() {
    let renumbered = renumber_graph(vec![vec![source(&[2, 3])]]).unwrap();
    let expected_map: BTreeMap<u64, Option<u64>> = [
        (0, Some(0)),
        (1, Some(1)),
        (2, None),
        (3, None),
        (4, Some(2)),
        (5, Some(3)),
    ]
    .into();
    assert_eq!(renumbered.sn_map, expected_map);
    let [file] = renumbered.files.as_slice() else {
        panic!("one output");
    };
    assert_eq!(file.graph.first_sn, 0);
    assert_eq!(file.rows.len(), 4);
    assert_eq!(
        file.graph.rows[2],
        RowEdges {
            auth: vec![1, 0],
            prev: vec![1]
        }
    );
    assert_eq!(
        file.graph.rows[3],
        RowEdges {
            auth: vec![2, 0],
            prev: vec![2]
        }
    );
    assert!(file.graph.inbound.is_empty());
}

#[test]
fn a_group_with_no_surviving_event_and_no_sources_are_refused() {
    let error = renumber_graph(vec![vec![source(&[0, 1, 2, 3, 4, 5])]]).unwrap_err();
    assert!(matches!(error, FormatError::Structural { .. }));
    assert!(renumber_graph(Vec::new()).is_err());
}
