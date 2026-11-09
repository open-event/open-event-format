use crate::support;
use oe::indexes::summary::{FilePredicate, can_prune_file, summary_from_footer};

#[test]
fn prune_without_opening_files() {
    let built = support::built_file(8);
    let summary = summary_from_footer(&built.footer);

    let predicate = FilePredicate {
        min_sequence: Some(summary.max_sequence + 1_000),
        ..FilePredicate::default()
    };

    let mut files_opened = 0usize;
    if !can_prune_file(&summary, &predicate) {
        files_opened += 1;
        let _ = &built.bytes;
    }

    assert!(
        can_prune_file(&summary, &predicate),
        "out-of-range predicate must prune the file"
    );
    assert_eq!(files_opened, 0, "pruned file must not be opened");

    let overlapping = FilePredicate {
        min_sequence: Some(summary.min_sequence),
        max_sequence: Some(summary.max_sequence),
        ..FilePredicate::default()
    };
    assert!(!can_prune_file(&summary, &overlapping));
}
