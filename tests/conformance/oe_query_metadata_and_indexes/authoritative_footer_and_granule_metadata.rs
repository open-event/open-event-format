use crate::support;
use oe::indexes::summary::{footer_is_authoritative_for_planning, plan_granules_from_footer, summary_from_footer};

#[test]
fn footer_drives_planning() {
    let built = support::built_file(8);
    let footer = &built.footer;

    let summary = summary_from_footer(footer);
    assert_eq!(summary.granule_count as usize, footer.granules.len());

    let planned = plan_granules_from_footer(footer);
    assert!(!footer.granules.is_empty(), "fixture should build granules");
    assert!(footer_is_authoritative_for_planning());
    let from_footer: Vec<u32> = footer.granules.iter().map(|g| g.granule_id).collect();
    assert_eq!(planned, from_footer);

    assert_eq!(planned.len(), summary.granule_count as usize);
}
