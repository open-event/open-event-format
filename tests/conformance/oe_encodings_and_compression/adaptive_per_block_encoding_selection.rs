use crate::support;
use oe::layout::reader::OeFile;

#[test]
fn pipeline_recorded() {
    let built = support::built_file(40);
    assert!(!built.footer.marks.is_empty());
    let file = OeFile::open(built.bytes, None).unwrap();
    for written in &built.footer.marks {
        let mark = file
            .mark(written.column_id, written.projection_id, written.granule_id)
            .unwrap()
            .expect("the file records a mark for every block the writer emitted");
        assert_eq!(mark.codec_pipeline_id, written.codec_pipeline_id);
        mark.codec_pipeline_id.transform().unwrap();
        mark.codec_pipeline_id.compression().unwrap();
        mark.codec_pipeline_id.value_kind().unwrap();
        assert!(
            file.footer()
                .page_stats
                .iter()
                .any(|stats| { stats.column_id == mark.column_id && stats.granule_id == mark.granule_id })
        );
    }
}
