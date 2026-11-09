use crate::support;
use arrow_array::Array;
use oe::columns::column_ids;
use oe::encoding::ColumnData;
use oe::layout::reader::OeFile;

#[test]
fn view_read_matches_the_materializing_read() {
    let built = support::built_file(48);
    let shredded_string = built
        .footer
        .shredded
        .iter()
        .find(|entry| entry.path == "kind")
        .expect("the kind path is shredded")
        .column_id;
    let file = OeFile::open(built.bytes, None).unwrap();

    for granule in &file.footer().granules {
        for column_id in [shredded_string, column_ids::ENTITY_ID] {
            let materialized = file.read_column(column_id, granule.granule_id).unwrap();
            let (presence, views) = file
                .read_column_string_views(column_id, granule.granule_id)
                .unwrap()
                .expect("a string column has a view read");
            assert_eq!(presence, materialized.presence);
            let ColumnData::Strings(values) = &materialized.data else {
                panic!("string columns decode as strings");
            };
            assert_eq!(views.len(), values.len());
            for (index, value) in values.iter().enumerate() {
                match value {
                    Some(text) => assert_eq!(views.value(index), text, "row {index}"),
                    None => assert!(views.is_null(index), "row {index} must be null"),
                }
            }
        }
    }
}

#[test]
fn uncovered_blocks_fall_back_identically() {
    let built = support::built_file(32);
    let file = OeFile::open(built.bytes, None).unwrap();
    for granule in &file.footer().granules {
        assert!(
            file.read_column_string_views(column_ids::SEQUENCE, granule.granule_id)
                .unwrap()
                .is_none()
        );
        let read = file.read_column(column_ids::SEQUENCE, granule.granule_id).unwrap();
        assert!(read.data.row_count() > 0);
    }
}
