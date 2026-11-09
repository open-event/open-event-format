use crate::support;
use oe::columns::column_ids;
use oe::encoding::ColumnData;
use oe::layout::optional_features;
use oe::layout::reader::OeFile;

#[test]
fn read_one_page_without_the_rest_of_the_granule() {
    let built = support::built_file(48);
    let file = OeFile::open(built.bytes, None).unwrap();

    assert!(
        file.footer().optional_feature_flags & optional_features::PER_PAGE_MARKS != 0,
        "writer must declare PER_PAGE_MARKS"
    );

    let granule = file.footer().granules.last().copied().unwrap();

    let via_page = file.read_page(column_ids::SEQUENCE, granule.granule_id, 0).unwrap();
    let via_block = file.read_column(column_ids::SEQUENCE, granule.granule_id).unwrap();

    let ColumnData::U64(page_values) = via_page.data else {
        panic!("expected u64 column")
    };
    let ColumnData::U64(block_values) = via_block.data else {
        panic!("expected u64 column")
    };

    assert_eq!(
        page_values, block_values,
        "page-level read must match the full granule-block read (equivalence oracle)"
    );
    assert_eq!(page_values.len(), granule.row_count as usize);
    assert_eq!(page_values[0], granule.first_sequence);

    let page_entry = file
        .page_directory_for(column_ids::SEQUENCE, 0, granule.granule_id)
        .unwrap()
        .into_iter()
        .find(|entry| entry.page_index == 0)
        .expect("page directory entry must exist");
    let mark = file
        .mark(column_ids::SEQUENCE, 0, granule.granule_id)
        .unwrap()
        .expect("column mark must exist");

    assert_eq!(page_entry.compressed_offset, mark.compressed_offset);
    assert_eq!(page_entry.compressed_len, mark.compressed_size);
    assert_eq!(page_entry.row_count, granule.row_count);
    assert_eq!(page_entry.first_row_ordinal, granule.first_row_ordinal);
}

#[test]
fn older_reader_falls_back_to_granule_granularity() {
    let built = support::built_file(32);
    let file = OeFile::open(built.bytes, None).unwrap();

    assert!(
        file.footer().optional_feature_flags & optional_features::PER_PAGE_MARKS != 0,
        "file must carry PER_PAGE_MARKS for this test to be meaningful"
    );

    for granule in file.footer().granules.clone() {
        let granule_read = file.read_column(column_ids::SEQUENCE, granule.granule_id).unwrap();
        let page_read = file.read_page(column_ids::SEQUENCE, granule.granule_id, 0).unwrap();

        let ColumnData::U64(granule_values) = granule_read.data else {
            panic!("expected u64 column")
        };
        let ColumnData::U64(page_values) = page_read.data else {
            panic!("expected u64 column")
        };
        assert_eq!(
            granule_values, page_values,
            "granule-level read must match page-level read (backward-compatibility equivalence)"
        );
    }
}
