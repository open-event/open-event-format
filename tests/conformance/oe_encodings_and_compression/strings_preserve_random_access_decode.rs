use crate::support;
use oe::columns::column_ids;
use oe::encoding::ColumnData;
use oe::layout::reader::OeFile;

#[test]
fn decode_one_granule_of_an_fsst_column() {
    let built = support::built_file(48);
    let file = OeFile::open(built.bytes, None).unwrap();
    assert!(file.footer().granules.len() > 1);
    let target = file.footer().granules[1];
    let read = file.read_column(column_ids::ENTITY_ID, target.granule_id).unwrap();
    let ColumnData::Strings(values) = read.data else {
        panic!("string column")
    };
    assert_eq!(values.len(), target.row_count as usize);
    let first_row = target.first_row_ordinal;
    assert_eq!(values.get(0).flatten(), Some(format!("opp-{first_row}").as_str()));
}
