use oe::encoding::predicate::{StringPredicate, filter_string_block};
use oe::encoding::{ColumnData, StringColumn, Transform, decode_block, encode_block};

#[test]
fn repeating_shredded_strings_become_a_dictionary() {
    let values: StringColumn = (0..8192usize)
        .map(|i| Some(format!("!room{}:example.org", (i * 7919) % 500)))
        .collect();
    let data = ColumnData::Strings(values.clone());
    let block = encode_block(&data, true);
    assert_eq!(block.pipeline.transform().unwrap(), Transform::DictionaryString);
    assert_eq!(decode_block(block.pipeline, &block.bytes).unwrap(), data);
    let predicate = StringPredicate::Equals("!room42:example.org".to_owned());
    let mask = filter_string_block(block.pipeline, &block.bytes, &predicate)
        .unwrap()
        .expect("dictionary equality answered from codes");
    assert_eq!(mask, predicate.filter_decoded(&values));
}
