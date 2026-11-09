use oe::encoding::descriptor::{PageDescriptor, extract_descriptor};
use oe::encoding::predicate::{StringPredicate, filter_string_block};
use oe::encoding::{ColumnData, StringColumn, Transform, decode_block, encode_block};

fn status_values(count: usize) -> StringColumn {
    (0..count)
        .map(|i| match i % 4 {
            0 => None,
            1 => Some("lost"),
            2 => Some("pending"),
            _ => Some("won"),
        })
        .collect()
}

fn id_values(count: usize) -> StringColumn {
    (0..count)
        .map(|i| {
            if i % 13 == 0 {
                None
            } else {
                Some(format!("entity-{i:06}"))
            }
        })
        .collect()
}

#[test]
fn predicate_evaluated_on_dictionary_codes_via_the_descriptor() {
    let values = status_values(200);
    let encoded = encode_block(&ColumnData::Strings(values.clone()), true);
    assert_eq!(encoded.pipeline.transform().unwrap(), Transform::DictionaryString);

    let descriptor = extract_descriptor(encoded.pipeline, &encoded.bytes).unwrap();
    let PageDescriptor::Dictionary { ref entries } = descriptor else {
        panic!("expected Dictionary descriptor for low-cardinality strings");
    };

    let won_code = entries
        .binary_search(&"won".to_owned())
        .expect("'won' must be present in the dictionary");

    let lost_code = entries.binary_search(&"lost".to_owned()).unwrap();
    let pending_code = entries.binary_search(&"pending".to_owned()).unwrap();
    assert!(
        lost_code < pending_code && pending_code < won_code,
        "sorted dictionary must preserve value order so codes are comparable"
    );

    let predicate = StringPredicate::Equals("won".to_owned());
    let mask = filter_string_block(encoded.pipeline, &encoded.bytes, &predicate)
        .unwrap()
        .expect("dictionary block must answer equality from codes");
    assert_eq!(mask, predicate.filter_decoded(&values));

    for (index, value) in values.iter().enumerate() {
        if value.is_none() {
            assert!(!mask[index], "null row {index} should not match");
        }
    }

    assert_eq!(
        decode_block(encoded.pipeline, &encoded.bytes).unwrap(),
        ColumnData::Strings(values),
        "descriptor use must not change decoded output"
    );
}

#[test]
fn decode_cost_planning_from_the_descriptor() {
    let status = status_values(200);
    let ids = id_values(200);

    let dict_block = encode_block(&ColumnData::Strings(status), true);
    let fsst_block = encode_block(&ColumnData::Strings(ids), true);

    assert_eq!(dict_block.pipeline.transform().unwrap(), Transform::DictionaryString);
    assert!(matches!(
        fsst_block.pipeline.transform().unwrap(),
        Transform::FsstString | Transform::FsstPackedString
    ));

    let dict_desc = extract_descriptor(dict_block.pipeline, &dict_block.bytes).unwrap();
    let fsst_desc = extract_descriptor(fsst_block.pipeline, &fsst_block.bytes).unwrap();

    assert!(
        dict_desc.decode_cost_per_row() < fsst_desc.decode_cost_per_row(),
        "dictionary cost ({}) must be cheaper than FSST cost ({})",
        dict_desc.decode_cost_per_row(),
        fsst_desc.decode_cost_per_row()
    );

    let mut predicate_pages = [
        (fsst_desc, Transform::FsstString),
        (dict_desc, Transform::DictionaryString),
    ];
    predicate_pages.sort_by_key(|(desc, _)| desc.decode_cost_per_row());
    assert_eq!(
        predicate_pages[0].1,
        Transform::DictionaryString,
        "cheapest predicate (dictionary) must be ordered first"
    );
}
