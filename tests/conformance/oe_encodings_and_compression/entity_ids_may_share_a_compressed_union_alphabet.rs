use crate::support;
use oe::columns::column_ids;
use oe::encoding::predicate::{StringPredicate, count_string_block_shared};
use oe::encoding::{ColumnData, SideStream, decode_block_range_shared, take_block_rows_shared};
use oe::layout::reader::OeFile;
use oe::layout::required_features;
use oe::writer::build::{BuiltOe, OeRow, build_oe_file};

fn room(i: u64) -> String {
    format!("!room{:06}:example.org", (i * 37) % 1_500)
}

fn build(rows: u64, entity: impl Fn(u64) -> String) -> (Vec<String>, BuiltOe) {
    let ids: Vec<String> = (0..rows).map(&entity).collect();
    let rows: Vec<OeRow> = (0..rows)
        .map(|i| {
            let mut event = support::event(i);
            event.envelope.entity_id = Some(entity(i));
            OeRow {
                epoch: 1,
                sequence: i + 1,
                event,
            }
        })
        .collect();
    let mut config = support::build_config();
    config.targets.index_granularity = 1_024;
    (ids, build_oe_file(rows, &config).unwrap())
}

#[test]
fn recurring_entity_ids_store_only_codes() {
    let (ids, built) = build(4_096, room);
    assert_ne!(
        built.footer.required_feature_flags & required_features::PACKED_SHARED_DICTIONARIES,
        0
    );
    let packed = built
        .footer
        .packed_shared_dictionaries
        .iter()
        .find(|entry| entry.column_id == column_ids::ENTITY_ID)
        .unwrap();
    let mut distinct = ids.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(packed.values().unwrap(), distinct);
    assert!(
        built
            .footer
            .marks
            .iter()
            .filter(|mark| mark.column_id == column_ids::ENTITY_ID)
            .all(|mark| mark.codec_pipeline_id.side_stream() == Ok(SideStream::FileScopeDictionary))
    );
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    for granule in &file.footer().granules {
        let first = granule.first_row_ordinal as usize;
        let expected: Vec<&str> = ids[first..first + granule.row_count as usize]
            .iter()
            .map(String::as_str)
            .collect();
        let ColumnData::Strings(values) = file
            .read_column(column_ids::ENTITY_ID, granule.granule_id)
            .unwrap()
            .data
        else {
            panic!("entity ids are strings")
        };
        assert_eq!(values.iter().map(Option::unwrap).collect::<Vec<_>>(), expected);
        let (_, views) = file
            .read_column_string_views(column_ids::ENTITY_ID, granule.granule_id)
            .unwrap()
            .unwrap();
        assert_eq!(views.iter().map(Option::unwrap).collect::<Vec<_>>(), expected);
        let raw = file.read_column_raw(column_ids::ENTITY_ID, granule.granule_id).unwrap();
        let alphabet = file.shared_alphabet(column_ids::ENTITY_ID);
        assert_eq!(
            decode_block_range_shared(raw.pipeline, &raw.body, alphabet, 10, 20).unwrap(),
            ColumnData::Strings(values.slice(10, 20))
        );
        let ColumnData::Strings(taken) =
            take_block_rows_shared(raw.pipeline, &raw.body, alphabet, &[1, 2, 900]).unwrap()
        else {
            panic!("taken entity ids are strings")
        };
        assert_eq!(
            taken.iter().map(Option::unwrap).collect::<Vec<_>>(),
            [expected[1], expected[2], expected[900]]
        );
    }
}

#[test]
fn an_entity_lookup_searches_the_alphabet_once_and_compares_codes() {
    let (ids, built) = build(4_096, room);
    let file = OeFile::open(built.bytes, Some(&built.file_seal)).unwrap();
    let wanted = room(42);
    let predicate = StringPredicate::Equals(wanted.clone());
    let translated = file
        .shared_dictionary_predicate(column_ids::ENTITY_ID, &predicate)
        .unwrap();
    let mut found = 0;
    for granule in &file.footer().granules {
        let raw = file.read_column_raw(column_ids::ENTITY_ID, granule.granule_id).unwrap();
        found += count_string_block_shared(raw.pipeline, &raw.body, &predicate, Some(&translated))
            .unwrap()
            .expect("a shared-scope block answers equality from its codes");
    }
    assert_eq!(found, ids.iter().filter(|id| **id == wanted).count());
}

#[test]
fn ids_that_never_recur_keep_block_local_forms() {
    let (_, built) = build(4_096, |i| format!("opp-{i}"));
    assert!(built.footer.packed_shared_dictionaries.is_empty());
    assert_eq!(
        built.footer.required_feature_flags & required_features::PACKED_SHARED_DICTIONARIES,
        0
    );
}
