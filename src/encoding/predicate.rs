use super::constant::{
    BRANCH_FREE_SET_NARROW, BRANCH_FREE_SET_WIDE, FSST_KEY_SCAN_CHUNK, PARALLEL_SCAN_MIN_GRANULE_BYTES,
    PARALLEL_SCAN_MIN_GRANULES, PREFIX_KEY_BYTES, PREFIX_KEY_PREFIX_LEN,
};
use super::seekable_zstd::Window;
use super::sidecar::{PrefixKey, StringFingerprint};
use super::{
    ALP_POWERS, ColumnData, Compression, DictionaryValues, FASTLANES_VECTOR, PackedStream, PipelineId, SideStream,
    StringColumn, Transform, ValueKind, decode_block, decode_block_shared_with_body, mask, prefix_sum,
    read_fsst_compressor, read_packed_stream, unzigzag, zigzag,
};
use crate::error::FormatError;
use crate::file::bytes::{Reader, slice};
use aho_corasick::{AhoCorasick, Input, MatchKind};
use memchr::memmem::Finder;
use multiversion::multiversion;
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StringBound {
    pub inclusive: bool,
    pub value: String,
}

#[derive(Clone, Debug)]
pub struct ContainsMatcher {
    case_insensitive: bool,
    fingerprint: StringFingerprint,
    needles: Vec<String>,
    search: ContainsSearch,
}

#[derive(Clone, Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "the inline finder keeps the single-needle hot path free of an extra indirection"
)]
enum ContainsSearch {
    MatchAll,
    Multiple(Arc<AhoCorasick>),
    Single(Finder<'static>),
}

impl ContainsMatcher {
    pub fn new(needles: Vec<String>, case_insensitive: bool) -> Self {
        let search = match needles.as_slice() {
            [] => ContainsSearch::MatchAll,
            [needle] if needle.is_empty() => ContainsSearch::MatchAll,
            [needle] if !case_insensitive => ContainsSearch::Single(Finder::new(needle).into_owned()),
            _ => {
                #[allow(clippy::expect_used)]
                let automaton = AhoCorasick::builder()
                    .match_kind(MatchKind::Standard)
                    .ascii_case_insensitive(case_insensitive)
                    .build(&needles)
                    .expect("an Aho-Corasick automaton over plain string needles always builds");
                ContainsSearch::Multiple(Arc::new(automaton))
            }
        };
        let fingerprint = needles.iter().fold(StringFingerprint::default(), |bits, needle| {
            bits.union(StringFingerprint::of(needle.as_bytes()))
        });
        Self {
            case_insensitive,
            fingerprint,
            needles,
            search,
        }
    }

    pub fn case_insensitive(&self) -> bool {
        self.case_insensitive
    }

    pub fn needles(&self) -> &[String] {
        &self.needles
    }

    fn fingerprint(&self) -> StringFingerprint {
        self.fingerprint
    }

    pub fn matches(&self, value: &str) -> bool {
        self.matches_bytes(value.as_bytes())
    }

    fn matches_bytes(&self, value: &[u8]) -> bool {
        match &self.search {
            ContainsSearch::MatchAll => true,
            ContainsSearch::Single(finder) => finder.find(value).is_some(),
            ContainsSearch::Multiple(automaton) => self.matches_automaton(automaton, value),
        }
    }

    fn matches_automaton(&self, automaton: &AhoCorasick, value: &[u8]) -> bool {
        match (self.needles.len(), self.mark_words()) {
            (1, _) => automaton.is_match(value),
            (_, 0 | 1) => self.marks_every_needle(automaton, value, &mut [0u64; 1]),
            (_, words) => self.marks_every_needle(automaton, value, &mut vec![0u64; words]),
        }
    }

    fn marks_every_needle(&self, automaton: &AhoCorasick, value: &[u8], seen: &mut [u64]) -> bool {
        let mut missing = self.needles.len();
        if missing == 0 {
            return true;
        }
        for hit in automaton.find_overlapping_iter(value) {
            if set_bit(seen, hit.pattern().as_usize()) {
                missing -= 1;
                if missing == 0 {
                    return true;
                }
            }
        }
        false
    }

    fn mark_words(&self) -> usize {
        self.needles.len().div_ceil(64)
    }

    fn mark_arena(&self, text: &[u8], bounds: &[usize]) -> Vec<bool> {
        match &self.search {
            ContainsSearch::MatchAll => vec![true; bounds.len().saturating_sub(1)],
            ContainsSearch::Single(finder) => {
                let mut marks = vec![false; bounds.len().saturating_sub(1)];
                visit_single_matches(finder, text, bounds, |value| {
                    if let Some(mark) = marks.get_mut(value) {
                        *mark = true;
                    }
                });
                marks
            }
            ContainsSearch::Multiple(automaton) if self.needles.len() == 1 => {
                let mut marks = vec![false; bounds.len().saturating_sub(1)];
                visit_lone_needle_matches(automaton, text, bounds, |value| {
                    if let Some(mark) = marks.get_mut(value) {
                        *mark = true;
                    }
                });
                marks
            }
            ContainsSearch::Multiple(automaton) => self.mark_arena_automaton(automaton, text, bounds),
        }
    }

    fn count_arena(&self, text: &[u8], bounds: &[usize]) -> usize {
        match &self.search {
            ContainsSearch::MatchAll => bounds.len().saturating_sub(1),
            ContainsSearch::Single(finder) => {
                let mut count = 0;
                visit_single_matches(finder, text, bounds, |_| count += 1);
                count
            }
            ContainsSearch::Multiple(automaton) if self.needles.len() == 1 => {
                let mut count = 0;
                visit_lone_needle_matches(automaton, text, bounds, |_| count += 1);
                count
            }
            ContainsSearch::Multiple(automaton) => self
                .mark_arena_automaton(automaton, text, bounds)
                .into_iter()
                .filter(|matched| *matched)
                .count(),
        }
    }

    fn mark_arena_automaton(&self, automaton: &AhoCorasick, text: &[u8], bounds: &[usize]) -> Vec<bool> {
        let count = bounds.len().saturating_sub(1);
        let words = self.mark_words();
        if words == 0 {
            return vec![true; count];
        }
        let mut seen = vec![0u64; count * words];
        if bounds.windows(2).any(|pair| matches!(pair, [low, high] if high < low)) {
            for hit in automaton.find_overlapping_iter(text) {
                let Some(value) = bounds.partition_point(|start| *start <= hit.start()).checked_sub(1) else {
                    continue;
                };
                let (Some(start), Some(end)) = (bounds.get(value), bounds.get(value + 1)) else {
                    continue;
                };
                if hit.start() >= *start
                    && hit.end() <= *end
                    && let Some(mark) = seen.get_mut(value * words..(value + 1) * words)
                {
                    set_bit(mark, hit.pattern().as_usize());
                }
            }
        } else {
            let (mut closing, mut beyond) = (0usize, 0usize);
            for hit in automaton.find_overlapping_iter(text) {
                while bounds.get(closing).is_some_and(|bound| *bound < hit.end()) {
                    closing += 1;
                }
                let value = if hit.start() < hit.end() {
                    closing.checked_sub(1).filter(|value| {
                        closing < bounds.len() && bounds.get(*value).is_some_and(|start| *start <= hit.start())
                    })
                } else {
                    beyond = beyond.max(closing);
                    while bounds.get(beyond).is_some_and(|bound| *bound <= hit.end()) {
                        beyond += 1;
                    }
                    beyond.checked_sub(1).filter(|_| beyond < bounds.len())
                };
                if let Some(value) = value
                    && let Some(mark) = seen.get_mut(value * words..(value + 1) * words)
                {
                    set_bit(mark, hit.pattern().as_usize());
                }
            }
        }
        seen.chunks_exact(words)
            .map(|mark| all_bits_set(mark, self.needles.len()))
            .collect()
    }
}

fn visit_single_matches(finder: &Finder<'_>, text: &[u8], bounds: &[usize], mut visit: impl FnMut(usize)) {
    let mut search_from = 0;
    while search_from <= text.len() {
        let Some(relative) = text.get(search_from..).and_then(|rest| finder.find(rest)) else {
            break;
        };
        let start = search_from + relative;
        let Some(value) = bounds.partition_point(|bound| *bound <= start).checked_sub(1) else {
            break;
        };
        let Some(&end) = bounds.get(value + 1) else {
            break;
        };
        if start + finder.needle().len() <= end {
            visit(value);
        }
        search_from = end;
    }
}

fn visit_lone_needle_matches(automaton: &AhoCorasick, text: &[u8], bounds: &[usize], mut visit: impl FnMut(usize)) {
    let mut search_from = 0;
    while search_from <= text.len() {
        let Some(hit) = automaton.find(Input::new(text).span(search_from..text.len())) else {
            break;
        };
        let Some(value) = bounds.partition_point(|bound| *bound <= hit.start()).checked_sub(1) else {
            search_from = hit.start() + 1;
            continue;
        };
        let Some(&end) = bounds.get(value + 1) else {
            search_from = hit.start() + 1;
            continue;
        };
        if hit.end() <= end {
            visit(value);
        }
        search_from = end;
    }
}

fn set_bit(mark: &mut [u64], at: usize) -> bool {
    let Some(word) = mark.get_mut(at / 64) else {
        return false;
    };
    let bit = 1u64 << (at % 64);
    let newly_set = *word & bit == 0;
    *word |= bit;
    newly_set
}

fn all_bits_set(mark: &[u64], count: usize) -> bool {
    mark.iter().enumerate().all(|(index, word)| {
        let bits = (count - index * 64).min(64);
        let full = if bits == 64 { u64::MAX } else { (1u64 << bits) - 1 };
        *word & full == full
    })
}

impl PartialEq for ContainsMatcher {
    fn eq(&self, other: &Self) -> bool {
        self.case_insensitive == other.case_insensitive && self.needles == other.needles
    }
}

impl Eq for ContainsMatcher {}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(
    clippy::large_enum_variant,
    reason = "the contains matcher is held inline so the scan kernels read it without an extra indirection"
)]
pub enum StringPredicate {
    Contains(ContainsMatcher),
    Equals(String),
    InSet(Vec<String>),
    NotEquals(String),
    Range {
        lower: Option<StringBound>,
        upper: Option<StringBound>,
    },
}

impl StringPredicate {
    pub fn matches_value(&self, value: Option<&str>) -> bool {
        let Some(value) = value else { return false };
        match self {
            StringPredicate::Contains(matcher) => matcher.matches(value),
            StringPredicate::Equals(needle) => value == needle.as_str(),
            StringPredicate::InSet(set) => set.iter().any(|needle| needle.as_str() == value),
            StringPredicate::NotEquals(needle) => value != needle.as_str(),
            StringPredicate::Range { lower, upper } => {
                let above_lower = match lower {
                    Some(bound) if bound.inclusive => value >= bound.value.as_str(),
                    Some(bound) => value > bound.value.as_str(),
                    None => true,
                };
                let below_upper = match upper {
                    Some(bound) if bound.inclusive => value <= bound.value.as_str(),
                    Some(bound) => value < bound.value.as_str(),
                    None => true,
                };
                above_lower && below_upper
            }
        }
    }

    pub fn filter_decoded(&self, values: &StringColumn) -> Vec<bool> {
        if let StringPredicate::InSet(needles) = self {
            let set = sorted_needles(needles);
            return values
                .iter()
                .map(|value| value.is_some_and(|value| set.binary_search(&value).is_ok()))
                .collect();
        }
        values.iter().map(|value| self.matches_value(value)).collect()
    }
}

fn sorted_needles(needles: &[String]) -> Vec<&str> {
    let mut set: Vec<&str> = needles.iter().map(String::as_str).collect();
    set.sort_unstable();
    set.dedup();
    set
}

pub fn filter_string_block(
    pipeline: PipelineId,
    bytes: &[u8],
    predicate: &StringPredicate,
) -> Result<Option<Vec<bool>>, FormatError> {
    filter_string_block_shared(pipeline, bytes, predicate, None)
}

pub struct SharedDictionaryPredicate {
    test: CodeTest,
}

pub fn translate_for_shared_dictionary<T: AsRef<str>>(
    predicate: &StringPredicate,
    alphabet: &[T],
) -> SharedDictionaryPredicate {
    SharedDictionaryPredicate {
        test: dictionary_code_test(predicate, alphabet),
    }
}

pub fn filter_string_block_shared(
    pipeline: PipelineId,
    bytes: &[u8],
    predicate: &StringPredicate,
    shared: Option<&SharedDictionaryPredicate>,
) -> Result<Option<Vec<bool>>, FormatError> {
    if !string_block_has_fast_path(pipeline, predicate)? {
        return Ok(None);
    }
    let body = super::remove_trailing(pipeline.compression()?, bytes)?;
    filter_string_block_shared_with_body(pipeline, &body, predicate, shared)
}

pub fn count_string_block_shared(
    pipeline: PipelineId,
    bytes: &[u8],
    predicate: &StringPredicate,
    shared: Option<&SharedDictionaryPredicate>,
) -> Result<Option<usize>, FormatError> {
    if !string_block_has_fast_path(pipeline, predicate)? {
        return Ok(None);
    }
    let transform = pipeline.transform()?;
    let side = pipeline.side_stream()?;
    let compression = pipeline.compression()?;
    if transform == Transform::FsstString
        && compression == Compression::SeekableZstd
        && let StringPredicate::Contains(matcher) = predicate
    {
        return Ok(Some(count_fsst_framed_contains(bytes, side, matcher)?));
    }
    let body = super::remove_trailing(compression, bytes)?;
    match (transform, predicate) {
        (Transform::FsstString, StringPredicate::Contains(matcher)) => {
            Ok(Some(count_fsst_contains(&body, side, matcher)?))
        }
        (Transform::RawString, StringPredicate::Contains(matcher)) => Ok(Some(count_raw_contains(&body, matcher)?)),
        (Transform::DictionaryString, _) if side != SideStream::FileScopeDictionary => {
            Ok(Some(count_dictionary(&body, side, predicate)?))
        }
        _ => Ok(
            filter_string_block_shared_with_body(pipeline, &body, predicate, shared)?
                .map(|mask| mask.into_iter().filter(|matched| *matched).count()),
        ),
    }
}

pub fn scan_in_parallel(granules: usize, bytes_per_granule: u64) -> bool {
    granules >= PARALLEL_SCAN_MIN_GRANULES && bytes_per_granule >= PARALLEL_SCAN_MIN_GRANULE_BYTES
}

pub fn filter_string_block_shared_with_body(
    pipeline: PipelineId,
    body: &[u8],
    predicate: &StringPredicate,
    shared: Option<&SharedDictionaryPredicate>,
) -> Result<Option<Vec<bool>>, FormatError> {
    if !string_block_has_fast_path(pipeline, predicate)? {
        return Ok(None);
    }
    if pipeline.transform()? == Transform::FsstPackedString
        && matches!(
            predicate,
            StringPredicate::Equals(_) | StringPredicate::NotEquals(_) | StringPredicate::InSet(_)
        )
    {
        return Ok(Some(filter_fsst_packed(body, predicate)?));
    }
    if let Some((classic, body)) = super::classic_fsst_view(pipeline, body)? {
        return filter_string_block_shared_with_body(classic, &body, predicate, shared);
    }
    let transform = pipeline.transform()?;
    let side = pipeline.side_stream()?;
    if matches!(transform, Transform::DictionaryString) && side == SideStream::FileScopeDictionary {
        let Some(translated) = shared else {
            return Ok(None);
        };
        return Ok(Some(filter_shared_dictionary(body, translated)?));
    }
    let mask = match (transform, predicate) {
        (Transform::FsstString, StringPredicate::Contains(matcher)) => filter_fsst_contains(body, side, matcher)?,
        (Transform::RawString, StringPredicate::Contains(matcher)) => filter_raw_contains(body, matcher)?,
        (Transform::DictionaryString, _) => filter_dictionary(body, side, predicate)?,
        (Transform::FsstString, StringPredicate::Range { .. }) => filter_fsst_range(body, predicate)?,
        (Transform::FsstString, _) => filter_fsst(body, predicate)?,
        _ => return Ok(None),
    };
    Ok(Some(mask))
}

fn string_block_has_fast_path(pipeline: PipelineId, predicate: &StringPredicate) -> Result<bool, FormatError> {
    if pipeline.value_kind()? != ValueKind::String {
        return Ok(false);
    }
    let transform = pipeline.transform()?;
    if matches!(predicate, StringPredicate::Contains(_)) {
        return Ok(matches!(
            transform,
            Transform::DictionaryString | Transform::FsstString | Transform::FsstPackedString | Transform::RawString
        ));
    }
    if matches!(transform, Transform::FsstPackedString) && matches!(predicate, StringPredicate::Range { .. }) {
        return Ok(false);
    }
    if matches!(transform, Transform::FsstString) && matches!(predicate, StringPredicate::Range { .. }) {
        return Ok(pipeline.side_stream()? == SideStream::FsstValueKeys);
    }
    Ok(!matches!(transform, Transform::RawString))
}

fn filter_shared_dictionary(body: &[u8], translated: &SharedDictionaryPredicate) -> Result<Vec<bool>, FormatError> {
    let mut reader = Reader::new(body);
    let nulls = super::NullStream::read(&mut reader)?;
    let stream = read_packed_stream(&mut reader)?;
    let mut present = vec![false; stream.padded_count()];
    stream.unpack_into(&mut present, |code| translated.test.matches(code));
    present.truncate(stream.count);
    weave_present(&nulls, present)
}

pub fn filter_string_block_or_decode(
    pipeline: PipelineId,
    bytes: &[u8],
    predicate: &StringPredicate,
) -> Result<Vec<bool>, FormatError> {
    let body = super::remove_trailing(pipeline.compression()?, bytes)?;
    if let Some(mask) = filter_string_block_shared_with_body(pipeline, &body, predicate, None)? {
        return Ok(mask);
    }
    match decode_block_shared_with_body(pipeline, &body, None)? {
        ColumnData::Strings(values) => Ok(predicate.filter_decoded(&values)),
        _ => Err(FormatError::Structural {
            rule: "string predicate applied to a non-string column",
        }),
    }
}

enum CodeTest {
    All,
    None,
    NotCode(u64),
    Range(usize, usize),
    Set(Vec<u64>),
}

impl CodeTest {
    fn matches(&self, code: u64) -> bool {
        match self {
            CodeTest::All => true,
            CodeTest::None => false,
            CodeTest::NotCode(excluded) => code != *excluded,
            CodeTest::Range(lo, hi) => code >= *lo as u64 && code < *hi as u64,
            CodeTest::Set(codes) => codes.binary_search(&code).is_ok(),
        }
    }
}

fn dictionary_code_test<T: AsRef<str>>(predicate: &StringPredicate, dictionary: &[T]) -> CodeTest {
    let search = |needle: &str| dictionary.binary_search_by(|entry| entry.as_ref().cmp(needle));
    match predicate {
        StringPredicate::Contains(matcher) => CodeTest::Set(
            dictionary
                .iter()
                .enumerate()
                .filter(|(_, entry)| matcher.matches(entry.as_ref()))
                .map(|(index, _)| index as u64)
                .collect(),
        ),
        StringPredicate::Equals(needle) => match search(needle) {
            Ok(index) => CodeTest::Range(index, index + 1),
            Err(_) => CodeTest::None,
        },
        StringPredicate::NotEquals(needle) => match search(needle) {
            Ok(index) => CodeTest::NotCode(index as u64),
            Err(_) => CodeTest::All,
        },
        StringPredicate::InSet(needles) => {
            let mut codes: Vec<u64> = needles
                .iter()
                .filter_map(|needle| search(needle).ok())
                .map(|index| index as u64)
                .collect();
            codes.sort_unstable();
            codes.dedup();
            CodeTest::Set(codes)
        }
        StringPredicate::Range { lower, upper } => {
            let lo = match lower {
                Some(bound) if bound.inclusive => {
                    dictionary.partition_point(|entry| entry.as_ref() < bound.value.as_str())
                }
                Some(bound) => dictionary.partition_point(|entry| entry.as_ref() <= bound.value.as_str()),
                None => 0,
            };
            let hi = match upper {
                Some(bound) if bound.inclusive => {
                    dictionary.partition_point(|entry| entry.as_ref() <= bound.value.as_str())
                }
                Some(bound) => dictionary.partition_point(|entry| entry.as_ref() < bound.value.as_str()),
                None => dictionary.len(),
            };
            if lo >= hi {
                CodeTest::None
            } else {
                CodeTest::Range(lo, hi)
            }
        }
    }
}

fn block_dictionary_code_test(predicate: &StringPredicate, dictionary: &[&str]) -> CodeTest {
    if dictionary
        .windows(2)
        .all(|pair| matches!(pair, [low, high] if low < high))
    {
        return dictionary_code_test(predicate, dictionary);
    }
    CodeTest::Set(
        dictionary
            .iter()
            .enumerate()
            .filter(|(_, entry)| predicate.matches_value(Some(entry)))
            .map(|(index, _)| index as u64)
            .collect(),
    )
}

fn count_dictionary(body: &[u8], side: SideStream, predicate: &StringPredicate) -> Result<usize, FormatError> {
    let mut reader = Reader::new(body);
    let nulls = super::NullStream::read(&mut reader)?;
    let dict_count = reader.u32("dictionary count")? as usize;
    let values = DictionaryValues::read(&mut reader, dict_count, side)?;
    let dictionary = values.entries()?;
    let stream = read_packed_stream(&mut reader)?;
    if stream.count != nulls.present_count() {
        return Err(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        });
    }
    let mut counts = vec![0usize; dictionary.len()];
    let mut in_range = true;
    stream.for_each_vector(|codes| {
        for &code in codes {
            match usize::try_from(code).ok().and_then(|code| counts.get_mut(code)) {
                Some(count) => *count += 1,
                None => in_range = false,
            }
        }
    });
    if !in_range {
        return Err(FormatError::RefOutOfRange {
            what: "dictionary code",
        });
    }
    let test = block_dictionary_code_test(predicate, &dictionary);
    Ok(counts
        .iter()
        .enumerate()
        .filter(|(code, _)| test.matches(*code as u64))
        .map(|(_, count)| *count)
        .sum())
}

fn filter_dictionary(body: &[u8], side: SideStream, predicate: &StringPredicate) -> Result<Vec<bool>, FormatError> {
    let mut reader = Reader::new(body);
    let nulls = super::NullStream::read(&mut reader)?;
    let dict_count = reader.u32("dictionary count")? as usize;
    if let StringPredicate::Equals(needle) = predicate {
        let term: Cow<'_, [u8]> = if side == SideStream::FsstDictionaryValues {
            let encoder = super::FsstDecodeTable::read(&mut reader)?.needle_encoder();
            Cow::Owned(encoder.compress(needle.as_bytes()))
        } else {
            Cow::Borrowed(needle.as_bytes())
        };
        let table = reader.take(dict_count.saturating_add(1).saturating_mul(4), "string offset")?;
        let (entries, _) = table.as_chunks::<4>();
        let data_len = entries.last().map_or(0, |entry| u32::from_le_bytes(*entry) as usize);
        let data = reader.take(data_len, "dictionary data")?;
        let want = term.len();
        let mut codes: Vec<u64> = Vec::new();
        let mut start = entries.first().map_or(0, |entry| u32::from_le_bytes(*entry) as usize);
        for (index, entry) in entries.iter().skip(1).enumerate() {
            let end = u32::from_le_bytes(*entry) as usize;
            if end < start {
                return Err(FormatError::Structural {
                    rule: "dictionary offsets must be non-decreasing",
                });
            }
            if end - start == want && data.get(start..end).is_some_and(|value| value == &*term) {
                codes.push(index as u64);
            }
            start = end;
        }
        let stream = read_packed_stream(&mut reader)?;
        let present = match codes.as_slice() {
            [] => vec![false; stream.count],
            [code] => {
                let code = *code;
                let mut present = vec![false; stream.padded_count()];
                stream.unpack_into(&mut present, |value| value == code);
                present.truncate(stream.count);
                present
            }
            codes => {
                let mut present = vec![false; stream.padded_count()];
                stream.unpack_into(&mut present, |value| codes.binary_search(&value).is_ok());
                present.truncate(stream.count);
                present
            }
        };
        return weave_present(&nulls, present);
    }
    let values = DictionaryValues::read(&mut reader, dict_count, side)?;
    let dictionary = values.entries()?;
    let test = block_dictionary_code_test(predicate, &dictionary);
    let stream = read_packed_stream(&mut reader)?;
    let mut present = vec![false; stream.padded_count()];
    stream.unpack_into(&mut present, |code| test.matches(code));
    present.truncate(stream.count);
    weave_present(&nulls, present)
}

enum FsstTest {
    Equals(Vec<u8>),
    InSet(Vec<Vec<u8>>),
    NotEquals(Vec<u8>),
}

impl FsstTest {
    fn matches(&self, compressed: &[u8]) -> bool {
        match self {
            FsstTest::Equals(term) => compressed == term.as_slice(),
            FsstTest::InSet(terms) => terms.binary_search_by(|term| term.as_slice().cmp(compressed)).is_ok(),
            FsstTest::NotEquals(term) => compressed != term.as_slice(),
        }
    }
}

fn filter_fsst(body: &[u8], predicate: &StringPredicate) -> Result<Vec<bool>, FormatError> {
    let mut reader = Reader::new(body);
    let nulls = super::NullStream::read(&mut reader)?;
    let test = fsst_test(&super::FsstDecodeTable::read(&mut reader)?, predicate)?;
    filter_fsst_classic(reader, &nulls, &test)
}

fn fsst_test(table: &super::FsstDecodeTable, predicate: &StringPredicate) -> Result<FsstTest, FormatError> {
    let encoder = table.needle_encoder();
    Ok(match predicate {
        StringPredicate::Equals(needle) => FsstTest::Equals(encoder.compress(needle.as_bytes())),
        StringPredicate::NotEquals(needle) => FsstTest::NotEquals(encoder.compress(needle.as_bytes())),
        StringPredicate::InSet(needles) => {
            let mut terms: Vec<Vec<u8>> = needles
                .iter()
                .map(|needle| encoder.compress(needle.as_bytes()))
                .collect();
            terms.sort_unstable();
            terms.dedup();
            FsstTest::InSet(terms)
        }
        StringPredicate::Contains(_) => {
            return Err(FormatError::Structural {
                rule: "FSST substring predicates are answered by the arena kernel, not the equality kernel",
            });
        }
        StringPredicate::Range { .. } => {
            return Err(FormatError::Structural {
                rule: "FSST blocks cannot answer range predicates from compressed bytes",
            });
        }
    })
}

fn filter_fsst_packed(body: &[u8], predicate: &StringPredicate) -> Result<Vec<bool>, FormatError> {
    let mut reader = Reader::new(body);
    let nulls = super::NullStream::read(&mut reader)?;
    let test = fsst_test(&super::FsstDecodeTable::read(&mut reader)?, predicate)?;
    let present_count = reader.u32("fsst present count")? as usize;
    let lens = super::bitunpack(&mut reader)?;
    if lens.len() != present_count {
        return Err(FormatError::Structural {
            rule: "fsst packed length count disagrees with the present count",
        });
    }
    let data = reader.take(reader.remaining(), "fsst data")?;
    let total = lens.iter().try_fold(0u64, |sum, len| sum.checked_add(*len));
    if total != Some(data.len() as u64) {
        return Err(FormatError::Structural {
            rule: "fsst packed lengths disagree with the arena",
        });
    }
    let mut present = Vec::with_capacity(present_count);
    let mut start = 0usize;
    if let FsstTest::Equals(term) = &test {
        let want = term.len() as u64;
        for len in lens {
            let end = start + len as usize;
            present.push(len == want && data.get(start..end).is_some_and(|value| value == term.as_slice()));
            start = end;
        }
        return weave_present(&nulls, present);
    }
    for len in lens {
        let end = start + len as usize;
        let compressed = slice(data, start, end - start, "fsst value")?;
        present.push(test.matches(compressed));
        start = end;
    }
    weave_present(&nulls, present)
}

fn filter_fsst_classic(
    mut reader: Reader<'_>,
    nulls: &super::NullStream<'_>,
    test: &FsstTest,
) -> Result<Vec<bool>, FormatError> {
    let present_count = reader.u32("fsst present count")? as usize;
    let table = reader.take(present_count.saturating_add(1).saturating_mul(4), "string offset")?;
    let (entries, _) = table.as_chunks::<4>();
    let data_len = entries.last().map_or(0, |entry| u32::from_le_bytes(*entry) as usize);
    let data = reader.take(data_len, "fsst data")?;
    let mut present = Vec::with_capacity(present_count);
    let mut start = entries.first().map_or(0, |entry| u32::from_le_bytes(*entry) as usize);
    if let FsstTest::Equals(term) = test {
        let want = term.len();
        let mut monotone = true;
        for entry in entries.iter().skip(1) {
            let end = u32::from_le_bytes(*entry) as usize;
            monotone &= end >= start;
            let hit =
                end.wrapping_sub(start) == want && data.get(start..end).is_some_and(|value| value == term.as_slice());
            present.push(hit);
            start = end;
        }
        if !monotone {
            return Err(FormatError::Structural {
                rule: "fsst offsets must be non-decreasing",
            });
        }
        return weave_present(nulls, present);
    }
    for entry in entries.iter().skip(1) {
        let end = u32::from_le_bytes(*entry) as usize;
        if end < start {
            return Err(FormatError::Structural {
                rule: "fsst offsets must be non-decreasing",
            });
        }
        let compressed = slice(data, start, end - start, "fsst value")?;
        present.push(test.matches(compressed));
        start = end;
    }
    weave_present(nulls, present)
}

struct FsstValueKeys<'a> {
    fingerprints: &'a [u8],
    prefixes: &'a [u8],
}

impl FsstValueKeys<'_> {
    fn contains_candidates(&self, needles: StringFingerprint) -> impl Iterator<Item = usize> + use<> {
        let values = self.fingerprints.len() / StringFingerprint::STORED_BYTES;
        let mut words = vec![0u64; values.div_ceil(FSST_KEY_SCAN_CHUNK)];
        mark_fingerprint_candidates(self.fingerprints, needles, &mut words);
        set_bit_positions(words)
    }
}

#[multiversion(targets("x86_64+avx512f+avx512bw+avx512dq+avx512vl", "x86_64+avx2", "aarch64+neon"))]
fn mark_fingerprint_candidates(fingerprints: &[u8], needles: StringFingerprint, words: &mut [u64]) {
    let (stored, _) = fingerprints.as_chunks::<{ StringFingerprint::STORED_BYTES }>();
    let (full, tail) = stored.as_chunks::<FSST_KEY_SCAN_CHUNK>();
    let mut words = words.iter_mut();
    for (chunk, word) in full.iter().zip(&mut words) {
        *word = fingerprint_candidate_word(chunk, needles);
    }
    if let Some(word) = words.next() {
        *word = fingerprint_candidate_word(tail, needles);
    }
}

#[inline(always)]
fn fingerprint_candidate_word(chunk: &[[u8; StringFingerprint::STORED_BYTES]], needles: StringFingerprint) -> u64 {
    chunk.iter().enumerate().fold(0, |bits, (bit, stored)| {
        let fingerprint = StringFingerprint::from_bits(u32::from_le_bytes(*stored));
        bits | u64::from(fingerprint.might_contain(needles)) << bit
    })
}

fn set_bit_positions(words: Vec<u64>) -> impl Iterator<Item = usize> {
    let mut words = words.into_iter();
    let mut bits = 0u64;
    let mut base = 0usize.wrapping_sub(FSST_KEY_SCAN_CHUNK);
    std::iter::from_fn(move || {
        while bits == 0 {
            bits = words.next()?;
            base = base.wrapping_add(FSST_KEY_SCAN_CHUNK);
        }
        let bit = bits.trailing_zeros() as usize;
        bits &= bits.wrapping_sub(1);
        Some(base + bit)
    })
}

fn read_fsst_value_keys<'a>(reader: &mut Reader<'a>, present_count: usize) -> Result<FsstValueKeys<'a>, FormatError> {
    let prefixes = reader.take(present_count.saturating_mul(PREFIX_KEY_BYTES), "fsst prefix key")?;
    let fingerprints = reader.take(
        present_count.saturating_mul(StringFingerprint::STORED_BYTES),
        "fsst fingerprint",
    )?;
    Ok(FsstValueKeys { fingerprints, prefixes })
}

fn fsst_value_codes<'a>(data: &'a [u8], offsets: &[usize], index: usize) -> Result<&'a [u8], FormatError> {
    let (Some(&start), Some(&end)) = (offsets.get(index), offsets.get(index + 1)) else {
        return Err(FormatError::RefOutOfRange { what: "fsst value" });
    };
    if end < start {
        return Err(FormatError::Structural {
            rule: "fsst offsets must be non-decreasing",
        });
    }
    Ok(slice(data, start, end - start, "fsst value")?)
}

fn filter_fsst_contains(body: &[u8], side: SideStream, matcher: &ContainsMatcher) -> Result<Vec<bool>, FormatError> {
    let mut reader = Reader::new(body);
    let nulls = super::NullStream::read(&mut reader)?;
    let compressor = read_fsst_compressor(&mut reader)?;
    let present_count = reader.u32("fsst present count")? as usize;
    let offsets = super::read_offsets(&mut reader, present_count)?;
    let data_len = offsets.last().copied().unwrap_or(0);
    let data = reader.take(data_len, "fsst data")?;
    if side == SideStream::FsstValueKeys {
        let keys = read_fsst_value_keys(&mut reader, present_count)?;
        let candidates: Vec<usize> = keys.contains_candidates(matcher.fingerprint()).collect();
        let verdicts = super::with_fsst_selected_bytes(
            &compressor,
            data,
            &offsets,
            &candidates,
            "fsst value",
            "fsst offsets must be non-decreasing",
            |text, bounds| matcher.mark_arena(text, bounds),
        )?;
        return weave_present(&nulls, scatter(present_count, &candidates, verdicts));
    }
    let present = super::with_fsst_arena_bytes(
        &compressor,
        data,
        &offsets,
        "fsst value",
        "fsst offsets must be non-decreasing",
        |text, bounds| matcher.mark_arena(text, bounds),
    )?;
    weave_present(&nulls, present)
}

fn count_fsst_contains(body: &[u8], side: SideStream, matcher: &ContainsMatcher) -> Result<usize, FormatError> {
    let mut reader = Reader::new(body);
    let nulls = super::NullStream::read(&mut reader)?;
    let compressor = read_fsst_compressor(&mut reader)?;
    let present_count = reader.u32("fsst present count")? as usize;
    if nulls.present_count() != present_count {
        return Err(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        });
    }
    let offsets = super::read_offsets(&mut reader, present_count)?;
    let data_len = offsets.last().copied().unwrap_or(0);
    let data = reader.take(data_len, "fsst data")?;
    if side == SideStream::FsstValueKeys {
        let keys = read_fsst_value_keys(&mut reader, present_count)?;
        return count_fsst_candidates(
            &compressor,
            data,
            &offsets,
            keys.contains_candidates(matcher.fingerprint()),
            matcher,
        );
    }
    count_fsst_candidates(&compressor, data, &offsets, 0..present_count, matcher)
}

fn count_fsst_candidates(
    compressor: &fsst::Compressor,
    data: &[u8],
    offsets: &[usize],
    candidates: impl Iterator<Item = usize>,
    matcher: &ContainsMatcher,
) -> Result<usize, FormatError> {
    let table = super::FsstDecodeTable::new(compressor);
    super::with_arena_buffer(|buffer| {
        let mut count = 0;
        for index in candidates {
            let compressed = fsst_value_codes(data, offsets, index)?;
            buffer.clear();
            super::fsst_decompress_value(&table, compressed, buffer)?;
            count += usize::from(matcher.matches_bytes(buffer));
        }
        Ok(count)
    })
}

fn count_fsst_framed_contains(
    stored: &[u8],
    side: SideStream,
    matcher: &ContainsMatcher,
) -> Result<usize, FormatError> {
    let mut window = Window::open(stored)?;
    let header = super::read_windowed_string_header(Transform::FsstString, &mut window)?;
    let present_count = header.present_count;
    if header.nulls.present_count() != present_count {
        return Err(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        });
    }
    let offsets_len = present_count.saturating_add(1).saturating_mul(4);
    let offsets = super::decode_offsets(&window.read(header.offsets_start, offsets_len)?);
    let data_start = header.offsets_start.saturating_add(offsets_len);
    let data_len = offsets.last().copied().unwrap_or(0);
    if data_start.saturating_add(data_len) > window.plain_len() {
        return Err(FormatError::Truncated { what: "fsst data" });
    }
    let fingerprints = match side {
        SideStream::FsstValueKeys => Some(
            window.read(
                data_start
                    .saturating_add(data_len)
                    .saturating_add(present_count.saturating_mul(PREFIX_KEY_BYTES)),
                present_count.saturating_mul(StringFingerprint::STORED_BYTES),
            )?,
        ),
        _ => None,
    };
    let compressor = header.compressor()?;
    let mut arena = FramedArena {
        data_len,
        data_start,
        offsets: &offsets,
        window: &mut window,
    };
    match &fingerprints {
        Some(fingerprints) => {
            let keys = FsstValueKeys {
                fingerprints,
                prefixes: &[],
            };
            count_framed_candidates(
                &mut arena,
                &compressor,
                keys.contains_candidates(matcher.fingerprint()),
                matcher,
            )
        }
        None => count_framed_candidates(&mut arena, &compressor, 0..present_count, matcher),
    }
}

struct FramedArena<'a, 'b> {
    data_len: usize,
    data_start: usize,
    offsets: &'a [usize],
    window: &'a mut Window<'b>,
}

fn count_framed_candidates(
    arena: &mut FramedArena<'_, '_>,
    compressor: &fsst::Compressor,
    candidates: impl Iterator<Item = usize>,
    matcher: &ContainsMatcher,
) -> Result<usize, FormatError> {
    let table = super::FsstDecodeTable::new(compressor);
    super::with_arena_buffer(|buffer| {
        let mut count = 0;
        for index in candidates {
            let (Some(&start), Some(&end)) = (arena.offsets.get(index), arena.offsets.get(index + 1)) else {
                return Err(FormatError::RefOutOfRange { what: "fsst value" });
            };
            if end < start {
                return Err(FormatError::Structural {
                    rule: "fsst offsets must be non-decreasing",
                });
            }
            if end > arena.data_len {
                return Err(FormatError::Truncated { what: "fsst value" });
            }
            let compressed = arena
                .window
                .read_forward(arena.data_start.saturating_add(start), end - start)?;
            buffer.clear();
            super::fsst_decompress_value(&table, &compressed, buffer)?;
            count += usize::from(matcher.matches_bytes(buffer));
        }
        Ok(count)
    })
}

fn filter_fsst_range(body: &[u8], predicate: &StringPredicate) -> Result<Vec<bool>, FormatError> {
    let StringPredicate::Range { lower, upper } = predicate else {
        return Err(FormatError::Structural {
            rule: "the FSST prefix-key kernel answers only range predicates",
        });
    };
    let mut reader = Reader::new(body);
    let nulls = super::NullStream::read(&mut reader)?;
    let compressor = read_fsst_compressor(&mut reader)?;
    let present_count = reader.u32("fsst present count")? as usize;
    let offsets = super::read_offsets(&mut reader, present_count)?;
    let data_len = offsets.last().copied().unwrap_or(0);
    let data = reader.take(data_len, "fsst data")?;
    let keys = read_fsst_value_keys(&mut reader, present_count)?;

    let mut present = vec![false; present_count];
    let mut undecided = vec![0u64; present_count.div_ceil(FSST_KEY_SCAN_CHUNK)];
    let range = PrefixKeyRange::new(lower.as_ref(), upper.as_ref());
    if classify_prefix_keys(keys.prefixes, range, &mut present, &mut undecided) {
        return Err(FormatError::Structural {
            rule: "fsst prefix key length byte runs past the prefix",
        });
    }
    let ambiguous: Vec<usize> = set_bit_positions(undecided).collect();
    if !ambiguous.is_empty() {
        let verdicts = super::with_fsst_selected_bytes(
            &compressor,
            data,
            &offsets,
            &ambiguous,
            "fsst value",
            "fsst offsets must be non-decreasing",
            |text, bounds| range_verdicts(predicate, text, bounds),
        )??;
        for (index, verdict) in ambiguous.iter().zip(verdicts) {
            if let Some(slot) = present.get_mut(*index) {
                *slot = verdict;
            }
        }
    }
    weave_present(&nulls, present)
}

#[derive(Clone, Copy, Debug)]
struct PrefixKeyRange {
    lower: u64,
    lower_tie_accepts: bool,
    lower_tie_rejects: bool,
    upper: u64,
    upper_tie_accepts: bool,
    upper_tie_rejects: bool,
}

impl PrefixKeyRange {
    fn new(lower: Option<&StringBound>, upper: Option<&StringBound>) -> Self {
        let (lower, lower_tie_accepts, lower_tie_rejects) = Self::bound(lower, 0);
        let (upper, upper_tie_accepts, upper_tie_rejects) = Self::bound(upper, u64::MAX);
        Self {
            lower,
            lower_tie_accepts,
            lower_tie_rejects,
            upper,
            upper_tie_accepts,
            upper_tie_rejects,
        }
    }

    fn bound(bound: Option<&StringBound>, missing: u64) -> (u64, bool, bool) {
        let Some(bound) = bound else {
            return (missing, true, false);
        };
        let stored = PrefixKey::of(&bound.value).to_stored();
        let whole = stored
            .last()
            .is_some_and(|&len| usize::from(len) < PREFIX_KEY_PREFIX_LEN);
        (
            u64::from_be_bytes(stored),
            whole && bound.inclusive,
            whole && !bound.inclusive,
        )
    }
}

#[multiversion(targets("x86_64+avx512f+avx512bw+avx512dq+avx512vl", "x86_64+avx2", "aarch64+neon"))]
fn classify_prefix_keys(prefixes: &[u8], range: PrefixKeyRange, accepted: &mut [bool], undecided: &mut [u64]) -> bool {
    let (stored, _) = prefixes.as_chunks::<PREFIX_KEY_BYTES>();
    let (full, tail) = stored.as_chunks::<FSST_KEY_SCAN_CHUNK>();
    let (full_accepted, tail_accepted) = accepted.as_chunks_mut::<FSST_KEY_SCAN_CHUNK>();
    let mut words = undecided.iter_mut();
    let mut forged = false;
    for ((chunk, accepted), word) in full.iter().zip(full_accepted.iter_mut()).zip(&mut words) {
        let (bits, chunk_forged) = classify_prefix_key_chunk(chunk, range, accepted);
        *word = bits;
        forged |= chunk_forged;
    }
    if let Some(word) = words.next() {
        let (bits, chunk_forged) = classify_prefix_key_chunk(tail, range, tail_accepted);
        *word = bits;
        forged |= chunk_forged;
    }
    forged
}

#[inline(always)]
fn classify_prefix_key_chunk(
    chunk: &[[u8; PREFIX_KEY_BYTES]],
    range: PrefixKeyRange,
    accepted: &mut [bool],
) -> (u64, bool) {
    let mut bits = 0u64;
    let mut forged = false;
    for (bit, (stored, slot)) in chunk.iter().zip(accepted.iter_mut()).enumerate() {
        let key = u64::from_be_bytes(*stored);
        forged |= key & u64::from(u8::MAX) > PREFIX_KEY_PREFIX_LEN as u64;
        let at_lower = key == range.lower;
        let at_upper = key == range.upper;
        let (above, below) = (key > range.lower, key < range.upper);
        let (under, over) = (key < range.lower, key > range.upper);
        let clears = (above | at_lower & range.lower_tie_accepts) & (below | at_upper & range.upper_tie_accepts);
        let fails = under | at_lower & range.lower_tie_rejects | over | at_upper & range.upper_tie_rejects;
        *slot = clears;
        bits |= u64::from(!clears & !fails) << bit;
    }
    (bits, forged)
}

fn range_verdicts(predicate: &StringPredicate, text: &[u8], bounds: &[usize]) -> Result<Vec<bool>, FormatError> {
    bounds
        .windows(2)
        .filter_map(|pair| match pair {
            [start, end] => Some((*start, *end)),
            _ => None,
        })
        .map(|(start, end)| {
            let value = text
                .get(start..end)
                .and_then(|bytes| simdutf8::basic::from_utf8(bytes).ok())
                .ok_or(FormatError::InvalidUtf8 { what: "fsst value" })?;
            Ok(predicate.matches_value(Some(value)))
        })
        .collect()
}

fn scatter(present_count: usize, selected: &[usize], verdicts: Vec<bool>) -> Vec<bool> {
    let mut present = vec![false; present_count];
    for (index, verdict) in selected.iter().zip(verdicts) {
        if let Some(slot) = present.get_mut(*index) {
            *slot = verdict;
        }
    }
    present
}

fn filter_raw_contains(body: &[u8], matcher: &ContainsMatcher) -> Result<Vec<bool>, FormatError> {
    let mut reader = Reader::new(body);
    let nulls = super::NullStream::read(&mut reader)?;
    let present_count = reader.u32("raw present count")? as usize;
    let offsets = super::read_offsets(&mut reader, present_count)?;
    let data_len = offsets.last().copied().unwrap_or(0);
    let data = reader.take(data_len, "raw string data")?;
    weave_present(&nulls, matcher.mark_arena(data, &offsets))
}

fn count_raw_contains(body: &[u8], matcher: &ContainsMatcher) -> Result<usize, FormatError> {
    let mut reader = Reader::new(body);
    let nulls = super::NullStream::read(&mut reader)?;
    let present_count = reader.u32("raw present count")? as usize;
    if nulls.present_count() != present_count {
        return Err(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        });
    }
    let offsets = super::read_offsets(&mut reader, present_count)?;
    let data_len = offsets.last().copied().unwrap_or(0);
    let data = reader.take(data_len, "raw string data")?;
    Ok(matcher.count_arena(data, &offsets))
}

fn weave_present(nulls: &super::NullStream, present: Vec<bool>) -> Result<Vec<bool>, FormatError> {
    if matches!(nulls.form, super::NullStreamForm::AllPresent) {
        if present.len() != nulls.row_count() {
            return Err(FormatError::Structural {
                rule: "null bitmap disagrees with present count",
            });
        }
        return Ok(present);
    }
    let mut mask = Vec::with_capacity(super::lazy::bounded_row_count(nulls)?);
    let mut verdicts = present.iter();
    nulls.each_row(|set| {
        if set {
            let verdict = verdicts.next().ok_or(FormatError::Structural {
                rule: "null bitmap disagrees with present count",
            })?;
            mask.push(*verdict);
        } else {
            mask.push(false);
        }
        Ok(())
    })?;
    if verdicts.next().is_some() {
        return Err(FormatError::Structural {
            rule: "null bitmap disagrees with present count",
        });
    }
    Ok(mask)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NumericBound {
    pub inclusive: bool,
    pub value: i128,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NumericPredicate {
    Equals(i128),
    InSet(Vec<i128>),
    NotEquals(i128),
    Range {
        lower: Option<NumericBound>,
        upper: Option<NumericBound>,
    },
}

impl NumericPredicate {
    fn matches_i128(&self, v: i128) -> bool {
        match self {
            NumericPredicate::Equals(n) => v == *n,
            NumericPredicate::InSet(set) => set.contains(&v),
            NumericPredicate::NotEquals(n) => v != *n,
            NumericPredicate::Range { lower, upper } => {
                let above = match lower {
                    Some(b) if b.inclusive => v >= b.value,
                    Some(b) => v > b.value,
                    None => true,
                };
                let below = match upper {
                    Some(b) if b.inclusive => v <= b.value,
                    Some(b) => v < b.value,
                    None => true,
                };
                above && below
            }
        }
    }

    fn matches_overflowing_u128(&self) -> bool {
        match self {
            NumericPredicate::Equals(_) | NumericPredicate::InSet(_) => false,
            NumericPredicate::NotEquals(_) => true,
            NumericPredicate::Range { upper, .. } => upper.is_none(),
        }
    }

    pub fn filter_decoded(&self, data: &ColumnData) -> Vec<bool> {
        match data {
            ColumnData::U64(values) => {
                let (intervals, negate) = u64_value_ranges(self);
                filter_native_ints(values, |value| value, intervals, negate)
            }
            ColumnData::I64(values) => {
                let (intervals, negate) = i64_value_ranges(self);
                let keys = intervals
                    .into_iter()
                    .map(|(lo, hi)| (i64_order_key(lo), i64_order_key(hi)))
                    .collect();
                filter_native_ints(values, i64_order_key, keys, negate)
            }
            ColumnData::Decimal { values, .. } => {
                let test = NativeRangeTest::translate(self);
                values.iter().map(|v| test.matches(*v)).collect()
            }
            ColumnData::U128(values) => {
                let test = NativeRangeTest::translate(self);
                values
                    .iter()
                    .map(|v| match i128::try_from(*v) {
                        Ok(v) => test.matches(v),
                        Err(_) => self.matches_overflowing_u128(),
                    })
                    .collect()
            }
            ColumnData::F64(_) | ColumnData::Strings(_) => vec![false; data.row_count()],
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FloatBound {
    pub inclusive: bool,
    pub value: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FloatPredicate {
    Equals(f64),
    InSet(Vec<f64>),
    NotEquals(f64),
    Range {
        lower: Option<FloatBound>,
        upper: Option<FloatBound>,
    },
}

impl FloatPredicate {
    fn matches_f64(&self, v: f64) -> bool {
        match self {
            FloatPredicate::Equals(n) => v == *n,
            FloatPredicate::InSet(set) => set.contains(&v),
            FloatPredicate::NotEquals(n) => v != *n,
            FloatPredicate::Range { lower, upper } => {
                let above = match lower {
                    Some(b) if b.inclusive => v >= b.value,
                    Some(b) => v > b.value,
                    None => true,
                };
                let below = match upper {
                    Some(b) if b.inclusive => v <= b.value,
                    Some(b) => v < b.value,
                    None => true,
                };
                above && below
            }
        }
    }

    pub fn filter_decoded(&self, values: &[f64]) -> Vec<bool> {
        match self {
            FloatPredicate::Equals(n) => {
                let n = *n;
                compare_each(values, move |v| v == n)
            }
            FloatPredicate::InSet(set) => filter_float_set(values, set),
            FloatPredicate::NotEquals(n) => {
                let n = *n;
                compare_each(values, move |v| v != n)
            }
            FloatPredicate::Range {
                lower: None,
                upper: None,
            } => vec![true; values.len()],
            FloatPredicate::Range { lower, upper } => {
                match (inclusive_float_lower(lower), inclusive_float_upper(upper)) {
                    (Some(lo), Some(hi)) => compare_each(values, move |v| (v >= lo) & (v <= hi)),
                    _ => vec![false; values.len()],
                }
            }
        }
    }
}

fn inclusive_float_lower(lower: &Option<FloatBound>) -> Option<f64> {
    match lower {
        None => Some(f64::NEG_INFINITY),
        Some(bound) if bound.inclusive => Some(bound.value),
        Some(bound) if bound.value == f64::INFINITY => None,
        Some(bound) => Some(bound.value.next_up()),
    }
}

fn inclusive_float_upper(upper: &Option<FloatBound>) -> Option<f64> {
    match upper {
        None => Some(f64::INFINITY),
        Some(bound) if bound.inclusive => Some(bound.value),
        Some(bound) if bound.value == f64::NEG_INFINITY => None,
        Some(bound) => Some(bound.value.next_down()),
    }
}

fn filter_float_set(values: &[f64], set: &[f64]) -> Vec<bool> {
    let members: Vec<f64> = set.iter().copied().filter(|member| !member.is_nan()).collect();
    if members.len() <= BRANCH_FREE_SET_NARROW {
        let padded = padded_set::<f64, BRANCH_FREE_SET_NARROW>(&members, f64::NAN);
        return compare_each(values, move |v| {
            padded.iter().fold(false, |hit, &member| hit | (v == member))
        });
    }
    if members.len() <= BRANCH_FREE_SET_WIDE {
        let padded = padded_set::<f64, BRANCH_FREE_SET_WIDE>(&members, f64::NAN);
        return compare_each(values, move |v| {
            padded.iter().fold(false, |hit, &member| hit | (v == member))
        });
    }
    let mut sorted: Vec<f64> = members.into_iter().map(|member| member + 0.0).collect();
    sorted.sort_unstable_by(f64::total_cmp);
    compare_each(values, |v| {
        !v.is_nan() && sorted.binary_search_by(|member| member.total_cmp(&(v + 0.0))).is_ok()
    })
}

#[multiversion(targets("x86_64+avx512f+avx512bw+avx512dq+avx512vl", "x86_64+avx2", "aarch64+neon"))]
fn compare_each<T: Copy>(values: &[T], test: impl Fn(T) -> bool) -> Vec<bool> {
    let mut out = vec![false; values.len()];
    for (slot, &value) in out.iter_mut().zip(values) {
        *slot = test(value);
    }
    out
}

fn padded_set<T: Copy, const N: usize>(items: &[T], fill: T) -> [T; N] {
    let mut set = [fill; N];
    for (slot, &item) in set.iter_mut().zip(items) {
        *slot = item;
    }
    set
}

fn interval_set_hit<const N: usize>(set: &[(u64, u64); N], value: u64) -> bool {
    set.iter()
        .fold(false, |hit, &(lo, span)| hit | (value.wrapping_sub(lo) <= span))
}

fn i64_order_key(value: i64) -> u64 {
    (value as u64) ^ (i64::MIN as u64)
}

fn filter_native_ints<T: Copy>(
    values: &[T],
    key: impl Fn(T) -> u64 + Copy,
    intervals: Vec<(u64, u64)>,
    negate: bool,
) -> Vec<bool> {
    let intervals = merge_intervals(intervals);
    let spans: Vec<(u64, u64)> = intervals.iter().map(|&(lo, hi)| (lo, hi - lo)).collect();
    let fill = spans.first().copied().unwrap_or((0, 0));
    match spans.as_slice() {
        [] => vec![negate; values.len()],
        &[(lo, 0)] => compare_each(values, move |v| (key(v) == lo) != negate),
        &[(lo, span)] => compare_each(values, move |v| (key(v).wrapping_sub(lo) <= span) != negate),
        set if set.len() <= BRANCH_FREE_SET_NARROW => {
            let set = padded_set::<_, BRANCH_FREE_SET_NARROW>(set, fill);
            compare_each(values, move |v| interval_set_hit(&set, key(v)) != negate)
        }
        set if set.len() <= BRANCH_FREE_SET_WIDE => {
            let set = padded_set::<_, BRANCH_FREE_SET_WIDE>(set, fill);
            compare_each(values, move |v| interval_set_hit(&set, key(v)) != negate)
        }
        _ => compare_each(values, |v| interval_contains(&intervals, key(v)) != negate),
    }
}

enum PackedLaneRange {
    Delta { hi: u64, lo: u64 },
    Zigzag { even: (u64, u64), odd: (u64, u64) },
}

fn interval_contains(intervals: &[(u64, u64)], value: u64) -> bool {
    let idx = intervals.partition_point(|&(lo, _)| lo <= value);
    idx.checked_sub(1)
        .and_then(|i| intervals.get(i))
        .is_some_and(|&(_, hi)| value <= hi)
}

fn merge_intervals(intervals: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    merge_intervals_in_place(intervals, |hi| hi.saturating_add(1))
}

fn merge_intervals_in_place<T: Copy + Ord>(mut intervals: Vec<(T, T)>, after: fn(T) -> T) -> Vec<(T, T)> {
    intervals.sort_unstable_by_key(|&(lo, _)| lo);
    let mut len: usize = 0;
    for read in 0..intervals.len() {
        let Some(&(lo, hi)) = intervals.get(read) else {
            break;
        };
        match len.checked_sub(1).and_then(|last| intervals.get_mut(last)) {
            Some((_, last_hi)) if lo <= after(*last_hi) => {
                if hi > *last_hi {
                    *last_hi = hi;
                }
            }
            _ => {
                if let Some(slot) = intervals.get_mut(len) {
                    *slot = (lo, hi);
                }
                len += 1;
            }
        }
    }
    intervals.truncate(len);
    intervals
}

struct MergedLaneRanges {
    delta: Vec<(u64, u64)>,
    even: Vec<(u64, u64)>,
    odd: Vec<(u64, u64)>,
}

impl MergedLaneRanges {
    fn from_ranges(ranges: Vec<PackedLaneRange>) -> Self {
        let mut delta = Vec::new();
        let mut even = Vec::new();
        let mut odd = Vec::new();
        for range in ranges {
            match range {
                PackedLaneRange::Delta { hi, lo } => delta.push((lo, hi)),
                PackedLaneRange::Zigzag { even: e, odd: o } => {
                    if e.0 <= e.1 {
                        even.push(e);
                    }
                    if o.0 <= o.1 {
                        odd.push(o);
                    }
                }
            }
        }
        Self {
            delta: merge_intervals(delta),
            even: merge_intervals(even),
            odd: merge_intervals(odd),
        }
    }

    fn contains(&self, base: u64, delta: u64) -> bool {
        if interval_contains(&self.delta, delta) {
            return true;
        }
        let code = base.wrapping_add(delta);
        if code & 1 == 0 {
            interval_contains(&self.even, code)
        } else {
            interval_contains(&self.odd, code)
        }
    }
}

struct PackedLaneTest {
    negate: bool,
    ranges: MergedLaneRanges,
}

impl PackedLaneTest {
    fn translate(predicate: &NumericPredicate, kind: ValueKind, base: u64) -> Self {
        if kind == ValueKind::I64 {
            let (value_ranges, negate) = i64_value_ranges(predicate);
            let ranges = value_ranges
                .into_iter()
                .filter_map(|range| zigzag_lane_range(base, range))
                .collect();
            Self {
                negate,
                ranges: MergedLaneRanges::from_ranges(ranges),
            }
        } else {
            let (value_ranges, negate) = u64_value_ranges(predicate);
            let ranges = value_ranges
                .into_iter()
                .filter_map(|range| delta_lane_range(base, range))
                .collect();
            Self {
                negate,
                ranges: MergedLaneRanges::from_ranges(ranges),
            }
        }
    }

    fn apply(&self, stream: &PackedStream<'_>, base: u64, out: &mut [bool]) {
        let negate = self.negate;
        match (
            self.ranges.delta.as_slice(),
            self.ranges.even.as_slice(),
            self.ranges.odd.as_slice(),
        ) {
            ([], [], []) => out.fill(negate),
            (&[(lo, hi)], [], []) if lo == hi => stream.unpack_into(out, move |delta| (delta == lo) != negate),
            (&[(lo, hi)], [], []) => {
                let span = hi - lo;
                stream.unpack_into(out, move |delta| (delta.wrapping_sub(lo) <= span) != negate);
            }
            (delta, [], []) if delta.len() <= BRANCH_FREE_SET_NARROW => {
                apply_lane_set::<BRANCH_FREE_SET_NARROW>(stream, delta, negate, out);
            }
            (delta, [], []) if delta.len() <= BRANCH_FREE_SET_WIDE => {
                apply_lane_set::<BRANCH_FREE_SET_WIDE>(stream, delta, negate, out);
            }
            ([], even, odd) if even.len() <= 1 && odd.len() <= 1 => {
                let (even_lo, even_span) = even.first().map_or((1, 0), |&(lo, hi)| (lo, hi - lo));
                let (odd_lo, odd_span) = odd.first().map_or((0, 0), |&(lo, hi)| (lo, hi - lo));
                stream.unpack_into(out, move |delta| {
                    let code = base.wrapping_add(delta);
                    let odd_mask = 0u64.wrapping_sub(code & 1);
                    let lo = (even_lo & !odd_mask) | (odd_lo & odd_mask);
                    let span = (even_span & !odd_mask) | (odd_span & odd_mask);
                    (code.wrapping_sub(lo) <= span) != negate
                });
            }
            _ => stream.unpack_into(out, |delta| self.ranges.contains(base, delta) != negate),
        }
    }

    fn matches(&self, base: u64, value: u64) -> bool {
        self.ranges.contains(base, value) != self.negate
    }
}

fn apply_lane_set<const N: usize>(stream: &PackedStream<'_>, intervals: &[(u64, u64)], negate: bool, out: &mut [bool]) {
    let spans: Vec<(u64, u64)> = intervals.iter().map(|&(lo, hi)| (lo, hi - lo)).collect();
    let set = padded_set::<_, N>(&spans, spans.first().copied().unwrap_or((0, 0)));
    stream.unpack_into(out, move |delta| interval_set_hit(&set, delta) != negate);
}

fn u64_value_ranges(predicate: &NumericPredicate) -> (Vec<(u64, u64)>, bool) {
    fn point(n: i128) -> Option<(u64, u64)> {
        u64::try_from(n).ok().map(|value| (value, value))
    }
    match predicate {
        NumericPredicate::Equals(n) => (point(*n).into_iter().collect(), false),
        NumericPredicate::InSet(set) => (set.iter().copied().filter_map(point).collect(), false),
        NumericPredicate::NotEquals(n) => (point(*n).into_iter().collect(), true),
        NumericPredicate::Range { lower, upper } => {
            let lo = match lower {
                None => Some(0),
                Some(bound) => {
                    let first = if bound.inclusive {
                        bound.value
                    } else {
                        bound.value.saturating_add(1)
                    };
                    if first > i128::from(u64::MAX) {
                        None
                    } else {
                        Some(first.max(0) as u64)
                    }
                }
            };
            let hi = match upper {
                None => Some(u64::MAX),
                Some(bound) => {
                    let last = if bound.inclusive {
                        bound.value
                    } else {
                        bound.value.saturating_sub(1)
                    };
                    if last < 0 {
                        None
                    } else {
                        Some(last.min(i128::from(u64::MAX)) as u64)
                    }
                }
            };
            match (lo, hi) {
                (Some(lo), Some(hi)) if lo <= hi => (vec![(lo, hi)], false),
                _ => (Vec::new(), false),
            }
        }
    }
}

fn i64_value_ranges(predicate: &NumericPredicate) -> (Vec<(i64, i64)>, bool) {
    fn point(n: i128) -> Option<(i64, i64)> {
        i64::try_from(n).ok().map(|value| (value, value))
    }
    match predicate {
        NumericPredicate::Equals(n) => (point(*n).into_iter().collect(), false),
        NumericPredicate::InSet(set) => (set.iter().copied().filter_map(point).collect(), false),
        NumericPredicate::NotEquals(n) => (point(*n).into_iter().collect(), true),
        NumericPredicate::Range { lower, upper } => {
            let lo = match lower {
                None => Some(i64::MIN),
                Some(bound) => {
                    let first = if bound.inclusive {
                        bound.value
                    } else {
                        bound.value.saturating_add(1)
                    };
                    if first > i128::from(i64::MAX) {
                        None
                    } else {
                        Some(first.max(i128::from(i64::MIN)) as i64)
                    }
                }
            };
            let hi = match upper {
                None => Some(i64::MAX),
                Some(bound) => {
                    let last = if bound.inclusive {
                        bound.value
                    } else {
                        bound.value.saturating_sub(1)
                    };
                    if last < i128::from(i64::MIN) {
                        None
                    } else {
                        Some(last.min(i128::from(i64::MAX)) as i64)
                    }
                }
            };
            match (lo, hi) {
                (Some(lo), Some(hi)) if lo <= hi => (vec![(lo, hi)], false),
                _ => (Vec::new(), false),
            }
        }
    }
}

fn delta_lane_range(base: u64, (lo, hi): (u64, u64)) -> Option<PackedLaneRange> {
    if hi < base {
        return None;
    }
    Some(PackedLaneRange::Delta {
        hi: hi - base,
        lo: lo.saturating_sub(base),
    })
}

fn zigzag_lane_range(base: u64, (lo, hi): (i64, i64)) -> Option<PackedLaneRange> {
    if lo == hi {
        let delta = zigzag(lo).checked_sub(base)?;
        return Some(PackedLaneRange::Delta { hi: delta, lo: delta });
    }
    let even = if hi >= 0 {
        ((lo.max(0) as u64) * 2, (hi as u64) * 2)
    } else {
        (1, 0)
    };
    let odd = if lo < 0 {
        let nearest = hi.min(-1);
        ((-2 * i128::from(nearest) - 1) as u64, (-2 * i128::from(lo) - 1) as u64)
    } else {
        (1, 0)
    };
    Some(PackedLaneRange::Zigzag { even, odd })
}

fn filter_rle_block(body: &[u8], predicate: &NumericPredicate, kind: ValueKind) -> Result<Vec<bool>, FormatError> {
    let mut reader = Reader::new(body);
    let run_count = reader.u32("rle run count")? as usize;
    let mut mask = Vec::new();
    for _ in 0..run_count {
        let value = reader.u64("rle value")?;
        let count = reader.u32("rle count")? as usize;
        if mask.len() + count > super::constant::MAX_RLE_EXPANSION_VALUES {
            return Err(FormatError::Structural {
                rule: "rle expansion too large",
            });
        }
        let value = if kind == ValueKind::I64 {
            i128::from(unzigzag(value))
        } else {
            i128::from(value)
        };
        mask.extend(std::iter::repeat_n(predicate.matches_i128(value), count));
    }
    Ok(mask)
}

fn filter_for_block(body: &[u8], predicate: &NumericPredicate, kind: ValueKind) -> Result<Vec<bool>, FormatError> {
    let mut reader = Reader::new(body);
    let base = reader.u64("for base")?;
    let stream = read_packed_stream(&mut reader)?;
    if stream.width == 0 {
        let value = if kind == ValueKind::I64 {
            i128::from(unzigzag(base))
        } else {
            i128::from(base)
        };
        return Ok(vec![predicate.matches_i128(value); stream.count]);
    }
    if base.checked_add(mask(stream.width)).is_none() {
        let to_i128: fn(u64) -> i128 = if kind == ValueKind::I64 {
            |value| i128::from(unzigzag(value))
        } else {
            i128::from
        };
        let mut deltas = vec![0u64; stream.padded_count()];
        stream.unpack_into(&mut deltas, |packed| packed);
        deltas.truncate(stream.count);
        return deltas
            .into_iter()
            .map(|delta| {
                base.checked_add(delta)
                    .map(|value| predicate.matches_i128(to_i128(value)))
                    .ok_or(FormatError::Structural {
                        rule: "frame-of-reference overflow",
                    })
            })
            .collect();
    }
    let test = PackedLaneTest::translate(predicate, kind, base);
    let mut verdicts = vec![false; stream.padded_count()];
    test.apply(&stream, base, &mut verdicts);
    verdicts.truncate(stream.count);
    Ok(verdicts)
}

fn filter_delta_block(body: &[u8], predicate: &NumericPredicate, kind: ValueKind) -> Result<Vec<bool>, FormatError> {
    let mut reader = Reader::new(body);
    let first = reader.u64("delta first")?;
    let deltas = read_packed_stream(&mut reader)?;
    let test = PackedLaneTest::translate(predicate, kind, 0);
    let mut result = vec![false; deltas.count + 1];
    if let Some((head, rest)) = result.split_first_mut() {
        *head = test.matches(0, first);
        let mut current = first;
        let mut totals = [0u64; FASTLANES_VECTOR];
        let mut verdict_vectors = rest.chunks_mut(FASTLANES_VECTOR);
        deltas.for_each_vector(|vector| {
            let (Some(verdicts), Some(totals)) = (verdict_vectors.next(), totals.get_mut(..vector.len())) else {
                return;
            };
            for (total, &delta) in totals.iter_mut().zip(vector) {
                *total = unzigzag(delta) as u64;
            }
            current = prefix_sum(totals, current);
            for (verdict, &value) in verdicts.iter_mut().zip(totals.iter()) {
                *verdict = test.matches(0, value);
            }
        });
    }
    Ok(result)
}

fn filter_decimal128_block(body: &[u8], predicate: &NumericPredicate) -> Result<Vec<bool>, FormatError> {
    let mut reader = Reader::new(body);
    let _scale = reader.u8("decimal scale")?;
    let count = reader.u32("decimal count")? as usize;
    let test = NativeRangeTest::translate(predicate);
    let bytes = reader.take(count.saturating_mul(16), "decimal mantissa")?;
    Ok(bytes
        .chunks_exact(16)
        .map(|chunk| test.matches(i128::from_le_bytes(chunk.try_into().unwrap_or([0; 16]))))
        .collect())
}

fn merge_intervals_i128(intervals: Vec<(i128, i128)>) -> Vec<(i128, i128)> {
    merge_intervals_in_place(intervals, |hi| hi.saturating_add(1))
}

fn interval_contains_i128(intervals: &[(i128, i128)], value: i128) -> bool {
    let idx = intervals.partition_point(|&(lo, _)| lo <= value);
    idx.checked_sub(1)
        .and_then(|i| intervals.get(i))
        .is_some_and(|&(_, hi)| value <= hi)
}

struct NativeRangeTest {
    negate: bool,
    ranges: Vec<(i128, i128)>,
}

impl NativeRangeTest {
    fn translate(predicate: &NumericPredicate) -> Self {
        match predicate {
            NumericPredicate::Equals(n) => Self {
                negate: false,
                ranges: vec![(*n, *n)],
            },
            NumericPredicate::NotEquals(n) => Self {
                negate: true,
                ranges: vec![(*n, *n)],
            },
            NumericPredicate::InSet(set) => Self {
                negate: false,
                ranges: merge_intervals_i128(set.iter().map(|n| (*n, *n)).collect()),
            },
            NumericPredicate::Range { lower, upper } => {
                let lo = match lower {
                    None => Some(i128::MIN),
                    Some(bound) if bound.inclusive => Some(bound.value),
                    Some(bound) => bound.value.checked_add(1),
                };
                let hi = match upper {
                    None => Some(i128::MAX),
                    Some(bound) if bound.inclusive => Some(bound.value),
                    Some(bound) => bound.value.checked_sub(1),
                };
                Self {
                    negate: false,
                    ranges: match (lo, hi) {
                        (Some(lo), Some(hi)) if lo <= hi => vec![(lo, hi)],
                        _ => Vec::new(),
                    },
                }
            }
        }
    }

    fn matches(&self, value: i128) -> bool {
        let hit = interval_contains_i128(&self.ranges, value);
        hit != self.negate
    }
}

pub fn filter_numeric_block(
    pipeline: PipelineId,
    bytes: &[u8],
    predicate: &NumericPredicate,
) -> Result<Option<Vec<bool>>, FormatError> {
    if !numeric_block_has_fast_path(pipeline)? {
        return Ok(None);
    }
    let body = super::remove_trailing(pipeline.compression()?, bytes)?;
    filter_numeric_block_with_body(pipeline, &body, predicate)
}

pub fn filter_numeric_block_with_body(
    pipeline: PipelineId,
    body: &[u8],
    predicate: &NumericPredicate,
) -> Result<Option<Vec<bool>>, FormatError> {
    if !numeric_block_has_fast_path(pipeline)? {
        return Ok(None);
    }
    let kind = pipeline.value_kind()?;
    let transform = pipeline.transform()?;
    if transform == Transform::Decimal128 {
        return Ok(Some(filter_decimal128_block(body, predicate)?));
    }
    let mask = match transform {
        Transform::DeltaBitpack => filter_delta_block(body, predicate, kind)?,
        Transform::Rle => filter_rle_block(body, predicate, kind)?,
        _ => filter_for_block(body, predicate, kind)?,
    };
    Ok(Some(mask))
}

fn numeric_block_has_fast_path(pipeline: PipelineId) -> Result<bool, FormatError> {
    let kind = pipeline.value_kind()?;
    let transform = pipeline.transform()?;
    if matches!(kind, ValueKind::F64 | ValueKind::String) {
        return Ok(false);
    }
    Ok(transform == Transform::Decimal128
        || (matches!(
            transform,
            Transform::ForBitpack | Transform::DeltaBitpack | Transform::Rle
        ) && matches!(kind, ValueKind::U64 | ValueKind::I64)))
}

pub fn filter_numeric_block_or_decode(
    pipeline: PipelineId,
    bytes: &[u8],
    predicate: &NumericPredicate,
) -> Result<Vec<bool>, FormatError> {
    if let Some(mask) = filter_numeric_block(pipeline, bytes, predicate)? {
        return Ok(mask);
    }
    let data = decode_block(pipeline, bytes)?;
    Ok(predicate.filter_decoded(&data))
}

fn alp_value(int: i64, power: f64) -> f64 {
    (int as f64) / power
}

fn alp_first_int(power: f64, satisfied: impl Fn(f64) -> bool) -> Option<i64> {
    if !satisfied(alp_value(i64::MAX, power)) {
        return None;
    }
    if satisfied(alp_value(i64::MIN, power)) {
        return Some(i64::MIN);
    }
    let mut below = i64::MIN;
    let mut at = i64::MAX;
    while i128::from(at) - i128::from(below) > 1 {
        let mid = ((i128::from(at) + i128::from(below)) / 2) as i64;
        if satisfied(alp_value(mid, power)) {
            at = mid;
        } else {
            below = mid;
        }
    }
    Some(at)
}

fn alp_last_int(power: f64, satisfied: impl Fn(f64) -> bool) -> Option<i64> {
    if !satisfied(alp_value(i64::MIN, power)) {
        return None;
    }
    if satisfied(alp_value(i64::MAX, power)) {
        return Some(i64::MAX);
    }
    let mut at = i64::MIN;
    let mut above = i64::MAX;
    while i128::from(above) - i128::from(at) > 1 {
        let mid = ((i128::from(above) + i128::from(at)) / 2) as i64;
        if satisfied(alp_value(mid, power)) {
            at = mid;
        } else {
            above = mid;
        }
    }
    Some(at)
}

fn alp_equal_range(n: f64, power: f64) -> Option<(i64, i64)> {
    let lo = alp_first_int(power, |value| value >= n)?;
    let hi = alp_last_int(power, |value| value <= n)?;
    (lo <= hi).then_some((lo, hi))
}

struct AlpIntTest {
    negate: bool,
    ranges: Vec<(i64, i64)>,
}

impl AlpIntTest {
    fn translate(predicate: &FloatPredicate, power: f64) -> Self {
        match predicate {
            FloatPredicate::Equals(n) => Self {
                negate: false,
                ranges: alp_equal_range(*n, power).into_iter().collect(),
            },
            FloatPredicate::InSet(set) => Self {
                negate: false,
                ranges: merge_intervals_i128(
                    set.iter()
                        .filter_map(|n| alp_equal_range(*n, power))
                        .map(|(lo, hi)| (i128::from(lo), i128::from(hi)))
                        .collect(),
                )
                .into_iter()
                .filter_map(|(lo, hi)| Some((i64::try_from(lo).ok()?, i64::try_from(hi).ok()?)))
                .collect(),
            },
            FloatPredicate::NotEquals(n) => Self {
                negate: true,
                ranges: alp_equal_range(*n, power).into_iter().collect(),
            },
            FloatPredicate::Range { lower, upper } => {
                let lo = match lower {
                    None => Some(i64::MIN),
                    Some(bound) if bound.inclusive => alp_first_int(power, |value| value >= bound.value),
                    Some(bound) => alp_first_int(power, |value| value > bound.value),
                };
                let hi = match upper {
                    None => Some(i64::MAX),
                    Some(bound) if bound.inclusive => alp_last_int(power, |value| value <= bound.value),
                    Some(bound) => alp_last_int(power, |value| value < bound.value),
                };
                let ranges = match (lo, hi) {
                    (Some(lo), Some(hi)) if lo <= hi => vec![(lo, hi)],
                    _ => Vec::new(),
                };
                Self { negate: false, ranges }
            }
        }
    }

    fn apply(&self, stream: &PackedStream<'_>, out: &mut [bool]) {
        match self.ranges.as_slice() {
            [] => out.fill(false),
            [(lo, hi)] => {
                let (lo, hi) = (*lo, *hi);
                stream.unpack_into(out, |packed| {
                    let int = unzigzag(packed);
                    lo <= int && int <= hi
                });
            }
            ranges => stream.unpack_into(out, |packed| {
                let int = unzigzag(packed);
                let above = ranges.partition_point(|&(lo, _)| lo <= int);
                above
                    .checked_sub(1)
                    .and_then(|index| ranges.get(index))
                    .is_some_and(|&(_, hi)| int <= hi)
            }),
        }
        if self.negate {
            for slot in out.iter_mut() {
                *slot = !*slot;
            }
        }
    }
}

fn filter_alp_block(body: &[u8], side: SideStream, predicate: &FloatPredicate) -> Result<Vec<bool>, FormatError> {
    let mut reader = Reader::new(body);
    let mut exponent_index = reader.u8("alp exponent")? as usize;
    let mut escaped = Vec::new();
    if exponent_index == usize::from(super::ALP_VECTOR_ESCAPE_SENTINEL) {
        exponent_index = reader.u8("alp exponent")? as usize;
        escaped = super::read_alp_escaped_vectors(&mut reader)?;
    }
    let power = ALP_POWERS.get(exponent_index).copied().ok_or(FormatError::Structural {
        rule: "alp exponent out of range",
    })?;
    let exceptions = super::decode_alp_exceptions(&mut reader, side)?;
    let test = AlpIntTest::translate(predicate, power);
    let stream = read_packed_stream(&mut reader)?;
    let mut verdicts = vec![false; stream.padded_count()];
    test.apply(&stream, &mut verdicts);
    verdicts.truncate(stream.count);
    exceptions.try_for_each(|index, bits| {
        let slot = verdicts.get_mut(index as usize).ok_or(FormatError::RefOutOfRange {
            what: "alp exception index",
        })?;
        *slot = predicate.matches_f64(f64::from_bits(bits));
        Ok(())
    })?;
    for vector_index in &escaped {
        let start = *vector_index as usize * FASTLANES_VECTOR;
        if start >= verdicts.len() {
            return Err(FormatError::RefOutOfRange {
                what: "alp escaped vector index",
            });
        }
        let end = (start + FASTLANES_VECTOR).min(verdicts.len());
        for slot in verdicts.iter_mut().take(end).skip(start) {
            *slot = predicate.matches_f64(f64::from_bits(reader.u64("alp escaped value")?));
        }
    }
    Ok(verdicts)
}

pub fn filter_float_block(
    pipeline: PipelineId,
    bytes: &[u8],
    predicate: &FloatPredicate,
) -> Result<Option<Vec<bool>>, FormatError> {
    if !float_block_has_fast_path(pipeline)? {
        return Ok(None);
    }
    let body = super::remove_trailing(pipeline.compression()?, bytes)?;
    filter_float_block_with_body(pipeline, &body, predicate)
}

pub fn filter_float_block_with_body(
    pipeline: PipelineId,
    body: &[u8],
    predicate: &FloatPredicate,
) -> Result<Option<Vec<bool>>, FormatError> {
    if !float_block_has_fast_path(pipeline)? {
        return Ok(None);
    }
    Ok(Some(filter_alp_block(body, pipeline.side_stream()?, predicate)?))
}

fn float_block_has_fast_path(pipeline: PipelineId) -> Result<bool, FormatError> {
    Ok(pipeline.value_kind()? == ValueKind::F64 && pipeline.transform()? == Transform::Alp)
}

pub fn filter_float_block_or_decode(
    pipeline: PipelineId,
    bytes: &[u8],
    predicate: &FloatPredicate,
) -> Result<Vec<bool>, FormatError> {
    if let Some(mask) = filter_float_block(pipeline, bytes, predicate)? {
        return Ok(mask);
    }
    match decode_block(pipeline, bytes)? {
        ColumnData::F64(values) => Ok(predicate.filter_decoded(&values)),
        _ => Err(FormatError::Structural {
            rule: "float predicate applied to a non-float column",
        }),
    }
}

#[cfg(test)]
#[path = "test/predicate.rs"]
mod tests;
