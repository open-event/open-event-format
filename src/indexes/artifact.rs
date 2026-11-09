use super::{
    Exactness,
    bitmap::BitmapIndex,
    learned_position::LearnedPositionIndex,
    path_presence::{PathPresenceIndex, hash_path},
    probabilistic::{BinaryFuseFilter, SplitBlockBloomFilter},
    pruning::{FilterClause, PredicateKind, PruningTerm, Statistic},
    range_filter::RangeFilter,
};
use crate::error::FormatError;
use crate::events::variant::VariantValue;
use crate::file::bytes::{Reader, Writer, slice};
use crate::layout::reader::{ColumnRead, PayloadRead};
use hashbrown::HashSet;
use std::sync::OnceLock;
use std::{borrow::Cow, collections::BTreeMap};

pub const ARTIFACT_MAGIC: [u8; 4] = *b"HIA1";

const BLOOM_FALLBACK_BITS_PER_KEY: u32 = 16;

const BLOOM_FALLBACK_FALSE_POSITIVE_PPM: u32 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ArtifactKind {
    BinaryFuse = 1,
    Bitmap = 5,
    LearnedPosition = 4,
    PathPresence = 0,
    RangeFilter = 3,
    SplitBlockBloom = 2,
}

impl ArtifactKind {
    fn from_u8(value: u8) -> Result<Self, FormatError> {
        Ok(match value {
            0 => ArtifactKind::PathPresence,
            1 => ArtifactKind::BinaryFuse,
            2 => ArtifactKind::SplitBlockBloom,
            3 => ArtifactKind::RangeFilter,
            4 => ArtifactKind::LearnedPosition,
            5 => ArtifactKind::Bitmap,
            _ => {
                return Err(FormatError::Structural {
                    rule: "unknown index artifact kind",
                });
            }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactHeader {
    pub column_id: u32,
    pub exact: bool,
    pub false_positive_ppm: u32,
    pub kind: ArtifactKind,
    pub projection_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactCoverage {
    pub deletion_vector_generation: u64,
    pub file_id: u128,
    pub granule_ranges: Vec<(u32, u32)>,
    pub schema_fingerprint: [u8; 32],
}

impl ArtifactCoverage {
    pub fn covers(&self, granule_id: u32) -> bool {
        self.granule_ranges
            .iter()
            .any(|(first, last)| (*first..=*last).contains(&granule_id))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactPage {
    pub bytes: Vec<u8>,
    pub first_granule: u32,
    pub last_granule: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactPageEntry {
    pub first_granule: u32,
    pub last_granule: u32,
    pub len: u64,
    pub offset: u64,
}

#[derive(Debug)]
pub struct IndexArtifact {
    pub coverage: ArtifactCoverage,
    decoded_pages: Vec<OnceLock<Option<DecodedPage>>>,
    pub header: ArtifactHeader,
    page_by_granule: Vec<(u32, u32, usize)>,
    pub pages: Vec<ArtifactPage>,
}

impl Clone for IndexArtifact {
    fn clone(&self) -> Self {
        Self::new(self.coverage.clone(), self.header, self.pages.clone())
    }
}

impl PartialEq for IndexArtifact {
    fn eq(&self, other: &Self) -> bool {
        self.coverage == other.coverage && self.header == other.header && self.pages == other.pages
    }
}

#[derive(Debug)]
enum DecodedPage {
    BinaryFuse(BinaryFuseFilter),
    Bitmap(BitmapIndex),
    PathPresence(PathPresenceIndex),
    RangeFilter(RangeFilter),
    SplitBlockBloom(SplitBlockBloomFilter),
}

impl DecodedPage {
    fn statistic(&self, clause: &FilterClause) -> Option<Statistic> {
        Some(match self {
            DecodedPage::BinaryFuse(filter) => {
                let key = equality_key(clause)?;
                Statistic::SingleMember {
                    column_id: clause.column_id,
                    member: filter.contains(key).then_some(clause.value_lo),
                }
            }
            DecodedPage::Bitmap(index) => {
                let key = equality_key(clause)?;
                let present = index.bitmap(key).is_some_and(|rows| !rows.is_empty());
                Statistic::SingleMember {
                    column_id: clause.column_id,
                    member: present.then_some(clause.value_lo),
                }
            }
            DecodedPage::PathPresence(index) => {
                if clause.predicate != PredicateKind::Presence {
                    return None;
                }
                let hash = u64::try_from(clause.value_lo).ok()?;
                Statistic::PathPresence {
                    column_id: clause.column_id,
                    present: index.might_contain_hash(hash),
                }
            }
            DecodedPage::RangeFilter(filter) => {
                let lo = u64::try_from(clause.value_lo).ok()?;
                let hi = u64::try_from(clause.value_hi).ok()?;
                Statistic::SingleSpan {
                    column_id: clause.column_id,
                    span: filter
                        .range_nonempty(lo, hi)
                        .then_some((clause.value_lo, clause.value_hi)),
                }
            }
            DecodedPage::SplitBlockBloom(filter) => {
                let key = equality_key(clause)?;
                Statistic::SingleMember {
                    column_id: clause.column_id,
                    member: filter.contains(key).then_some(clause.value_lo),
                }
            }
        })
    }
}

impl IndexArtifact {
    pub fn new(coverage: ArtifactCoverage, header: ArtifactHeader, pages: Vec<ArtifactPage>) -> Self {
        let mut page_by_granule: Vec<(u32, u32, usize)> = pages
            .iter()
            .enumerate()
            .map(|(index, page)| (page.first_granule, page.last_granule, index))
            .collect();
        page_by_granule.sort_unstable();
        let decoded_pages = pages.iter().map(|_| OnceLock::new()).collect();
        Self {
            coverage,
            decoded_pages,
            header,
            page_by_granule,
            pages,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Writer::new();
        out.put_slice(&ARTIFACT_MAGIC);
        out.put_u8(self.header.kind as u8);
        out.put_u32(self.header.column_id);
        out.put_u32(self.header.projection_id);
        out.put_u8(u8::from(self.header.exact));
        out.put_u32(self.header.false_positive_ppm);
        out.put_u128(self.coverage.file_id);
        out.put_u64(self.coverage.deletion_vector_generation);
        out.put_slice(&self.coverage.schema_fingerprint);
        out.put_u32(self.coverage.granule_ranges.len() as u32);
        for (first, last) in &self.coverage.granule_ranges {
            out.put_u32(*first);
            out.put_u32(*last);
        }
        out.put_u32(self.pages.len() as u32);
        let mut offset = 0u64;
        for page in &self.pages {
            out.put_u32(page.first_granule);
            out.put_u32(page.last_granule);
            out.put_u64(offset);
            out.put_u64(page.bytes.len() as u64);
            offset += page.bytes.len() as u64;
        }
        for page in &self.pages {
            out.put_slice(&page.bytes);
        }
        let mut bytes = out.into_bytes();
        let seal = crate::file::integrity::hash_tree(&bytes);
        bytes.extend_from_slice(seal.as_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        if bytes.len() < 32 {
            return Err(FormatError::Truncated {
                what: "index artifact seal",
            });
        }
        let (sealed, seal) = bytes.split_at(bytes.len() - 32);
        if crate::file::integrity::hash_tree(sealed).as_bytes() != seal {
            return Err(FormatError::Structural {
                rule: "index artifact seal does not match its bytes",
            });
        }
        let (header, coverage, directory, pages_area) = decode_artifact_prefix(sealed)?;
        let mut pages = Vec::with_capacity(directory.len());
        for entry in directory {
            let page = slice(pages_area, entry.offset as usize, entry.len as usize, "artifact page")?;
            pages.push(ArtifactPage {
                bytes: page.to_vec(),
                first_granule: entry.first_granule,
                last_granule: entry.last_granule,
            });
        }
        Ok(Self::new(coverage, header, pages))
    }

    pub fn usable_for(&self, file_id: u128, schema_fingerprint: &[u8; 32], deletion_vector_generation: u64) -> bool {
        if self.coverage.file_id != file_id || self.coverage.schema_fingerprint != *schema_fingerprint {
            return false;
        }
        if self.header.exact {
            self.coverage.deletion_vector_generation == deletion_vector_generation
        } else {
            self.coverage.deletion_vector_generation <= deletion_vector_generation
        }
    }

    pub fn page_for(&self, granule_id: u32) -> Option<&ArtifactPage> {
        self.pages.get(self.page_index_for(granule_id)?)
    }

    fn page_index_for(&self, granule_id: u32) -> Option<usize> {
        let after = self.page_by_granule.partition_point(|(first, ..)| *first <= granule_id);
        let (_, last_granule, index) = *self.page_by_granule.get(after.checked_sub(1)?)?;
        (granule_id <= last_granule).then_some(index)
    }

    pub fn term_for(&self, granule_id: u32, clause: &FilterClause) -> Option<PruningTerm> {
        if clause.column_id != self.header.column_id || !self.coverage.covers(granule_id) {
            return None;
        }
        let page_index = self.page_index_for(granule_id)?;
        let statistic = self.decoded_page(page_index)?.statistic(clause)?;
        let exactness = if self.header.exact {
            Exactness::Exact
        } else {
            Exactness::InexactNoFalseNegative
        };
        Some(PruningTerm {
            clause: *clause,
            exactness,
            statistic,
        })
    }

    fn decoded_page(&self, page_index: usize) -> Option<&DecodedPage> {
        let slot = self.decoded_pages.get(page_index)?;
        let pages = &self.pages;
        let kind = self.header.kind;
        slot.get_or_init(move || {
            let bytes = &pages.get(page_index)?.bytes;
            Some(match kind {
                ArtifactKind::BinaryFuse => DecodedPage::BinaryFuse(BinaryFuseFilter::decode(bytes).ok()?),
                ArtifactKind::Bitmap => DecodedPage::Bitmap(BitmapIndex::decode(bytes).ok()?),
                ArtifactKind::LearnedPosition => return None,
                ArtifactKind::PathPresence => DecodedPage::PathPresence(PathPresenceIndex::decode(bytes).ok()?),
                ArtifactKind::RangeFilter => DecodedPage::RangeFilter(RangeFilter::decode(bytes).ok()?),
                ArtifactKind::SplitBlockBloom => {
                    DecodedPage::SplitBlockBloom(SplitBlockBloomFilter::decode(bytes).ok()?)
                }
            })
        })
        .as_ref()
    }

    pub fn decoded_page_count(&self) -> usize {
        self.decoded_pages
            .iter()
            .filter(|slot| matches!(slot.get(), Some(Some(_))))
            .count()
    }
}

fn equality_key(clause: &FilterClause) -> Option<u64> {
    if !matches!(clause.predicate, PredicateKind::Equality | PredicateKind::Membership) {
        return None;
    }
    if clause.value_lo != clause.value_hi {
        return None;
    }
    u64::try_from(clause.value_lo).ok()
}

pub fn decode_artifact_directory(
    bytes: &[u8],
) -> Result<(ArtifactHeader, ArtifactCoverage, Vec<ArtifactPageEntry>), FormatError> {
    let (header, coverage, directory, _) = decode_artifact_prefix(bytes)?;
    Ok((header, coverage, directory))
}

type ArtifactPrefix<'a> = (ArtifactHeader, ArtifactCoverage, Vec<ArtifactPageEntry>, &'a [u8]);

fn decode_artifact_prefix(bytes: &[u8]) -> Result<ArtifactPrefix<'_>, FormatError> {
    let mut reader = Reader::new(bytes);
    let magic = reader.take(4, "artifact magic")?;
    if magic != ARTIFACT_MAGIC {
        return Err(FormatError::Structural {
            rule: "index artifact magic mismatch",
        });
    }
    let kind = ArtifactKind::from_u8(reader.u8("artifact kind")?)?;
    let column_id = reader.u32("artifact column")?;
    let projection_id = reader.u32("artifact projection")?;
    let exact = reader.u8("artifact exactness")? != 0;
    let false_positive_ppm = reader.u32("artifact fpr")?;
    let file_id = reader.u128("artifact file id")?;
    let deletion_vector_generation = reader.u64("artifact dv generation")?;
    let mut schema_fingerprint = [0u8; 32];
    schema_fingerprint.copy_from_slice(reader.take(32, "artifact schema fingerprint")?);
    let range_count = reader.u32("artifact coverage range count")? as usize;
    let range_count = range_count.min(reader.remaining() / 8 + 1);
    let mut granule_ranges = Vec::with_capacity(range_count);
    for _ in 0..range_count {
        let first = reader.u32("artifact coverage first granule")?;
        let last = reader.u32("artifact coverage last granule")?;
        if last < first {
            return Err(FormatError::Structural {
                rule: "artifact coverage range is inverted",
            });
        }
        granule_ranges.push((first, last));
    }
    let page_count = reader.u32("artifact page count")? as usize;
    let page_count = page_count.min(reader.remaining() / 24 + 1);
    let mut directory = Vec::with_capacity(page_count);
    for _ in 0..page_count {
        let first_granule = reader.u32("artifact page first granule")?;
        let last_granule = reader.u32("artifact page last granule")?;
        let offset = reader.u64("artifact page offset")?;
        let len = reader.u64("artifact page length")?;
        if last_granule < first_granule {
            return Err(FormatError::Structural {
                rule: "artifact page range is inverted",
            });
        }
        directory.push(ArtifactPageEntry {
            first_granule,
            last_granule,
            len,
            offset,
        });
    }
    let pages_area = reader.take(reader.remaining(), "artifact pages area")?;
    Ok((
        ArtifactHeader {
            column_id,
            exact,
            false_positive_ppm,
            kind,
            projection_id,
        },
        ArtifactCoverage {
            deletion_vector_generation,
            file_id,
            granule_ranges,
            schema_fingerprint,
        },
        directory,
        pages_area,
    ))
}

pub fn plan_artifact_builds(file: &crate::layout::reader::OeFile) -> Vec<(ArtifactKind, u32)> {
    file.footer()
        .presence
        .iter()
        .map(|entry| (ArtifactKind::BinaryFuse, entry.column_id))
        .collect()
}

pub fn build_membership_artifact(
    file: &crate::layout::reader::OeFile,
    column_id: u32,
    deletion_vector_generation: u64,
) -> Result<IndexArtifact, FormatError> {
    let mut granule_keys: Vec<(u32, Vec<u64>)> = Vec::new();
    let mut granule_ranges: Vec<(u32, u32)> = Vec::new();
    for granule in &file.footer().granules {
        let read = file.read_column(column_id, granule.granule_id)?;
        let Some(keys) = membership_keys(&read.data) else {
            continue;
        };
        extend_granule_ranges(&mut granule_ranges, granule.granule_id);
        granule_keys.push((granule.granule_id, keys.into_owned()));
    }
    let page = |granule_id: u32, bytes: Vec<u8>| ArtifactPage {
        bytes,
        first_granule: granule_id,
        last_granule: granule_id,
    };
    let fuse: Result<Vec<ArtifactPage>, FormatError> = granule_keys
        .iter()
        .map(|(granule_id, keys)| BinaryFuseFilter::build(keys).map(|filter| page(*granule_id, filter.encode())))
        .collect();
    let (kind, false_positive_ppm, pages) = match fuse {
        Ok(pages) => (ArtifactKind::BinaryFuse, 4_000, pages),
        Err(_) => (
            ArtifactKind::SplitBlockBloom,
            BLOOM_FALLBACK_FALSE_POSITIVE_PPM,
            granule_keys
                .iter()
                .map(|(granule_id, keys)| {
                    page(
                        *granule_id,
                        SplitBlockBloomFilter::build(keys, BLOOM_FALLBACK_BITS_PER_KEY).encode(),
                    )
                })
                .collect(),
        ),
    };
    Ok(IndexArtifact::new(
        ArtifactCoverage {
            deletion_vector_generation,
            file_id: file.header().file_id,
            granule_ranges,
            schema_fingerprint: file.footer().schema_fingerprint,
        },
        ArtifactHeader {
            column_id,
            exact: false,
            false_positive_ppm,
            kind,
            projection_id: 0,
        },
        pages,
    ))
}

fn membership_keys(data: &crate::encoding::ColumnData) -> Option<Cow<'_, [u64]>> {
    Some(match data {
        crate::encoding::ColumnData::Strings(values) => Cow::Owned(
            values
                .iter()
                .flatten()
                .map(|value| super::stable_hash(value.as_bytes()))
                .collect(),
        ),
        crate::encoding::ColumnData::U64(values) => Cow::Borrowed(values.as_slice()),
        crate::encoding::ColumnData::I64(values) => {
            Cow::Owned(values.iter().filter_map(|value| u64::try_from(*value).ok()).collect())
        }
        _ => return None,
    })
}

fn stored_keys(data: &crate::encoding::ColumnData) -> Option<Vec<Option<u64>>> {
    Some(match data {
        crate::encoding::ColumnData::Strings(values) => values
            .iter()
            .map(|value| value.map(|value| super::stable_hash(value.as_bytes())))
            .collect(),
        crate::encoding::ColumnData::U64(values) => values.iter().map(|value| Some(*value)).collect(),
        crate::encoding::ColumnData::I64(values) => values.iter().map(|value| u64::try_from(*value).ok()).collect(),
        _ => return None,
    })
}

fn stored_rows(read: &ColumnRead, row_count: u32) -> Vec<u64> {
    if read.presence.is_empty() {
        (0..read.data.row_count() as u64).collect()
    } else {
        (0..u64::from(row_count))
            .filter(|row| {
                read.presence
                    .get((row / 8) as usize)
                    .is_some_and(|byte| byte & (1 << (row % 8)) != 0)
            })
            .collect()
    }
}

fn granule_bitmap(read: &ColumnRead, row_count: u32) -> Option<BitmapIndex> {
    let keys = stored_keys(&read.data)?;
    let rows = stored_rows(read, row_count);
    let mut rows_by_value: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for (key, row) in keys.into_iter().zip(rows) {
        if let Some(key) = key {
            rows_by_value.entry(key).or_default().push(row);
        }
    }
    let mut index = BitmapIndex::new();
    for (value, value_rows) in rows_by_value {
        index.insert(value, value_rows);
    }
    Some(index)
}

fn extend_granule_ranges(ranges: &mut Vec<(u32, u32)>, granule_id: u32) {
    match ranges.last_mut() {
        Some((_, last)) if *last + 1 == granule_id => *last = granule_id,
        _ => ranges.push((granule_id, granule_id)),
    }
}

const PATH_PRESENCE_FALSE_POSITIVE_PPM: u32 = 10_000;

const RANGE_FILTER_FALSE_POSITIVE_PPM: u32 = 10_000;

fn collect_payload_paths(prefix: &mut String, value: &VariantValue, out: &mut HashSet<u64>) {
    let VariantValue::Object(fields) = value else {
        return;
    };
    for (key, field_value) in fields {
        let parent_len = prefix.len();
        if parent_len > 0 {
            prefix.push('.');
        }
        prefix.push_str(key);
        collect_payload_paths(prefix, field_value, out);
        out.insert(hash_path(prefix));
        prefix.truncate(parent_len);
    }
}

pub fn build_path_presence_artifact(
    file: &crate::layout::reader::OeFile,
    deletion_vector_generation: u64,
) -> Result<IndexArtifact, FormatError> {
    let mut pages = Vec::new();
    let mut granule_ranges: Vec<(u32, u32)> = Vec::new();
    for granule in &file.footer().granules {
        let mut path_hashes: HashSet<u64> = HashSet::new();
        let mut path = String::new();
        for row in granule.first_row_ordinal..granule.first_row_ordinal + u64::from(granule.row_count) {
            if let PayloadRead::Value(value) = file.payload(row)? {
                collect_payload_paths(&mut path, &value, &mut path_hashes);
            }
        }
        let hash_count = path_hashes.len();
        extend_granule_ranges(&mut granule_ranges, granule.granule_id);
        pages.push(ArtifactPage {
            bytes: PathPresenceIndex::build_from_hashes(path_hashes, hash_count).encode(),
            first_granule: granule.granule_id,
            last_granule: granule.granule_id,
        });
    }
    Ok(IndexArtifact::new(
        ArtifactCoverage {
            deletion_vector_generation,
            file_id: file.header().file_id,
            granule_ranges,
            schema_fingerprint: file.footer().schema_fingerprint,
        },
        ArtifactHeader {
            column_id: crate::columns::column_ids::PAYLOAD_REF,
            exact: false,
            false_positive_ppm: PATH_PRESENCE_FALSE_POSITIVE_PPM,
            kind: ArtifactKind::PathPresence,
            projection_id: 0,
        },
        pages,
    ))
}

pub fn build_range_filter_artifact(
    file: &crate::layout::reader::OeFile,
    column_id: u32,
    deletion_vector_generation: u64,
) -> Result<IndexArtifact, FormatError> {
    let mut pages = Vec::new();
    let mut granule_ranges: Vec<(u32, u32)> = Vec::new();
    for granule in &file.footer().granules {
        let read = file.read_column(column_id, granule.granule_id)?;
        let Some(keys) = membership_keys(&read.data) else {
            continue;
        };
        extend_granule_ranges(&mut granule_ranges, granule.granule_id);
        pages.push(ArtifactPage {
            bytes: RangeFilter::build(&keys).encode(),
            first_granule: granule.granule_id,
            last_granule: granule.granule_id,
        });
    }
    Ok(IndexArtifact::new(
        ArtifactCoverage {
            deletion_vector_generation,
            file_id: file.header().file_id,
            granule_ranges,
            schema_fingerprint: file.footer().schema_fingerprint,
        },
        ArtifactHeader {
            column_id,
            exact: false,
            false_positive_ppm: RANGE_FILTER_FALSE_POSITIVE_PPM,
            kind: ArtifactKind::RangeFilter,
            projection_id: 0,
        },
        pages,
    ))
}

pub fn build_learned_position_artifact(
    file: &crate::layout::reader::OeFile,
    column_id: u32,
    deletion_vector_generation: u64,
    error_bound: u64,
) -> Result<IndexArtifact, FormatError> {
    let mut keys: Vec<i64> = Vec::new();
    let mut rows: Vec<u64> = Vec::new();
    let mut row_count = 0u64;
    let mut granule_ranges: Vec<(u32, u32)> = Vec::new();
    for granule in &file.footer().granules {
        let read = file.read_column(column_id, granule.granule_id)?;
        let crate::encoding::ColumnData::I64(values) = &read.data else {
            return Err(FormatError::Structural {
                rule: "a learned position artifact models an i64 column",
            });
        };
        extend_granule_ranges(&mut granule_ranges, granule.granule_id);
        keys.extend_from_slice(values);
        rows.extend(
            stored_rows(&read, granule.row_count)
                .into_iter()
                .map(|row| granule.first_row_ordinal + row),
        );
        row_count = row_count.max(granule.first_row_ordinal + u64::from(granule.row_count));
    }
    if keys.windows(2).any(|pair| matches!(pair, [prev, next] if next < prev)) {
        return Err(FormatError::Structural {
            rule: "a learned position artifact requires the column sorted ascending",
        });
    }
    let (first_granule, last_granule) = (
        granule_ranges.first().map_or(0, |range| range.0),
        granule_ranges.last().map_or(0, |range| range.1),
    );
    Ok(IndexArtifact::new(
        ArtifactCoverage {
            deletion_vector_generation,
            file_id: file.header().file_id,
            granule_ranges,
            schema_fingerprint: file.footer().schema_fingerprint,
        },
        ArtifactHeader {
            column_id,
            exact: false,
            false_positive_ppm: 0,
            kind: ArtifactKind::LearnedPosition,
            projection_id: 0,
        },
        vec![ArtifactPage {
            bytes: LearnedPositionIndex::build_at_rows(column_id, 0, &keys, &rows, row_count, error_bound).encode(),
            first_granule,
            last_granule,
        }],
    ))
}

pub fn build_bitmap_artifact(
    file: &crate::layout::reader::OeFile,
    column_id: u32,
    deletion_vector_generation: u64,
) -> Result<IndexArtifact, FormatError> {
    let mut pages = Vec::new();
    let mut granule_ranges: Vec<(u32, u32)> = Vec::new();
    for granule in &file.footer().granules {
        let read = file.read_column(column_id, granule.granule_id)?;
        let Some(index) = granule_bitmap(&read, granule.row_count) else {
            continue;
        };
        extend_granule_ranges(&mut granule_ranges, granule.granule_id);
        pages.push(ArtifactPage {
            bytes: index.encode(),
            first_granule: granule.granule_id,
            last_granule: granule.granule_id,
        });
    }
    Ok(IndexArtifact::new(
        ArtifactCoverage {
            deletion_vector_generation,
            file_id: file.header().file_id,
            granule_ranges,
            schema_fingerprint: file.footer().schema_fingerprint,
        },
        ArtifactHeader {
            column_id,
            exact: true,
            false_positive_ppm: 0,
            kind: ArtifactKind::Bitmap,
            projection_id: 0,
        },
        pages,
    ))
}

#[cfg(test)]
#[path = "test/artifact.rs"]
mod tests;
