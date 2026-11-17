use super::codec::{CodecParams, SegmentCodec};
use super::constant::*;
use super::error::VectorError;
use super::kernel::{TopK, for_each_row};
use super::model::*;
use super::seal::{FileKeys, WrappingKey};
use crate::file::bytes::{Reader, Writer};
use crate::indexes::bitmap::RoaringRangeBitmap;
use crate::layout::footer::ColumnKind;
use arrow_array::types::{Float16Type, Float32Type, UInt8Type};
use arrow_array::{ArrayRef, FixedSizeListArray, PrimitiveArray};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_schema::Field;
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use zeroize::Zeroizing;

#[derive(Debug, Clone)]
pub struct SegmentSpec {
    pub model: EmbeddingModel,
    pub producer_device: String,
    pub representations: Vec<VectorElement>,
    pub room_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SegmentInput {
    pub event_ids: Vec<String>,
    pub integer_columns: Vec<(String, Vec<i64>)>,
    pub string_columns: Vec<(String, Vec<String>)>,
    pub vectors: Vec<Vec<f32>>,
}

impl SegmentSpec {
    /// A spec with `default_representations` for `model`.
    pub fn new(model: EmbeddingModel, producer_device: impl Into<String>, room_id: Option<String>) -> Self {
        Self {
            representations: default_representations(&model),
            model,
            producer_device: producer_device.into(),
            room_id,
        }
    }
}

/// The writer's default representations: a coarse code plus `F32` for exact rerank. The coarse code is U1 (RaBitQ,
/// `d/8 + 8` bytes per row) from `RABITQ_MIN_DIMENSION` up and per-dimension I8 below it, where RaBitQ's bound is
/// too loose to prune and int8 is barely larger.
pub fn default_representations(model: &EmbeddingModel) -> Vec<VectorElement> {
    let coarse = if model.dimension >= RABITQ_MIN_DIMENSION {
        VectorElement::U1
    } else {
        VectorElement::I8
    };
    vec![coarse, VectorElement::F32]
}

pub fn representation_column_id(element: VectorElement) -> u32 {
    EMBEDDING_COLUMN_ID + 1 + u32::from(element.code())
}

pub fn write_segment(spec: &SegmentSpec, input: &SegmentInput, wrapping: &WrappingKey) -> Result<Vec<u8>, VectorError> {
    write_segment_sealed_by(spec, input, wrapping, &FileKeys::random()?)
}

pub(crate) fn write_segment_sealed_by(
    spec: &SegmentSpec,
    input: &SegmentInput,
    wrapping: &WrappingKey,
    keys: &FileKeys,
) -> Result<Vec<u8>, VectorError> {
    write_segment_inner(spec, input, wrapping, keys, None)
}

/// Token rows of multivector documents: `documents[d]` is document `d`'s first row, `documents[len - 1]` the row
/// count. `keys` must be a version 5 key set exactly when `documents` is given.
pub(crate) fn write_segment_inner(
    spec: &SegmentSpec,
    input: &SegmentInput,
    wrapping: &WrappingKey,
    keys: &FileKeys,
    documents: Option<Vec<u64>>,
) -> Result<Vec<u8>, VectorError> {
    validate(spec, input)?;
    if documents.is_some() != keys.multivector() {
        return Err(VectorError::InvalidInput {
            rule: "document offsets are written exactly in version 5 segments".to_owned(),
        });
    }
    let rows = input.event_ids.len();
    let granules: Vec<(usize, usize)> = if rows == 0 {
        vec![(0, 0)]
    } else {
        (0..rows.div_ceil(GRANULE_ROWS))
            .map(|granule| {
                let start = granule * GRANULE_ROWS;
                (start, (start + GRANULE_ROWS).min(rows))
            })
            .collect()
    };

    let params = keys
        .segment_codec()
        .then(|| CodecParams::build(&spec.model, &input.vectors, &spec.representations));
    let codec = match &params {
        Some(params) => SegmentCodec::v4(&spec.model, params.clone()),
        None => SegmentCodec::legacy(&spec.model),
    };
    let mut file = keys.header(wrapping)?;
    let mut columns = Vec::new();
    let mut block_index = 0u64;
    let mut seal_block = |file: &mut Vec<u8>, plaintext: &[u8]| -> Result<BlockRef, VectorError> {
        file.resize(file.len().next_multiple_of(BLOCK_ALIGNMENT), 0);
        let sealed = keys.seal(block_index, plaintext)?;
        let reference = BlockRef {
            index: block_index,
            offset: file.len() as u64,
            sealed_len: sealed.len() as u64,
        };
        block_index += 1;
        file.extend_from_slice(&sealed);
        Ok(reference)
    };

    let mut string_columns: Vec<(&str, &[String])> = vec![(EVENT_ID_COLUMN, &input.event_ids)];
    string_columns.extend(
        input
            .string_columns
            .iter()
            .map(|(name, values)| (name.as_str(), values.as_slice())),
    );
    for (position, (name, values)) in string_columns.iter().enumerate() {
        let mut blocks = Vec::with_capacity(granules.len());
        for (start, end) in &granules {
            let slice = values.get(*start..*end).unwrap_or_default();
            let plaintext = Zeroizing::new(encode_strings(slice));
            let stats = string_stats(slice);
            blocks.push((vec![seal_block(&mut file, &plaintext)?], stats));
        }
        columns.push(StoredColumn {
            blocks,
            column_id: position as u32,
            part_rows: GRANULE_ROWS,
            kind: ColumnKind::String,
            name: (*name).to_owned(),
        });
    }
    for (offset, (name, values)) in input.integer_columns.iter().enumerate() {
        let mut blocks = Vec::with_capacity(granules.len());
        for (start, end) in &granules {
            let slice = values.get(*start..*end).unwrap_or_default();
            let plaintext = Zeroizing::new(slice.iter().flat_map(|value| value.to_le_bytes()).collect::<Vec<u8>>());
            let stats = match (slice.iter().min(), slice.iter().max()) {
                (Some(min), Some(max)) => Some(ColumnStats::Integer { max: *max, min: *min }),
                _ => None,
            };
            blocks.push((vec![seal_block(&mut file, &plaintext)?], stats));
        }
        columns.push(StoredColumn {
            blocks,
            column_id: (string_columns.len() + offset) as u32,
            part_rows: GRANULE_ROWS,
            kind: ColumnKind::I64,
            name: name.clone(),
        });
    }
    let mut vector_columns = Vec::with_capacity(spec.representations.len());
    for element in &spec.representations {
        let meta = VectorColumnMeta {
            column_id: representation_column_id(*element),
            element: *element,
            model: spec.model.clone(),
            representation_of: EMBEDDING_COLUMN_ID,
        };
        let part_rows = if keys.vector_subblocks() {
            VECTOR_SUBBLOCK_ROWS
        } else {
            GRANULE_ROWS
        };
        let mut blocks = Vec::with_capacity(granules.len());
        for (start, end) in &granules {
            let mut parts = Vec::with_capacity(part_count(end - start, part_rows));
            for part in 0..part_count(end - start, part_rows) {
                let part_start = start + part * part_rows;
                let part_end = (part_start + part_rows).min(*end);
                let mut plaintext = Zeroizing::new(Vec::with_capacity((part_end - part_start) * meta.row_width()));
                for vector in input.vectors.get(part_start..part_end).unwrap_or_default() {
                    codec.encode_row(*element, vector, &mut plaintext);
                }
                parts.push(seal_block(&mut file, &plaintext)?);
            }
            blocks.push((parts, None));
        }
        columns.push(StoredColumn {
            blocks,
            column_id: meta.column_id,
            part_rows,
            kind: ColumnKind::Vector,
            name: format!("embedding_{:?}", element).to_lowercase(),
        });
        vector_columns.push(meta);
    }

    file.resize(file.len().next_multiple_of(BLOCK_ALIGNMENT), 0);
    let centroid = centroid(&spec.model, &input.vectors);
    let subblock_balls = keys
        .segment_codec()
        .then(|| subblock_balls(spec, &codec, &input.vectors, &granules));
    let content_hash = *blake3::hash(file.get(HEADER_LEN..).unwrap_or_default()).as_bytes();
    let footer = SegmentFooter {
        centroid,
        codec: params,
        columns,
        documents,
        granule_rows: granules.iter().map(|(start, end)| (end - start) as u32).collect(),
        manifest: SegmentManifest {
            content_hash,
            model: spec.model.clone(),
            producer_device: spec.producer_device.clone(),
            representations: spec.representations.clone(),
            row_count: rows as u64,
            window: SegmentWindow {
                first_event: input.event_ids.first().cloned().unwrap_or_default(),
                last_event: input.event_ids.last().cloned().unwrap_or_default(),
                room_id: spec.room_id.clone(),
            },
        },
        subblock_balls,
        vector_columns,
    };
    let plaintext = Zeroizing::new(footer.encode(keys.vector_subblocks()));
    debug_assert_eq!(footer.codec.is_some(), keys.segment_codec());
    let sealed = keys.seal(FOOTER_BLOCK_INDEX, &plaintext)?;
    let footer_offset = file.len() as u64;
    file.extend_from_slice(&sealed);
    file.extend_from_slice(&footer_offset.to_le_bytes());
    file.extend_from_slice(&(sealed.len() as u64).to_le_bytes());
    file.extend_from_slice(SEGMENT_MAGIC);
    Ok(file)
}

/// Per vector sub-block, the centre and radius of its rows as the finest representation stores them (so the ball
/// bounds the exact scores the rerank computes).
fn subblock_balls(
    spec: &SegmentSpec,
    codec: &SegmentCodec,
    vectors: &[Vec<f32>],
    granules: &[(usize, usize)],
) -> Vec<(Vec<f32>, f32)> {
    let dimension = spec.model.dimension as usize;
    let finest = spec
        .representations
        .iter()
        .copied()
        .max_by_key(|element| element.precision());
    let mut balls = Vec::new();
    for (start, end) in granules {
        for part in 0..part_count(end - start, VECTOR_SUBBLOCK_ROWS) {
            let part_start = start + part * VECTOR_SUBBLOCK_ROWS;
            let part_end = (part_start + VECTOR_SUBBLOCK_ROWS).min(*end);
            let rows: Vec<Vec<f32>> = vectors
                .get(part_start..part_end)
                .unwrap_or_default()
                .iter()
                .map(|vector| match finest {
                    Some(element @ (VectorElement::F16 | VectorElement::I8 | VectorElement::U1)) => {
                        let mut bytes = Vec::new();
                        codec.encode_row(element, vector, &mut bytes);
                        codec.decode_row(element, &bytes)
                    }
                    _ => vector.clone(),
                })
                .collect();
            balls.push(super::kernel::ball_of(dimension, rows.iter().map(Vec::as_slice)));
        }
    }
    balls
}

/// Rewrites a version 4+ segment's footer without the sub-block ball tail, as segments written before it had none.
#[cfg(test)]
pub(crate) fn strip_subblock_balls(bytes: &[u8], wrapping: &WrappingKey) -> Result<Vec<u8>, VectorError> {
    let reader = SegmentReader::open(bytes, wrapping)?;
    let mut footer = reader.footer.clone();
    footer.subblock_balls = None;
    let trailer = bytes.len().checked_sub(TRAILER_LEN).ok_or_else(truncated)?;
    let footer_offset = usize::try_from(u64::from_le_bytes(array8(
        bytes.get(trailer..trailer + 8).unwrap_or_default(),
    )))
    .map_err(|_| truncated())?;
    let sealed = reader
        .keys
        .seal(FOOTER_BLOCK_INDEX, &footer.encode(reader.keys.vector_subblocks()))?;
    let mut out = bytes.get(..footer_offset).ok_or_else(truncated)?.to_vec();
    out.extend_from_slice(&sealed);
    out.extend_from_slice(&(footer_offset as u64).to_le_bytes());
    out.extend_from_slice(&(sealed.len() as u64).to_le_bytes());
    out.extend_from_slice(SEGMENT_MAGIC);
    Ok(out)
}

#[cfg(test)]
fn array8(bytes: &[u8]) -> [u8; 8] {
    bytes.try_into().unwrap_or([0; 8])
}

#[cfg(test)]
impl<S> SegmentReader<S> {
    pub(crate) fn has_subblock_balls(&self) -> bool {
        self.footer.subblock_balls.is_some()
    }
}

pub fn rewrap_segment(bytes: &[u8], old: &WrappingKey, new: &WrappingKey) -> Result<Vec<u8>, VectorError> {
    let header = bytes
        .get(..HEADER_LEN)
        .ok_or(VectorError::Format(crate::error::FormatError::Truncated {
            what: "vector segment header",
        }))?;
    let keys = FileKeys::from_header(header, old)?;
    let mut out = keys.header(new)?;
    out.extend_from_slice(bytes.get(HEADER_LEN..).unwrap_or_default());
    Ok(out)
}

fn validate(spec: &SegmentSpec, input: &SegmentInput) -> Result<(), VectorError> {
    let invalid = |rule: String| Err(VectorError::InvalidInput { rule });
    let rows = input.event_ids.len();
    if spec.representations.is_empty() {
        return invalid("a segment stores at least one vector representation".to_owned());
    }
    let mut seen = Vec::new();
    for element in &spec.representations {
        if seen.contains(element) {
            return invalid(format!("representation {element:?} listed twice"));
        }
        seen.push(*element);
    }
    if input.vectors.len() != rows {
        return invalid(format!("{} vectors for {rows} event ids", input.vectors.len()));
    }
    let mut names = vec![EVENT_ID_COLUMN];
    for (name, len) in input
        .string_columns
        .iter()
        .map(|(name, values)| (name, values.len()))
        .chain(input.integer_columns.iter().map(|(name, values)| (name, values.len())))
    {
        if names.contains(&name.as_str()) {
            return invalid(format!("column {name} declared twice"));
        }
        names.push(name);
        if len != rows {
            return invalid(format!("column {name} has {len} values for {rows} rows"));
        }
    }
    let dimension = spec.model.dimension as usize;
    for (row, vector) in input.vectors.iter().enumerate() {
        if vector.len() != dimension {
            return invalid(format!("row {row} has width {} not {dimension}", vector.len()));
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return invalid(format!("row {row} holds a value that is not finite"));
        }
        if spec.model.metric == VectorMetric::Cosine {
            let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
            if (norm - 1.0).abs() > NORMALIZATION_TOLERANCE {
                return invalid(format!("row {row} is not normalized (length {norm}) for cosine"));
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockRef {
    index: u64,
    offset: u64,
    sealed_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnStats {
    Integer { max: i64, min: i64 },
    String { max: String, min: String },
}

/// A granule and the index of one sealed block within it.
pub(super) type SubBlock = (usize, usize);

/// One sealed block of a column with score bounds for its rows.
#[derive(Debug, Clone, Copy)]
struct PartRef {
    first_row: u64,
    granule: usize,
    lower: f32,
    part: usize,
    upper: f32,
}
/// Opened (decrypted) blocks by `(column id, granule, part)`, shared by the probe rounds of one segment search so
/// no block is decrypted twice.
pub(crate) type PartCache =
    std::cell::RefCell<std::collections::HashMap<(u32, usize, usize), std::rc::Rc<Zeroizing<Vec<u8>>>>>;

/// Sealed blocks covering `rows` rows of `part_rows` each: one for an empty granule, at least one otherwise.
fn part_count(rows: usize, part_rows: usize) -> usize {
    rows.div_ceil(part_rows.max(1)).max(1)
}

#[derive(Debug, Clone)]
pub(super) struct StoredColumn {
    /// Per granule: its sealed blocks in row order (one, except vector columns from version 3) and its stats.
    blocks: Vec<(Vec<BlockRef>, Option<ColumnStats>)>,
    column_id: u32,
    pub(super) part_rows: usize,
    kind: ColumnKind,
    name: String,
}

#[derive(Debug, Clone)]
struct SegmentFooter {
    centroid: Vec<f32>,
    /// Version 4 only.
    codec: Option<CodecParams>,
    columns: Vec<StoredColumn>,
    /// Version 5 only: first token row of each document, then the row count.
    documents: Option<Vec<u64>>,
    granule_rows: Vec<u32>,
    manifest: SegmentManifest,
    /// Optional footer tail (version 4 and later): per vector sub-block, in row order, the centre and radius of
    /// its finest-representation rows. Lets a scan skip sub-blocks whose score bound cannot reach the k-th score.
    subblock_balls: Option<Vec<(Vec<f32>, f32)>>,
    vector_columns: Vec<VectorColumnMeta>,
}

/// Footer tail tag for `SegmentFooter::subblock_balls`.
const FOOTER_TAIL_SUBBLOCK_BALLS: u8 = 1;

impl SegmentFooter {
    fn encode(&self, vector_subblocks: bool) -> Vec<u8> {
        let mut out = Writer::new();
        self.manifest.encode_into(&mut out);
        out.put_u32(self.granule_rows.len() as u32);
        self.granule_rows.iter().for_each(|rows| out.put_u32(*rows));
        out.put_u32(self.columns.len() as u32);
        for column in &self.columns {
            out.put_u32(column.column_id);
            out.put_u8(column.kind as u8);
            put_str(&mut out, &column.name);
            for (parts, stats) in &column.blocks {
                if vector_subblocks && column.kind == ColumnKind::Vector {
                    out.put_u32(parts.len() as u32);
                }
                for block in parts {
                    out.put_u64(block.index);
                    out.put_u64(block.offset);
                    out.put_u64(block.sealed_len);
                }
                match stats {
                    None => out.put_u8(0),
                    Some(ColumnStats::Integer { max, min }) => {
                        out.put_u8(1);
                        out.put_i64(*min);
                        out.put_i64(*max);
                    }
                    Some(ColumnStats::String { max, min }) => {
                        out.put_u8(2);
                        put_str(&mut out, min);
                        put_str(&mut out, max);
                    }
                }
            }
        }
        out.put_u32(self.vector_columns.len() as u32);
        self.vector_columns.iter().for_each(|meta| meta.encode_into(&mut out));
        out.put_u32(self.centroid.len() as u32);
        self.centroid
            .iter()
            .for_each(|value| out.put_slice(&value.to_le_bytes()));
        if let Some(codec) = &self.codec {
            out.put_u64(codec.seed);
            out.put_u32(codec.centre.len() as u32);
            codec
                .centre
                .iter()
                .for_each(|value| out.put_slice(&value.to_le_bytes()));
            out.put_slice(&codec.radius.to_le_bytes());
            out.put_u32(codec.i8_dimensions.len() as u32);
            for (scale, offset) in &codec.i8_dimensions {
                out.put_slice(&scale.to_le_bytes());
                out.put_slice(&offset.to_le_bytes());
            }
        }
        if let Some(documents) = &self.documents {
            out.put_u64(documents.len().saturating_sub(1) as u64);
            documents.iter().for_each(|offset| out.put_u64(*offset));
        }
        if let Some(balls) = &self.subblock_balls {
            out.put_u8(FOOTER_TAIL_SUBBLOCK_BALLS);
            out.put_u32(balls.len() as u32);
            for (centre, radius) in balls {
                centre.iter().for_each(|value| out.put_slice(&value.to_le_bytes()));
                out.put_slice(&radius.to_le_bytes());
            }
        }
        out.into_bytes()
    }

    fn decode(
        bytes: &[u8],
        vector_subblocks: bool,
        segment_codec: bool,
        multivector: bool,
    ) -> Result<Self, VectorError> {
        let mut reader = Reader::new(bytes);
        let manifest = SegmentManifest::decode_from(&mut reader)?;
        let granule_count = reader.u32("granule count")? as usize;
        let granule_rows = reader.u32_vec(granule_count, "granule rows")?;
        let column_count = reader.u32("column count")? as usize;
        let mut columns = Vec::with_capacity(reader.capacity_hint(column_count, 9));
        for _ in 0..column_count {
            let column_id = reader.u32("column id")?;
            let kind = ColumnKind::from_u8(reader.u8("column kind")?)?;
            let name = take_str(&mut reader, "column name")?;
            let split = vector_subblocks && kind == ColumnKind::Vector;
            let part_rows = if split { VECTOR_SUBBLOCK_ROWS } else { GRANULE_ROWS };
            let mut blocks = Vec::with_capacity(reader.capacity_hint(granule_count, 25));
            for granule in 0..granule_count {
                let count = if split {
                    let count = reader.u32("sub-block count")? as usize;
                    let rows = granule_rows.get(granule).map_or(0, |rows| *rows as usize);
                    if count != part_count(rows, part_rows) {
                        return Err(VectorError::Format(crate::error::FormatError::Structural {
                            rule: "vector sub-blocks cover their granule's rows",
                        }));
                    }
                    count
                } else {
                    1
                };
                let mut parts = Vec::with_capacity(reader.capacity_hint(count, 24));
                for _ in 0..count {
                    parts.push(BlockRef {
                        index: reader.u64("block index")?,
                        offset: reader.u64("block offset")?,
                        sealed_len: reader.u64("block length")?,
                    });
                }
                let stats = match reader.u8("stats tag")? {
                    0 => None,
                    1 => Some(ColumnStats::Integer {
                        min: reader.i64("stats min")?,
                        max: reader.i64("stats max")?,
                    }),
                    2 => Some(ColumnStats::String {
                        min: take_str(&mut reader, "stats min")?,
                        max: take_str(&mut reader, "stats max")?,
                    }),
                    _ => {
                        return Err(VectorError::Format(crate::error::FormatError::Structural {
                            rule: "unknown vector segment stats tag",
                        }));
                    }
                };
                blocks.push((parts, stats));
            }
            columns.push(StoredColumn {
                blocks,
                column_id,
                part_rows,
                kind,
                name,
            });
        }
        let meta_count = reader.u32("vector column count")? as usize;
        let mut vector_columns = Vec::with_capacity(reader.capacity_hint(meta_count, 46));
        for _ in 0..meta_count {
            vector_columns.push(VectorColumnMeta::decode_from(&mut reader)?);
        }
        let mut centroid = Vec::new();
        if segment_codec || reader.remaining() != 0 {
            let len = reader.u32("centroid length")? as usize;
            centroid.reserve(reader.capacity_hint(len, 4));
            for _ in 0..len {
                centroid.push(f32::from_le_bytes(array4(reader.take(4, "centroid value")?)));
            }
            if len != 0 && len != manifest.model.dimension as usize {
                return Err(VectorError::Format(crate::error::FormatError::Structural {
                    rule: "vector segment centroid has the model's dimension",
                }));
            }
        }
        let codec = if segment_codec {
            let dimension = manifest.model.dimension as usize;
            let structural = |rule| VectorError::Format(crate::error::FormatError::Structural { rule });
            let seed = reader.u64("rabitq seed")?;
            let len = reader.u32("rabitq centre length")? as usize;
            if len != 0 && len != dimension {
                return Err(structural("rabitq centre has the model's dimension"));
            }
            let mut centre = Vec::with_capacity(reader.capacity_hint(len, 4));
            for _ in 0..len {
                centre.push(f32::from_le_bytes(array4(reader.take(4, "rabitq centre value")?)));
            }
            let radius = f32::from_le_bytes(array4(reader.take(4, "segment radius")?));
            let len = reader.u32("int8 dimension count")? as usize;
            if len != 0 && len != dimension {
                return Err(structural("int8 dimension parameters have the model's dimension"));
            }
            let mut i8_dimensions = Vec::with_capacity(reader.capacity_hint(len, 8));
            for _ in 0..len {
                let scale = f32::from_le_bytes(array4(reader.take(4, "int8 scale")?));
                let offset = f32::from_le_bytes(array4(reader.take(4, "int8 offset")?));
                i8_dimensions.push((scale, offset));
            }
            Some(CodecParams {
                centre,
                i8_dimensions,
                radius,
                seed,
            })
        } else {
            None
        };
        let documents = if multivector {
            let count = reader.u64("document count")?;
            let count = usize::try_from(count).map_err(|_| truncated())?;
            let mut offsets = Vec::with_capacity(reader.capacity_hint(count.saturating_add(1), 8));
            for _ in 0..=count {
                offsets.push(reader.u64("document offset")?);
            }
            let ordered = offsets.windows(2).all(|pair| pair.first() < pair.get(1));
            if offsets.first() != Some(&0) || offsets.last() != Some(&manifest.row_count) || !ordered {
                return Err(VectorError::Format(crate::error::FormatError::Structural {
                    rule: "multivector document offsets start at 0, increase and end at the row count",
                }));
            }
            Some(offsets)
        } else {
            None
        };
        let mut subblock_balls = None;
        if segment_codec && reader.remaining() != 0 {
            let structural = |rule| VectorError::Format(crate::error::FormatError::Structural { rule });
            if reader.u8("footer tail tag")? != FOOTER_TAIL_SUBBLOCK_BALLS {
                return Err(structural("unknown vector segment footer tail"));
            }
            let count = reader.u32("sub-block ball count")? as usize;
            let expected: usize = granule_rows
                .iter()
                .map(|rows| part_count(*rows as usize, VECTOR_SUBBLOCK_ROWS))
                .sum();
            if count != expected {
                return Err(structural("one sub-block ball per vector sub-block"));
            }
            let dimension = manifest.model.dimension as usize;
            let mut balls = Vec::with_capacity(reader.capacity_hint(count, 4 * dimension + 4));
            for _ in 0..count {
                let mut centre = Vec::with_capacity(dimension);
                for _ in 0..dimension {
                    centre.push(f32::from_le_bytes(array4(reader.take(4, "sub-block centre")?)));
                }
                let radius = f32::from_le_bytes(array4(reader.take(4, "sub-block radius")?));
                if !radius.is_finite() || radius < 0.0 || centre.iter().any(|value| !value.is_finite()) {
                    return Err(structural("sub-block ball is finite"));
                }
                balls.push((centre, radius));
            }
            subblock_balls = Some(balls);
        }
        if reader.remaining() != 0 {
            return Err(VectorError::Format(crate::error::FormatError::Structural {
                rule: "vector segment footer has trailing bytes",
            }));
        }
        if codec.as_ref().is_some_and(|codec| {
            codec.i8_dimensions.is_empty() && vector_columns.iter().any(|meta| meta.element == VectorElement::I8)
        }) {
            return Err(VectorError::Format(crate::error::FormatError::Structural {
                rule: "a version 4 int8 column has per-dimension parameters",
            }));
        }
        let footer = SegmentFooter {
            centroid,
            codec,
            columns,
            documents,
            granule_rows,
            manifest,
            subblock_balls,
            vector_columns,
        };
        if footer.granule_rows.iter().map(|rows| u64::from(*rows)).sum::<u64>() != footer.manifest.row_count
            || !footer.columns.iter().any(|column| column.name == EVENT_ID_COLUMN)
        {
            return Err(VectorError::Format(crate::error::FormatError::Structural {
                rule: "vector segment footer rows and event id column agree",
            }));
        }
        Ok(footer)
    }
}

pub trait SegmentSource {
    fn byte_len(&self) -> Result<u64, VectorError>;

    fn read_range(&self, offset: u64, len: usize) -> Result<Vec<u8>, VectorError>;

    fn borrow_range(&self, _offset: u64, _len: usize) -> Option<&[u8]> {
        None
    }
}

impl<T: AsRef<[u8]>> SegmentSource for T {
    fn byte_len(&self) -> Result<u64, VectorError> {
        Ok(self.as_ref().len() as u64)
    }

    fn read_range(&self, offset: u64, len: usize) -> Result<Vec<u8>, VectorError> {
        let start = usize::try_from(offset).map_err(|_| truncated())?;
        self.as_ref()
            .get(start..start.checked_add(len).ok_or_else(truncated)?)
            .map(<[u8]>::to_vec)
            .ok_or_else(truncated)
    }

    fn borrow_range(&self, offset: u64, len: usize) -> Option<&[u8]> {
        let start = usize::try_from(offset).ok()?;
        self.as_ref().get(start..start.checked_add(len)?)
    }
}

/// The query side of `SegmentReader::search_bounded`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BoundedSearch<'a> {
    pub(crate) cap: Option<usize>,
    pub(crate) filter: Option<&'a RoaringRangeBitmap>,
    pub(crate) k: usize,
    pub(crate) model: &'a EmbeddingModel,
    pub(crate) query: &'a [f32],
    /// Rows scoring below this cannot enter the caller's result (`-∞` when nothing is known yet).
    pub(crate) threshold: f32,
    /// Range search: rows scoring above this are excluded (`+∞` for a plain top-k search).
    pub(crate) ceiling: f32,
    /// Whether the search has score bounds (forces an exact fine scan for codes without bounds).
    pub(crate) ranged: bool,
    /// Blocks already opened by earlier calls for the same query (partition probe rounds).
    pub(crate) cache: Option<&'a PartCache>,
}

fn check_query_width(meta: &VectorColumnMeta, query: &[f32]) -> Result<(), VectorError> {
    if query.len() != meta.model.dimension as usize {
        return Err(VectorError::InvalidInput {
            rule: format!("query width {} not {}", query.len(), meta.model.dimension),
        });
    }
    Ok(())
}

fn truncated() -> VectorError {
    VectorError::Format(crate::error::FormatError::Truncated { what: "vector segment" })
}

pub struct SegmentReader<S> {
    blocks_opened: AtomicU64,
    codec: SegmentCodec,
    footer: SegmentFooter,
    keys: FileKeys,
    source: S,
}

impl<S: SegmentSource> SegmentReader<S> {
    pub fn open(source: S, wrapping: &WrappingKey) -> Result<Self, VectorError> {
        let len = source.byte_len()?;
        if len < (HEADER_LEN + TRAILER_LEN) as u64 {
            return Err(truncated());
        }
        let keys = FileKeys::from_header(&source.read_range(0, HEADER_LEN)?, wrapping)?;
        let trailer = source.read_range(len - TRAILER_LEN as u64, TRAILER_LEN)?;
        if trailer.get(16..20) != Some(SEGMENT_MAGIC.as_slice()) {
            return Err(VectorError::Format(crate::error::FormatError::BadMagic {
                expected: "OEFV",
            }));
        }
        let mut reader = Reader::new(&trailer);
        let footer_offset = reader.u64("footer offset")?;
        let footer_len = reader.u64("footer length")?;
        if footer_offset.checked_add(footer_len) != Some(len - TRAILER_LEN as u64) {
            return Err(truncated());
        }
        let sealed = source.read_range(footer_offset, usize::try_from(footer_len).map_err(|_| truncated())?)?;
        let footer = SegmentFooter::decode(
            &keys.open(FOOTER_BLOCK_INDEX, &sealed)?,
            keys.vector_subblocks(),
            keys.segment_codec(),
            keys.multivector(),
        )?;
        let codec = match &footer.codec {
            Some(params) => SegmentCodec::v4(&footer.manifest.model, params.clone()),
            None => SegmentCodec::legacy(&footer.manifest.model),
        };
        Ok(SegmentReader {
            blocks_opened: AtomicU64::new(0),
            codec,
            footer,
            keys,
            source,
        })
    }

    /// The segment layout version from the header.
    pub fn version(&self) -> u16 {
        self.keys.version
    }

    /// A query prepared for one representation of this segment.
    pub(super) fn coded_query(&self, element: VectorElement, query: &[f32]) -> super::codec::CodedQuery {
        self.codec.query(element, query)
    }

    /// Lower and upper bounds on any row's exact score (`±∞` without version 4 parameters).
    pub(super) fn segment_bounds(&self, query: &[f32]) -> (f32, f32) {
        self.codec.segment_bounds(query)
    }

    #[cfg(test)]
    pub(crate) fn codec(&self) -> &SegmentCodec {
        &self.codec
    }

    /// Decodes one stored row of `element` as this segment encodes it (for U1, the RaBitQ reconstruction).
    pub fn decode_vector_row(&self, element: VectorElement, row: &[u8]) -> Vec<f32> {
        self.codec.decode_row(element, row)
    }

    pub fn manifest(&self) -> &SegmentManifest {
        &self.footer.manifest
    }

    pub fn row_count(&self) -> u64 {
        self.footer.manifest.row_count
    }

    pub fn granule_count(&self) -> usize {
        self.footer.granule_rows.len()
    }

    pub fn granule_rows(&self, granule: usize) -> usize {
        self.footer.granule_rows.get(granule).map_or(0, |rows| *rows as usize)
    }

    pub fn granule_first_row(&self, granule: usize) -> u64 {
        self.footer
            .granule_rows
            .iter()
            .take(granule)
            .map(|rows| u64::from(*rows))
            .sum()
    }

    /// Multivector (version 5) segments: the first token row of each document, then the row count.
    pub fn document_offsets(&self) -> Option<&[u64]> {
        self.footer.documents.as_deref()
    }

    /// Documents in a multivector segment (0 otherwise).
    pub fn document_count(&self) -> u64 {
        self.footer
            .documents
            .as_ref()
            .map_or(0, |offsets| offsets.len().saturating_sub(1) as u64)
    }

    /// The document holding token `row` of a multivector segment.
    pub fn document_of(&self, row: u64) -> Option<u64> {
        let offsets = self.footer.documents.as_ref()?;
        (row < self.row_count()).then(|| offsets.partition_point(|first| *first <= row).saturating_sub(1) as u64)
    }

    /// The token rows of `document` in a multivector segment.
    pub fn document_rows(&self, document: u64) -> Option<std::ops::Range<u64>> {
        let offsets = self.footer.documents.as_ref()?;
        let index = usize::try_from(document).ok()?;
        Some(*offsets.get(index)?..*offsets.get(index.checked_add(1)?)?)
    }

    pub fn centroid(&self) -> Option<&[f32]> {
        (!self.footer.centroid.is_empty()).then_some(self.footer.centroid.as_slice())
    }

    pub fn vector_columns(&self) -> &[VectorColumnMeta] {
        &self.footer.vector_columns
    }

    pub fn representations(&self, embedding_column: u32) -> Vec<&VectorColumnMeta> {
        let mut found: Vec<&VectorColumnMeta> = self
            .footer
            .vector_columns
            .iter()
            .filter(|meta| meta.representation_of == embedding_column)
            .collect();
        found.sort_by_key(|meta| meta.element.precision());
        found
    }

    pub fn column_stats(&self, name: &str, granule: usize) -> Option<&ColumnStats> {
        self.column(name).ok()?.blocks.get(granule)?.1.as_ref()
    }

    pub fn blocks_opened(&self) -> u64 {
        self.blocks_opened.load(Ordering::Relaxed)
    }

    pub fn verify_content(&self) -> Result<(), VectorError> {
        let trailer = self
            .source
            .read_range(self.source.byte_len()? - TRAILER_LEN as u64, 8)?;
        let footer_offset = u64::from_le_bytes(trailer.try_into().unwrap_or([0; 8]));
        let body = self.source.read_range(
            HEADER_LEN as u64,
            usize::try_from(footer_offset.saturating_sub(HEADER_LEN as u64)).map_err(|_| truncated())?,
        )?;
        if *blake3::hash(&body).as_bytes() != self.footer.manifest.content_hash {
            return Err(VectorError::Authentication { what: "content hash" });
        }
        Ok(())
    }

    pub fn strings(&self, name: &str, granule: usize) -> Result<Vec<String>, VectorError> {
        let column = self.typed_column(name, ColumnKind::String)?;
        decode_strings(&self.open_block(column, granule)?)
    }

    /// Calls `visit(local_row, value_bytes)` for each string of a granule without allocating per value.
    pub(crate) fn for_each_string(
        &self,
        name: &str,
        granule: usize,
        mut visit: impl FnMut(usize, &[u8]),
    ) -> Result<(), VectorError> {
        let column = self.typed_column(name, ColumnKind::String)?;
        let plaintext = self.open_block(column, granule)?;
        let mut reader = Reader::new(&plaintext);
        let count = reader.u32("string count")? as usize;
        for local in 0..count {
            let len = reader.u32("string value")? as usize;
            visit(local, reader.take(len, "string value")?);
        }
        Ok(())
    }

    pub fn integers(&self, name: &str, granule: usize) -> Result<Vec<i64>, VectorError> {
        let column = self.typed_column(name, ColumnKind::I64)?;
        let plaintext = self.open_block(column, granule)?;
        Ok(plaintext
            .chunks_exact(8)
            .map(|bytes| i64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])))
            .collect())
    }

    pub fn vector_granule(
        &self,
        column_id: u32,
        granule: usize,
        model: &EmbeddingModel,
    ) -> Result<FixedSizeListArray, VectorError> {
        let meta = self.vector_meta(column_id, model)?;
        let column = self.column_by_id(column_id)?;
        let plaintext = self.open_block(column, granule)?;
        let buffer = Buffer::from(plaintext.as_slice());
        let (values, size): (ArrayRef, usize) = match meta.element {
            VectorElement::F32 => (
                Arc::new(PrimitiveArray::<Float32Type>::new(
                    ScalarBuffer::new(buffer, 0, plaintext.len() / 4),
                    None,
                )),
                meta.model.dimension as usize,
            ),
            VectorElement::F16 => (
                Arc::new(PrimitiveArray::<Float16Type>::new(
                    ScalarBuffer::new(buffer, 0, plaintext.len() / 2),
                    None,
                )),
                meta.model.dimension as usize,
            ),
            VectorElement::I8 | VectorElement::U1 => (
                Arc::new(PrimitiveArray::<UInt8Type>::new(
                    ScalarBuffer::new(buffer, 0, plaintext.len()),
                    None,
                )),
                meta.row_width(),
            ),
        };
        let field = Arc::new(Field::new("item", values.data_type().clone(), false));
        let size = i32::try_from(size).map_err(|_| VectorError::InvalidInput {
            rule: "vector row too wide for arrow".to_owned(),
        })?;
        FixedSizeListArray::try_new(field, size, values, None).map_err(|error| VectorError::InvalidInput {
            rule: error.to_string(),
        })
    }

    pub fn vector_row(&self, column_id: u32, row: u64, model: &EmbeddingModel) -> Result<Vec<u8>, VectorError> {
        let meta = self.vector_meta(column_id, model)?;
        let width = meta.row_width();
        let (granule, local) = self.locate(row)?;
        let column = self.column_by_id(column_id)?;
        let plaintext = self.open_part(column, granule, local / column.part_rows)?;
        let local = local % column.part_rows;
        plaintext
            .get(local * width..(local + 1) * width)
            .map(<[u8]>::to_vec)
            .ok_or_else(truncated)
    }

    pub fn vector_prefix(
        &self,
        column_id: u32,
        row: u64,
        dimensions: u32,
        model: &EmbeddingModel,
    ) -> Result<Vec<f32>, VectorError> {
        let meta = self.vector_meta(column_id, model)?;
        if dimensions > meta.model.dimension {
            return Err(VectorError::InvalidInput {
                rule: format!("prefix of {dimensions} past the model's {}", meta.model.dimension),
            });
        }
        let element_bytes = match meta.element {
            VectorElement::F16 => 2,
            VectorElement::F32 => 4,
            VectorElement::I8 | VectorElement::U1 => {
                return Err(VectorError::InvalidInput {
                    rule: format!("{:?} rows have no prefix read", meta.element),
                });
            }
        };
        let width = meta.row_width();
        let (granule, local) = self.locate(row)?;
        let column = self.column_by_id(column_id)?;
        let plaintext = self.open_part(column, granule, local / column.part_rows)?;
        let start = (local % column.part_rows) * width;
        let prefix = plaintext
            .get(start..start + dimensions as usize * element_bytes)
            .ok_or_else(truncated)?;
        Ok(super::quantize::decode_row(meta.element, dimensions, prefix))
    }

    pub fn read_all(&self) -> Result<SegmentInput, VectorError> {
        self.read_live(None)
    }

    pub fn read_live(&self, deletes: Option<&super::deletes::SegmentDeletes>) -> Result<SegmentInput, VectorError> {
        let mut input = SegmentInput::default();
        let fine = *self
            .representations(EMBEDDING_COLUMN_ID)
            .last()
            .ok_or_else(|| VectorError::MissingColumn {
                name: "embedding".to_owned(),
            })?;
        let width = fine.row_width();
        let fine_column = self.column_by_id(fine.column_id)?;
        for column in &self.footer.columns {
            match column.kind {
                ColumnKind::String if column.name != EVENT_ID_COLUMN => {
                    input.string_columns.push((column.name.clone(), Vec::new()))
                }
                ColumnKind::I64 => input.integer_columns.push((column.name.clone(), Vec::new())),
                _ => {}
            }
        }
        let deletes = deletes.filter(|deletes| deletes.deleted_count() > 0);
        for granule in 0..self.granule_count() {
            let first = self.granule_first_row(granule);
            let live = |local: usize| deletes.is_none_or(|deletes| !deletes.is_deleted(first + local as u64));
            let keep_live =
                |values: Vec<String>| values.into_iter().enumerate().filter(|(l, _)| live(*l)).map(|(_, v)| v);
            input
                .event_ids
                .extend(keep_live(self.strings(EVENT_ID_COLUMN, granule)?));
            for (name, values) in &mut input.string_columns {
                values.extend(keep_live(self.strings(name, granule)?));
            }
            for (name, values) in &mut input.integer_columns {
                let integers = self.integers(name, granule)?;
                values.extend(
                    integers
                        .into_iter()
                        .enumerate()
                        .filter(|(l, _)| live(*l))
                        .map(|(_, v)| v),
                );
            }
            let plaintext = self.open_block(fine_column, granule)?;
            for (local, row) in plaintext.chunks_exact(width.max(1)).enumerate() {
                if !live(local) {
                    continue;
                }
                let mut vector = self.codec.decode_row(fine.element, row);
                if fine.model.metric == VectorMetric::Cosine {
                    normalize(&mut vector);
                }
                input.vectors.push(vector);
            }
        }
        Ok(input)
    }

    pub fn top_k(
        &self,
        column_id: u32,
        query: &[f32],
        model: &EmbeddingModel,
        filter: Option<&RoaringRangeBitmap>,
        k: usize,
    ) -> Result<Vec<ScoredRow>, VectorError> {
        let meta = self.vector_meta(column_id, model)?;
        check_query_width(meta, query)?;
        let prepared = self.codec.query(meta.element, query);
        let width = meta.row_width();
        let mut best = TopK::new(k);
        self.scan_parts(column_id, filter, |first_row, plaintext| {
            for_each_row(width, plaintext, first_row, filter, |row, bytes| {
                best.offer(ScoredRow {
                    row,
                    score: prepared.score(bytes),
                });
            });
        })?;
        Ok(best.into_sorted())
    }

    /// Calls `visit(first_row, plaintext)` for each sealed block of a column that `filter` touches, in row order.
    pub(super) fn scan_parts(
        &self,
        column_id: u32,
        filter: Option<&RoaringRangeBitmap>,
        mut visit: impl FnMut(u64, &[u8]),
    ) -> Result<(), VectorError> {
        let column = self.column_by_id(column_id)?;
        for part in self.parts_touched(column, filter) {
            visit(part.first_row, &self.open_part(column, part.granule, part.part)?);
        }
        Ok(())
    }

    /// `scan_parts` through a cache of opened blocks, so repeated passes decrypt each block once.
    pub(super) fn scan_parts_cached(
        &self,
        column_id: u32,
        filter: Option<&RoaringRangeBitmap>,
        cache: &PartCache,
        mut visit: impl FnMut(u64, &[u8]),
    ) -> Result<(), VectorError> {
        let column = self.column_by_id(column_id)?;
        for part in self.parts_touched(column, filter) {
            visit(
                part.first_row,
                &self.cached_part(column, part.granule, part.part, Some(cache))?,
            );
        }
        Ok(())
    }

    /// How many sealed blocks of a column `filter` touches.
    pub(super) fn parts_touched_count(
        &self,
        column_id: u32,
        filter: &RoaringRangeBitmap,
    ) -> Result<usize, VectorError> {
        Ok(self.parts_touched(self.column_by_id(column_id)?, Some(filter)).len())
    }

    /// The blocks of `column` that `filter` touches, in row order, with unbounded scores.
    fn parts_touched(&self, column: &StoredColumn, filter: Option<&RoaringRangeBitmap>) -> Vec<PartRef> {
        let part_rows = column.part_rows as u64;
        let mut parts = Vec::new();
        let mut granule_first = 0u64;
        for granule in 0..self.granule_count() {
            let granule_end = granule_first + self.granule_rows(granule) as u64;
            let mut first_row = granule_first;
            let mut part = 0;
            while first_row < granule_end {
                let rows = part_rows.min(granule_end - first_row);
                let wanted = filter.is_none_or(|filter| {
                    let ranges = filter.ranges();
                    ranges
                        .get(ranges.partition_point(|range| range.end <= first_row))
                        .is_some_and(|range| range.start < first_row + rows)
                });
                if wanted {
                    parts.push(PartRef {
                        first_row,
                        granule,
                        lower: f32::NEG_INFINITY,
                        part,
                        upper: f32::INFINITY,
                    });
                }
                first_row += rows;
                part += 1;
            }
            granule_first = granule_end;
        }
        parts
    }

    /// `open_part` through an optional cache of opened blocks.
    fn cached_part(
        &self,
        column: &StoredColumn,
        granule: usize,
        part: usize,
        cache: Option<&PartCache>,
    ) -> Result<std::rc::Rc<Zeroizing<Vec<u8>>>, VectorError> {
        let Some(cache) = cache else {
            return Ok(std::rc::Rc::new(self.open_part(column, granule, part)?));
        };
        let key = (column.column_id, granule, part);
        if let Some(found) = cache.borrow().get(&key) {
            return Ok(found.clone());
        }
        let opened = std::rc::Rc::new(self.open_part(column, granule, part)?);
        cache.borrow_mut().insert(key, opened.clone());
        Ok(opened)
    }

    /// Two-stage search of one segment: scan `coarse`, then score candidates exactly with `fine`.
    ///
    /// With a bounded coarse code (version 4 U1 or I8) a row is a candidate only while its upper bound reaches
    /// both `threshold` (the caller's running k-th best exact score from other segments) and the k-th best lower
    /// bound seen so far. Candidates are reranked in decreasing upper bound until the next upper bound falls below
    /// the k-th best exact score, so the rerank set follows the bounds rather than a fixed `k × oversample`.
    /// `cap`, when set, stops the rerank after a run of rows that did not enter the top k (`k × cap` rows, or
    /// `RERANK_PATIENCE_GROWTH` × the rows reranked up to the last improvement, whichever is longer). A whole
    /// segment whose score bound is below
    /// `threshold` is skipped without opening a block. Older (unbounded) codes keep the fixed oversample rerank.
    pub(crate) fn search_bounded(
        &self,
        coarse: u32,
        fine: u32,
        search: BoundedSearch<'_>,
        stats: &mut super::search::SearchStats,
    ) -> Result<Vec<ScoredRow>, VectorError> {
        let BoundedSearch {
            cap,
            filter,
            k,
            model,
            query,
            threshold,
            ceiling,
            ranged,
            cache,
        } = search;
        let in_range = |score: f32| score >= threshold && score <= ceiling;
        let coarse_meta = self.vector_meta(coarse, model)?;
        let fine_meta = self.vector_meta(fine, model)?;
        check_query_width(coarse_meta, query)?;
        let coarse_query = self.codec.query(coarse_meta.element, query);
        if coarse == fine || (ranged && !coarse_query.bounded()) {
            stats.rows_scanned += filter.map_or(self.row_count(), RoaringRangeBitmap::count);
            if !ranged {
                return self.top_k(fine, query, model, filter, k);
            }
            let fine_query = self.codec.query(fine_meta.element, query);
            let width = fine_meta.row_width();
            let mut best = TopK::new(k);
            self.scan_parts(fine, filter, |first_row, plaintext| {
                for_each_row(width, plaintext, first_row, filter, |row, bytes| {
                    let score = fine_query.score(bytes);
                    if in_range(score) {
                        best.offer(ScoredRow { row, score });
                    }
                });
            })?;
            return Ok(best.into_sorted());
        }
        if !coarse_query.bounded() {
            let wanted = k.saturating_mul(cap.unwrap_or(DEFAULT_OVERSAMPLE).max(1));
            stats.rows_scanned += filter.map_or(self.row_count(), RoaringRangeBitmap::count);
            let candidates = self.top_k(coarse, query, model, filter, wanted)?;
            stats.rows_reranked += candidates.len() as u64;
            let rows: Vec<u64> = candidates.iter().map(|row| row.row).collect();
            return self.rescore(fine, query, model, &rows);
        }
        let (segment_lower, segment_upper) = self.codec.segment_bounds(query);
        if segment_upper < threshold || segment_lower > ceiling {
            stats.segments_skipped += 1;
            return Ok(Vec::new());
        }

        let local_cache = PartCache::default();
        let shared = cache;
        let cache = shared.unwrap_or(&local_cache);
        let fine_query = self.codec.query(fine_meta.element, query);
        let fine_width = fine_meta.row_width();
        let fine_column = self.column_by_id(fine)?;
        let mut seeded: std::collections::HashMap<u64, f32> = std::collections::HashMap::new();
        let width = coarse_meta.row_width();
        let mut lowers = TopK::new(k);
        let mut bar = threshold;
        let mut candidates: Vec<(u64, f32)> = Vec::new();
        let mut prune_at = k.saturating_mul(4).saturating_add(1024);
        // Visit coarse sub-blocks best ball bound first (row order without balls), skipping those whose ball
        // cannot reach the bar: on clustered rows the bar rises within the first few sub-blocks.
        let coarse_column = self.column_by_id(coarse)?;
        let mut parts = self.parts_touched(coarse_column, filter);
        let balls = self
            .footer
            .subblock_balls
            .as_ref()
            .filter(|_| coarse_column.part_rows == VECTOR_SUBBLOCK_ROWS);
        if let Some(balls) = balls {
            let norm = query.iter().map(|v| v * v).sum::<f32>().sqrt();
            for part in &mut parts {
                if let Some((centre, radius)) = balls.get((part.first_row / VECTOR_SUBBLOCK_ROWS as u64) as usize) {
                    (part.lower, part.upper) = super::kernel::ball_bounds(model.metric, centre, *radius, query, norm);
                }
            }
            parts.sort_by(|left, right| {
                right
                    .upper
                    .total_cmp(&left.upper)
                    .then(left.first_row.cmp(&right.first_row))
            });
            // Seed the bar with exact scores from the best-bound sub-blocks until `k` rows are known: coarse lower
            // bounds are loose, while an exact k-th score lets the ball bounds skip most far sub-blocks.
            // Only worth a fine block when the balls separate: enough sub-blocks bound below the best one's centre.
            let separated = parts.first().is_some_and(|best| {
                let centre = best.lower / 2.0 + best.upper / 2.0;
                let below = parts.iter().filter(|part| part.upper < centre).count();
                below >= 2 && below * 4 >= parts.len()
            });
            if separated && fine_column.part_rows == VECTOR_SUBBLOCK_ROWS {
                let mut seed = TopK::new(k);
                for part in parts.iter().take_while(|part| part.upper >= bar) {
                    if seed.threshold().is_some() {
                        break;
                    }
                    let plaintext = self.cached_part(fine_column, part.granule, part.part, Some(cache))?;
                    for_each_row(fine_width, &plaintext, part.first_row, filter, |row, bytes| {
                        let score = fine_query.score(bytes);
                        stats.rows_reranked += 1;
                        seeded.insert(row, score);
                        if in_range(score) {
                            seed.offer(ScoredRow { row, score });
                        }
                    });
                }
                if let Some(kth) = seed.threshold() {
                    bar = bar.max(kth);
                }
            }
        }
        for part in parts {
            if part.upper < bar || part.lower > ceiling {
                stats.subblocks_skipped += 1;
                continue;
            }
            let plaintext = self.cached_part(coarse_column, part.granule, part.part, shared)?;
            for_each_row(width, &plaintext, part.first_row, filter, |row, bytes| {
                stats.rows_scanned += 1;
                let estimate = coarse_query.estimate(bytes);
                if estimate.upper < bar || estimate.lower > ceiling {
                    return;
                }
                // A row whose exact score may exceed the ceiling may be dropped, so its lower bound does not
                // guarantee a result and cannot raise the bar.
                if estimate.upper <= ceiling {
                    lowers.offer(ScoredRow {
                        row,
                        score: estimate.lower,
                    });
                }
                candidates.push((row, estimate.upper));
            });
            if let Some(lower) = lowers.threshold() {
                bar = bar.max(lower);
            }
            if candidates.len() > prune_at {
                candidates.retain(|(_, upper)| *upper >= bar);
                prune_at = candidates.len().saturating_mul(2).saturating_add(1024);
            }
        }
        candidates.retain(|(_, upper)| *upper >= bar);
        candidates.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
        stats.rows_candidate += candidates.len() as u64;

        // Rows are reranked in decreasing upper bound until the next upper bound falls below the k-th exact score,
        // which is exact. `cap` adds a patience: stop after a run of reranks that all failed to enter the top k,
        // the run being `k × cap` rows or as many rows as had been reranked when the top k last improved,
        // whichever is longer. Within a tight cluster (hundreds of rows whose upper bounds all reach the k-th
        // score) the coarse code cannot order the rows, so improvements keep arriving at random positions with
        // gaps that grow with the position; a fixed run stops before the last of them (recall@10 0.87–0.90 on
        // 630-row topics at 100k rows), a run scaled with the position does not. Where the bounds do order the
        // rows, improvements stop early and the fixed run ends the rerank as before.
        let patience = cap.map(|cap| k.saturating_mul(cap.max(1)));
        let mut last_improvement = 0usize;
        let mut since_improvement = 0usize;
        let mut exact = TopK::new(k);
        for (position, (row, upper)) in candidates.into_iter().enumerate() {
            let bar = exact.threshold().map_or(threshold, |kth| kth.max(threshold));
            if upper < bar
                || patience.is_some_and(|patience| {
                    since_improvement >= patience.max(last_improvement.saturating_mul(RERANK_PATIENCE_GROWTH))
                })
            {
                break;
            }
            let score = match seeded.get(&row) {
                Some(score) => *score,
                None => {
                    let (granule, local) = self.locate(row)?;
                    let plaintext =
                        self.cached_part(fine_column, granule, local / fine_column.part_rows, Some(cache))?;
                    let local = local % fine_column.part_rows;
                    let bytes = plaintext
                        .get(local * fine_width..(local + 1) * fine_width)
                        .ok_or_else(truncated)?;
                    stats.rows_reranked += 1;
                    fine_query.score(bytes)
                }
            };
            if in_range(score) && score >= bar {
                since_improvement = 0;
                last_improvement = position + 1;
                exact.offer(ScoredRow { row, score });
            } else {
                since_improvement += 1;
            }
        }
        Ok(exact.into_sorted())
    }

    pub fn rescore(
        &self,
        column_id: u32,
        query: &[f32],
        model: &EmbeddingModel,
        rows: &[u64],
    ) -> Result<Vec<ScoredRow>, VectorError> {
        let meta = self.vector_meta(column_id, model)?;
        let prepared = self.codec.query(meta.element, query);
        let column = self.column_by_id(column_id)?;
        let width = meta.row_width();
        let mut sorted = rows.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        let mut scored = Vec::with_capacity(sorted.len());
        let mut open: Option<(SubBlock, Zeroizing<Vec<u8>>)> = None;
        for row in sorted {
            let (granule, local) = self.locate(row)?;
            let at = (granule, local / column.part_rows);
            let local = local % column.part_rows;
            if open.as_ref().is_none_or(|(current, _)| *current != at) {
                open = Some((at, self.open_part(column, at.0, at.1)?));
            }
            let bytes = open
                .as_ref()
                .and_then(|(_, plaintext)| plaintext.get(local * width..(local + 1) * width))
                .ok_or_else(truncated)?;
            scored.push(ScoredRow {
                row,
                score: prepared.score(bytes),
            });
        }
        Ok(scored)
    }

    pub fn event_ids(&self, rows: &[u64]) -> Result<Vec<String>, VectorError> {
        let mut cache: Option<(usize, Vec<String>)> = None;
        rows.iter()
            .map(|row| {
                let (granule, local) = self.locate(*row)?;
                if cache.as_ref().is_none_or(|(current, _)| *current != granule) {
                    cache = Some((granule, self.strings(EVENT_ID_COLUMN, granule)?));
                }
                cache
                    .as_ref()
                    .and_then(|(_, ids)| ids.get(local).cloned())
                    .ok_or_else(truncated)
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn footer_block_offsets(&self) -> Vec<Vec<u64>> {
        self.footer
            .columns
            .iter()
            .map(|column| {
                column
                    .blocks
                    .iter()
                    .flat_map(|(parts, _)| parts.iter().map(|block| block.offset))
                    .collect()
            })
            .collect()
    }

    pub(super) fn locate(&self, row: u64) -> Result<(usize, usize), VectorError> {
        let mut first = 0u64;
        for (granule, rows) in self.footer.granule_rows.iter().enumerate() {
            let rows = u64::from(*rows);
            if row < first + rows {
                return Ok((granule, (row - first) as usize));
            }
            first += rows;
        }
        Err(VectorError::InvalidInput {
            rule: format!("row {row} past the segment's {first} rows"),
        })
    }

    pub(super) fn vector_meta(&self, column_id: u32, model: &EmbeddingModel) -> Result<&VectorColumnMeta, VectorError> {
        let meta = self
            .footer
            .vector_columns
            .iter()
            .find(|meta| meta.column_id == column_id)
            .ok_or_else(|| VectorError::MissingColumn {
                name: column_id.to_string(),
            })?;
        meta.model.require_same(model)?;
        Ok(meta)
    }

    fn column(&self, name: &str) -> Result<&StoredColumn, VectorError> {
        self.footer
            .columns
            .iter()
            .find(|column| column.name == name)
            .ok_or_else(|| VectorError::MissingColumn { name: name.to_owned() })
    }

    fn typed_column(&self, name: &str, kind: ColumnKind) -> Result<&StoredColumn, VectorError> {
        let column = self.column(name)?;
        if column.kind != kind {
            return Err(VectorError::MissingColumn {
                name: format!("{name} as {kind:?}"),
            });
        }
        Ok(column)
    }

    pub(super) fn column_by_id(&self, column_id: u32) -> Result<&StoredColumn, VectorError> {
        self.footer
            .columns
            .iter()
            .find(|column| column.column_id == column_id)
            .ok_or_else(|| VectorError::MissingColumn {
                name: column_id.to_string(),
            })
    }

    fn granule_parts<'a>(&self, column: &'a StoredColumn, granule: usize) -> Result<&'a [BlockRef], VectorError> {
        column
            .blocks
            .get(granule)
            .map(|(parts, _)| parts.as_slice())
            .ok_or_else(|| VectorError::InvalidInput {
                rule: format!("granule {granule} past the segment's last"),
            })
    }

    /// Opens every sealed block of one granule, concatenated in row order.
    fn open_block(&self, column: &StoredColumn, granule: usize) -> Result<Zeroizing<Vec<u8>>, VectorError> {
        let parts = self.granule_parts(column, granule)?;
        if let [only] = parts {
            return self.open_sealed(only);
        }
        let mut plaintext = Zeroizing::new(Vec::new());
        for part in parts {
            plaintext.extend_from_slice(&self.open_sealed(part)?);
        }
        Ok(plaintext)
    }

    /// Opens the one sealed block holding a granule's rows `part × part_rows ..`.
    pub(super) fn open_part(
        &self,
        column: &StoredColumn,
        granule: usize,
        part: usize,
    ) -> Result<Zeroizing<Vec<u8>>, VectorError> {
        let block = self.granule_parts(column, granule)?.get(part).ok_or_else(truncated)?;
        self.open_sealed(block)
    }

    fn open_sealed(&self, block: &BlockRef) -> Result<Zeroizing<Vec<u8>>, VectorError> {
        let sealed_len = usize::try_from(block.sealed_len).map_err(|_| truncated())?;
        let sealed = match self.source.borrow_range(block.offset, sealed_len) {
            Some(sealed) => Cow::Borrowed(sealed),
            None => Cow::Owned(self.source.read_range(block.offset, sealed_len)?),
        };
        self.blocks_opened.fetch_add(1, Ordering::Relaxed);
        self.keys.open(block.index, &sealed)
    }
}

impl<S> std::fmt::Debug for SegmentReader<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentReader")
            .field("rows", &self.footer.manifest.row_count)
            .finish_non_exhaustive()
    }
}

/// Merges segments keeping their rows in order. For compaction prefer `compact_segments` (clustered order).
pub fn merge_segments<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    spec: &SegmentSpec,
    wrapping: &WrappingKey,
) -> Result<Vec<u8>, VectorError> {
    merge_segments_with_deletes(segments, &[], spec, wrapping)
}

pub fn merge_segments_with_deletes<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    deletes: &[Option<&super::deletes::SegmentDeletes>],
    spec: &SegmentSpec,
    wrapping: &WrappingKey,
) -> Result<Vec<u8>, VectorError> {
    if all_multivector(segments)? {
        return super::multivector::merge_multivector_segments(segments, deletes, spec, wrapping, None);
    }
    write_segment(spec, &merged_input(segments, deletes, spec)?, wrapping)
}

/// Whether every segment is multivector (false when none is; a mix is an error).
fn all_multivector<S: SegmentSource>(segments: &[SegmentReader<S>]) -> Result<bool, VectorError> {
    let multivector = segments
        .iter()
        .filter(|segment| segment.document_offsets().is_some())
        .count();
    if multivector != 0 && multivector != segments.len() {
        return Err(VectorError::InvalidInput {
            rule: "multivector and single-vector segments cannot be merged together".to_owned(),
        });
    }
    Ok(multivector != 0)
}

/// `merge_segments_with_deletes`, writing the live rows grouped by k-means partition (`params`) instead of in
/// segment order, so similar vectors share sealed sub-blocks. Event ids and every column move with their rows.
///
/// Time-ordered segments mix every topic in each 128-row sub-block, so neither a partition index nor the
/// sub-block balls can avoid decrypting most blocks. After a clustered merge a query's neighbours sit in a few
/// sub-blocks: the ball bounds skip the rest in a plain scan and a partition index's probes touch few blocks.
/// Granule timestamp statistics become less selective in exchange.
pub fn merge_segments_clustered<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    deletes: &[Option<&super::deletes::SegmentDeletes>],
    spec: &SegmentSpec,
    wrapping: &WrappingKey,
    params: &super::ivf::PartitionParams,
) -> Result<Vec<u8>, VectorError> {
    if all_multivector(segments)? {
        return super::multivector::merge_multivector_segments(segments, deletes, spec, wrapping, Some(params));
    }
    let merged = merged_input(segments, deletes, spec)?;
    let order = clustered_order(&merged.vectors, spec.model.dimension as usize, params);
    write_segment(spec, &permuted(merged, &order), wrapping)
}

/// The compaction entry point: merges `segments` (minus `deletes`) into one segment in clustered row order with
/// default `PartitionParams`, the order partition indexes and sub-block balls need. Compacting time-ordered
/// segments with `merge_segments` keeps every topic spread over every sub-block, so an index then saves almost
/// nothing (100k × 768 benchmark: 1,007 of 1,023 blocks opened per query, against 21 after a clustered merge).
/// Use `merge_segments_with_deletes` only when row order must be preserved.
pub fn compact_segments<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    deletes: &[Option<&super::deletes::SegmentDeletes>],
    spec: &SegmentSpec,
    wrapping: &WrappingKey,
) -> Result<Vec<u8>, VectorError> {
    merge_segments_clustered(
        segments,
        deletes,
        spec,
        wrapping,
        &super::ivf::PartitionParams::default(),
    )
}

/// Row order grouping rows by k-means partition, partitions chained nearest centroid next, rows within a partition
/// in their original order.
pub(super) fn clustered_order(
    vectors: &[Vec<f32>],
    dimension: usize,
    params: &super::ivf::PartitionParams,
) -> Vec<usize> {
    let n = vectors.len();
    if n == 0 {
        return Vec::new();
    }
    let wanted = params
        .partitions
        .unwrap_or_else(|| (n as f64).sqrt().ceil() as usize)
        .clamp(1, n);
    let assignment = super::ivf::kmeans(vectors, dimension, wanted, params);
    let mut groups: Vec<Vec<usize>> = vec![Vec::new(); wanted];
    for (row, slot) in assignment.iter().enumerate() {
        if let Some(group) = groups.get_mut(*slot) {
            group.push(row);
        }
    }
    groups.retain(|rows| !rows.is_empty());
    let centres: Vec<Vec<f32>> = groups
        .iter()
        .map(|rows| {
            super::kernel::ball_of(
                dimension,
                rows.iter().filter_map(|row| vectors.get(*row)).map(Vec::as_slice),
            )
            .0
        })
        .collect();
    let distance = |a: &[f32], b: &[f32]| a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum::<f32>();
    let mut chain = Vec::with_capacity(groups.len());
    let mut used = vec![false; groups.len()];
    let mut current = 0usize;
    for _ in 0..groups.len() {
        if let Some(slot) = used.get_mut(current) {
            *slot = true;
        }
        chain.push(current);
        let Some(from) = centres.get(current) else {
            break;
        };
        let next = centres
            .iter()
            .enumerate()
            .filter(|(index, _)| !used.get(*index).copied().unwrap_or(true))
            .min_by(|left, right| {
                distance(from, left.1)
                    .total_cmp(&distance(from, right.1))
                    .then(left.0.cmp(&right.0))
            });
        match next {
            Some((index, _)) => current = index,
            None => break,
        }
    }
    chain
        .into_iter()
        .filter_map(|group| groups.get(group))
        .flat_map(|rows| rows.iter().copied())
        .collect()
}

/// `input` with its rows in `order` (every column, event ids included).
pub(super) fn permuted(input: SegmentInput, order: &[usize]) -> SegmentInput {
    fn pick<T: Clone>(values: &[T], order: &[usize]) -> Vec<T> {
        order.iter().filter_map(|row| values.get(*row).cloned()).collect()
    }
    SegmentInput {
        event_ids: pick(&input.event_ids, order),
        integer_columns: input
            .integer_columns
            .iter()
            .map(|(name, values)| (name.clone(), pick(values, order)))
            .collect(),
        string_columns: input
            .string_columns
            .iter()
            .map(|(name, values)| (name.clone(), pick(values, order)))
            .collect(),
        vectors: pick(&input.vectors, order),
    }
}

/// The live rows of `segments`, concatenated in segment order.
fn merged_input<S: SegmentSource>(
    segments: &[SegmentReader<S>],
    deletes: &[Option<&super::deletes::SegmentDeletes>],
    spec: &SegmentSpec,
) -> Result<SegmentInput, VectorError> {
    let mut merged: Option<SegmentInput> = None;
    for (index, segment) in segments.iter().enumerate() {
        segment.manifest().model.require_same(&spec.model)?;
        let input = segment.read_live(deletes.get(index).copied().flatten())?;
        match &mut merged {
            None => merged = Some(input),
            Some(merged) => {
                let names = |input: &SegmentInput| {
                    (
                        input
                            .string_columns
                            .iter()
                            .map(|(name, _)| name.clone())
                            .collect::<Vec<_>>(),
                        input
                            .integer_columns
                            .iter()
                            .map(|(name, _)| name.clone())
                            .collect::<Vec<_>>(),
                    )
                };
                if names(merged) != names(&input) {
                    return Err(VectorError::InvalidInput {
                        rule: "merged segments hold the same columns".to_owned(),
                    });
                }
                merged.event_ids.extend(input.event_ids);
                merged.vectors.extend(input.vectors);
                for ((_, into), (_, from)) in merged.string_columns.iter_mut().zip(input.string_columns) {
                    into.extend(from);
                }
                for ((_, into), (_, from)) in merged.integer_columns.iter_mut().zip(input.integer_columns) {
                    into.extend(from);
                }
            }
        }
    }
    Ok(merged.unwrap_or_default())
}

fn centroid(model: &EmbeddingModel, vectors: &[Vec<f32>]) -> Vec<f32> {
    let mut sum = vec![0f32; model.dimension as usize];
    for vector in vectors {
        sum.iter_mut().zip(vector).for_each(|(slot, value)| *slot += value);
    }
    let count = vectors.len() as f32;
    sum.iter_mut().for_each(|value| *value /= count.max(1.0));
    if model.metric == VectorMetric::Cosine {
        normalize(&mut sum);
    }
    if sum.iter().all(|value| *value == 0.0) {
        return Vec::new();
    }
    sum
}

fn normalize(vector: &mut [f32]) {
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        vector.iter_mut().for_each(|value| *value /= norm);
    }
}

fn array4(bytes: &[u8]) -> [u8; 4] {
    bytes.try_into().unwrap_or([0; 4])
}

fn encode_strings(values: &[String]) -> Vec<u8> {
    let mut out = Writer::new();
    out.put_u32(values.len() as u32);
    values.iter().for_each(|value| put_str(&mut out, value));
    out.into_bytes()
}

fn decode_strings(bytes: &[u8]) -> Result<Vec<String>, VectorError> {
    let mut reader = Reader::new(bytes);
    let count = reader.u32("string count")? as usize;
    let mut values = Vec::with_capacity(reader.capacity_hint(count, 4));
    for _ in 0..count {
        values.push(take_str(&mut reader, "string value")?);
    }
    Ok(values)
}

fn string_stats(values: &[String]) -> Option<ColumnStats> {
    Some(ColumnStats::String {
        max: values.iter().max()?.clone(),
        min: values.iter().min()?.clone(),
    })
}
