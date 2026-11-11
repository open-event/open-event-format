use super::envelope::TimestampValue;
use crate::error::FormatError;
use crate::file::bytes::{Reader, Writer, slice};
use std::collections::BTreeMap;

const BASIC_PRIMITIVE: u8 = 0;
const BASIC_SHORT_STRING: u8 = 1;
const BASIC_OBJECT: u8 = 2;
const BASIC_ARRAY: u8 = 3;

const PRIM_NULL: u8 = 0;
const PRIM_TRUE: u8 = 1;
const PRIM_FALSE: u8 = 2;
const PRIM_INT8: u8 = 3;
const PRIM_INT16: u8 = 4;
const PRIM_INT32: u8 = 5;
const PRIM_INT64: u8 = 6;
const PRIM_DOUBLE: u8 = 7;
const PRIM_DECIMAL4: u8 = 8;
const PRIM_DECIMAL8: u8 = 9;
const PRIM_DECIMAL16: u8 = 10;
const PRIM_FLOAT: u8 = 14;
const PRIM_BINARY: u8 = 15;
const PRIM_STRING: u8 = 16;
const PRIM_TIMESTAMP_NANOS_UTC: u8 = 18;
const PRIM_UUID: u8 = 20;

const DIRECT_SCALAR_OBJECT_MAX_FIELDS: usize = 8;

pub(super) const MAX_DEPTH: usize = 128;

const SHORT_STRING_MAX_LEN: usize = 64;

const DECODE_ARRAY_PRE_ALLOCATE_CAP: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeyDictionary {
    keys: Vec<String>,
}

impl KeyDictionary {
    pub fn build<I: IntoIterator<Item = String>>(keys: I) -> Self {
        let mut keys: Vec<String> = keys.into_iter().collect();
        keys.sort();
        keys.dedup();
        Self { keys }
    }

    pub fn from_sorted_unique(keys: Vec<String>) -> Result<Self, FormatError> {
        let sorted = keys.windows(2).all(|pair| match pair {
            [a, b] => a < b,
            _ => true,
        });
        if !sorted {
            return Err(FormatError::Structural {
                rule: "dictionary keys must be unique and sorted ascending bytewise",
            });
        }
        Ok(Self { keys })
    }

    pub fn key_count(&self) -> u32 {
        self.keys.len() as u32
    }

    pub fn field_id(&self, key: &str) -> Option<u32> {
        self.keys
            .binary_search_by(|probe| probe.as_str().cmp(key))
            .ok()
            .map(|index| index as u32)
    }

    fn field_id_from(&self, key: &str, cursor: &mut usize) -> Option<u32> {
        let start = (*cursor).min(self.keys.len());
        if self.keys.get(start).is_some_and(|next| next == key) {
            *cursor = start + 1;
            return Some(start as u32);
        }
        let suffix = self.keys.get(start..).unwrap_or(&[]);
        match suffix.binary_search_by(|probe| probe.as_str().cmp(key)) {
            Ok(offset) => {
                let index = start + offset;
                *cursor = index + 1;
                Some(index as u32)
            }
            Err(offset) => {
                *cursor = start + offset;
                None
            }
        }
    }

    pub fn key(&self, field_id: u32) -> Option<&str> {
        self.keys.get(field_id as usize).map(String::as_str)
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.keys.iter().map(String::as_str)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum VariantValue {
    Array(Vec<VariantValue>),
    Binary(Vec<u8>),
    Bool(bool),
    Decimal { unscaled: i128, scale: u8 },
    Double(f64),
    Float(f32),
    Int(i64),
    Null,
    Object(BTreeMap<String, VariantValue>),
    String(String),
    Timestamp(TimestampValue),
    Uuid(u128),
}

impl VariantValue {
    pub fn collect_keys<'a>(&'a self, keys: &mut std::collections::BTreeSet<&'a str>) {
        match self {
            VariantValue::Object(fields) => {
                for (key, value) in fields {
                    keys.insert(key.as_str());
                    value.collect_keys(keys);
                }
            }
            VariantValue::Array(items) => {
                for item in items {
                    item.collect_keys(keys);
                }
            }
            _ => {}
        }
    }
}

fn metadata_byte(basic: u8, header: u8) -> u8 {
    (header << 2) | basic
}

fn width_for(max_value: usize) -> usize {
    if max_value <= 0xFF {
        1
    } else if max_value <= 0xFFFF {
        2
    } else if max_value <= 0xFF_FFFF {
        3
    } else {
        4
    }
}

#[derive(Debug, Default)]
pub struct EncodeScratch {
    bodies: Vec<Writer>,
    offsets: Vec<Vec<u32>>,
    placed: Vec<Vec<(u32, u32)>>,
    remap: Vec<Option<u32>>,
}

impl EncodeScratch {
    pub fn new() -> Self {
        Self::default()
    }

    fn take_body(&mut self, depth: usize) -> Writer {
        while self.bodies.len() <= depth {
            self.bodies.push(Writer::new());
        }
        self.bodies.get_mut(depth).map(std::mem::take).unwrap_or_default()
    }

    fn give_body(&mut self, depth: usize, mut body: Writer) {
        body.clear();
        if let Some(slot) = self.bodies.get_mut(depth) {
            *slot = body;
        }
    }

    fn take_offsets(&mut self, depth: usize) -> Vec<u32> {
        while self.offsets.len() <= depth {
            self.offsets.push(Vec::new());
        }
        self.offsets.get_mut(depth).map(std::mem::take).unwrap_or_default()
    }

    fn give_offsets(&mut self, depth: usize, mut offsets: Vec<u32>) {
        offsets.clear();
        if let Some(slot) = self.offsets.get_mut(depth) {
            *slot = offsets;
        }
    }

    fn take_placed(&mut self, depth: usize) -> Vec<(u32, u32)> {
        while self.placed.len() <= depth {
            self.placed.push(Vec::new());
        }
        self.placed.get_mut(depth).map(std::mem::take).unwrap_or_default()
    }

    fn give_placed(&mut self, depth: usize, mut placed: Vec<(u32, u32)>) {
        placed.clear();
        if let Some(slot) = self.placed.get_mut(depth) {
            *slot = placed;
        }
    }
}

pub fn encode_value(value: &VariantValue, dictionary: &KeyDictionary) -> Result<Vec<u8>, FormatError> {
    encode_value_with_scratch(value, dictionary, &mut EncodeScratch::new())
}

pub fn encode_value_with_scratch(
    value: &VariantValue,
    dictionary: &KeyDictionary,
    scratch: &mut EncodeScratch,
) -> Result<Vec<u8>, FormatError> {
    let mut out = Writer::new();
    encode_value_into_with_scratch(value, dictionary, scratch, &mut out)?;
    Ok(out.into_bytes())
}

pub fn encode_value_into(
    value: &VariantValue,
    dictionary: &KeyDictionary,
    out: &mut Writer,
) -> Result<(), FormatError> {
    encode_value_into_with_scratch(value, dictionary, &mut EncodeScratch::new(), out)
}

pub fn encode_value_into_with_scratch(
    value: &VariantValue,
    dictionary: &KeyDictionary,
    scratch: &mut EncodeScratch,
    out: &mut Writer,
) -> Result<(), FormatError> {
    encode_into(value, dictionary, scratch, out, 0)
}

pub fn encode_object_fields_into(
    fields: &[(&str, &VariantValue)],
    dictionary: &KeyDictionary,
    out: &mut Writer,
) -> Result<(), FormatError> {
    encode_object_fields_into_with_scratch(fields, dictionary, &mut EncodeScratch::new(), out)
}

pub fn encode_object_fields_into_with_scratch(
    fields: &[(&str, &VariantValue)],
    dictionary: &KeyDictionary,
    scratch: &mut EncodeScratch,
    out: &mut Writer,
) -> Result<(), FormatError> {
    encode_object_into(fields.iter().copied(), fields.len(), dictionary, scratch, out, 0)
}

pub fn transcode_value_into(
    value: VariantRef<'_>,
    from: &KeyDictionary,
    to: &KeyDictionary,
    out: &mut Writer,
) -> Result<(), FormatError> {
    transcode_value_into_with_scratch(value, from, to, &mut EncodeScratch::new(), out)
}

pub fn transcode_value_into_with_scratch(
    value: VariantRef<'_>,
    from: &KeyDictionary,
    to: &KeyDictionary,
    scratch: &mut EncodeScratch,
    out: &mut Writer,
) -> Result<(), FormatError> {
    let mut remap = std::mem::take(&mut scratch.remap);
    fill_id_remap(from, to, &mut remap);
    let transcoded = transcode_into(value, &remap, scratch, out, 0);
    scratch.remap = remap;
    transcoded
}

fn fill_id_remap(from: &KeyDictionary, to: &KeyDictionary, remap: &mut Vec<Option<u32>>) {
    remap.clear();
    remap.resize(from.keys.len(), None);
    let mut to_index = 0usize;
    for (from_id, from_key) in from.keys.iter().enumerate() {
        while to
            .keys
            .get(to_index)
            .is_some_and(|key| key.as_str() < from_key.as_str())
        {
            to_index += 1;
        }
        if to
            .keys
            .get(to_index)
            .is_some_and(|key| key.as_str() == from_key.as_str())
            && let Some(slot) = remap.get_mut(from_id)
        {
            *slot = Some(to_index as u32);
        }
    }
}

fn transcode_into(
    value: VariantRef<'_>,
    remap: &[Option<u32>],
    scratch: &mut EncodeScratch,
    out: &mut Writer,
    depth: usize,
) -> Result<(), FormatError> {
    if depth > MAX_DEPTH {
        return Err(FormatError::Structural {
            rule: "variant nesting exceeds maximum depth",
        });
    }
    let (basic, header) = value.metadata()?;
    match basic {
        BASIC_OBJECT => {
            let geometry = value.object_header(header)?;
            let mut placed = scratch.take_placed(depth);
            placed.reserve(geometry.count);
            let mut body = scratch.take_body(depth);
            for index in 0..geometry.count {
                let id = value.read_uint_at(
                    geometry.ids_start + index * geometry.id_width,
                    geometry.id_width,
                    "field id",
                )?;
                let id = match remap.get(id as usize) {
                    Some(Some(id)) => *id,
                    Some(None) => {
                        return Err(FormatError::RefOutOfRange {
                            what: "object key absent from governing dictionary",
                        });
                    }
                    None => {
                        return Err(FormatError::RefOutOfRange {
                            what: "field id beyond the source dictionary",
                        });
                    }
                };
                placed.push((id, body.len() as u32));
                transcode_into(
                    value.child_slice(&geometry, index)?,
                    remap,
                    scratch,
                    &mut body,
                    depth + 1,
                )?;
            }
            write_object(&placed, &body, out);
            scratch.give_placed(depth, placed);
            scratch.give_body(depth, body);
        }
        BASIC_ARRAY => {
            let geometry = value.array_header(header)?;
            let mut offsets = scratch.take_offsets(depth);
            offsets.reserve(geometry.count);
            let mut body = scratch.take_body(depth);
            for index in 0..geometry.count {
                offsets.push(body.len() as u32);
                transcode_into(
                    value.child_slice(&geometry, index)?,
                    remap,
                    scratch,
                    &mut body,
                    depth + 1,
                )?;
            }
            write_array(&offsets, &body, out);
            scratch.give_offsets(depth, offsets);
            scratch.give_body(depth, body);
        }
        _ => out.put_slice(value.as_bytes()),
    }
    Ok(())
}

fn scalar_encoded_len(value: &VariantValue) -> Option<usize> {
    match value {
        VariantValue::Null | VariantValue::Bool(_) => Some(1),
        VariantValue::Int(value) => Some(if i8::try_from(*value).is_ok() {
            2
        } else if i16::try_from(*value).is_ok() {
            3
        } else if i32::try_from(*value).is_ok() {
            5
        } else {
            9
        }),
        VariantValue::Float(_) => Some(5),
        VariantValue::Double(_) | VariantValue::Timestamp(_) => Some(9),
        VariantValue::Decimal { unscaled, .. } => Some(if i32::try_from(*unscaled).is_ok() {
            6
        } else if i64::try_from(*unscaled).is_ok() {
            10
        } else {
            18
        }),
        VariantValue::String(value) => Some(value.len() + if value.len() < SHORT_STRING_MAX_LEN { 1 } else { 5 }),
        VariantValue::Binary(value) => Some(value.len() + 5),
        VariantValue::Uuid(_) => Some(17),
        VariantValue::Array(_) | VariantValue::Object(_) => None,
    }
}

fn encode_object_into<'a>(
    fields: impl Iterator<Item = (&'a str, &'a VariantValue)> + Clone,
    field_count: usize,
    dictionary: &KeyDictionary,
    scratch: &mut EncodeScratch,
    out: &mut Writer,
    depth: usize,
) -> Result<(), FormatError> {
    if field_count <= DIRECT_SCALAR_OBJECT_MAX_FIELDS
        && depth < MAX_DEPTH
        && let Some((placed, data_len)) = direct_scalar_object_geometry(fields.clone(), dictionary)?
    {
        write_object_header(placed.get(..field_count).unwrap_or_default(), data_len, out);
        for (_, value) in fields {
            encode_into(value, dictionary, scratch, out, depth + 1)?;
        }
        return Ok(());
    }

    encode_object_buffered(fields, field_count, dictionary, scratch, out, depth)
}

type ScalarObjectGeometry = ([(u32, u32); DIRECT_SCALAR_OBJECT_MAX_FIELDS], usize);

fn direct_scalar_object_geometry<'a>(
    fields: impl Iterator<Item = (&'a str, &'a VariantValue)>,
    dictionary: &KeyDictionary,
) -> Result<Option<ScalarObjectGeometry>, FormatError> {
    let mut placed = [(0, 0); DIRECT_SCALAR_OBJECT_MAX_FIELDS];
    let mut data_len = 0usize;
    let mut cursor = 0usize;
    for (index, (key, value)) in fields.enumerate() {
        let Some(value_len) = scalar_encoded_len(value) else {
            return Ok(None);
        };
        let id = dictionary
            .field_id_from(key, &mut cursor)
            .ok_or(FormatError::RefOutOfRange {
                what: "object key absent from governing dictionary",
            })?;
        let Some(slot) = placed.get_mut(index) else {
            return Ok(None);
        };
        *slot = (id, data_len as u32);
        data_len = data_len.checked_add(value_len).ok_or(FormatError::Structural {
            rule: "variant object body length overflows address space",
        })?;
    }
    Ok(Some((placed, data_len)))
}

fn encode_object_buffered<'a>(
    fields: impl Iterator<Item = (&'a str, &'a VariantValue)>,
    field_count: usize,
    dictionary: &KeyDictionary,
    scratch: &mut EncodeScratch,
    out: &mut Writer,
    depth: usize,
) -> Result<(), FormatError> {
    let mut placed = scratch.take_placed(depth);
    placed.reserve(field_count);
    let mut body = scratch.take_body(depth);
    let mut cursor = 0usize;
    for (key, value) in fields {
        let id = dictionary
            .field_id_from(key, &mut cursor)
            .ok_or(FormatError::RefOutOfRange {
                what: "object key absent from governing dictionary",
            })?;
        placed.push((id, body.len() as u32));
        encode_into(value, dictionary, scratch, &mut body, depth + 1)?;
    }
    write_object(&placed, &body, out);
    scratch.give_placed(depth, placed);
    scratch.give_body(depth, body);
    Ok(())
}

fn write_object(placed: &[(u32, u32)], body: &Writer, out: &mut Writer) {
    let data_len = body.len();
    write_object_header(placed, data_len, out);
    out.put_slice(body.bytes());
}

fn write_object_header(placed: &[(u32, u32)], data_len: usize, out: &mut Writer) {
    let offset_width = width_for(data_len);
    let id_width = width_for(placed.iter().map(|(id, _)| *id).max().unwrap_or(0) as usize);
    let is_large = placed.len() > 0xFF;
    let header = ((is_large as u8) << 4) | ((id_width as u8 - 1) << 2) | (offset_width as u8 - 1);
    out.put_u8(metadata_byte(BASIC_OBJECT, header));
    if is_large {
        out.put_u32(placed.len() as u32);
    } else {
        out.put_u8(placed.len() as u8);
    }
    for (id, _) in placed {
        out.put_uint_width(*id, id_width);
    }
    for (_, offset) in placed {
        out.put_uint_width(*offset, offset_width);
    }
    out.put_uint_width(data_len as u32, offset_width);
}

fn write_array(offsets: &[u32], body: &Writer, out: &mut Writer) {
    let data_len = body.len();
    let offset_width = width_for(data_len);
    let is_large = offsets.len() > 0xFF;
    let header = ((is_large as u8) << 2) | (offset_width as u8 - 1);
    out.put_u8(metadata_byte(BASIC_ARRAY, header));
    if is_large {
        out.put_u32(offsets.len() as u32);
    } else {
        out.put_u8(offsets.len() as u8);
    }
    for offset in offsets {
        out.put_uint_width(*offset, offset_width);
    }
    out.put_uint_width(data_len as u32, offset_width);
    out.put_slice(body.bytes());
}

fn encode_into(
    value: &VariantValue,
    dictionary: &KeyDictionary,
    scratch: &mut EncodeScratch,
    out: &mut Writer,
    depth: usize,
) -> Result<(), FormatError> {
    if depth > MAX_DEPTH {
        return Err(FormatError::Structural {
            rule: "variant nesting exceeds maximum depth",
        });
    }
    match value {
        VariantValue::Null => out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_NULL)),
        VariantValue::Bool(true) => out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_TRUE)),
        VariantValue::Bool(false) => out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_FALSE)),
        VariantValue::Int(value) => {
            if let Ok(v) = i8::try_from(*value) {
                out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_INT8));
                out.put_u8(v as u8);
            } else if let Ok(v) = i16::try_from(*value) {
                out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_INT16));
                out.put_u16(v as u16);
            } else if let Ok(v) = i32::try_from(*value) {
                out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_INT32));
                out.put_u32(v as u32);
            } else {
                out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_INT64));
                out.put_i64(*value);
            }
        }
        VariantValue::Float(value) => {
            out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_FLOAT));
            out.put_u32(value.to_bits());
        }
        VariantValue::Double(value) => {
            out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_DOUBLE));
            out.put_u64(value.to_bits());
        }
        VariantValue::Decimal { unscaled, scale } => {
            if let Ok(v) = i32::try_from(*unscaled) {
                out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_DECIMAL4));
                out.put_u8(*scale);
                out.put_u32(v as u32);
            } else if let Ok(v) = i64::try_from(*unscaled) {
                out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_DECIMAL8));
                out.put_u8(*scale);
                out.put_i64(v);
            } else {
                out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_DECIMAL16));
                out.put_u8(*scale);
                out.put_u128(*unscaled as u128);
            }
        }
        VariantValue::String(value) => {
            if value.len() < SHORT_STRING_MAX_LEN {
                out.put_u8(metadata_byte(BASIC_SHORT_STRING, value.len() as u8));
                out.put_slice(value.as_bytes());
            } else {
                out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_STRING));
                out.put_u32(value.len() as u32);
                out.put_slice(value.as_bytes());
            }
        }
        VariantValue::Binary(value) => {
            out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_BINARY));
            out.put_u32(value.len() as u32);
            out.put_slice(value);
        }
        VariantValue::Timestamp(value) => {
            out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_TIMESTAMP_NANOS_UTC));
            out.put_i64(value.physical_nanos());
        }
        VariantValue::Uuid(value) => {
            out.put_u8(metadata_byte(BASIC_PRIMITIVE, PRIM_UUID));
            out.put_u128(*value);
        }
        VariantValue::Array(items) => {
            let mut offsets = scratch.take_offsets(depth);
            offsets.reserve(items.len());
            let mut body = scratch.take_body(depth);
            for item in items {
                offsets.push(body.len() as u32);
                encode_into(item, dictionary, scratch, &mut body, depth + 1)?;
            }
            write_array(&offsets, &body, out);
            scratch.give_offsets(depth, offsets);
            scratch.give_body(depth, body);
        }
        VariantValue::Object(fields) => {
            encode_object_into(
                fields.iter().map(|(key, value)| (key.as_str(), value)),
                fields.len(),
                dictionary,
                scratch,
                out,
                depth,
            )?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathSegment<'p> {
    Field(&'p str),
    Index(usize),
}

#[derive(Debug, Clone, Copy)]
pub struct VariantRef<'a> {
    bytes: &'a [u8],
}

struct ContainerHeader {
    count: usize,
    id_width: usize,
    ids_start: usize,
    offset_width: usize,
    offsets_start: usize,
    values_start: usize,
}

impl<'a> VariantRef<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    fn metadata(&self) -> Result<(u8, u8), FormatError> {
        let byte = self
            .bytes
            .first()
            .copied()
            .ok_or(FormatError::Truncated { what: "variant value" })?;
        Ok((byte & 0b11, byte >> 2))
    }

    fn object_header(&self, header: u8) -> Result<ContainerHeader, FormatError> {
        let offset_width = (header & 0b11) as usize + 1;
        let id_width = ((header >> 2) & 0b11) as usize + 1;
        let is_large = (header >> 4) & 1 == 1;
        let mut reader = Reader::new(self.bytes);
        let _ = reader.u8("variant metadata")?;
        let count = if is_large {
            reader.u32("object count")? as usize
        } else {
            reader.u8("object count")? as usize
        };
        let ids_start = reader.position();
        let ids_len = count.checked_mul(id_width).ok_or(FormatError::Structural {
            rule: "object id table overflow",
        })?;
        let offsets_start = ids_start.checked_add(ids_len).ok_or(FormatError::Structural {
            rule: "object id table overflow",
        })?;
        let offsets_len =
            count
                .checked_add(1)
                .and_then(|n| n.checked_mul(offset_width))
                .ok_or(FormatError::Structural {
                    rule: "object offset table overflow",
                })?;
        let values_start = offsets_start.checked_add(offsets_len).ok_or(FormatError::Structural {
            rule: "object offset table overflow",
        })?;
        Ok(ContainerHeader {
            count,
            id_width,
            offset_width,
            ids_start,
            offsets_start,
            values_start,
        })
    }

    fn array_header(&self, header: u8) -> Result<ContainerHeader, FormatError> {
        let offset_width = (header & 0b11) as usize + 1;
        let is_large = (header >> 2) & 1 == 1;
        let mut reader = Reader::new(self.bytes);
        let _ = reader.u8("variant metadata")?;
        let count = if is_large {
            reader.u32("array count")? as usize
        } else {
            reader.u8("array count")? as usize
        };
        let offsets_start = reader.position();
        let offsets_len =
            count
                .checked_add(1)
                .and_then(|n| n.checked_mul(offset_width))
                .ok_or(FormatError::Structural {
                    rule: "array offset table overflow",
                })?;
        let values_start = offsets_start.checked_add(offsets_len).ok_or(FormatError::Structural {
            rule: "array offset table overflow",
        })?;
        Ok(ContainerHeader {
            count,
            id_width: 0,
            offsets_start,
            offset_width,
            ids_start: 0,
            values_start,
        })
    }

    fn read_uint_at(&self, start: usize, width: usize, what: &'static str) -> Result<u32, FormatError> {
        let bytes = slice(self.bytes, start, width, what)?;
        let mut value: u32 = 0;
        for (index, byte) in bytes.iter().enumerate() {
            value |= u32::from(*byte) << (8 * index);
        }
        Ok(value)
    }

    fn child_slice(&self, geometry: &ContainerHeader, index: usize) -> Result<VariantRef<'a>, FormatError> {
        let start_offset = self.read_uint_at(
            geometry.offsets_start + index * geometry.offset_width,
            geometry.offset_width,
            "value offset",
        )? as usize;
        let end_offset = self.read_uint_at(
            geometry.offsets_start + (index + 1) * geometry.offset_width,
            geometry.offset_width,
            "value offset",
        )? as usize;
        if end_offset < start_offset {
            return Err(FormatError::Structural {
                rule: "value offsets must be non-decreasing",
            });
        }
        let bytes = slice(
            self.bytes,
            geometry.values_start + start_offset,
            end_offset - start_offset,
            "child value",
        )?;
        Ok(VariantRef::new(bytes))
    }

    fn field_id_at(&self, geometry: &ContainerHeader, index: usize) -> Result<u32, FormatError> {
        self.read_uint_at(
            geometry.ids_start + index * geometry.id_width,
            geometry.id_width,
            "field id",
        )
    }

    fn object_field(&self, geometry: &ContainerHeader, target_id: u32) -> Result<Option<VariantRef<'a>>, FormatError> {
        let mut low = 0usize;
        let mut high = geometry.count;
        while low < high {
            let mid = low + (high - low) / 2;
            let id = self.field_id_at(geometry, mid)?;
            if mid > 0 && self.field_id_at(geometry, mid - 1)? >= id {
                return Err(FormatError::Structural {
                    rule: "object field ids must be strictly ascending",
                });
            }
            if mid + 1 < geometry.count && self.field_id_at(geometry, mid + 1)? <= id {
                return Err(FormatError::Structural {
                    rule: "object field ids must be strictly ascending",
                });
            }
            match id.cmp(&target_id) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return self.child_slice(geometry, mid).map(Some),
            }
        }
        Ok(None)
    }

    pub fn get_path(
        &self,
        dictionary: &KeyDictionary,
        path: &[PathSegment<'_>],
    ) -> Result<Option<VariantRef<'a>>, FormatError> {
        let mut current = *self;
        for segment in path {
            let (basic, header) = current.metadata()?;
            match (segment, basic) {
                (PathSegment::Field(key), BASIC_OBJECT) => {
                    let Some(field_id) = dictionary.field_id(key) else {
                        return Ok(None);
                    };
                    let geometry = current.object_header(header)?;
                    match current.object_field(&geometry, field_id)? {
                        Some(child) => current = child,
                        None => return Ok(None),
                    }
                }
                (PathSegment::Index(index), BASIC_ARRAY) => {
                    let geometry = current.array_header(header)?;
                    if *index >= geometry.count {
                        return Ok(None);
                    }
                    current = current.child_slice(&geometry, *index)?;
                }
                _ => return Ok(None),
            }
        }
        Ok(Some(current))
    }

    pub fn decode(&self, dictionary: &KeyDictionary) -> Result<VariantValue, FormatError> {
        self.decode_at_depth(dictionary, 0)
    }

    fn decode_at_depth(&self, dictionary: &KeyDictionary, depth: usize) -> Result<VariantValue, FormatError> {
        if depth > MAX_DEPTH {
            return Err(FormatError::Structural {
                rule: "variant nesting exceeds maximum depth",
            });
        }
        let (basic, header) = self.metadata()?;
        match basic {
            BASIC_SHORT_STRING => {
                let len = header as usize;
                let bytes = slice(self.bytes, 1, len, "short string")?;
                let text =
                    simdutf8::basic::from_utf8(bytes).map_err(|_| FormatError::InvalidUtf8 { what: "short string" })?;
                if self.bytes.len() != 1 + len {
                    return Err(FormatError::Structural {
                        rule: "short string has trailing bytes",
                    });
                }
                Ok(VariantValue::String(text.to_owned()))
            }
            BASIC_PRIMITIVE => self.decode_primitive(header),
            BASIC_ARRAY => {
                let geometry = self.array_header(header)?;
                let mut items = Vec::with_capacity(geometry.count.min(DECODE_ARRAY_PRE_ALLOCATE_CAP));
                for index in 0..geometry.count {
                    items.push(
                        self.child_slice(&geometry, index)?
                            .decode_at_depth(dictionary, depth + 1)?,
                    );
                }
                self.check_container_fills_slice(&geometry)?;
                Ok(VariantValue::Array(items))
            }
            BASIC_OBJECT => {
                let geometry = self.object_header(header)?;
                let mut fields = Vec::with_capacity(geometry.count.min(DECODE_ARRAY_PRE_ALLOCATE_CAP));
                let mut previous_id: Option<u32> = None;
                for index in 0..geometry.count {
                    let id = self.read_uint_at(
                        geometry.ids_start + index * geometry.id_width,
                        geometry.id_width,
                        "field id",
                    )?;
                    if previous_id.is_some_and(|previous| previous >= id) {
                        return Err(FormatError::Structural {
                            rule: "object field ids must be strictly ascending",
                        });
                    }
                    previous_id = Some(id);
                    let key = dictionary.key(id).ok_or(FormatError::RefOutOfRange {
                        what: "field id beyond governing dictionary",
                    })?;
                    let value = self
                        .child_slice(&geometry, index)?
                        .decode_at_depth(dictionary, depth + 1)?;
                    fields.push((key.to_owned(), value));
                }
                self.check_container_fills_slice(&geometry)?;
                Ok(VariantValue::Object(BTreeMap::from_iter(fields)))
            }
            _ => Err(FormatError::Structural {
                rule: "unknown basic type",
            }),
        }
    }

    fn decode_primitive(&self, type_id: u8) -> Result<VariantValue, FormatError> {
        let mut reader = Reader::new(self.bytes);
        let _ = reader.u8("variant metadata")?;
        let value = match type_id {
            PRIM_NULL => VariantValue::Null,
            PRIM_TRUE => VariantValue::Bool(true),
            PRIM_FALSE => VariantValue::Bool(false),
            PRIM_INT8 => VariantValue::Int(i64::from(reader.u8("int8")? as i8)),
            PRIM_INT16 => VariantValue::Int(i64::from(reader.u16("int16")? as i16)),
            PRIM_INT32 => VariantValue::Int(i64::from(reader.u32("int32")? as i32)),
            PRIM_INT64 => VariantValue::Int(reader.i64("int64")?),
            PRIM_DOUBLE => VariantValue::Double(f64::from_bits(reader.u64("double")?)),
            PRIM_FLOAT => VariantValue::Float(f32::from_bits(reader.u32("float")?)),
            PRIM_DECIMAL4 => {
                let scale = reader.u8("decimal scale")?;
                let unscaled = reader.u32("decimal4")? as i32;
                VariantValue::Decimal {
                    unscaled: i128::from(unscaled),
                    scale,
                }
            }
            PRIM_DECIMAL8 => {
                let scale = reader.u8("decimal scale")?;
                let unscaled = reader.i64("decimal8")?;
                VariantValue::Decimal {
                    unscaled: i128::from(unscaled),
                    scale,
                }
            }
            PRIM_DECIMAL16 => {
                let scale = reader.u8("decimal scale")?;
                let unscaled = reader.u128("decimal16")? as i128;
                VariantValue::Decimal { unscaled, scale }
            }
            PRIM_BINARY => {
                let len = reader.u32("binary length")? as usize;
                VariantValue::Binary(reader.take(len, "binary")?.to_vec())
            }
            PRIM_STRING => {
                let len = reader.u32("string length")? as usize;
                let bytes = reader.take(len, "string")?;
                let text =
                    simdutf8::basic::from_utf8(bytes).map_err(|_| FormatError::InvalidUtf8 { what: "string" })?;
                VariantValue::String(text.to_owned())
            }
            PRIM_TIMESTAMP_NANOS_UTC => {
                VariantValue::Timestamp(TimestampValue::from_physical_nanos(reader.i64("timestamp")?))
            }
            PRIM_UUID => VariantValue::Uuid(reader.u128("uuid")?),
            _ => {
                return Err(FormatError::Structural {
                    rule: "unsupported primitive type id",
                });
            }
        };
        if reader.remaining() != 0 {
            return Err(FormatError::Structural {
                rule: "primitive value has trailing bytes",
            });
        }
        Ok(value)
    }

    pub fn validate(&self, dictionary: &KeyDictionary) -> Result<(), FormatError> {
        self.validate_at_depth(dictionary, 0)
    }

    fn validate_at_depth(&self, dictionary: &KeyDictionary, depth: usize) -> Result<(), FormatError> {
        if depth > MAX_DEPTH {
            return Err(FormatError::Structural {
                rule: "variant nesting exceeds maximum depth",
            });
        }
        let (basic, header) = self.metadata()?;
        match basic {
            BASIC_SHORT_STRING => {
                let len = header as usize;
                let bytes = slice(self.bytes, 1, len, "short string")?;
                simdutf8::basic::from_utf8(bytes).map_err(|_| FormatError::InvalidUtf8 { what: "short string" })?;
                if self.bytes.len() != 1 + len {
                    return Err(FormatError::Structural {
                        rule: "short string has trailing bytes",
                    });
                }
                Ok(())
            }
            BASIC_PRIMITIVE => self.validate_primitive(header),
            BASIC_ARRAY => {
                let geometry = self.array_header(header)?;
                for index in 0..geometry.count {
                    self.child_slice(&geometry, index)?
                        .validate_at_depth(dictionary, depth + 1)?;
                }
                self.check_container_fills_slice(&geometry)
            }
            BASIC_OBJECT => {
                let geometry = self.object_header(header)?;
                let mut previous_id: Option<u32> = None;
                for index in 0..geometry.count {
                    let id = self.read_uint_at(
                        geometry.ids_start + index * geometry.id_width,
                        geometry.id_width,
                        "field id",
                    )?;
                    if previous_id.is_some_and(|previous| previous >= id) {
                        return Err(FormatError::Structural {
                            rule: "object field ids must be strictly ascending",
                        });
                    }
                    previous_id = Some(id);
                    if dictionary.key(id).is_none() {
                        return Err(FormatError::RefOutOfRange {
                            what: "field id beyond governing dictionary",
                        });
                    }
                    self.child_slice(&geometry, index)?
                        .validate_at_depth(dictionary, depth + 1)?;
                }
                self.check_container_fills_slice(&geometry)
            }
            _ => Err(FormatError::Structural {
                rule: "unknown basic type",
            }),
        }
    }

    fn check_container_fills_slice(&self, geometry: &ContainerHeader) -> Result<(), FormatError> {
        let first = self.read_uint_at(geometry.offsets_start, geometry.offset_width, "value offset")? as usize;
        if first != 0 {
            return Err(FormatError::Structural {
                rule: "container values must start at offset 0",
            });
        }
        let end = self.read_uint_at(
            geometry.offsets_start + geometry.count * geometry.offset_width,
            geometry.offset_width,
            "value offset",
        )? as usize;
        if geometry.values_start.checked_add(end) != Some(self.bytes.len()) {
            return Err(FormatError::Structural {
                rule: "container value must end exactly at the slice end",
            });
        }
        Ok(())
    }

    fn validate_primitive(&self, type_id: u8) -> Result<(), FormatError> {
        let mut reader = Reader::new(self.bytes);
        let _ = reader.u8("variant metadata")?;
        match type_id {
            PRIM_NULL | PRIM_TRUE | PRIM_FALSE => {}
            PRIM_INT8 => {
                reader.take(1, "int8")?;
            }
            PRIM_INT16 => {
                reader.take(2, "int16")?;
            }
            PRIM_INT32 => {
                reader.take(4, "int32")?;
            }
            PRIM_INT64 => {
                reader.take(8, "int64")?;
            }
            PRIM_DOUBLE => {
                reader.take(8, "double")?;
            }
            PRIM_FLOAT => {
                reader.take(4, "float")?;
            }
            PRIM_DECIMAL4 => {
                reader.take(5, "decimal4")?;
            }
            PRIM_DECIMAL8 => {
                reader.take(9, "decimal8")?;
            }
            PRIM_DECIMAL16 => {
                reader.take(17, "decimal16")?;
            }
            PRIM_BINARY => {
                let len = reader.u32("binary length")? as usize;
                reader.take(len, "binary")?;
            }
            PRIM_STRING => {
                let len = reader.u32("string length")? as usize;
                let bytes = reader.take(len, "string")?;
                simdutf8::basic::from_utf8(bytes).map_err(|_| FormatError::InvalidUtf8 { what: "string" })?;
            }
            PRIM_TIMESTAMP_NANOS_UTC => {
                reader.take(8, "timestamp")?;
            }
            PRIM_UUID => {
                reader.take(16, "uuid")?;
            }
            _ => {
                return Err(FormatError::Structural {
                    rule: "unsupported primitive type id",
                });
            }
        }
        if reader.remaining() != 0 {
            return Err(FormatError::Structural {
                rule: "primitive value has trailing bytes",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "test/variant.rs"]
mod tests;
