use super::{PipelineId, Transform};
use crate::error::FormatError;
use crate::file::bytes::Reader;

#[derive(Clone, Debug)]
pub enum PageDescriptor {
    Alp { exception_count: u32, exponent_index: u8 },
    DeltaBitpack { first_value: u64, width: u32 },
    Dictionary { entries: Vec<String> },
    ForBitpack { base: u64, width: u32 },
    Fsst { symbol_count: u16 },
    Plain,
    Rle { run_count: u32 },
}

impl PageDescriptor {
    pub fn decode_cost_per_row(&self) -> u32 {
        match self {
            PageDescriptor::Plain => 1,
            PageDescriptor::Rle { .. } => 1,
            PageDescriptor::ForBitpack { width, .. } => 1 + width / 8,
            PageDescriptor::DeltaBitpack { width, .. } => 2 + width / 8,
            PageDescriptor::Alp { exception_count, .. } => 3 + (*exception_count / 64).min(4),
            PageDescriptor::Dictionary { entries } => 2 + (entries.len().min(255) as u32) / 32,
            PageDescriptor::Fsst { symbol_count } => 4 + (*symbol_count as u32) / 64,
        }
    }
}

pub fn extract_descriptor(pipeline: PipelineId, bytes: &[u8]) -> Result<PageDescriptor, FormatError> {
    let body = super::remove_trailing(pipeline.compression()?, bytes)?;
    extract_descriptor_with_body(pipeline, &body)
}

pub fn extract_descriptor_with_body(pipeline: PipelineId, body: &[u8]) -> Result<PageDescriptor, FormatError> {
    let transform = pipeline.transform()?;
    let mut reader = Reader::new(body);
    match transform {
        Transform::ForBitpack => {
            let base = reader.u64("for base")?;
            let _count = reader.u32("bitpack count")?;
            let width = u32::from(reader.u8("bitpack width")?);
            Ok(PageDescriptor::ForBitpack { base, width })
        }
        Transform::DeltaBitpack => {
            let first_value = reader.u64("delta first")?;
            let _count = reader.u32("bitpack count")?;
            let width = u32::from(reader.u8("bitpack width")?);
            Ok(PageDescriptor::DeltaBitpack { first_value, width })
        }
        Transform::Rle => {
            let run_count = reader.u32("rle run count")?;
            Ok(PageDescriptor::Rle { run_count })
        }
        Transform::Alp => {
            let mut exponent_index = reader.u8("alp exponent")?;
            if exponent_index == super::ALP_VECTOR_ESCAPE_SENTINEL {
                exponent_index = reader.u8("alp exponent")?;
                let escaped = reader.u32("alp escaped vector count")? as usize;
                reader.take(escaped.saturating_mul(4), "alp escaped vector index")?;
            }
            let exception_count = reader.u32("alp exception count")?;
            Ok(PageDescriptor::Alp {
                exception_count,
                exponent_index,
            })
        }
        Transform::DictionaryString => {
            let side = pipeline.side_stream()?;
            if side == super::SideStream::FileScopeDictionary {
                return Err(FormatError::Structural {
                    rule: "shared-scope dictionary block without its file alphabet",
                });
            }
            let _ = super::NullStream::read(&mut reader)?;
            let dict_count = reader.u32("dictionary count")? as usize;
            let entries = super::read_dictionary_values(&mut reader, dict_count, side)?;
            Ok(PageDescriptor::Dictionary { entries })
        }
        Transform::FsstString | Transform::FsstPackedString => {
            let _ = super::NullStream::read(&mut reader)?;
            let symbol_count = reader.u16("fsst symbol count")?;
            Ok(PageDescriptor::Fsst { symbol_count })
        }
        _ => Ok(PageDescriptor::Plain),
    }
}

#[cfg(test)]
#[path = "test/descriptor.rs"]
mod tests;
