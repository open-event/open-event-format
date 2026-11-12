use super::envelope::TimestampValue;
use super::variant::{MAX_DEPTH, VariantValue};
use crate::error::FormatError;
use prost_reflect::{DynamicMessage, EnumDescriptor, MapKey, MessageDescriptor, ReflectMessage, Value};
use serde::de::{Deserialize, DeserializeSeed, Deserializer, Error as _, MapAccess, SeqAccess, Visitor};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceFormat<'a> {
    Json,
    Protobuf(&'a MessageDescriptor),
    RawBytes,
}

pub fn transcode(format: SourceFormat<'_>, body: &[u8]) -> Result<VariantValue, FormatError> {
    let mut scratch = Vec::new();
    transcode_with_scratch(format, body, &mut scratch)
}

pub fn transcode_with_scratch(
    format: SourceFormat<'_>,
    body: &[u8],
    scratch: &mut Vec<u8>,
) -> Result<VariantValue, FormatError> {
    match format {
        SourceFormat::Json => {
            scratch.clear();
            scratch.extend_from_slice(body);
            let parsed: JsonBody = simd_json::serde::from_slice(scratch).map_err(|_| FormatError::Structural {
                rule: "declared-JSON ingest body did not parse as JSON",
            })?;
            Ok(parsed.0)
        }
        SourceFormat::Protobuf(descriptor) => {
            let message = DynamicMessage::decode(descriptor.clone(), body).map_err(|_| FormatError::Structural {
                rule: "declared-Protobuf ingest body did not decode against its registered schema",
            })?;
            protobuf_message(message)
        }
        SourceFormat::RawBytes => Ok(VariantValue::Binary(body.to_vec())),
    }
}

fn protobuf_message(mut message: DynamicMessage) -> Result<VariantValue, FormatError> {
    if message.descriptor().full_name() == "google.protobuf.Timestamp" {
        return protobuf_timestamp(&message);
    }
    let mut fields = BTreeMap::new();
    for (field, value) in message.take_fields() {
        let enum_kind = match field.kind().as_message() {
            Some(entry) if field.is_map() => entry.map_entry_value_field().kind(),
            _ => field.kind(),
        }
        .as_enum()
        .cloned();
        let enum_kind = enum_kind.as_ref();
        let value = match value {
            Value::List(items) => {
                let mut array = Vec::with_capacity(items.len());
                for item in items {
                    array.push(protobuf_value(enum_kind, item)?);
                }
                VariantValue::Array(array)
            }
            Value::Map(entries) => {
                let mut object = BTreeMap::new();
                for (key, item) in entries {
                    object.insert(protobuf_map_key(key).into(), protobuf_value(enum_kind, item)?);
                }
                VariantValue::Object(object)
            }
            other => protobuf_value(enum_kind, other)?,
        };
        fields.insert(field.name().into(), value);
    }
    Ok(VariantValue::Object(fields))
}

fn protobuf_value(enum_kind: Option<&EnumDescriptor>, value: Value) -> Result<VariantValue, FormatError> {
    Ok(match value {
        Value::Bool(value) => VariantValue::Bool(value),
        Value::Bytes(value) => VariantValue::Binary(value.to_vec()),
        Value::EnumNumber(number) => match enum_kind.and_then(|kind| kind.get_value(number)) {
            Some(known) => VariantValue::String(known.name().to_owned()),
            None => VariantValue::Int(i64::from(number)),
        },
        Value::F32(value) => VariantValue::Float(value),
        Value::F64(value) => VariantValue::Double(value),
        Value::I32(value) => VariantValue::Int(i64::from(value)),
        Value::I64(value) => VariantValue::Int(value),
        Value::List(_) | Value::Map(_) => {
            return Err(FormatError::Structural {
                rule: "declared-Protobuf ingest body nests a repeated or map value directly",
            });
        }
        Value::Message(message) => protobuf_message(message)?,
        Value::String(value) => VariantValue::String(value),
        Value::U32(value) => VariantValue::Int(i64::from(value)),
        Value::U64(value) => match i64::try_from(value) {
            Ok(value) => VariantValue::Int(value),
            Err(_) => VariantValue::Decimal {
                unscaled: i128::from(value),
                scale: 0,
            },
        },
    })
}

fn protobuf_map_key(key: MapKey) -> String {
    match key {
        MapKey::Bool(value) => value.to_string(),
        MapKey::I32(value) => value.to_string(),
        MapKey::I64(value) => value.to_string(),
        MapKey::String(value) => value,
        MapKey::U32(value) => value.to_string(),
        MapKey::U64(value) => value.to_string(),
    }
}

fn protobuf_timestamp(message: &DynamicMessage) -> Result<VariantValue, FormatError> {
    let seconds = message
        .get_field_by_name("seconds")
        .and_then(|value| value.as_i64())
        .unwrap_or(0);
    let nanos = message
        .get_field_by_name("nanos")
        .and_then(|value| value.as_i32())
        .unwrap_or(0);
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i64::from(nanos)))
        .map(|nanos| VariantValue::Timestamp(TimestampValue::from_physical_nanos(nanos)))
        .ok_or(FormatError::Structural {
            rule: "declared-Protobuf timestamp is outside the representable range",
        })
}

struct JsonBody(VariantValue);

struct JsonBodySeed {
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for JsonBodySeed {
    type Value = JsonBody;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(JsonBodyVisitor { depth: self.depth })
    }
}

struct JsonBodyVisitor {
    depth: usize,
}

impl<'de> Visitor<'de> for JsonBodyVisitor {
    type Value = JsonBody;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(JsonBody(VariantValue::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(JsonBody(VariantValue::Int(value)))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(JsonBody(match i64::try_from(value) {
            Ok(value) => VariantValue::Int(value),
            Err(_) => VariantValue::Decimal {
                unscaled: i128::from(value),
                scale: 0,
            },
        }))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
        Ok(JsonBody(VariantValue::Double(value)))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(JsonBody(VariantValue::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(JsonBody(VariantValue::String(value)))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(JsonBody(VariantValue::Null))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(JsonBody(VariantValue::Null))
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(JsonBodyVisitor { depth: self.depth })
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
        let depth = self.depth + 1;
        if depth > MAX_DEPTH {
            return Err(A::Error::custom(
                "declared-JSON ingest body exceeds the maximum nesting depth",
            ));
        }
        let mut items = Vec::with_capacity(access.size_hint().unwrap_or(0));
        while let Some(JsonBody(item)) = access.next_element_seed(JsonBodySeed { depth })? {
            items.push(item);
        }
        Ok(JsonBody(VariantValue::Array(items)))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
        let depth = self.depth + 1;
        if depth > MAX_DEPTH {
            return Err(A::Error::custom(
                "declared-JSON ingest body exceeds the maximum nesting depth",
            ));
        }
        let mut fields = BTreeMap::new();
        while let Some(key) = access.next_key::<String>()? {
            let JsonBody(value) = access.next_value_seed(JsonBodySeed { depth })?;
            fields.insert(key.into(), value);
        }
        Ok(JsonBody(VariantValue::Object(fields)))
    }
}

impl<'de> Deserialize<'de> for JsonBody {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(JsonBodyVisitor { depth: 0 })
    }
}

#[cfg(test)]
#[path = "test/transcode.rs"]
mod tests;
