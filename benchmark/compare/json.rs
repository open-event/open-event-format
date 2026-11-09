
use crate::dataset::{Column, Dataset};
use crate::format::{Format, Opened};
use crate::query::{Col, Partial, Query, fold_chunk, needed};
use anyhow::Result;
use serde_json::{Map, Value};
use std::io::Write;

pub struct Json {
    pub zstd_level: Option<i32>,
}

impl Format for Json {
    fn name(&self) -> &'static str {
        if self.zstd_level.is_some() { "json+zstd" } else { "json" }
    }

    fn write(&self, dataset: &Dataset) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(dataset.rows * 256);
        for row in 0..dataset.rows {
            let mut event = Map::new();
            let mut payload = Map::new();
            for (name, column) in &dataset.columns {
                let value = match column {
                    Column::F64(values) => Value::from(values[row]),
                    Column::I64(values) => Value::from(values[row]),
                    Column::Str(values) => Value::from(values[row].as_str()),
                };
                match name.strip_prefix("payload.") {
                    Some(field) => payload.insert(field.to_owned(), value),
                    None => event.insert(name.clone(), value),
                };
            }
            event.insert("payload".to_owned(), Value::Object(payload));
            serde_json::to_writer(&mut out, &Value::Object(event))?;
            out.write_all(b"\n")?;
        }
        Ok(match self.zstd_level {
            Some(level) => zstd::encode_all(out.as_slice(), level)?,
            None => out,
        })
    }

    fn open(&self, _dataset: &Dataset, bytes: Vec<u8>) -> Result<Box<dyn Opened>> {
        Ok(Box::new(JsonFile {
            bytes,
            compressed: self.zstd_level.is_some(),
        }))
    }
}

struct JsonFile {
    bytes: Vec<u8>,
    compressed: bool,
}

fn field<'a>(event: &'a Value, name: &str) -> &'a Value {
    match name.strip_prefix("payload.") {
        Some(field) => &event["payload"][field],
        None => &event[name],
    }
}

impl Opened for JsonFile {
    fn query(&self, query: &Query) -> Result<Partial> {
        let decompressed;
        let text: &[u8] = if self.compressed {
            decompressed = zstd::decode_all(self.bytes.as_slice())?;
            &decompressed
        } else {
            &self.bytes
        };
        let names = needed(query);
        let mut ints: Vec<Vec<i64>> = vec![Vec::new(); names.len()];
        let mut floats: Vec<Vec<f64>> = vec![Vec::new(); names.len()];
        let mut strings: Vec<Vec<String>> = vec![Vec::new(); names.len()];
        let mut rows = 0;
        for line in text.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            let event: Value = serde_json::from_slice(line)?;
            for (i, name) in names.iter().enumerate() {
                match field(&event, name) {
                    Value::String(s) => strings[i].push(s.clone()),
                    Value::Number(n) if n.is_f64() => floats[i].push(n.as_f64().unwrap_or(0.0)),
                    Value::Number(n) => ints[i].push(n.as_i64().unwrap_or(0)),
                    other => panic!("unexpected json value {other}"),
                }
            }
            rows += 1;
        }
        let columns: Vec<(&str, Col)> = names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let col = if !strings[i].is_empty() {
                    Col::Str(strings[i].iter().map(String::as_str).collect())
                } else if !floats[i].is_empty() {
                    Col::F64(&floats[i])
                } else {
                    Col::I64(&ints[i])
                };
                (*name, col)
            })
            .collect();
        Ok(fold_chunk(&columns, rows, query))
    }
}
