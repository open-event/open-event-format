//! Vortex with its default compressor, written from the same Arrow columns Parquet uses. Queries push the projection
//! and the filter down into the scan.

#![allow(deprecated)]

use crate::dataset::Dataset;
use crate::format::{Format, Opened};
use crate::parquet::arrow_batch;
use crate::query::{Col, Partial, Predicate, Query, fold_chunk, needed};
use anyhow::{Result, anyhow};
use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use futures::StreamExt;
use tokio::runtime::{Builder, Runtime};
use vortex::VortexSessionDefault;
use vortex::io::session::RuntimeSessionExt;
use vortex::array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex::arrow::ArrowArrayExecutor;
use vortex::arrow::convert::from_arrow_batch;
use vortex::buffer::{ByteBuffer, ByteBufferMut};
use vortex::expr::{Expression, and, eq, get_item, gt, gt_eq, lit, lt, root, select};
use vortex::file::{OpenOptionsSessionExt, VortexFile, WriteOptionsSessionExt};
use vortex::session::VortexSession;

pub struct Vortex {
    runtime: Runtime,
    session: VortexSession,
}

/// A single-threaded runtime and a session bound to it, so a scan uses no more threads than the other formats.
fn runtime_and_session() -> Result<(Runtime, VortexSession)> {
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let session = {
        let _guard = runtime.enter();
        VortexSession::default().with_tokio()
    };
    Ok((runtime, session))
}

impl Vortex {
    pub fn new() -> Result<Self> {
        let (runtime, session) = runtime_and_session()?;
        Ok(Self { runtime, session })
    }
}

impl Format for Vortex {
    fn name(&self) -> &'static str {
        "vortex"
    }

    fn write(&self, dataset: &Dataset) -> Result<Vec<u8>> {
        let session = &self.session;
        let array: ArrayRef = from_arrow_batch(&arrow_batch(dataset), false)?;
        let mut bytes = ByteBufferMut::empty();
        self.runtime.block_on(async {
            session
                .write_options()
                .write(&mut bytes, array.to_array_stream())
                .await
        })?;
        Ok(bytes.freeze().as_slice().to_vec())
    }

    fn open(&self, _dataset: &Dataset, bytes: Vec<u8>) -> Result<Box<dyn Opened>> {
        let (runtime, session) = runtime_and_session()?;
        let file = session.open_options().open_buffer(ByteBuffer::from(bytes))?;
        Ok(Box::new(VortexReader { file, runtime, session }))
    }
}

struct VortexReader {
    file: VortexFile,
    runtime: Runtime,
    session: VortexSession,
}

fn condition(predicate: &Predicate) -> Expression {
    let field = |column: &str| get_item(column.to_owned(), root());
    match predicate {
        Predicate::EqStr { column, value } => eq(field(column), lit(value.as_str())),
        Predicate::GreaterI64 { column, value } => gt(field(column), lit(*value)),
        Predicate::RangeI64 { column, high, low } => {
            and(gt_eq(field(column), lit(*low)), lt(field(column), lit(*high)))
        }
    }
}

impl Opened for VortexReader {
    fn query(&self, query: &Query) -> Result<Partial> {
        let names = needed(query);
        let dtype = self.file.dtype();
        let mut scan = self.file.scan()?.with_projection(
            select(names.clone(), root()).bind(dtype)?,
        );
        let mut predicates = query.predicates.iter().map(condition);
        if let Some(first) = predicates.next() {
            let combined = predicates.fold(first, and);
            scan = scan.with_filter(combined.bind(dtype)?.optimize_recursive()?);
        }
        let mut ctx = self.session.create_execution_ctx();
        let mut total = Partial::default();
        self.runtime.block_on(async {
            let mut stream = scan.into_array_stream()?;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                let rows = chunk.len();
                let arrow = chunk.into_array().execute_arrow(None, &mut ctx)?;
                let batch = arrow.as_struct();
                let mut strings: Vec<Vec<&str>> = Vec::new();
                let mut kinds = Vec::new();
                for name in &names {
                    let array = batch.column_by_name(name).ok_or_else(|| anyhow!("no column {name}"))?;
                    match array.data_type() {
                        DataType::Utf8View => {
                            let values = array.as_string_view();
                            strings.push((0..values.len()).map(|i| values.value(i)).collect());
                            kinds.push(true);
                        }
                        DataType::Utf8 => {
                            let values = array.as_string::<i32>();
                            strings.push((0..values.len()).map(|i| values.value(i)).collect());
                            kinds.push(true);
                        }
                        _ => {
                            strings.push(Vec::new());
                            kinds.push(false);
                        }
                    }
                }
                let mut next_string = strings.into_iter();
                let columns: Vec<(&str, Col)> = names
                    .iter()
                    .zip(kinds)
                    .map(|(name, is_text)| {
                        let text = next_string.next().unwrap_or_default();
                        let array = batch.column_by_name(name).expect("checked above");
                        let col = if is_text {
                            Col::Str(text)
                        } else if matches!(array.data_type(), DataType::Float64) {
                            Col::F64(array.as_primitive::<Float64Type>().values())
                        } else {
                            Col::I64(array.as_primitive::<Int64Type>().values())
                        };
                        (*name, col)
                    })
                    .collect();
                total.add(&fold_chunk(&columns, rows, query));
            }
            anyhow::Ok(())
        })?;
        Ok(total)
    }
}
