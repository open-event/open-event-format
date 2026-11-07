//! The queries every format answers, and the one way their results are folded so formats can be checked against
//! each other.

use crate::dataset::{Dataset, Profile};

#[derive(Clone, Debug)]
pub enum Predicate {
    EqStr { column: &'static str, value: String },
    GreaterI64 { column: &'static str, value: i64 },
    RangeI64 { column: &'static str, high: i64, low: i64 },
}

impl Predicate {
    pub fn column(&self) -> &'static str {
        match self {
            Predicate::EqStr { column, .. } | Predicate::GreaterI64 { column, .. } | Predicate::RangeI64 { column, .. } => {
                column
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Query {
    pub fold: Vec<&'static str>,
    pub name: &'static str,
    pub predicates: Vec<Predicate>,
}

/// A decoded column, borrowed from whatever the format decoded it into.
pub enum Col<'a> {
    F64(&'a [f64]),
    I64(&'a [i64]),
    Str(Vec<&'a str>),
}

/// What a query found: how many rows matched and one total per folded column (integer sum, float sum in thousandths,
/// or string bytes). Equal across formats or the benchmark stops.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Partial {
    pub rows: u64,
    pub sums: Vec<i128>,
}

impl Partial {
    pub fn add(&mut self, other: &Partial) {
        self.rows += other.rows;
        if self.sums.len() < other.sums.len() {
            self.sums.resize(other.sums.len(), 0);
        }
        for (total, value) in self.sums.iter_mut().zip(&other.sums) {
            *total += value;
        }
    }
}

/// Applies the query to one decoded chunk. `columns` must hold every folded and filtered column by name.
pub fn fold_chunk(columns: &[(&str, Col)], rows: usize, query: &Query) -> Partial {
    let find = |name: &str| &columns.iter().find(|(column, _)| *column == name).unwrap_or_else(|| panic!("{name} missing")).1;
    let mut keep = vec![true; rows];
    for predicate in &query.predicates {
        match (predicate, find(predicate.column())) {
            (Predicate::EqStr { value, .. }, Col::Str(values)) => {
                for (flag, v) in keep.iter_mut().zip(values) {
                    *flag &= *v == value.as_str();
                }
            }
            (Predicate::GreaterI64 { value, .. }, Col::I64(values)) => {
                for (flag, v) in keep.iter_mut().zip(*values) {
                    *flag &= v > value;
                }
            }
            (Predicate::RangeI64 { low, high, .. }, Col::I64(values)) => {
                for (flag, v) in keep.iter_mut().zip(*values) {
                    *flag &= v >= low && v < high;
                }
            }
            _ => panic!("predicate on a column of the wrong type"),
        }
    }
    let mut partial = Partial {
        rows: keep.iter().filter(|k| **k).count() as u64,
        sums: Vec::with_capacity(query.fold.len()),
    };
    for name in &query.fold {
        let total: i128 = match find(name) {
            Col::I64(values) => values.iter().zip(&keep).filter(|(_, k)| **k).map(|(v, _)| *v as i128).sum(),
            Col::F64(values) => values
                .iter()
                .zip(&keep)
                .filter(|(_, k)| **k)
                .map(|(v, _)| (v * 1000.0).round() as i128)
                .sum(),
            Col::Str(values) => values.iter().zip(&keep).filter(|(_, k)| **k).map(|(v, _)| v.len() as i128).sum(),
        };
        partial.sums.push(total);
    }
    partial
}

/// Every column a query reads: folded and filtered.
pub fn needed(query: &Query) -> Vec<&'static str> {
    let mut names = query.fold.clone();
    for predicate in &query.predicates {
        if !names.contains(&predicate.column()) {
            names.push(predicate.column());
        }
    }
    names
}

pub fn queries(dataset: &Dataset) -> Vec<Query> {
    let all: Vec<&'static str> = match dataset.profile {
        Profile::Messages => vec![
            "event_id", "occurred_at", "entity_id", "event_type", "source", "payload.body", "payload.lang",
            "payload.length", "payload.room", "payload.user_id",
        ],
        Profile::Telemetry => vec![
            "event_id", "occurred_at", "entity_id", "event_type", "source", "payload.battery", "payload.error_code",
            "payload.firmware", "payload.reading", "payload.region",
        ],
    };
    let (number, text, filter): (&'static str, &'static str, Vec<Predicate>) = match dataset.profile {
        Profile::Messages => (
            "payload.user_id",
            "payload.body",
            vec![
                Predicate::EqStr {
                    column: "event_type",
                    value: "msg.redact".to_owned(),
                },
                Predicate::GreaterI64 {
                    column: "payload.length",
                    value: 120,
                },
            ],
        ),
        Profile::Telemetry => (
            "payload.reading",
            "payload.region",
            vec![
                Predicate::EqStr {
                    column: "event_type",
                    value: "metric.fault".to_owned(),
                },
                Predicate::GreaterI64 {
                    column: "payload.error_code",
                    value: 0,
                },
            ],
        ),
    };
    let occurred = dataset.i64s("occurred_at");
    let low = occurred[dataset.rows * 40 / 100];
    let high = occurred[dataset.rows * 41 / 100];
    vec![
        Query {
            fold: all,
            name: "scan_all_columns",
            predicates: vec![],
        },
        Query {
            fold: vec!["occurred_at"],
            name: "scan_one_number",
            predicates: vec![],
        },
        Query {
            fold: vec![number],
            name: "scan_one_payload_number",
            predicates: vec![],
        },
        Query {
            fold: vec![text],
            name: "scan_one_payload_text",
            predicates: vec![],
        },
        Query {
            fold: vec!["occurred_at", number],
            name: "filter_type_and_payload",
            predicates: filter,
        },
        Query {
            fold: vec!["occurred_at", number],
            name: "time_window_1pct",
            predicates: vec![Predicate::RangeI64 {
                column: "occurred_at",
                high,
                low,
            }],
        },
    ]
}

/// The entity lookups: each looks up one entity and totals its events' timestamps.
pub fn lookups(dataset: &Dataset) -> Vec<Query> {
    let entities = dataset.strs("entity_id");
    (0..20)
        .map(|i| {
            let entity = entities[(dataset.rows / 21) * (i + 1)].clone();
            Query {
                fold: vec!["occurred_at"],
                name: "entity_lookup",
                predicates: vec![Predicate::EqStr {
                    column: "entity_id",
                    value: entity,
                }],
            }
        })
        .collect()
}
