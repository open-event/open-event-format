
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
    pub last_n: Option<usize>,
    pub name: &'static str,
    pub predicates: Vec<Predicate>,
}

pub enum Col<'a> {
    F64(&'a [f64]),
    I64(&'a [i64]),
    Str(Vec<&'a str>),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Partial {
    pub rows: u64,
    pub sums: Vec<i128>,
    pub tail: Vec<i64>,
}

impl Partial {
    pub fn add(&mut self, other: &Partial, last_n: Option<usize>) {
        self.rows += other.rows;
        self.tail.extend_from_slice(&other.tail);
        if let Some(n) = last_n {
            let excess = self.tail.len().saturating_sub(n);
            self.tail.drain(..excess);
        }
        if self.sums.len() < other.sums.len() {
            self.sums.resize(other.sums.len(), 0);
        }
        for (total, value) in self.sums.iter_mut().zip(&other.sums) {
            *total += value;
        }
    }
}

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
        tail: Vec::new(),
    };
    if let Some(n) = query.last_n {
        let Col::I64(times) = find("occurred_at") else {
            panic!("occurred_at must be an integer column")
        };
        let matching: Vec<i64> = times.iter().zip(&keep).filter(|(_, k)| **k).map(|(t, _)| *t).collect();
        partial.tail = matching[matching.len().saturating_sub(n)..].to_vec();
    }
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
        Profile::Chat => vec![
            "event_id", "occurred_at", "entity_id", "event_type", "source", "payload.body", "payload.length",
            "payload.msgtype", "payload.sender",
        ],
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
        Profile::Chat => (
            "payload.length",
            "payload.body",
            vec![
                Predicate::EqStr {
                    column: "event_type",
                    value: "m.room.redaction".to_owned(),
                },
                Predicate::GreaterI64 {
                    column: "payload.length",
                    value: -1,
                },
            ],
        ),
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
            last_n: None,
            name: "scan_all_columns",
            predicates: vec![],
        },
        Query {
            fold: vec!["occurred_at"],
            last_n: None,
            name: "scan_one_number",
            predicates: vec![],
        },
        Query {
            fold: vec![number],
            last_n: None,
            name: "scan_one_payload_number",
            predicates: vec![],
        },
        Query {
            fold: vec![text],
            last_n: None,
            name: "scan_one_payload_text",
            predicates: vec![],
        },
        Query {
            fold: vec!["occurred_at", number],
            last_n: None,
            name: "filter_type_and_payload",
            predicates: filter,
        },
        Query {
            fold: vec!["occurred_at", number],
            last_n: None,
            name: "time_window_1pct",
            predicates: vec![Predicate::RangeI64 {
                column: "occurred_at",
                high,
                low,
            }],
        },
    ]
}

pub fn lookups(dataset: &Dataset) -> Vec<Query> {
    let entities = dataset.strs("entity_id");
    let eq = |column: &'static str, value: &str| Predicate::EqStr {
        column,
        value: value.to_owned(),
    };
    let mut out: Vec<Query> = (0..20)
        .map(|i| Query {
            fold: vec!["occurred_at"],
            last_n: None,
            name: "entity_lookup",
            predicates: vec![eq("entity_id", &entities[(dataset.rows / 21) * (i + 1)])],
        })
        .collect();
    if dataset.profile != Profile::Chat {
        return out;
    }
    let roots = dataset.strs("thread_root");
    let ids = dataset.strs("event_id");
    for i in 0..20 {
        let row = (dataset.rows / 21) * (i + 1);
        out.push(Query {
            fold: vec!["occurred_at"],
            last_n: Some(50),
            name: "latest_50_in_room",
            predicates: vec![eq("entity_id", &entities[row]), eq("event_type", "m.room.message")],
        });
    }
    let threaded: Vec<&String> = {
        let mut seen = std::collections::BTreeSet::new();
        roots.iter().filter(|r| !r.is_empty() && seen.insert(r.as_str())).collect()
    };
    for i in 0..20 {
        let root = threaded[(threaded.len() / 21).max(1) * (i + 1) % threaded.len()];
        out.push(Query {
            fold: vec!["occurred_at"],
            last_n: None,
            name: "thread_replies",
            predicates: vec![eq("thread_root", root)],
        });
    }
    for i in 0..20 {
        out.push(Query {
            fold: vec!["occurred_at"],
            last_n: None,
            name: "event_by_id",
            predicates: vec![eq("event_id", &ids[(dataset.rows / 21) * (i + 1)])],
        });
    }
    out
}
