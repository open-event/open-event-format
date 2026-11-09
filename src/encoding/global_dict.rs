use super::predicate::StringPredicate;
use std::{
    cmp::Reverse,
    collections::{BTreeSet, BinaryHeap},
};

pub struct GlobalDictionary {
    entries: Vec<String>,
}

pub struct GlobalCodeFilter {
    absent_local_matches: bool,
    kind: FilterKind,
}

enum FilterKind {
    NotEqualsAbsent,
    AcceptsNone,
    ExcludeCode(u64),
    Range { hi: u64, lo: u64 },
    Set(Vec<u64>),
}

impl GlobalDictionary {
    pub fn from_sorted(entries: Vec<String>) -> Self {
        Self { entries }
    }

    pub fn from_file_alphabets<'a>(alphabets: impl IntoIterator<Item = &'a [String]>) -> Self {
        let alphabets: Vec<&[String]> = alphabets.into_iter().collect();
        if !alphabets.iter().all(|alphabet| alphabet.is_sorted()) {
            let merged: BTreeSet<&str> = alphabets.into_iter().flatten().map(String::as_str).collect();
            return Self::from_sorted(merged.into_iter().map(str::to_owned).collect());
        }
        let mut heap: BinaryHeap<Reverse<(&str, usize, usize)>> = alphabets
            .iter()
            .enumerate()
            .filter_map(|(which, alphabet)| alphabet.first().map(|value| Reverse((value.as_str(), which, 0))))
            .collect();
        let mut entries: Vec<String> = Vec::new();
        while let Some(Reverse((value, which, cursor))) = heap.pop() {
            if entries.last().map(String::as_str) != Some(value) {
                entries.push(value.to_owned());
            }
            if let Some(next) = alphabets.get(which).and_then(|alphabet| alphabet.get(cursor + 1)) {
                heap.push(Reverse((next.as_str(), which, cursor + 1)));
            }
        }
        Self::from_sorted(entries)
    }

    pub fn code_for(&self, value: &str) -> Option<u64> {
        self.entries
            .binary_search_by_key(&value, |e| e.as_str())
            .ok()
            .map(|i| i as u64)
    }

    pub fn translate(&self, predicate: &StringPredicate) -> GlobalCodeFilter {
        let kind = match predicate {
            StringPredicate::Contains(matcher) => FilterKind::Set(
                self.entries
                    .iter()
                    .enumerate()
                    .filter(|(_, entry)| matcher.matches(entry))
                    .map(|(index, _)| index as u64)
                    .collect(),
            ),
            StringPredicate::Equals(needle) => match self.code_for(needle) {
                Some(i) => FilterKind::Range { lo: i, hi: i + 1 },
                None => FilterKind::AcceptsNone,
            },
            StringPredicate::NotEquals(needle) => match self.code_for(needle) {
                Some(i) => FilterKind::ExcludeCode(i),
                None => FilterKind::NotEqualsAbsent,
            },
            StringPredicate::InSet(needles) => {
                let mut codes: Vec<u64> = needles.iter().filter_map(|n| self.code_for(n)).collect();
                codes.sort_unstable();
                codes.dedup();
                FilterKind::Set(codes)
            }
            StringPredicate::Range { lower, upper } => {
                let lo = match lower {
                    Some(b) if b.inclusive => self.entries.partition_point(|e| e.as_str() < b.value.as_str()),
                    Some(b) => self.entries.partition_point(|e| e.as_str() <= b.value.as_str()),
                    None => 0,
                };
                let hi = match upper {
                    Some(b) if b.inclusive => self.entries.partition_point(|e| e.as_str() <= b.value.as_str()),
                    Some(b) => self.entries.partition_point(|e| e.as_str() < b.value.as_str()),
                    None => self.entries.len(),
                };
                if lo >= hi {
                    FilterKind::AcceptsNone
                } else {
                    FilterKind::Range {
                        lo: lo as u64,
                        hi: hi as u64,
                    }
                }
            }
        };
        let absent_local_matches = match predicate {
            StringPredicate::Contains(_) => true,
            StringPredicate::Equals(needle) => self.code_for(needle).is_none(),
            StringPredicate::InSet(needles) => needles.iter().any(|needle| self.code_for(needle).is_none()),
            StringPredicate::NotEquals(_) => true,
            StringPredicate::Range { .. } => true,
        };
        GlobalCodeFilter {
            absent_local_matches,
            kind,
        }
    }

    pub fn local_to_global_map(&self, local_entries: &[String]) -> Vec<Option<u64>> {
        let sorted = local_entries
            .iter()
            .zip(local_entries.iter().skip(1))
            .all(|(a, b)| a <= b);
        if !sorted {
            return local_entries.iter().map(|e| self.code_for(e.as_str())).collect();
        }
        let mut from = 0;
        local_entries
            .iter()
            .map(|value| {
                let rest = self.entries.get(from..).unwrap_or(&[]);
                let mut step = 1;
                while rest.get(step - 1).is_some_and(|entry| entry < value) {
                    step = step.saturating_mul(2);
                }
                let window = rest.get(..step.min(rest.len())).unwrap_or(rest);
                let offset = window.partition_point(|entry| entry < value);
                from += offset;
                rest.get(offset).filter(|entry| *entry == value).map(|_| from as u64)
            })
            .collect()
    }
}

impl GlobalCodeFilter {
    pub fn matches_global_code(&self, code: u64) -> bool {
        match &self.kind {
            FilterKind::NotEqualsAbsent => true,
            FilterKind::AcceptsNone => false,
            FilterKind::ExcludeCode(excluded) => code != *excluded,
            FilterKind::Range { hi, lo } => code >= *lo && code < *hi,
            FilterKind::Set(codes) => codes.binary_search(&code).is_ok(),
        }
    }

    pub fn matches_local_code(&self, local_code: u64, local_to_global: &[Option<u64>]) -> bool {
        match local_to_global.get(local_code as usize) {
            Some(&Some(global_code)) => self.matches_global_code(global_code),
            Some(&None) => self.absent_local_matches,
            _ => false,
        }
    }
}

#[cfg(test)]
#[path = "test/global_dict.rs"]
mod tests;
