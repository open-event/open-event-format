use std::fmt;

#[derive(Clone, PartialEq, Eq)]
pub struct StringColumn {
    offsets: Vec<u64>,
    present: Vec<bool>,
    text: String,
}

impl StringColumn {
    pub fn new() -> Self {
        Self {
            offsets: vec![0],
            present: Vec::new(),
            text: String::new(),
        }
    }

    pub fn with_capacity(values: usize, text_bytes: usize) -> Self {
        let mut offsets = Vec::with_capacity(values + 1);
        offsets.push(0);
        Self {
            offsets,
            present: Vec::with_capacity(values),
            text: String::with_capacity(text_bytes),
        }
    }

    pub(crate) fn from_parts(offsets: Vec<u64>, present: Vec<bool>, text: String) -> Self {
        debug_assert_eq!(offsets.len(), present.len() + 1);
        Self { offsets, present, text }
    }

    pub fn push(&mut self, value: Option<&str>) {
        if let Some(text) = value {
            self.text.push_str(text);
        }
        self.offsets.push(self.text.len() as u64);
        self.present.push(value.is_some());
    }

    pub fn len(&self) -> usize {
        self.present.len()
    }

    pub fn is_empty(&self) -> bool {
        self.present.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<Option<&str>> {
        if !self.present.get(index).copied()? {
            return Some(None);
        }
        Some(self.span(index))
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = Option<&str>> + '_ {
        self.present
            .iter()
            .zip(self.offsets.windows(2))
            .map(move |(&present, span)| {
                let [start, end] = span else { return None };
                present.then(|| self.text.get(*start as usize..*end as usize)).flatten()
            })
    }

    pub(crate) fn presence(&self) -> &[bool] {
        &self.present
    }

    pub fn iter_present(&self) -> impl Iterator<Item = &str> + '_ {
        self.iter().flatten()
    }

    pub fn null_count(&self) -> usize {
        self.present.iter().filter(|present| !**present).count()
    }

    pub fn text_len(&self) -> usize {
        self.text.len()
    }

    pub fn slice(&self, start: usize, end: usize) -> Self {
        let end = end.min(self.len());
        if start >= end {
            return Self::new();
        }
        let Some(offsets) = self.offsets.get(start..=end) else {
            return Self::new();
        };
        let Some(&text_start) = offsets.first() else {
            return Self::new();
        };
        let text_end = offsets.last().copied().unwrap_or(text_start);
        let text = self
            .text
            .get(text_start as usize..text_end as usize)
            .unwrap_or("")
            .to_owned();
        let offsets = offsets.iter().map(|offset| offset - text_start).collect();
        let present = self.present.get(start..end).unwrap_or(&[]).to_vec();
        Self { offsets, present, text }
    }

    pub fn append(&mut self, other: &Self) {
        if other.is_empty() {
            return;
        }
        self.offsets.reserve(other.len());
        self.present.reserve(other.len());
        self.text.reserve(other.text_len());
        let base = self.text.len() as u64;
        self.text.push_str(&other.text);
        self.offsets
            .extend(other.offsets.get(1..).unwrap_or(&[]).iter().map(|offset| offset + base));
        self.present.extend_from_slice(&other.present);
    }

    fn span(&self, index: usize) -> Option<&str> {
        let start = self.offsets.get(index).copied()? as usize;
        let end = self.offsets.get(index + 1).copied()? as usize;
        self.text.get(start..end)
    }
}

impl Default for StringColumn {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for StringColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<T: AsRef<str>> FromIterator<Option<T>> for StringColumn {
    fn from_iter<I: IntoIterator<Item = Option<T>>>(values: I) -> Self {
        let values = values.into_iter();
        let mut column = Self::with_capacity(values.size_hint().0, 0);
        for value in values {
            column.push(value.as_ref().map(AsRef::as_ref));
        }
        column
    }
}

impl<T: AsRef<str>> From<Vec<Option<T>>> for StringColumn {
    fn from(values: Vec<Option<T>>) -> Self {
        values.into_iter().collect()
    }
}

#[cfg(test)]
#[path = "test/string_column.rs"]
mod tests;
