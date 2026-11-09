#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataAnswer {
    AllRowsMatch,
    MustDecode,
    NoRowsMatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnFlags {
    pub all_null: bool,
    pub constant_value: Option<i128>,
}

impl ColumnFlags {
    pub fn mixed() -> Self {
        Self {
            all_null: false,
            constant_value: None,
        }
    }

    pub fn all_null() -> Self {
        Self {
            all_null: true,
            constant_value: None,
        }
    }

    pub fn constant(value: i128) -> Self {
        Self {
            all_null: false,
            constant_value: Some(value),
        }
    }

    pub fn from_stats(row_count: u32, null_count: u32, min_i128: Option<i128>, max_i128: Option<i128>) -> Self {
        if row_count > 0 && null_count == row_count {
            return Self::all_null();
        }
        if let (0, Some(min), Some(max)) = (null_count, min_i128, max_i128)
            && min == max
            && row_count > 0
        {
            return Self::constant(min);
        }
        Self::mixed()
    }
}

pub fn answer_equality_from_flags(flags: &ColumnFlags, target: i128) -> MetadataAnswer {
    if flags.all_null {
        return MetadataAnswer::NoRowsMatch;
    }
    match flags.constant_value {
        Some(v) if v == target => MetadataAnswer::AllRowsMatch,
        Some(_) => MetadataAnswer::NoRowsMatch,
        None => MetadataAnswer::MustDecode,
    }
}

pub fn answer_presence_from_flags(flags: &ColumnFlags) -> MetadataAnswer {
    if flags.all_null {
        MetadataAnswer::NoRowsMatch
    } else {
        MetadataAnswer::MustDecode
    }
}

#[cfg(test)]
#[path = "test/constant_flags.rs"]
mod tests;
