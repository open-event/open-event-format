use super::{Exactness, Granularity, SkipIndexKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateShape {
    Equality,
    Membership,
    RangeBound,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SkipIndex {
    pub block_ref: u64,
    pub column_id: u32,
    pub exactness: Exactness,
    pub fpr: Option<f64>,
    pub granularity: Granularity,
    pub index_id: u32,
    pub kind: SkipIndexKind,
    pub projection_id: u32,
    pub row_range: RowRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRange {
    pub first_row_ordinal: u64,
    pub row_count: u64,
}

impl SkipIndexKind {
    pub fn answers(self, shape: PredicateShape) -> bool {
        match self {
            SkipIndexKind::MinMax => {
                matches!(shape, PredicateShape::Equality | PredicateShape::RangeBound)
            }
            SkipIndexKind::RangeFilter => matches!(shape, PredicateShape::RangeBound),
            SkipIndexKind::LearnedPosition => matches!(shape, PredicateShape::RangeBound),
            SkipIndexKind::AccountHashFilter
            | SkipIndexKind::BinaryFuseFilter
            | SkipIndexKind::EntityHashFilter
            | SkipIndexKind::OpportunityHashFilter
            | SkipIndexKind::RibbonFilter
            | SkipIndexKind::SplitBlockBloomFilter => {
                matches!(shape, PredicateShape::Equality | PredicateShape::Membership)
            }
            SkipIndexKind::SequenceRange | SkipIndexKind::TimeRange => {
                matches!(shape, PredicateShape::RangeBound)
            }
            SkipIndexKind::ContextLocator => {
                matches!(shape, PredicateShape::Equality | PredicateShape::Membership)
            }
            SkipIndexKind::PathPresence => matches!(shape, PredicateShape::Equality),
            SkipIndexKind::TextToken => {
                matches!(shape, PredicateShape::Equality | PredicateShape::Membership)
            }
        }
    }

    pub fn default_exactness(self) -> Exactness {
        match self {
            SkipIndexKind::LearnedPosition
            | SkipIndexKind::MinMax
            | SkipIndexKind::SequenceRange
            | SkipIndexKind::TimeRange => Exactness::Exact,
            SkipIndexKind::AccountHashFilter
            | SkipIndexKind::BinaryFuseFilter
            | SkipIndexKind::ContextLocator
            | SkipIndexKind::EntityHashFilter
            | SkipIndexKind::OpportunityHashFilter
            | SkipIndexKind::PathPresence
            | SkipIndexKind::RangeFilter
            | SkipIndexKind::RibbonFilter
            | SkipIndexKind::SplitBlockBloomFilter
            | SkipIndexKind::TextToken => Exactness::InexactNoFalseNegative,
        }
    }
}

pub fn choose_skip_index(shape: PredicateShape, available: &[SkipIndexKind]) -> Option<SkipIndexKind> {
    let mut best: Option<SkipIndexKind> = None;
    for &kind in available {
        if !kind.answers(shape) {
            continue;
        }
        match best {
            None => best = Some(kind),
            Some(current) => {
                let current_exact = current.default_exactness() == Exactness::Exact;
                let candidate_exact = kind.default_exactness() == Exactness::Exact;
                if candidate_exact && !current_exact {
                    best = Some(kind);
                }
            }
        }
    }
    best
}

pub fn pushdown_residual_required(kind: SkipIndexKind) -> bool {
    kind.default_exactness().requires_residual_filter()
}

#[cfg(test)]
#[path = "test/model.rs"]
mod tests;
