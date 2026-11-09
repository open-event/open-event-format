pub fn clustering_preserves_sequence_coverage(rows: &[(u64, u32)]) -> bool {
    let mut iter = rows.iter();
    let Some(&(_, first_bucket)) = iter.next() else {
        return true;
    };
    let mut seen = hashbrown::HashSet::new();
    let mut last_bucket = first_bucket;
    seen.insert(last_bucket);
    for &(_seq, bucket) in iter {
        if bucket != last_bucket {
            if !seen.insert(bucket) {
                return false;
            }
            last_bucket = bucket;
        }
    }
    true
}

#[cfg(test)]
#[path = "test/clustering.rs"]
mod tests;
