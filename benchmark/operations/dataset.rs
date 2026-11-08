//! Generates the one dataset every format is measured over, and picks the queries run against it.
//!
//! The rows are produced from the spec's seed by a fixed integer mixer rather than a random-number library, so the
//! same spec yields the identical dataset on every host and in every process — which is what lets a committed report
//! be compared against a later run rather than only against itself.

use crate::constant::*;
use crate::model::{BenchmarkRow, BenchmarkSpec};

/// The generated rows plus the queries the comparison runs against them.
pub struct Dataset {
    pub rows: Vec<BenchmarkRow>,
    pub workload: Workload,
}

/// The fixed queries every format answers.
///
/// Each one is derived from the generated rows, so its expected result is known before either format is asked: the
/// entity id belongs to exactly one row, the time window covers a known slice, and the substring was planted at a
/// known spacing.
pub struct Workload {
    /// The substring the absent free-text search looks for. No generated row carries it.
    pub absent_needle: String,
    /// Row ordinals that differ between the two versions the diff compares. Contiguous and at the end of the file —
    /// the shape of a late-arriving amendment, and the case where a format that can rule a whole region out without
    /// reading it has something to prove.
    pub diff_ordinals: Vec<u64>,
    /// The entity id the point lookup asks for.
    pub entity_id: String,
    /// The event type the equality search counts.
    pub event_type: String,
    /// Row ordinals an update or a delete touches. Spread evenly across the file, so a rewrite has to touch every
    /// group rather than a corner of one.
    pub mutated_ordinals: Vec<u64>,
    /// The substring the free-text search looks for.
    pub needle: String,
    /// The substring the rare free-text search looks for, carried by a handful of rows.
    pub rare_needle: String,
    pub occurred_at_hi: i64,
    pub occurred_at_lo: i64,
    /// The payload field the single-field probe reads.
    pub payload_field: String,
    /// Row ordinals the single-field payload probe reads, ascending and spread across the whole file.
    pub sample_ordinals: Vec<u64>,
}

/// Mixes one counter value into a well-spread 64-bit value. This is SplitMix64's finalizer: a fixed, dependency-free
/// bit mixer, used here only so field values look unrelated to each other and to the row index.
fn mix(value: u64) -> u64 {
    let mut z = value.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The 64-bit join-and-filter form of an identity string. OE stores one of these beside every identity column, and
/// every format's file carries the same value, so a point lookup compares integers everywhere rather than one
/// format comparing integers and the other comparing strings.
///
/// Folded a word at a time rather than a byte at a time: it runs several times per row on every write path, and as
/// harness cost it has no need to be stable across releases.
pub fn identity_hash(value: &str) -> u64 {
    let (words, tail) = value.as_bytes().as_chunks::<8>();
    let folded = words
        .iter()
        .fold(0xcbf2_9ce4_8422_2325, |acc, word| mix(acc ^ u64::from_le_bytes(*word)));
    // The length shares the final word with the tail, so a value and the same value with a NUL in front of its last
    // few bytes never hash alike.
    let tail = tail.iter().fold(0u64, |acc, byte| (acc << 8) | u64::from(*byte));
    mix(folded ^ tail ^ ((value.len() as u64) << 56))
}

/// Picks one of `choices` for row `index`, salted by `field` so two columns of the same width do not move together.
fn pick<'a>(choices: &[&'a str], seed: u64, index: u64, field: u64) -> &'a str {
    if choices.is_empty() {
        return "";
    }
    let slot = mix(seed ^ mix(index).wrapping_add(field)) as usize % choices.len();
    choices.get(slot).copied().unwrap_or_default()
}

/// Builds the dataset the spec describes.
pub fn generate(spec: &BenchmarkSpec) -> Dataset {
    let rows: Vec<BenchmarkRow> = (0..spec.row_count as u64).map(|index| row(spec.seed, index)).collect();
    let workload = workload(spec, &rows);
    Dataset { rows, workload }
}

/// Builds one row. Row `index` always comes out the same for a given seed, independent of how many rows are generated
/// around it, so a 10-row run and a 10-million-row run share their first ten rows.
fn row(seed: u64, index: u64) -> BenchmarkRow {
    // Jittered inside its step rather than landing exactly on it. A perfectly regular timeline is a single constant
    // delta, which every format's integer encoder crushes to almost nothing — a compression result that says more about
    // the generator than about either format. The jitter is smaller than the step, so rows still arrive in time order
    // and a granule's recorded time range stays tight enough to prune on.
    let occurred_at_nanos = OCCURRED_AT_ORIGIN_NANOS
        + index as i64 * OCCURRED_AT_STEP_NANOS
        + (mix(seed.wrapping_add(8) ^ index) % OCCURRED_AT_STEP_NANOS as u64) as i64;
    let note = if index
        .checked_sub(RARE_NEEDLE_FIRST_ROW)
        .is_some_and(|since_first| since_first.is_multiple_of(RARE_NEEDLE_EVERY))
    {
        format!("message {index} shares the {RARE_NEEDLE} with the whole room")
    } else if index.is_multiple_of(NEEDLE_EVERY) {
        format!("message {index} links the {NEEDLE} in a reply")
    } else {
        format!("message {index} says hello and moves on")
    };
    BenchmarkRow {
        actor_id: (!index.is_multiple_of(3)).then(|| format!("actor-{}", mix(seed ^ index) % 512)),
        char_count: (mix(seed.wrapping_add(1) ^ index) % 1_000_000) as i64,
        client: pick(CLIENTS, seed, index, 6).to_owned(),
        // Deliberately uncorrelated with row order: a sorted entity id would let per-column zone maps prune the point
        // lookup to a single group, and the benchmark would measure that head start rather than the scan itself.
        entity_id: format!("ent-{:016x}", mix(seed.wrapping_add(2) ^ index)),
        entity_type: pick(ENTITY_TYPES, seed, index, 2).to_owned(),
        // Kept inside 127 bits. The writer records a u128 column's bounds by converting them to `i128`, and a value
        // past `i128::MAX` drops one bound while keeping the other — a footer the reader then refuses to open.
        event_id: u128::from(mix(seed.wrapping_add(3) ^ index)) << 63 | u128::from(index),
        event_type: pick(EVENT_TYPES, seed, index, 4).to_owned(),
        ingested_at_nanos: occurred_at_nanos + INGEST_LAG_NANOS,
        kind: pick(KINDS, seed, index, 5).to_owned(),
        note,
        occurred_at_nanos,
        reactions: (!index.is_multiple_of(REACTIONS_ABSENT_EVERY)).then_some((index % 5) as i64),
        sequence: index + 1,
        source: pick(SOURCES, seed, index, 7).to_owned(),
        stream_sequence: index,
    }
}

/// Derives the queries from the generated rows.
///
/// The time window is `range_percent` of the timeline, centred: at the default tenth every format keeps several blocks
/// and prune the rest, so pruning is what decides the cost; widening it walks that advantage away.
fn workload(spec: &BenchmarkSpec, rows: &[BenchmarkRow]) -> Workload {
    let row_count = rows.len() as u64;
    // Centred on the middle of the timeline, so widening it grows symmetrically rather than sliding towards one end
    // and turning the measurement into a question about where the window sits.
    let half = spec.range_percent.min(100) as u64 * row_count / 200;
    let window_start = row_count / 2 - half;
    let window_end = row_count / 2 + half;
    let mutated_count = spec.mutated_row_count().max(1) as u64;
    // Spread the mutated rows across the file rather than clustering them, so an update touches many groups and both
    // formats face the same rewrite fan-out.
    let mutation_stride = (row_count / mutated_count).max(1);
    let sample_stride = (row_count / PAYLOAD_PROBE_ROWS as u64).max(1);
    Workload {
        diff_ordinals: (row_count.saturating_sub(mutated_count)..row_count).collect(),
        entity_id: rows
            .get(row_count as usize / 2)
            .map(|row| row.entity_id.clone())
            .unwrap_or_default(),
        // The rarest of the generated event types, so the equality search is a needle rather than half the file.
        event_type: EVENT_TYPES.last().copied().unwrap_or_default().to_owned(),
        mutated_ordinals: (0..mutated_count).map(|n| n * mutation_stride).collect(),
        absent_needle: ABSENT_NEEDLE.to_owned(),
        needle: NEEDLE.to_owned(),
        rare_needle: RARE_NEEDLE.to_owned(),
        occurred_at_hi: OCCURRED_AT_ORIGIN_NANOS + window_end as i64 * OCCURRED_AT_STEP_NANOS,
        occurred_at_lo: OCCURRED_AT_ORIGIN_NANOS + window_start as i64 * OCCURRED_AT_STEP_NANOS,
        payload_field: FIELD_CLIENT.to_owned(),
        sample_ordinals: (0..PAYLOAD_PROBE_ROWS as u64)
            .map(|n| n * sample_stride)
            .filter(|ordinal| *ordinal < row_count)
            .collect(),
    }
}

/// Returns the dataset with `ordinals` amended: `char_count` bumped and `note` rewritten.
///
/// Every format applies this same change, so the rewritten files hold identical data and the diff over them has the
/// same answer on either side.
pub fn amended(rows: &[BenchmarkRow], ordinals: &[u64]) -> Vec<BenchmarkRow> {
    let mut amended = rows.to_vec();
    for ordinal in ordinals {
        let Some(row) = amended.get_mut(*ordinal as usize) else {
            continue;
        };
        row.char_count += 1;
        row.note = format!("message {ordinal} edited by its sender");
    }
    amended
}

/// Returns the dataset with `ordinals` removed, in the same order.
pub fn without(rows: &[BenchmarkRow], ordinals: &[u64]) -> Vec<BenchmarkRow> {
    let dropped: std::collections::BTreeSet<u64> = ordinals.iter().copied().collect();
    rows.iter()
        .enumerate()
        .filter(|(index, _)| !dropped.contains(&(*index as u64)))
        .map(|(_, row)| row.clone())
        .collect()
}

#[cfg(test)]
#[path = "test/dataset.rs"]
mod tests;
