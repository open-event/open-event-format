
use crate::constant::*;
use crate::model::{BenchmarkRow, BenchmarkSpec};

pub struct Dataset {
    pub rows: Vec<BenchmarkRow>,
    pub workload: Workload,
}

pub struct Workload {
    pub absent_needle: String,
    pub diff_ordinals: Vec<u64>,
    pub entity_id: String,
    pub event_type: String,
    pub mutated_ordinals: Vec<u64>,
    pub needle: String,
    pub rare_needle: String,
    pub occurred_at_hi: i64,
    pub occurred_at_lo: i64,
    pub payload_field: String,
    pub sample_ordinals: Vec<u64>,
}

fn mix(value: u64) -> u64 {
    let mut z = value.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

pub fn identity_hash(value: &str) -> u64 {
    let (words, tail) = value.as_bytes().as_chunks::<8>();
    let folded = words
        .iter()
        .fold(0xcbf2_9ce4_8422_2325, |acc, word| mix(acc ^ u64::from_le_bytes(*word)));
    let tail = tail.iter().fold(0u64, |acc, byte| (acc << 8) | u64::from(*byte));
    mix(folded ^ tail ^ ((value.len() as u64) << 56))
}

fn pick<'a>(choices: &[&'a str], seed: u64, index: u64, field: u64) -> &'a str {
    if choices.is_empty() {
        return "";
    }
    let slot = mix(seed ^ mix(index).wrapping_add(field)) as usize % choices.len();
    choices.get(slot).copied().unwrap_or_default()
}

pub fn generate(spec: &BenchmarkSpec) -> Dataset {
    let rows: Vec<BenchmarkRow> = (0..spec.row_count as u64).map(|index| row(spec.seed, index)).collect();
    let workload = workload(spec, &rows);
    Dataset { rows, workload }
}

fn row(seed: u64, index: u64) -> BenchmarkRow {
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
        entity_id: format!("ent-{:016x}", mix(seed.wrapping_add(2) ^ index)),
        entity_type: pick(ENTITY_TYPES, seed, index, 2).to_owned(),
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

fn workload(spec: &BenchmarkSpec, rows: &[BenchmarkRow]) -> Workload {
    let row_count = rows.len() as u64;
    let half = spec.range_percent.min(100) as u64 * row_count / 200;
    let window_start = row_count / 2 - half;
    let window_end = row_count / 2 + half;
    let mutated_count = spec.mutated_row_count().max(1) as u64;
    let mutation_stride = (row_count / mutated_count).max(1);
    let sample_stride = (row_count / PAYLOAD_PROBE_ROWS as u64).max(1);
    Workload {
        diff_ordinals: (row_count.saturating_sub(mutated_count)..row_count).collect(),
        entity_id: rows
            .get(row_count as usize / 2)
            .map(|row| row.entity_id.clone())
            .unwrap_or_default(),
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
