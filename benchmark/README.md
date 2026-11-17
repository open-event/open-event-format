# OE benchmark

Compares OE with Parquet, Vortex and JSON on the same events, including the things a chat app needs from an event
store (latest messages in a room, threads, lookup by event id, redaction, durable ingest). Results are in
`results/results.md` (tables) and `results/results.json` (raw numbers).

Run it with `just bench-compare` from the repository root (see the `Justfile`), or from this directory:

```bash
cargo run --release --bin oe-benchmark -- --sizes 10000,100000,1000000 --reps 5 --write-reps 3 --out results
```

Other options: `--profiles chat,telemetry,messages`, `--formats oe,parquet`, `--scratch <dir>` (where the durable-ingest
test writes; use a directory on the disk you want to test) and `--skip-ingest yes`.

## What is measured

- **Data:** three deterministic datasets at 10,000, 100,000 and 1,000,000 events. `chat` is shaped like messaging
  traffic: a few very busy rooms and many quiet ones, bursts of activity, a quarter of messages replying into a
  thread, and edits, reactions and redactions alongside messages. `telemetry` (numeric payload) and `messages` (text
  payload) are plain analytical datasets.
- **Formats:** OE (opened with a full integrity check, and with lazy open); Parquet (zstd level 3, 131,072-row
  groups, min/max statistics); Vortex (default compressor); newline-delimited JSON, plain and with zstd level 3.
- **Scans and filters:** scan every column, scan one number, one payload number, one payload text column, filter on
  event type plus a payload field, and a 1% time window.
- **Lookups (median of 20, each timed on its own):** all events of one entity, plus for `chat` the latest 50 messages
  in a room, every reply in a thread, and one event by its id. OE uses its granule filters, its thread (root
  reference) lookup and its external-id index for these; the other formats filter columns.
- **Open:** time to open the file, and time to open a fresh copy and answer the first lookup (what a cold reader
  pays).
- **Redaction:** removing one event, and removing 1% of events. OE records a deletion vector. Parquet, Vortex and
  JSON cannot remove rows in place, so the time is reading the whole file plus writing it again without those rows.
- **Durable ingest:** batches of 1, 16 and 128 events, each acknowledged only after the file is synced to disk. OE
  appends one journal frame per batch to one file; JSON appends lines; Parquet and Vortex write a new file per batch
  because they cannot append.
- **Correctness:** every format must return identical row counts, totals and latest-50 timestamps for every query, or
  the run stops.

## Reading the numbers

- Everything runs on one thread with the file held in memory, so disk speed is not measured.
- OE stores more than the other files: all envelope columns (hashes, sequence numbers, ingest time), skip indexes and
  integrity checksums. Its larger size and slower write reflect that.
- OE opens with a full integrity check of the file. The other formats do not verify content on open.
- OE prunes granules by time window and entity hash. Parquet prunes row groups by min/max statistics. Vortex pushes
  filters into its scan. JSON has no way to skip data, so every query parses every line (with `serde_json::Value`).
- OE stores event ids as 16-byte values; the other formats store them as 32-character hex text. OE also keeps an
  external-id index (needed to find events to redact) and, in `chat`, thread references; the all-columns scan skips the thread column in every format.
- Parquet bloom filters, page indexes and Vortex layout tuning are left at their defaults.

## What it does not cover

- Reads that combine recent journal data with published files (OE's live overlay), late and out-of-order events,
  corrections, and the encryption and erasure features (crypto-shredding, subject-sealed payloads, sealed footers).
- Concurrency, cold reads from disk or an object store, and file layouts that cluster by room. Parquet and Vortex
  files could be sorted by room to speed up room queries; these files are in time order.
- The durable-ingest test is only as honest as the disk's sync. On virtual or cloud disks a sync can be much cheaper
  than on a physical drive, so use `--scratch` with a real device before drawing conclusions.
- OE's journal pads each frame to an aligned size (4 KiB minimum), so one-event batches use far more disk than the
  other formats; the cost falls as batches grow.

## Vector search benchmark

`vector/` (the `oe-vector-bench` binary) measures recall@10 and latency of OE's vector search against exact f32 brute
force. Run it with `just bench-vector` or:

```bash
cargo run --release --bin oe-vector-bench -- --sizes 10000,100000 --dims 128,768 --queries 100 --out results
```

`--large yes` adds 1,000,000 rows; `--oversample N` changes the rerank patience (minimum run, `k × N`). Results: `results/vector.md` (every
dataset, configuration and workload) and `results/vector.json`.

- **Data (seeded, deterministic):** unit vectors around ⌈√rows / 2⌉ random topic centres (cosine), 10,000 and
  100,000 rows at 128 and 768 dimensions, written in time order (topics mixed in every sub-block) and in cluster
  order; each row is in one of 100 rooms. 100 queries drawn around random centres.
- **Workloads:** top-10; top-10 within one room (~1% of rows) and within 50 rooms (~50%); a range query (every row
  above a per-query threshold set halfway between the 30th and 31st exact score, recall of the 30); MaxSim top-10
  over up to 2,000 multivector documents of 4–12 tokens (d = 128, 4-token queries).
- **Configurations:** exact f32 brute force over in-memory vectors (ground truth; no decryption); scan (default
  coarse code — RaBitQ U1 at d ≥ 64 — plus F32 rerank, sealed sub-blocks); indexed (partition index, default
  `PartitionParams`, `nprobes` 8); and for time-ordered data a clustered merge followed by a scan or an indexed search.
  Defaults throughout (`SegmentSpec::new`, `DEFAULT_OVERSAMPLE`, `SearchOptions::default`).
- **Reported:** recall@10, p50/p99 per-query latency (one thread, segment in memory, every opened block decrypted),
  blocks opened and rows reranked per query, segment bytes per vector (+ partition index), and build time (segment
  write, plus clustered merge where used, + index build).
- **Acceptance gate:** `cargo test --release` (about 15 s) runs 10,000 × 128, 100,000 × 128 and 5,000 × 768 in both
  orders and a small MaxSim set, and fails if any default-settings recall@10 is below 0.95 or if, on cluster-ordered
  data or after a clustered merge, the indexed search does not open fewer blocks than a scan (top-10 and range).

Top-10 and MaxSim (single thread; full table in `results/vector.md`):

| Dataset | Configuration | Recall@10 | p50 ms | p99 ms | Blocks opened | Rows reranked | Bytes/vector (+ index) | Build ms (segment + index) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 10000 × 128 time-ordered | exact (f32 brute force) | 1.000 | 0.721 | 0.939 | – | – | 512 | – |
| 10000 × 128 time-ordered | scan | 1.000 | 2.741 | 3.666 | 153.5 | 197 | 567 | 63 + 0 |
| 10000 × 128 time-ordered | indexed (IVF) | 1.000 | 2.675 | 4.127 | 153.3 | 197 | 567 + 6 | 63 + 47 |
| 10000 × 128 time-ordered | clustered merge + scan | 1.000 | 0.526 | 0.610 | 50.3 | 206 | 568 | 165 + 0 |
| 10000 × 128 time-ordered | clustered merge + indexed | 1.000 | 0.399 | 0.482 | 13.3 | 198 | 568 + 6 | 165 + 41 |
| 10000 × 128 cluster-ordered | exact (f32 brute force) | 1.000 | 0.691 | 0.913 | – | – | 512 | – |
| 10000 × 128 cluster-ordered | scan | 1.000 | 0.556 | 0.792 | 53.1 | 204 | 567 | 30 + 0 |
| 10000 × 128 cluster-ordered | indexed (IVF) | 1.000 | 0.406 | 0.648 | 14.4 | 199 | 567 + 6 | 30 + 54 |
| 10000 × 768 time-ordered | exact (f32 brute force) | 1.000 | 5.092 | 11.115 | – | – | 3072 | – |
| 10000 × 768 time-ordered | scan | 0.999 | 13.743 | 16.788 | 154.3 | 199 | 3228 | 176 + 0 |
| 10000 × 768 time-ordered | indexed (IVF) | 0.999 | 12.224 | 16.185 | 154.1 | 199 | 3228 + 24 | 176 + 244 |
| 10000 × 768 time-ordered | clustered merge + scan | 0.999 | 1.319 | 1.548 | 50.8 | 209 | 3228 | 571 + 0 |
| 10000 × 768 time-ordered | clustered merge + indexed | 0.999 | 0.887 | 1.056 | 12.6 | 202 | 3228 + 29 | 571 + 254 |
| 10000 × 768 cluster-ordered | exact (f32 brute force) | 1.000 | 5.060 | 6.371 | – | – | 3072 | – |
| 10000 × 768 cluster-ordered | scan | 0.999 | 1.327 | 1.552 | 51.8 | 212 | 3228 | 179 + 0 |
| 10000 × 768 cluster-ordered | indexed (IVF) | 0.999 | 0.913 | 1.102 | 13.4 | 198 | 3228 + 29 | 179 + 223 |
| multivector 1000 docs / 8185 tokens × 128 | exact (f32 MaxSim) | 1.000 | 2.032 | 4.447 | – | – | 512 | – |
| multivector 1000 docs / 8185 tokens × 128 | scan | 1.000 | 5.456 | 9.359 | 123.9 | 3996 | 551 | 27 + 0 |
| multivector 1000 docs / 8185 tokens × 128 | indexed (IVF) | 1.000 | 3.932 | 7.187 | 123.8 | 4244 | 551 + 7 | 27 + 41 |
| 100000 × 128 time-ordered | exact (f32 brute force) | 1.000 | 7.279 | 10.267 | – | – | 512 | – |
| 100000 × 128 time-ordered | scan | 0.996 | 17.341 | 25.144 | 1213.1 | 609 | 568 | 418 + 0 |
| 100000 × 128 time-ordered | indexed (IVF) | 0.996 | 15.512 | 17.355 | 1187.0 | 609 | 568 + 3 | 418 + 498 |
| 100000 × 128 time-ordered | clustered merge + scan | 0.996 | 1.183 | 1.487 | 156.7 | 631 | 568 | 1396 + 0 |
| 100000 × 128 time-ordered | clustered merge + indexed | 0.996 | 0.688 | 0.968 | 25.1 | 611 | 568 + 3 | 1396 + 465 |
| 100000 × 128 cluster-ordered | exact (f32 brute force) | 1.000 | 7.399 | 14.103 | – | – | 512 | – |
| 100000 × 128 cluster-ordered | scan | 1.000 | 1.278 | 1.609 | 169.4 | 632 | 568 | 355 + 0 |
| 100000 × 128 cluster-ordered | indexed (IVF) | 1.000 | 0.706 | 0.979 | 27.8 | 614 | 568 + 3 | 355 + 531 |
| 100000 × 768 time-ordered | exact (f32 brute force) | 1.000 | 64.370 | 73.568 | – | – | 3072 | – |
| 100000 × 768 time-ordered | scan | 0.997 | 119.124 | 130.926 | 1214.2 | 608 | 3228 | 1831 + 0 |
| 100000 × 768 time-ordered | indexed (IVF) | 0.997 | 112.520 | 127.741 | 1195.4 | 608 | 3228 + 10 | 1831 + 2572 |
| 100000 × 768 time-ordered | clustered merge + scan | 0.997 | 3.971 | 5.007 | 167.4 | 630 | 3228 | 6591 + 0 |
| 100000 × 768 time-ordered | clustered merge + indexed | 0.997 | 2.032 | 2.849 | 20.8 | 615 | 3228 + 9 | 6591 + 2468 |
| 100000 × 768 cluster-ordered | exact (f32 brute force) | 1.000 | 64.741 | 71.910 | – | – | 3072 | – |
| 100000 × 768 cluster-ordered | scan | 0.995 | 3.806 | 4.367 | 164.8 | 616 | 3228 | 2176 + 0 |
| 100000 × 768 cluster-ordered | indexed (IVF) | 0.995 | 1.984 | 2.318 | 21.2 | 600 | 3228 + 9 | 2176 + 2510 |
| multivector 2000 docs / 16158 tokens × 128 | exact (f32 MaxSim) | 1.000 | 4.058 | 4.466 | – | – | 512 | – |
| multivector 2000 docs / 16158 tokens × 128 | scan | 1.000 | 9.200 | 13.596 | 264.0 | 16158 | 552 | 51 + 0 |
| multivector 2000 docs / 16158 tokens × 128 | indexed (IVF) | 1.000 | 10.807 | 14.475 | 264.0 | 17285 | 552 + 5 | 51 + 64 |

100,000 × 768, time-ordered, other workloads:

| Workload | Configuration | Recall@10 | p50 ms | p99 ms | Blocks opened | Rows reranked | Bytes/vector (+ index) | Build ms (segment + index) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| room filter 1% | exact (f32 brute force) | 1.000 | 1.848 | 2.128 | – | – | 3072 | – |
| room filter 1% | scan | 0.990 | 26.503 | 81.052 | 756.9 | 173 | 3228 | 1831 + 0 |
| room filter 1% | indexed (IVF) | 0.990 | 29.708 | 86.301 | 677.0 | 135 | 3228 + 10 | 1831 + 2572 |
| room filter 1% | clustered merge + scan | 0.990 | 22.770 | 72.142 | 682.4 | 176 | 3228 | 6591 + 0 |
| room filter 1% | clustered merge + indexed | 0.989 | 25.538 | 114.464 | 708.1 | 256 | 3228 + 9 | 6591 + 2468 |
| room filter 50% | exact (f32 brute force) | 1.000 | 39.701 | 45.676 | – | – | 3072 | – |
| room filter 50% | scan | 1.000 | 97.999 | 119.832 | 1250.6 | 627 | 3228 | 1831 + 0 |
| room filter 50% | indexed (IVF) | 1.000 | 92.254 | 107.258 | 1233.1 | 627 | 3228 + 10 | 1831 + 2572 |
| room filter 50% | clustered merge + scan | 1.000 | 4.818 | 6.038 | 176.7 | 648 | 3228 | 6591 + 0 |
| room filter 50% | clustered merge + indexed | 1.000 | 2.738 | 3.660 | 26.1 | 633 | 3228 + 9 | 6591 + 2468 |
| range | exact (f32 brute force) | 1.000 | 64.787 | 76.548 | – | – | 3072 | – |
| range | scan | 1.000 | 96.058 | 122.679 | 1232.9 | 627 | 3228 | 1831 + 0 |
| range | indexed (IVF) | 1.000 | 88.530 | 104.026 | 983.9 | 627 | 3228 + 10 | 1831 + 2572 |
| range | clustered merge + scan | 1.000 | 30.516 | 57.947 | 263.0 | 13747 | 3228 | 6591 + 0 |
| range | clustered merge + indexed | 1.000 | 2.624 | 5.134 | 13.4 | 627 | 3228 + 9 | 6591 + 2468 |

What the numbers say:

- **Default recall clears the gate at every size.** The version 4 rerank stops when the next upper bound falls below
  the k-th exact score; `oversample` adds a patience that grows with the position of the last top-k improvement
  (`RERANK_PATIENCE_GROWTH` × that position, at least `k × oversample` rows). Within a ~630-row topic the U1 code
  cannot order the rows, so a fixed 80-row patience stopped early (recall@10 0.87–0.90 at 100,000 rows); the scaled
  patience gives 0.995–1.000 with ~610 rows reranked and about the same latency on clustered data (p50 0.7 ms indexed
  at d = 128, 2.0 ms at d = 768). The 100,000 × 128 gate test now runs by default.
- **Row order decides the block count.** On time-ordered data the partition index cannot avoid opening almost every
  block (~1,200 per query at 100,000 rows, and reranking a topic then opens a fine block per row: 119 ms at d = 768);
  after a clustered merge it opens 21–25 and answers top-10 at 768 dimensions in 2 ms against 65 ms for brute force
  over plain f32. Use `compact_segments` (clustered order by default) for compaction; `merge_segments` keeps rows in
  order.
- **Room filters estimate selectivity from a sample.** Min/max string statistics cannot tell a room spread over
  every granule from one in none, so the adaptive strategy decodes the filter columns of up to 4 granules to estimate
  the kept fraction and pre-filters below 25%. A 1% room filter at 100,000 × 768 went from 216 ms (3,178 blocks,
  1,376 rows reranked) to 27 ms (757 blocks, 173 rows). It is still slower than brute force over the ~1,000 in-room
  rows (1.8 ms, no decryption) because those rows are spread over most sealed blocks; a filter whose rows sit in a few
  fine blocks (≤ 4 × k) is scored exactly on the fine column without the coarse pass.
- **MaxSim with the partition index** shares opened blocks across probe rounds and scans once the probe would cover
  half the segment: 264 blocks like the scan (was 645), p50 10.8 ms against 9.2 ms for the scan at 2,000 documents,
  and fewer blocks and lower latency than the scan at 1,000 documents.
- Bytes per vector: 568 at d = 128 and 3,228 at d = 768 for U1 + F32 (F32 alone is 512 / 3,072); a partition
  index adds 3–10 bytes per vector. Index build trains k-means (⌈√rows⌉ partitions, 10 iterations) on a seeded sample
  of 64 rows per partition, then assigns every row once, in parallel and deterministically: 0.5 s at 100,000 × 128
  and 2.5 s at 100,000 × 768 (was 22 s and 3 minutes); a clustered merge of 100,000 × 768 takes 6.6 s (was 187 s).

## Operations benchmark

`operations/` (the `oe-vs-parquet` binary of this crate) holds a second comparison of OE against Parquet and Vortex over one chat-shaped dataset:
create, read, update, delete, search, and diff, with every answer checked across formats before timings are reported.
See `operations/README.md`.
