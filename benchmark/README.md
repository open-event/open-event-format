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

`--large yes` adds 1,000,000 rows; `--oversample N` changes the rerank patience. Results: `results/vector.md` (every
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
- **Acceptance gate:** `cargo test --release` (about 15 s) runs 10,000 × 128 and 5,000 × 768 in both orders and a small
  MaxSim set, and fails if any default-settings recall@10 is below 0.95 or if, on cluster-ordered data or after a
  clustered merge, the indexed search does not open fewer blocks than a scan (top-10 and range).

Top-10 and MaxSim (single thread; full table in `results/vector.md`):

| Dataset | Configuration | Recall@10 | p50 ms | p99 ms | Blocks opened | Rows reranked | Bytes/vector (+ index) | Build ms (segment + index) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 10000 × 128 time-ordered | exact (f32 brute force) | 1.000 | 0.697 | 1.326 | – | – | 512 | – |
| 10000 × 128 time-ordered | scan | 0.994 | 2.470 | 2.868 | 152.4 | 186 | 567 | 64 + 0 |
| 10000 × 128 time-ordered | indexed (IVF) | 0.994 | 2.435 | 4.551 | 152.3 | 186 | 567 + 6 | 64 + 735 |
| 10000 × 128 time-ordered | clustered merge + scan | 0.994 | 0.478 | 0.637 | 51.0 | 199 | 568 | 859 + 0 |
| 10000 × 128 time-ordered | clustered merge + indexed | 0.994 | 0.347 | 0.432 | 15.0 | 188 | 568 + 6 | 859 + 728 |
| 10000 × 128 cluster-ordered | exact (f32 brute force) | 1.000 | 0.708 | 1.081 | – | – | 512 | – |
| 10000 × 128 cluster-ordered | scan | 0.996 | 0.489 | 0.721 | 53.1 | 195 | 567 | 30 + 0 |
| 10000 × 128 cluster-ordered | indexed (IVF) | 0.996 | 0.341 | 0.521 | 14.3 | 188 | 567 + 6 | 30 + 716 |
| 10000 × 768 time-ordered | exact (f32 brute force) | 1.000 | 5.066 | 6.301 | – | – | 3072 | – |
| 10000 × 768 time-ordered | scan | 0.992 | 12.593 | 14.389 | 152.6 | 183 | 3228 | 171 + 0 |
| 10000 × 768 time-ordered | indexed (IVF) | 0.992 | 12.011 | 13.601 | 152.5 | 183 | 3228 + 24 | 171 + 5686 |
| 10000 × 768 time-ordered | clustered merge + scan | 0.992 | 1.250 | 1.446 | 51.0 | 199 | 3228 | 6075 + 0 |
| 10000 × 768 time-ordered | clustered merge + indexed | 0.992 | 0.888 | 1.050 | 13.5 | 189 | 3228 + 29 | 6075 + 5597 |
| 10000 × 768 cluster-ordered | exact (f32 brute force) | 1.000 | 4.938 | 6.209 | – | – | 3072 | – |
| 10000 × 768 cluster-ordered | scan | 0.994 | 1.301 | 1.511 | 51.8 | 203 | 3228 | 178 + 0 |
| 10000 × 768 cluster-ordered | indexed (IVF) | 0.994 | 0.871 | 1.141 | 13.9 | 188 | 3228 + 28 | 178 + 5774 |
| multivector 1000 docs / 8185 tokens × 128 | exact (f32 MaxSim) | 1.000 | 2.053 | 2.233 | – | – | 512 | – |
| multivector 1000 docs / 8185 tokens × 128 | scan | 1.000 | 3.791 | 5.441 | 123.9 | 3996 | 551 | 26 + 0 |
| multivector 1000 docs / 8185 tokens × 128 | indexed (IVF) | 1.000 | 4.604 | 14.657 | 223.0 | 4492 | 551 + 7 | 26 + 525 |
| 100000 × 128 time-ordered | exact (f32 brute force) | 1.000 | 7.669 | 10.348 | – | – | 512 | – |
| 100000 × 128 time-ordered | scan | 0.876 | 11.377 | 14.824 | 1026.2 | 283 | 568 | 331 + 0 |
| 100000 × 128 time-ordered | indexed (IVF) | 0.876 | 10.360 | 14.482 | 1004.0 | 283 | 568 + 3 | 331 + 22394 |
| 100000 × 128 time-ordered | clustered merge + scan | 0.876 | 1.224 | 2.930 | 163.0 | 340 | 568 | 22902 + 0 |
| 100000 × 128 time-ordered | clustered merge + indexed | 0.876 | 0.649 | 1.170 | 25.9 | 307 | 568 + 3 | 22902 + 22707 |
| 100000 × 128 cluster-ordered | exact (f32 brute force) | 1.000 | 7.383 | 10.391 | – | – | 512 | – |
| 100000 × 128 cluster-ordered | scan | 0.865 | 1.418 | 2.175 | 169.4 | 329 | 568 | 351 + 0 |
| 100000 × 128 cluster-ordered | indexed (IVF) | 0.865 | 0.806 | 1.271 | 28.9 | 296 | 568 + 3 | 351 + 22685 |
| 100000 × 768 time-ordered | exact (f32 brute force) | 1.000 | 64.862 | 77.208 | – | – | 3072 | – |
| 100000 × 768 time-ordered | scan | 0.885 | 55.391 | 84.488 | 1022.9 | 277 | 3228 | 1782 + 0 |
| 100000 × 768 time-ordered | indexed (IVF) | 0.885 | 51.760 | 72.465 | 1007.2 | 277 | 3228 + 10 | 1782 + 182711 |
| 100000 × 768 time-ordered | clustered merge + scan | 0.885 | 3.789 | 4.851 | 163.0 | 334 | 3228 | 186580 + 0 |
| 100000 × 768 time-ordered | clustered merge + indexed | 0.885 | 1.989 | 2.778 | 22.5 | 316 | 3228 + 9 | 186580 + 180338 |
| 100000 × 768 cluster-ordered | exact (f32 brute force) | 1.000 | 66.185 | 79.858 | – | – | 3072 | – |
| 100000 × 768 cluster-ordered | scan | 0.897 | 3.991 | 6.046 | 164.8 | 327 | 3228 | 1755 + 0 |
| 100000 × 768 cluster-ordered | indexed (IVF) | 0.897 | 2.219 | 3.100 | 21.8 | 310 | 3228 + 9 | 1755 + 182007 |
| multivector 2000 docs / 16158 tokens × 128 | exact (f32 MaxSim) | 1.000 | 4.049 | 4.864 | – | – | 512 | – |
| multivector 2000 docs / 16158 tokens × 128 | scan | 1.000 | 8.440 | 9.953 | 264.0 | 16158 | 552 | 53 + 0 |
| multivector 2000 docs / 16158 tokens × 128 | indexed (IVF) | 1.000 | 13.894 | 19.178 | 644.9 | 17528 | 552 + 5 | 53 + 1523 |

100,000 × 768, time-ordered, other workloads:

| Workload | Configuration | Recall@10 | p50 ms | p99 ms | Blocks opened | Rows reranked | Bytes/vector (+ index) | Build ms (segment + index) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| room filter 1% | exact (f32 brute force) | 1.000 | 1.817 | 2.388 | – | – | 3072 | – |
| room filter 1% | scan | 0.983 | 215.649 | 237.379 | 3177.8 | 1376 | 3228 | 1782 + 0 |
| room filter 1% | indexed (IVF) | 0.990 | 209.904 | 244.700 | 3118.3 | 1373 | 3228 + 10 | 1782 + 182711 |
| room filter 1% | clustered merge + scan | 0.983 | 32.323 | 54.124 | 989.1 | 1643 | 3228 | 186580 + 0 |
| room filter 1% | clustered merge + indexed | 0.990 | 29.622 | 120.062 | 711.5 | 1499 | 3228 + 9 | 186580 + 180338 |
| room filter 50% | exact (f32 brute force) | 1.000 | 40.076 | 45.308 | – | – | 3072 | – |
| room filter 50% | scan | 1.000 | 97.063 | 107.298 | 1246.6 | 627 | 3228 | 1782 + 0 |
| room filter 50% | indexed (IVF) | 1.000 | 90.317 | 105.543 | 1231.9 | 627 | 3228 + 10 | 1782 + 182711 |
| room filter 50% | clustered merge + scan | 1.000 | 4.151 | 6.868 | 169.8 | 647 | 3228 | 186580 + 0 |
| room filter 50% | clustered merge + indexed | 1.000 | 2.175 | 3.008 | 24.1 | 634 | 3228 + 9 | 186580 + 180338 |
| range | exact (f32 brute force) | 1.000 | 64.551 | 81.442 | – | – | 3072 | – |
| range | scan | 1.000 | 97.616 | 123.507 | 1232.9 | 627 | 3228 | 1782 + 0 |
| range | indexed (IVF) | 1.000 | 89.001 | 104.660 | 983.9 | 627 | 3228 + 10 | 1782 + 182711 |
| range | clustered merge + scan | 1.000 | 28.920 | 32.689 | 248.6 | 12757 | 3228 | 186580 + 0 |
| range | clustered merge + indexed | 1.000 | 2.303 | 3.329 | 16.7 | 707 | 3228 + 9 | 186580 + 180338 |

What the numbers say:

- **Default recall falls below the gate at 100,000 rows.** With topics of ~630 rows, the version 4 rerank patience
  (`oversample` 8: stop after 80 consecutive reranked rows that miss the top 10) stops early and top-10 recall is
  0.87–0.90 at both dimensions. Range queries, which ignore the patience, are exact. `--oversample 32` or `0` gives
  recall 1.000 on 100,000 × 128 with ~600 rows reranked and the same latency (p50 1.3 ms scan, 0.7 ms indexed,
  cluster order). The 100,000-row gate test is `#[ignore]`d until the default changes.
- **Row order decides the block count.** On time-ordered data the partition index cannot avoid opening almost every
  block (~1,000 per query at 100,000 rows); after a clustered merge it opens 22–26 and answers top-10 at 768
  dimensions in 2 ms against 65 ms for brute force over plain f32.
- **A 1% room filter is the slow case.** Pre-filtering decodes the room column of every granule and still opens
  700–3,200 blocks per query, slower than brute force over the ~1,000 in-room rows, because room membership is
  spread across all granules.
- **MaxSim with the partition index opens more blocks than the scan** (645 vs 264 at 2,000 documents) and is slower:
  its probe widens over most of a small segment.
- Bytes per vector: 568 at d = 128 and 3,228 at d = 768 for U1 + F32 (F32 alone is 512 / 3,072); a partition
  index adds 3–10 bytes per vector. Index build (k-means, ⌈√rows⌉ partitions, 10 iterations, one thread) dominates
  build time at 100,000 rows: 22 s at d = 128, 3 minutes at d = 768.

## Operations benchmark

`operations/` (the `oe-vs-parquet` binary of this crate) holds a second comparison of OE against Parquet and Vortex over one chat-shaped dataset:
create, read, update, delete, search, and diff, with every answer checked across formats before timings are reported.
See `operations/README.md`.
