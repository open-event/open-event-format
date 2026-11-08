# OE benchmark

Compares OE with Parquet, Vortex and JSON on the same events. Results are in `results/results.md` (tables) and
`results/results.json` (raw numbers).

Run it from this directory:

```bash
cargo run --release -- --sizes 10000,100000,1000000 --reps 5 --write-reps 3 --out results
```

## What is measured

- **Data:** two deterministic datasets, `telemetry` (numeric payload) and `messages` (text payload), at 10,000,
  100,000 and 1,000,000 events. Each event has an id, timestamp, entity id, event type and source, plus five payload
  fields.
- **Formats:** OE; Parquet (zstd level 3, 131,072-row groups, min/max statistics); Vortex (default compressor);
  newline-delimited JSON, plain and with zstd level 3.
- **Per format:** file size, write time, open time (footer/schema parse), and seven queries: scan every column, scan one
  number, scan one payload number, scan one payload text column, filter on event type plus a payload field, a 1% time
  window, and an entity lookup (median of 20 lookups).
- **Correctness:** every format must return identical row counts and column totals for every query, or the run stops.

## Reading the numbers

- Everything runs on one thread with the file held in memory, so disk speed is not measured.
- OE stores more than the other files: all envelope columns (hashes, sequence numbers, ingest time), skip indexes and
  integrity checksums. Its larger size and slower write reflect that.
- OE opens with a full integrity check of the file. The other formats do not verify content on open.
- OE prunes granules by time window and entity hash. Parquet prunes row groups by min/max statistics. Vortex pushes
  filters into its scan. JSON has no way to skip data, so every query parses every line (with `serde_json::Value`).
- OE stores event ids as 16-byte values; the other formats store them as 32-character hex text.
- Parquet bloom filters, page indexes and Vortex layout tuning are left at their defaults.
