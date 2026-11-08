# OE benchmark

Compares OE with Parquet, Vortex and JSON on the same events, including the things a chat app needs from an event
store (latest messages in a room, threads, lookup by event id, redaction, durable ingest). Results are in
`results/results.md` (tables) and `results/results.json` (raw numbers).

Run it from this directory:

```bash
cargo run --release -- --sizes 10000,100000,1000000 --reps 5 --write-reps 3 --out results
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
