<div align="center">
  <h1>Open Event Format</h1>
  <h3>A columnar file format and append-only journal built for event streams.<br/>Written once, queried fast, instant deletes.</h3>
  <p>
    0.04 ms 1% time windows, 0.19 ms event-by-id lookups, and instant redactions over 1,000,000 chat events
  </p>
  <p>
    <a href="#getting-started">Quickstart</a> •
    <a href="./openspec/specs">Specification</a> •
    <a href="./benchmark">Benchmarks</a> •
    <a href="./conformance">Conformance</a> •
    <a href="https://github.com/open-event/open-event-format/issues">Issues</a>
  </p>
</div>

## What is OE?

OE is the storage engine for events. Events first land in **OEJ**, an append-only journal that is the durable record, then get packed into **OE** files: immutable, self-describing column files for analytical queries. Everything is checksummed end to end, so corruption is always caught and never served.

- **Built for events, not generic tables**: Every file carries a full event envelope (ids, streams, tenants, sequence numbers, timestamps, relationships) next to a schemaless variant payload.
- **Point lookups without a database**: Granule filters by time window and entity hash, an external-id index, and thread (root reference) lookups answer "latest 50 in this room" or "this event by id" without scanning.
- **Redaction without rewrites**: Removing an event records a deletion vector of a few bytes instead of rewriting the file.
- **Integrity end to end**: CRC-64/NVMe and BLAKE3 checksums, optional proof trees, and authenticated (sealed) footers. A reader verifies on open, or opens lazily and verifies what it touches.
- **Erasure and isolation**: Crypto-shredding, subject-sealed payloads, and encrypted footers for data that has to be forgettable.

<br/>
## Use cases

- **Chat and messaging history**: busy rooms, threads, edits, reactions and redactions in one file
- **Audit logs**: tamper-evident, append-only records with sealed footers
- **Telemetry and product analytics**: numeric and text payloads scanned column by column
- **Agent and session transcripts**: one durable stream per session, queryable after the fact

<br/>
## Why a new format?

Parquet and Vortex are excellent general analytical formats, but an event store asks for more than scans: append with a durable acknowledgement, find one event by its id, follow a thread, remove a single message for a GDPR request, and prove the file was not tampered with. General formats cannot append, cannot remove rows in place, and do not verify content on open.

OE keeps the columnar layout and modern encodings (FSST, bit-packing, dictionaries, RLE, XOR floats, zstd and LZ4) and adds the envelope, indexes, deletion vectors and integrity an event stream needs. The cost is size: OE stores every envelope column, skip index and checksum, so its files are larger than Parquet's.

For comparison across a million chat events, single thread, file held in memory:

| Operation | OE | Parquet | Vortex |
| --- | --- | --- | --- |
| 1% time window | **0.04 ms** | 2.78 ms | 0.56 ms |
| Every reply in a thread | **0.28 ms** | 17.65 ms | 4.01 ms |
| One event by id | **0.19 ms** | 5.18 ms | 1.15 ms |
| Latest 50 messages in a room | 6.00 ms | 47.17 ms | **2.56 ms** |
| Redact 1 event | **0.00 ms, 9 bytes written** | 2,007.53 ms, 37,698,174 bytes | 2,861.48 ms, 48,035,456 bytes |
| Redact 1% of events | **12.87 ms, 160,005 bytes** | 2,145.08 ms, 37,167,023 bytes | 2,916.68 ms, 47,241,092 bytes |
| Size per event | 95.5 bytes | **37.7 bytes** | 48.1 bytes |
| Write | 4,246 ms | **1,188 ms** | 2,180 ms |

<br/>
## Features

- **OEJ journal**: Append-only frames, each acknowledged only after sync. 172,879 events/s at 128-event batches with a 1.322 ms p99 ack.
- **Columnar OE files**: Stripes, granules and projections with per-column encodings chosen per block.
- **Skip indexes**: Time-window and entity-hash granule filters, external-id index, thread references, IN-list pruning, and boolean (AND/OR) filter planning on encoded blocks.
- **Deletes & corrections**: Deletion vectors, corrections and splice rewrites that reuse untouched granules.
- **Vector columns**: Embeddings stored alongside events.
- **Integrity**: End-to-end CRC-64/NVMe, BLAKE3 proof trees, Ed25519-signed seals, and verification on open.
- **Security**: Encrypted footers, subject-sealed payloads and crypto-shredding with ChaCha20-Poly1305 and AES-GCM.
- **Arrow-friendly**: Column data maps onto Arrow arrays for the query engine that embeds OE.
- **Hardened**: `unsafe` is denied outside one documented FSST call site, `unwrap`/`panic`/indexing are denied by Clippy, and every file structure has a fuzz target.

<br/>
## Getting Started

```bash
# It is not yet published to crates.io
git clone https://github.com/open-event/open-event-format.git

# Run tests
cd open-event-format && cargo test --all-features

# Run benchmarks
cd benchmark && cargo run --release -- --sizes 10000,100000,1000000 --reps 5 --write-reps 3 --out results
```

<br/>
## FAQ

<details>
<summary><b>Why are OE files larger than Parquet?</b></summary>

OE stores more: all envelope columns (hashes, sequence numbers, ingest time), skip indexes, an external-id index and integrity checksums. On 1,000,000 chat events that is 95.5 bytes per event against Parquet's 37.7 bytes, and it buys the lookup and redaction times above.

</details>

<details>
<summary><b>Can I append to an OE file?</b></summary>

No. OE files are immutable. New events append to the OEJ journal, and published OE files are built from it. Readers can overlay recent journal data on top of published files.

</details>

<details>
<summary><b>How does deleting an event work?</b></summary>

A deletion vector marks the removed rows, so redacting one event writes 9 bytes instead of rewriting the file. Compaction later drops the rows physically, and crypto-shredding makes sealed payloads unreadable by destroying their keys.

</details>

<details>
<summary><b>Does OE verify data when it reads it?</b></summary>

Yes. A full open checks the whole file. A lazy open verifies each stripe range as it is read. Corruption is reported as an error, never returned as data.

</details>

<details>
<summary><b>Does OE include a query engine?</b></summary>

No. OE provides the file format, indexes and predicate evaluation on encoded blocks. Planning and SQL live in the embedding application.

</details>

<details>
<summary><b>Where is the specification?</b></summary>

In [`openspec/specs`](./openspec/specs), one directory per `oe-*` capability, with proposed changes under [`openspec/changes`](./openspec/changes). [`conformance/capability-map.toml`](./conformance/capability-map.toml) maps each capability to its module and tests.

</details>
