# OE versus Parquet versus Vortex

A standalone, dev/bench-only comparison of the OE columnar format against
Parquet and Vortex across the operations a storage format is actually asked
for: creating a file, reading it back, searching it, changing rows, removing
rows, and comparing two versions of it.

Each operation is measured against its own baseline: the format that was
slowest at it, marked `*` in the table. So every ratio in a row is at least 1
and reads as "this many times better than the worst way of doing it".

The generated rows are chat traffic: messages, edits, reactions and joins in rooms, threads and direct
conversations, each carrying a message kind, the sending client, a character count, a reaction count (absent on some
rows) and the message text.

Run it with `just bench-ops` from the repository root (see the `Justfile`), or from `benchmark/` (the crate root, one level up):

```sh
cargo run --release --bin oe-vs-parquet -- /tmp/oe-vs-parquet
```

The work directory is created if missing and left in place, so both files can
be inspected afterwards. Pass a spec file to change the shape of the run, and
`--toml` to print the report as TOML instead of a table:

```sh
cargo run --release --bin oe-vs-parquet -- /tmp/run spec.toml --toml
```

Repeat `--format` and `--operation` to isolate any subset. An OE-only run does not prepare Parquet or Vortex, and an
isolated OE create/write run also skips the untimed base and amended OE setup:

```sh
# Profile only OE create/write and include internal writer phases in the TOML report.
cargo run --release --bin oe-vs-parquet -- \
  /tmp/oe-create --format oe --operation create_write --phases --toml

# Measure only selected reads across OE and Parquet, retaining cross-format answer checks.
cargo run --release --bin oe-vs-parquet -- \
  /tmp/selected-reads --format oe --format parquet \
  --operation point_lookup --operation range_search
```

For IMP-002 update comparisons, `OE_BENCH_FULL_REWRITE_UPDATE=1` retains the former decode-all/full-rewrite control
path in the same release binary, and `OE_BENCH_SOURCE_CHANGES_UPDATE=1` runs the update from the change set alone
(`update_oe_file_from_source_changes_with_executor`): the writer reads the touched granules back from the source and
the harness converts only the changed rows; its `--phases` report adds a `SourceDecoding` phase. Alternate control and
candidate invocations to avoid build or thermal ordering bias:

```sh
OE_BENCH_FULL_REWRITE_UPDATE=1 target/release/oe-vs-parquet /tmp/oe-update-full \
  --format oe --operation update_rows --toml
target/release/oe-vs-parquet /tmp/oe-update-reuse \
  --format oe --operation update_rows --toml
```

For IMP-009 point-lookup comparisons, `OE_BENCH_NO_ENTITY_HASH_FILTERS=1` retains the former scan-every-granule
control path in the same release binary, so the identity-hash filters can be measured against the scan they replaced
over one identical file:

```sh
OE_BENCH_NO_ENTITY_HASH_FILTERS=1 target/release/oe-vs-parquet /tmp/oe-point-scan \
  --format oe --operation point_lookup --toml
target/release/oe-vs-parquet /tmp/oe-point-filtered \
  --format oe --operation point_lookup --toml
```

Every OE read iteration runs against a fresh reader over the file opened once in `prepare`, so an iteration never
inherits a decoded block, a warm mark page, or a cached filter from the one before it: the medians are cold-reader
numbers. `OE_BENCH_WARM_READER=1` runs every iteration against that one opened reader instead, so its caches stay
warm across iterations — the shape of a file held open and queried repeatedly — and the two can be compared over one
identical file:

```sh
OE_BENCH_WARM_READER=1 target/release/oe-vs-parquet /tmp/oe-warm \
  --format oe --operation read_text_column --toml
target/release/oe-vs-parquet /tmp/oe-cold \
  --format oe --operation read_text_column --toml
```

For READ-003 trailing-compression comparisons, `OE_BENCH_NO_TRAILING=1` publishes every OE file of the run with no
trailing compression stage at all — the same transforms and side streams as the default, the body stored as it stands,
no seekable frames on the free-text column — and `OE_BENCH_SIZE_OPTIMIZED=1` publishes it under the rewrite/compaction
cascade (a 5% savings bar instead of 25%). Both reach the writer through `OeBuildConfig::lifecycle`, resolved once
when the file is prepared, so no timed read pays for the control and the production default is untouched. A report's
`environment.controls` names every `OE_BENCH_*` variable that was in effect. Alternate the control and the default:

```sh
OE_BENCH_NO_TRAILING=1 target/release/oe-vs-parquet /tmp/oe-no-trailing \
  --format oe --operation read_all_columns --operation read_text_column --iterations 200 --toml
target/release/oe-vs-parquet /tmp/oe-trailing \
  --format oe --operation read_all_columns --operation read_text_column --iterations 200 --toml
```

Two ignored tests isolate what the controls trade. `trailing_stage_bytes_by_column` prints every column's stored bytes
before and after its trailing stage over the default dataset, and `string_decode_microbenchmarks` times each decode
family over one realistic block of it — each trailing family's inflate on its own, and each string transform's view
decode over the inflated body:

```sh
cargo test --release trailing_stage_bytes -- --ignored --nocapture
cargo test --release string_decode_microbenchmarks -- --ignored --nocapture
```

For READ-008 scan comparisons, `OE_BENCH_SERIAL_SCAN=1` keeps `read_all_columns` and `read_one_column` on the
engine's serial projected scan (`OeFile::scan_projected_serial`) in the same release binary, so the parallel scan
scheduler can be measured against a serial decode of the identical file:

```sh
OE_BENCH_SERIAL_SCAN=1 target/release/oe-vs-parquet /tmp/oe-scan-serial \
  --format oe --operation read_all_columns --toml
target/release/oe-vs-parquet /tmp/oe-scan-parallel \
  --format oe --operation read_all_columns --toml
```

`--no-check-answers` disables cross-format row-count checking when intentionally running unlike experiments. With no
filters, the command retains the original three-format, every-operation correctness and fairness mode.

## Allocation and peak memory

Wall-clock hides allocation churn, so a write-path change can trade a millisecond for temporary copies or hold more
memory at the peak. The `alloc-counting` feature installs a counting allocator (`allocation`) that reports, for every
operation and raw iteration, how many allocations and capacity growths the allocator was asked for, the bytes
requested, the peak live bytes the work added, and its largest single allocation. The medians appear in the terminal
table and both medians and raw samples appear in TOML. It is off by default, so an ordinary timing run measures the
same allocator the product does. On Unix, reports also carry the process-wide resident-set high-water mark from
`getrusage`.

```sh
cargo run --release --features alloc-counting --bin oe-vs-parquet -- \
  /tmp/oe-memory --format oe --operation create_write --toml > candidate.toml
```

`benchmark_writer_allocations_by_payload_shape` reports those totals for one OE create/write over narrow homogeneous
objects, wide objects, nested payloads, large strings, sparse fields, and a high-cardinality schema:

```sh
cargo test --release --features alloc-counting -- \
  --ignored --nocapture writer_allocation
```

The counters are process-wide rather than per-thread, which is what makes them right for a writer that fans its work
across a pool — and what makes two concurrent measurements meaningless.

To turn a committed TOML report into a gate, run the same filtered workload with a maximum permitted increase. The
command fails if any deterministic count exceeds the matching baseline value plus the configured percentage: the bytes
each operation read and each file's size in every build, and — when the baseline was written with `alloc-counting` —
allocation count, allocated bytes, peak live bytes, largest allocation, reallocations, and resident high-water memory
as well. Wall-clock is never gated. `read-baseline.toml` is the committed OE-only baseline over the default workload
(three iterations). Check a candidate against it, or regenerate it after a change that legitimately moves stored or
read bytes:

```sh
target/release/oe-vs-parquet /tmp/oe-gate --format oe --iterations 3 \
  --memory-baseline operations/read-baseline.toml --max-memory-regression-percent 0
target/release/oe-vs-parquet /tmp/oe-gate --format oe --iterations 3 --toml \
  | grep -v '^resident_high_water_bytes' > operations/read-baseline.toml
```

Any other report can be used the same way:

```sh
cargo run --release --features alloc-counting --bin oe-vs-parquet -- \
  /tmp/oe-memory-candidate --format oe --operation create_write --toml \
  --memory-baseline baseline.toml --max-memory-regression-percent 5
```

## Comparing results across commits

A run's TOML or table output is not kept anywhere once the terminal scrolls past it, so a number cited in a doc, PR,
or decision is otherwise unreproducible. `--results-file <path.jsonl>` appends the run — stamped with a fresh run id
and timestamp — as one JSON line to `path`, creating it and its parent directory if needed. Existing lines are never
read or rewritten, so the file is a durable, append-only history that can be diffed or grepped across commits:

```sh
cargo run --release --bin oe-vs-parquet -- \
  /tmp/oe-vs-parquet --results-file docs/benchmarks/results.jsonl
```

Any number cited from a run — in a doc, a PR, or a decision — should come from at least 3 iterations and be reported
as the median plus a tail figure (p99 or max), together with its run id, so the claim can be re-audited against the
history file later.

A spec is the six fields of `BenchmarkSpec`:

```toml
iterations = 5
mutation_percent = 1
range_percent = 10
row_count = 100000
rows_per_group = 8192
seed = 6840831955857080321
```

## Profiling with Time Profiler

Release builds carry `debug = "line-tables-only"`, enough for full symbolication. `--iterations` overrides the spec's
iteration count, so a profiling run needs no spec file; use a few dozen iterations for the write paths and a thousand
or more for the sub-millisecond reads. Record, export the sampled table as XML, and reduce it with
`tools/xctrace-reduce`:

```sh
xcrun xctrace record --template 'Time Profiler' --no-prompt --output reads.trace \
  --launch -- target/release/oe-vs-parquet /tmp/oe-profile --format oe --iterations 1500 \
  --operation read_one_column --operation full_text_search --operation full_text_search_rare
xcrun xctrace export --input reads.trace --output reads.xml \
  --xpath '/trace-toc/run[@number="1"]/data/table[@schema="time-profile"]'
python3 tools/xctrace-reduce/xctrace_reduce.py reads.xml --filter full_text_search_granule
python3 tools/xctrace-reduce/xctrace_reduce.py reads.xml --filter scan_block --callers memmove
```

One recording of several operations splits per operation without re-recording. The per-granule work on the OE
side runs in a named `#[inline(never)]` function, so samples taken on Rayon workers — which do not keep the
caller's stack — still carry the operation's name: `read_text_column_granule`, `read_payload_field_rows`,
`point_lookup_granule`, `range_search_granule`, `value_search_granule`, `full_text_search_granule`,
`full_text_search_rare_granule`, `full_text_search_absent_granule`, `diff_versions_granule`, and `oe_row` for the
input conversion that create/write and update share; `read_all_columns` and `read_one_column` decode their blocks in
the engine's own `scan_block`. Filter on the operation's function name to get that operation's tables (the
`profile_<operation>` wrappers in `run.rs` do not survive into the export; `--filter read_all_columns` keeps the
main-thread loop and its `scan_block` frames and drops the setup that a whole-process recording also holds),
`--exclude` to drop what is not wanted, and `--callers` to see where a hot symbol is reached from. Samples are 1 ms
across every running thread, so a count is CPU time, not wall time.

The reducer also groups samples into `codec:zstd`, `codec:fsst`, `allocator`, `copy`, `rayon-wait` and `validation`
buckets — once per sample per bucket, so overlapping stacks are not double-counted — and, with `--csv`, writes every
table in full plus a `.meta.json` sidecar naming the trace, the export command, the binary and its UUID (checked
against the binary on disk), the commit, the spec and iterations, and the recorded command line, so a before/after
comparison can be reviewed rather than trusted. A one-operation run of the committed binary, reduced for review:

```sh
xcrun xctrace record --template 'Time Profiler' --no-prompt --output read-all.trace \
  --launch -- target/release/oe-vs-parquet /tmp/oe-profile --format oe --operation read_all_columns \
  --iterations 200
xcrun xctrace export --input read-all.trace --output read-all.xml \
  --xpath '/trace-toc/run[@number="1"]/data/table[@schema="time-profile"]'
python3 tools/xctrace-reduce/xctrace_reduce.py read-all.xml --filter read_all_columns \
  --csv read-all.csv --commit "$(git rev-parse HEAD)"
```

The trace bundle beside the export is found on its own; `--commit` is needed only when the binary was copied out of
its checkout. The text output opens with the metadata and the bucket table:

```
binary   target/release/oe-vs-parquet (UUID 8C289D87-E3A7-35FC-91B1-90045974806B)
commit   d27fcbd4c6e240f683ccad31a2d7891e23b2a583 (--commit)
command  target/release/oe-vs-parquet /tmp/oe-profile --format oe --operation read_all_columns --iterations 200
samples  2215 of 2582 kept --filter read_all_columns

buckets (2215 samples; inclusive counts a sample once per bucket it touches and overlaps, nearest is the bucket of the frame nearest the leaf and partitions)
inclusive  share  nearest  share  bucket
     1042  47.0%     1029  46.5%  codec:zstd
       85   3.8%       85   3.8%  codec:fsst
       91   4.1%       90   4.1%  allocator
      198   8.9%      198   8.9%  copy
        0   0.0%        0   0.0%  rayon-wait
       65   2.9%       65   2.9%  validation
      748  33.8%      748  33.8%  (none)
```

`tools/xctrace-reduce/README.md` defines every bucket and column.

## What is measured

| Operation | OE | Parquet | Vortex |
| --- | --- | --- | --- |
| `create_write` | build one published file and fsync it | encode one file and fsync it | encode one file and fsync it |
| `open_metadata` | one range read of the file's tail | read the file's footer | parse the footer |
| `read_all_columns` | every column block of every granule | every column chunk of every row group | one unprojected scan |
| `read_one_column` | one column across every granule | one projected column across every row group | one scan projecting that column |
| `read_text_column` | the declared free-text column across every granule, view-decoded | that column projected across every row group | one scan projecting that column |
| `read_payload_field` | one payload field per row, addressed through its mark | project that column over the row groups the sample reaches | one scan over exactly those row indices |
| `point_lookup` | rule granules out on their identity-hash filters, scan the hash column of the rest, confirm against the id | scan the identity hash column per row group, confirm against the id | hash equality pushed into the scan, confirm against the id |
| `range_search` | prune granules on the footer's time ranges, then count | prune row groups on column statistics, then count | the window pushed into the scan |
| `value_search` | dictionary lookup, then an integer scan | scan the dictionary-encoded string column | the equality pushed into the scan |
| `full_text_search` | substring scan of the declared free-text column | substring scan of that column | substring scan of that column |
| `full_text_search_rare` | the same search for a needle a handful of rows carry: granules whose trigram filter rules it out are never read | substring scan of that column | substring scan of that column |
| `full_text_search_absent` | the same search for a needle no row carries: every granule's trigram filter rules it out, so no column bytes are read at all | substring scan of that column | substring scan of that column |
| `update_rows` | amend changed rows, reuse unchanged encoded blocks, publish a new file | decode, amend, encode a new file | decode, amend, encode a new file |
| `delete_rows` | write a row-position bitmap beside an untouched file | rewrite the file without those rows | rewrite the file without those rows |
| `diff_versions` | rule stripes out on their content hashes, compare the rest | compare column chunk bytes, decode what differs | compare whole columns, walk only the ones that moved |

## How the two sides are kept comparable

- One generated dataset feeds all three. Rows come from the spec's seed through
  a fixed integer mixer, so the same spec produces the same rows on every host.
- The same block size everywhere: `rows_per_group` is the OE granule row
  target, the Parquet row-group row count, and the Vortex chunk size, so no
  format prunes at a finer resolution than the others.
- Comparable compression: Parquet is written with Zstd at the level OE's own
  cascade can select, with dictionary encoding and page statistics on. Vortex
  chooses its own cascade, as OE does — neither is told which encoding to use.
- The same columns. Every payload field lands in a typed column everywhere —
  `kind` promoted by declaration, `note` declared free text, and `char_count`,
  `reactions`, and `client` lifted by OE's own shredding statistics — so
  nothing sits in OE's residual payload that the other two files do not carry.
- The same answer. Every operation is answered by every format and must produce
  the same row count; a disagreement fails the run rather than being reported as
  a speedup.
- Three needles, not one. The dense needle sits in roughly one row in a
  thousand, so every block holds a match and no index can prune — that search
  measures the scan kernel alone. The rare needle sits in a handful of rows and
  the absent one in none, so those two measure what a format's text metadata
  saves. OE is the only one of the three that carries such metadata; Parquet
  and Vortex answer all three by scanning, which is the comparison.
- The same clock rule. Opening a reader and parsing metadata is setup and sits
  outside the timing everywhere, because a caller does it once and then answers
  many queries. Each iteration gets a reader that has decoded nothing, so no
  iteration is subsidised by the one before it; for OE it is a fresh reader
  over a file opened and verified once in setup, so per-iteration setup does
  not re-hash every stripe. `open_metadata` measures that excluded setup on its
  own.

Each format is given the plan its own design intends, not a transliteration of
another's: OE walks granules through the marks in its footer, Parquet walks row
groups and pushes a projection mask, Vortex builds one scan with the predicate
and the row indices pushed into it. Comparing "how you would really ask this" is
the point; forcing all three through one shape would measure the shape.

Alongside wall-clock, each operation reports the file bytes it had to touch,
taken from the format's own directory — the surviving groups' column blocks,
not the whole file. Two formats can take the same time and read very different
amounts, which is what decides the cost over object storage.

TOML reports open with an `environment` table — the host name, OS, architecture, CPU, kernel, the git commit the
binary was built from (`-dirty` when the tree had uncommitted changes), whether the counting allocator was installed,
and every `OE_BENCH_*` control in effect — so a number is never separated from the machine and code that produced it.
They retain every raw iteration in `samples_nanos`. OE create/write and update convert the generated rows
into the writer's input shape inside the operation; that conversion is harness work, so `median_nanos` and
`samples_nanos` leave it out — that is the figure in the table and in every ratio — while
`median_with_input_conversion_nanos` and `samples_with_input_conversion_nanos` carry the same iterations with it
counted in, the figure earlier reports called `median_nanos`. Parquet and Vortex build their Arrow input inside the
clock, so their create and update numbers still include it. With `--phases`, OE create and update results also retain
raw samples for input conversion, normalization, dictionary construction, path statistics, granule construction,
block encoding, layout, footer construction, integrity, file assembly, row destruction, and durable write. The terminal
table remains summary-only.

### Integrity traversal audit (IMP-012)

The OE writer keeps BLAKE3 authoritative and CRC-64/NVME as an independent upload precheck. They cannot share an
algorithmic digest, so the large data area necessarily enters each algorithm once; no identity, seal, proof, or
finished-file step reads it again for integrity.

| Region | Authoritative BLAKE3 work | CRC-64/NVME work | Assembly/proof handling |
| --- | --- | --- | --- |
| Header | Hash once into the domain-separated segment seal | Absorb once after the header becomes final | Stamp into its reserved block in place |
| Stripes | One traversal builds both each stripe root and its outboard proof nodes; file identity and the segment seal compose those roots | Absorb each completed stripe range while it is still hot, then combine that suffix behind the late-bound header | Keep per-stripe proof buffers until their final destination |
| Alignment gaps | Hash each gap once into the data commitments | Included in the adjacent completed data range | Store only the small authenticated gap commitment in the footer |
| Footer and OE trailer | Hash together once into the segment seal | Absorb each once before emission | Append or stream each final buffer once |
| Proof trees and OET trailer | No extra BLAKE3 pass; nodes are derived from authenticated stripe roots | Absorb once as emitted because the upload precheck covers exact stored bytes | Write trees directly to the materialized file or stream; there is no concatenated intermediate appendix |

On the committed 100,000-row default workload before this final consolidation, `--phases` measured the integrity
phase at 1.26 ms median out of 145.74 ms create/write (0.86%), already below IMP-012's 5% completion threshold. Use
the same command shape to reproduce it:

```sh
target/release/oe-vs-parquet /tmp/oe-integrity \
  --format oe --operation create_write --phases --toml
```

## Reading the results

**The delete.** This compares *file* formats. Table formats layered over Parquet
— Iceberg v3, Delta Lake — carry deletion vectors of their own, and a delete
under one of those costs about what it costs OE here. The rewrite measured by
`delete_rows` is what a bare Parquet or Vortex file costs, nothing more.

**What the numbers leave out.** OE verifies every stripe's checksum when a file
is opened whole, which neither of the others does by default, and OE carries
internal bookkeeping columns that have no counterpart in the other two files and
are left out of them — both count against OE on file size and on a whole-file
scan.

**`open_metadata` on Vortex** reads from the copy already in memory: Vortex's
in-memory open takes the whole buffer and slices segments out of it, so there is
no tail-only read to time as there is for the other two. Its number is a lower
bound.

**What the identity-hash filters cost.** OE carries one membership filter per
granule over the entity-id hash column, which is what lets `point_lookup` skip
granules instead of reading every one of them. The bytes sit in the data area
beside each stripe's other filters, not in the footer, so a cold open never
fetches them: on the default dataset they add 1.5% to the file and 1.4% to the
footer (0.4 KiB), leave `open_metadata` unchanged, and cost about 2% of
create/write. `point_lookup` is charged for every filter byte it consults, so
its number includes the index it prunes with.

**The Vortex version.** The file format measured here is `vortex` 0.77.0,
pulled in through the umbrella crate. This benchmark is the only crate in the
workspace that depends on Vortex: `oe` no longer embeds Vortex arrays
inside OE blocks (shredded columns use the same adaptive encoder as every
other column), so nothing here is shared with the storage path.

**`read_all_columns` on Vortex** forces canonical decode of every scanned
column before the clock stops, so the number includes the same materialization
work the OE and Parquet sides pay rather than just chunk collection.
