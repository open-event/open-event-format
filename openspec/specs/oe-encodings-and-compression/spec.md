## Purpose

Defines how OE encodes and compresses column data:

- It samples the data and picks the best encoding pipeline automatically, choosing from required candidates per column type (monotonic, dictionary/low-cardinality, numeric measures, strings).
- Compression for payloads, indexes, and bitmaps.
- Optional hardware-accelerated compression is allowed, but a software path must always produce identical results.

The concrete adaptive encoding pipeline and per-type encoding/compression detail are embedded in [encodings.md](encodings.md).


## Requirements

### Requirement: Adaptive per-block encoding selection
OE SHALL select encoding per column block by sampling candidate pipelines and choosing the fastest valid pipeline that satisfies size, predicate-pushdown, random-access, and Arrow-decode constraints. Static column-to-codec assignment SHALL be forbidden except where this spec marks an encoding mandatory for correctness. The selected pipeline SHALL be recorded in the page header and column marks. Selection SHALL apply mandatory logical representation rules first (e.g. decimal128 for money, `TimestampValue` logical types), test lightweight transforms before heavyweight compression, and prefer pipelines exposing min/max/null_count and predicate preselection without full decompression.

#### Scenario: Pipeline recorded
- **WHEN** the writer selects an encoding pipeline for a column block
- **THEN** the chosen pipeline id is stored in the column marks and page metadata

### Requirement: Monotonic columns use FastLanes-style candidates
For `sequence`, `stream_sequence`, `occurred_at`, `ingested_at`, and other monotonic/near-monotonic columns, the writer SHALL choose among FastLanes bitpacked FOR, FastLanes DELTA (+bitpacking), base+offset mini-blocks, RLE, or delta-of-delta only when sampling proves it wins. When a FastLanes FOR or DELTA candidate is selected, its bit-packed integers SHALL be stored in the FastLanes transposed layout defined in "Bit-packed integer streams use the FastLanes transposed layout", not a naive value-after-value bit stream. LZ4-frame-only numeric/time storage SHALL NOT be a default for these columns; LZ4 MAY appear only as a trailing stage or measured fallback.

#### Scenario: Reject LZ4-only timestamp storage
- **WHEN** the writer considers storing `occurred_at` as LZ4-frame-only
- **THEN** it instead selects an adaptive transform (e.g. FastLanes DELTA), using LZ4 only as a trailing/fallback stage

### Requirement: Bit-packed integer streams use the FastLanes transposed layout
Every bit-packed integer stream OE writes — the frame-of-reference (FOR) deltas and DELTA residuals of monotonic columns, the scaled integers ALP produces for float columns, and the codes of a dictionary string block — SHALL be packed in the FastLanes transposed layout, not a naive value-after-value bit stream. The writer SHALL group the values into fixed vectors of 1024 (zero-padding the final vector) and, within each vector, store them in the FastLanes "04261537" transposed order packed into 64-bit words as sixteen lanes, so a decoder recovers a whole register of lanes at once using only shifts and masks. The bit width SHALL be recorded with the stream so a reader finds it without a second pass.

This layout SHALL be the correctness reference, consistent with "Optional acceleration with mandatory software parity": a vectorized (SIMD) or hardware decoder MAY read these bytes far faster, but the scalar software decode SHALL reproduce the original values byte for byte, and a reader with no accelerator SHALL always decode the stream correctly. Decoding SHALL reproduce the original values exactly regardless of where the value count falls relative to the 1024-value vector boundary. FSST string blocks are compressed through a shared symbol table rather than integer bit-packing, so this layout SHALL NOT apply to them; their decode kernel is governed by "Strings preserve random-access decode".

#### Scenario: Integer columns round-trip across vector boundaries
- **WHEN** a FastLanes FOR or DELTA bit-packed integer column whose row count is not a multiple of 1024 is encoded and then decoded
- **THEN** the decoded values equal the originals exactly, the final partial vector's zero padding having been discarded

#### Scenario: ALP and dictionary streams ride the same transposed layout
- **WHEN** an ALP-encoded float column and a dictionary-encoded string column, each spanning more than one 1024-value vector, are encoded and then decoded
- **THEN** both reproduce their original values exactly, because their packed integers — ALP's scaled integers and the dictionary codes — use the same FastLanes transposed layout

### Requirement: Mandatory representations for money and floats
Money SHALL use fixed-scale decimal aggregation (decimal128), not float. Floating metric columns MAY use ALP/ALP RD only when round-trip and aggregate correctness pass the declared tolerance contract for the metric.

#### Scenario: Float metric outside tolerance
- **WHEN** ALP/ALP RD encoding of an f64 metric column fails the declared round-trip/aggregate tolerance contract
- **THEN** that encoding is not selected for the column

### Requirement: Time-series float blocks may store Chimp XOR floats
A time-series file's float blocks SHALL also be tried as Chimp XOR floats (pipeline transform 14), and the Chimp form SHALL be stored only when it is strictly smaller than the block adaptive selection chooses; no other file's blocks use it. The block layout SHALL be a `u32` value count, a `u32` stream byte length, and the stream, packed most significant bit first with the last byte zero-padded: the first value's 64 bits, then for each later value its XOR with the previous one as a two-bit control code: `00` for a zero XOR; `01` when the XOR has more than six trailing zero bits, followed by a 3-bit leading-zero code, a 6-bit count of centre bits, and the centre bits; `10` when the XOR's rounded leading-zero count equals the one the last `11` set, followed by every bit after the leading zeros; `11` followed by a new 3-bit leading-zero code and every bit after the leading zeros. Leading-zero counts SHALL be rounded down to one of 0, 8, 12, 16, 18, 20, 22, or 24, coded 0 to 7, and a `00` or `01` value clears the count a `10` may reuse. The codec works on bit patterns, so decoding SHALL reproduce every value bit for bit, NaN payloads, both zeros, and both infinities included. A decoder SHALL refuse a value count the stream is too short to hold before allocating for it, a `10` with no count to reuse, centre bits that do not fit a value, a stream that ends mid-value, and bytes or set padding bits after the last value. A block has no per-value offsets, so a row range decodes the whole block and slices it.

#### Scenario: Special values round-trip
- **WHEN** a block holding NaNs with payloads, `-0.0`, both infinities, and subnormals is stored as Chimp and decoded
- **THEN** every value's bits equal the original's

#### Scenario: Slowly varying metric
- **WHEN** a memory-utilization gauge (used over total bytes, holding still about half the time) of 8192 readings is encoded for a time-series file
- **THEN** the block is stored as Chimp and is smaller than every encoding that predates it (measured: 2.96 bytes per value, against 3.07 for the best earlier encoding and 3.75 for what adaptive selection alone stores)

#### Scenario: Decimal readings keep ALP
- **WHEN** a block of two-decimal readings is encoded for a time-series file
- **THEN** it is stored exactly as adaptive selection chooses, because ALP is smaller there

### Requirement: Strings preserve random-access decode
String columns SHALL use dictionary encoding for low-cardinality, FSST for high-cardinality short strings, front coding for sorted strings, and a raw byte arena for long/opaque strings. FSST pages SHALL preserve random-access decode through marks or mini-block offsets so a reader can decode selected granules without decoding unrelated string pages. Low-cardinality dimensions SHALL prefer exact bitmap indexes over probabilistic filters.

#### Scenario: Decode one granule of an FSST column
- **WHEN** a query needs a single granule of an FSST-encoded `event_id` column
- **THEN** the reader decodes only that granule via marks/mini-block offsets

### Requirement: FSST blocks may store bitpacked value lengths
An FSST string block that stores no per-value keys SHALL be rewritten by the writer into the packed-lengths layout (pipeline transform 15) whenever that layout is smaller: the same null stream, symbol table and present count, then each present value's compressed length in a FastLanes bitpacked stream (count, bit width, packed words) in place of the four-byte offset table, then the same code arena. The lengths SHALL sum to exactly the arena's length and their count SHALL equal the present count; a block that disagrees SHALL be refused as structurally invalid. A reader SHALL rebuild the classic offset table from the lengths (an unpack and prefix sum, no entropy decode) and SHALL then answer every decode, range decode, row take, Arrow view decode and compressed-form string predicate exactly as it would for the equivalent transform-9 block. The packed layout SHALL NOT carry per-value keys and SHALL NOT ride the seekable Zstandard framing. A file that stores at least one packed-lengths block SHALL declare the required feature `packed_string_offsets` (bit 33), which is not part of the set every file declares, so a reader or rewriter that predates the transform refuses the file at open. A conformance run that forces transform 9 SHALL get the classic layout.

#### Scenario: Short high-cardinality strings drop the offset table
- **WHEN** a block of mostly distinct short strings is encoded as FSST without per-value keys
- **THEN** it is stored as transform 15, smaller than the transform-9 form, and every read path returns the same values and predicate masks as the transform-9 form

#### Scenario: An old reader refuses the file
- **WHEN** a reader that does not know required feature bit 33 opens a file holding a packed-lengths FSST block
- **THEN** it refuses the file at open instead of failing part-way through a query

### Requirement: Ingested-at blocks may be stored relative to occurred-at
A single-page, dense `ingested_at` block SHALL also be trial-encoded relative to the same granule's `occurred_at` (pipeline transform 16, value kind i64, no trailing stage on the outer id): the body is the four-byte pipeline id of an inner block followed by that inner block, an ordinary i64 integer block (frame-of-reference, delta, RLE or plain, with any trailing stage and no side stream) of the wrapping residuals `ingested_at - occurred_at` row by row. The writer SHALL keep the relative form only when it is strictly smaller than the direct encoding of `ingested_at`, and SHALL NOT use it for a block whose values are one constant (constant elision applies instead), a multi-page block, or any other column. The block's recorded statistics, the granule's `ingested_at` bounds and the page statistics SHALL describe the `ingested_at` values themselves, never the residuals, so time pruning on `ingested_at` is unchanged. A reader SHALL rebuild the block from the granule's decoded `occurred_at` (decoding it at most once per granule through its decoded-block cache) into the equivalent uncompressed plain i64 block before any decode, range decode, row take or predicate sees it, SHALL NOT share that rebuild with another granule's aliased extent, and SHALL refuse as structurally invalid a transform-16 block on any column other than `ingested_at`, on a page of a multi-page block, with an inner block of another kind, or whose residual count differs from the granule's `occurred_at` count. An update that rewrites a granule's `occurred_at` block SHALL re-encode that granule's `ingested_at` block rather than borrow it from the source. A file that stores at least one such block SHALL declare the required feature `occurred_relative_ingested_at` (bit 34), which is not part of the set every file declares.

#### Scenario: Ingested-at equal to occurred-at costs almost nothing
- **WHEN** a file's rows carry `ingested_at` equal to, or a few units after, their `occurred_at` and `occurred_at` is not itself trivially encodable
- **THEN** its `ingested_at` blocks are stored as transform 16, smaller than the direct encoding, every read path returns the original `ingested_at` values, and the granule `ingested_at` bounds equal the values' own minimum and maximum

#### Scenario: A rewritten occurred-at re-encodes ingested-at
- **WHEN** an update changes the `occurred_at` of a row in a granule whose `ingested_at` block is stored relative to `occurred_at`
- **THEN** the updated file's `ingested_at` reads back the updated rows' values exactly, never the source block decoded against the new `occurred_at`

#### Scenario: An old reader refuses an occurred-relative file
- **WHEN** a reader that does not know required feature bit 34 opens a file holding a transform-16 block
- **THEN** it refuses the file at open instead of failing part-way through a query

### Requirement: String blocks may be compressed whole with LZ4
Adaptive selection (and a replay whose captured trailing stage is not `None`) of a random-access string block SHALL also weigh the block compressed whole with LZ4; the writer SHALL weigh it only for shredded payload and free-text columns, so envelope and relationship columns, which equality lookups probe granule by granule, keep layouts a filter answers without inflating anything. The candidate is stored the way a Parquet column chunk is stored: the raw-arena form (transform 10) and, when the block's distinct values are at most three quarters of its present rows, the local dictionary form with plain entries (transform 8, side stream 0; or the file-scope form where the column's shared alphabet covers the block). The smaller candidate SHALL replace the block's otherwise chosen form only when it stores less than nine tenths of that form's bytes; a conformance run that forces a transform, the plain form the acceptance gate measures against, and the `NoTrailing` measurement strategy SHALL NOT take it. The candidate SHALL use trailing compression 6 (`string_zstd`) wherever the body has a relayout and plain `Lz4` (compression 1) otherwise. The compression-6 stored form is the classic body's byte length (`u32`), a relayout tag (`u8`), then the relaid body as an ordinary stored block: compression 1 (LZ4) when the tag's high bit `0x80` is set, which every writer now sets, else compression 3 (Zstandard), as earlier writers stored it; the tag's low bits select the relayout. Tag 0 relays a transform-10 body: the null stream, the present count, then every present value followed by a zero byte in place of the offset table and arena. Tag 1 relays a local transform-8 body with plain entries: the null stream, the entry count, every entry followed by a zero byte, the code count, the code bit width, then `ceil(width / 8)` byte planes of the codes, least significant plane first, in place of the offset table, entries and FastLanes code stream. A body whose values or entries contain a zero byte SHALL NOT be relaid. A reader SHALL undo compression 6 into exactly the classic body (rebuilding the offset table from the terminators and re-packing the codes in the FastLanes layout at the recorded width) and SHALL refuse as structurally invalid an unknown tag, a claimed classic length above 2^28 bytes or other than the rebuilt body's, a value without its terminator, a code wider than its width, or bytes after the last code. Every decode, range decode, row take, Arrow view decode, point read and compressed-form string predicate SHALL then read the block as the classic body, so dictionary equality still resolves one entry and compares codes. Whole-block LZ4 (or Zstandard) on a random-access (shredded or free-text) string block is permitted here because the block is one granule's values: a point read inflates the block once, and the reader's decoded-block cache keeps the inflated column for later reads of the granule; per-value keys and seekable framing are given up for such a block. Text token and freetext indexes are built from the values, not the block layout, and SHALL be unaffected. A file that stores at least one compression-6 block SHALL declare the required feature `string_zstd_blocks` (bit 37), and a file whose compression-6 blocks carry LZ4 SHALL also declare `string_lz4_blocks` (bit 39); neither is part of the set every file declares.

#### Scenario: Repeated identifiers take dictionary codes under LZ4
- **WHEN** a granule's shredded room-like identifier field holds a few hundred distinct long values repeated across its rows
- **THEN** its block is stored under compression 6 with LZ4, smaller than the FSST or packed-lengths form, and reads back and filters exactly as the uncompressed dictionary block

#### Scenario: Free text stores the smaller form
- **WHEN** a shredded free-text payload field holds a sentence per row
- **THEN** its block is stored as whichever of the FSST form and a transform-10 block under compression 6 with LZ4 is smaller, a point read of one row returns that row's text, and freetext and token-index queries over the field return the same rows as before

#### Scenario: An old reader refuses a string-zstd file
- **WHEN** a reader that does not know required feature bit 37 opens a file holding a compression-6 block
- **THEN** it refuses the file at open instead of failing part-way through a query

### Requirement: Entity ids may share a compressed union alphabet
A file-scope dictionary alphabet (side stream 3: a dictionary block that stores only its null stream and its bitpacked codes, resolving them against the column's sorted alphabet in the footer) SHALL be allowed for the `entity_id` column even when its granules do not repeat one value set: the alphabet is then the sorted, deduplicated union of every granule's present values, at most 2^20 values each at most 65,535 bytes. The writer SHALL select it only for a file of more than one granule, and only when an estimate of the column under it (every present row's code at the union's bit width plus the alphabet's text at half its raw size) is smaller than the estimate of per-granule storage (each granule's distinct values at half their raw size plus its rows' codes at the granule's own width, or, when smaller, half the granule's raw text). Each block SHALL still keep its block-local form when that is smaller in an exact size trial, and a block the union alphabet covers SHALL be trial-encoded as a dictionary however many distinct values it holds. A union alphabet SHALL be stored in footer section `PACKED_SHARED_DICTIONARIES` (45), never in the plain `SHARED_DICTIONARIES` section (24), and only when some block records the file scope: a `u32` entry count, then per entry the column id (`u32`), the value count (`u32`), the plaintext length (`u32`, at most 2^28), the compressed length (`u32`) and one Zstandard frame whose plaintext is every value's length as a little-endian `u16` followed by every value's bytes back to back. Decoding the footer SHALL check only this framing (and SHALL refuse a column carried by both sections or twice, and the section without required feature bit 38); a reader SHALL decompress and validate an alphabet only when it first resolves a block of its column or translates a predicate for it, refusing a frame that does not decompress to exactly the plaintext length, lengths that do not split the text into values exactly (on character boundaries), invalid UTF-8, and values that are not strictly ascending — a shared-scope block of a column whose alphabet failed to expand SHALL refuse like a block without its alphabet. Opening a file SHALL NOT expand any alphabet. Every decode, range decode, row take, Arrow view decode and compressed-form string predicate SHALL resolve the codes against the expanded alphabet, reading it in place rather than copying it per block. An equality lookup SHALL translate the id once per file by binary search of the alphabet (needing only the decompressed text, not one string per value) and then compare codes in each granule's block. An update reuses a source block only while the candidate's alphabet for its column is byte-identical to the source's; an edit that changes the alphabet rewrites the column's blocks. A file storing the section SHALL declare required feature `packed_shared_dictionaries` (bit 38), which is not part of the set every file declares.

#### Scenario: Recurring entity ids store only codes
- **WHEN** a file's entity ids recur across its granules, each granule drawing a different subset of more ids than a repeated-set alphabet may hold
- **THEN** the footer stores their union once in section 45, compressed, under required feature bit 38; every entity block records the file scope and stores only codes; and decode, range decode, take and Arrow view reads return exactly the stored ids

#### Scenario: An entity lookup searches the alphabet once and compares codes
- **WHEN** an equality lookup on such a column translates the id against the file's alphabet
- **THEN** each block answers the lookup from its codes alone, counting exactly the rows that hold the id

#### Scenario: Ids that never recur keep block-local forms
- **WHEN** every row carries an entity id no other row holds
- **THEN** the union alphabet would not pay, so the column keeps block-local forms and the file declares no section 45 and no bit 38

### Requirement: Dictionary trial covers random-access and replayed string blocks
A string block that selection sends to FSST SHALL also be trial-encoded as a dictionary whenever its distinct values are at most three quarters of its present rows, and the smaller form SHALL be kept. The trial SHALL run on random-access (shredded and free-text) blocks too, since a dictionary block's bitpacked codes are per-value addressable, and on blocks replaying an FSST capture, so a column whose segment head sampled as FSST still stores its repeating blocks as dictionaries.

#### Scenario: Repeating shredded strings become a dictionary
- **WHEN** a shredded payload string column holds a few hundred distinct values across a granule's rows
- **THEN** its block is stored as a dictionary block with bitpacked codes and answers equality and range filters from the codes

### Requirement: Index, bitmap, and payload compression families
Bitmaps SHALL use Roaring native compression, Ribbon filters their compact representation, and split-block Bloom filters their split-block representation with outer compression only when range reads still work. `variant_shredded_field_blocks` SHALL use adaptive encoding that preserves random access in compressed form (whole-block heavyweight compression forbidden on shredded scan-path columns, except the whole-block Zstandard string stage of "String blocks may be compressed whole with Zstandard"); `variant_residual_blocks` SHALL be uncompressed in hot granules with page-level Zstd-3 allowed for cold/rewritten granules (optionally with trained ZDICT dictionaries keyed by internal `(tenant_id, event_type_id)`, rebuilt at rewrite when the ratio regresses); `variant_dictionary_blocks` SHALL use dictionary+FSST supporting key→field_id binary search without full-block decode; text token indexes and footer metadata SHALL use Zstd level 1 or a better adaptive choice.

#### Scenario: Bitmap compression
- **WHEN** a low-cardinality dimension is stored as a bitmap index
- **THEN** it uses Roaring native compression

### Requirement: Compressed-data string predicates
Dictionary string blocks SHALL assign codes in ascending sorted order of their
distinct values, so a code's numeric order matches its value's byte order. The
reader SHALL provide a string-predicate evaluator that answers common filters
directly from a block's compressed form, without rebuilding every row's text:

- For a dictionary block, it SHALL answer equality (`=`), inequality (`!=`), set
  membership (`IN`), and range (`<`, `<=`, `>`, `>=`, open or closed on either
  side) by resolving the predicate against the sorted dictionary and testing the
  stored codes.
- For an FSST block, it SHALL answer the equality class (`=`, `!=`, `IN`) by
  compressing the comparison value(s) with that block's symbol table and
  byte-comparing against the stored compressed values, decompressing nothing.

FSST codes are not order-preserving, and a compressed value reveals neither the
order of the text it stands for nor which bytes that text holds. The writer
MAY therefore store two per-value keys after an FSST block's value arena, and
SHALL record in the block's pipeline id whether it did:

- a **prefix key** — the value's first seven bytes plus a length byte, eight in
  all — whose byte order is the values' order wherever two keys differ; and
- a **byte fingerprint** — a 32-bit mask with one bit per `byte & 31` bucket of
  the value — which a substring's own mask must be contained in for that
  substring to occur in the value.

The writer SHALL store the keys only where they earn their bytes, deciding by
the same size discipline that selects every other cascade level, and a block
without them SHALL keep the plain FSST layout so an existing block still
decodes unchanged.

Given those keys, the evaluator SHALL answer range and prefix predicates on an
FSST block by comparing prefix keys, decompressing only the values whose keys
tie with a bound on all seven bytes; and SHALL restrict a substring search to
the values whose fingerprint could hold the search terms, decompressing no
other value. On an FSST block that stores no such keys, the evaluator SHALL
decline range and prefix predicates so the caller falls back to a full decode.
The evaluator SHALL be an optimisation,
never a correctness dependency: a reader SHALL always be able to decode the block
and filter the values, and SHALL do so whenever the block's encoding or the
predicate is unsupported. The evaluator's result SHALL be exact — every selected
row truly satisfies the predicate — and SHALL equal a full decode-then-filter row
for row, with null rows never selected.

#### Scenario: Dictionary codes preserve value order
- **WHEN** the writer encodes a dictionary string block
- **THEN** the codes are assigned in ascending sorted order of the distinct
  values, so comparing codes orders rows the same way as comparing their strings

#### Scenario: Dictionary equality and range answered from codes
- **WHEN** an `=`, `IN`, or range filter runs on a dictionary-encoded column
- **THEN** the evaluator resolves the predicate to a set or interval of codes and
  selects rows by testing the stored codes, without rebuilding any row's string

#### Scenario: FSST equality answered from compressed bytes
- **WHEN** an `=` or `IN` filter runs on an FSST-encoded column
- **THEN** the evaluator compresses the comparison value(s) with the block's
  symbol table and byte-compares against the stored compressed values, without
  decompressing any value

#### Scenario: FSST range falls back to decode
- **WHEN** a range or prefix filter targets an FSST-encoded column whose block
  stores no per-value keys
- **THEN** the evaluator declines (FSST is not order-preserving) and the reader
  decodes the block and filters the values

#### Scenario: FSST range answered from per-value prefix keys
- **WHEN** a range or prefix filter targets an FSST-encoded column whose block
  stores per-value keys
- **THEN** the evaluator compares the stored prefix keys against the bounds and
  decompresses only the values whose prefixes tie with a bound, and the selected
  rows equal those of a full decode-then-filter

#### Scenario: FSST substring candidates pruned by byte fingerprints
- **WHEN** a substring filter runs on an FSST-encoded column whose block stores
  per-value keys
- **THEN** every value whose fingerprint lacks a bucket the search terms need is
  ruled out without being decompressed, and the rows selected from the values
  that remain equal those of a full decode-then-filter

#### Scenario: Compressed-data filter equals full decode
- **WHEN** the evaluator answers any predicate from the compressed form
- **THEN** the selected rows equal those of a full decode-then-filter, and null
  rows are never selected

### Requirement: Recursive cascade selection
The encoder SHALL be able to recursively encode an encoding's own side streams,
turning a single-level pipeline into a bounded cascade. An encoding rarely
produces a single clean stream: it leaves side streams — ALP/ALP RD exception
lists, dictionary code streams, FastLanes FOR residuals, front-coding
prefix-length streams, and similar metadata or secondary streams — that are
themselves compressible. The encoder MAY apply a further lightweight encoding to
any such exception, metadata, or secondary stream.

Each cascade level SHALL be chosen by the same sampling discipline that selects
the top-level pipeline (see "Adaptive per-block encoding selection"): the encoder
samples candidate inner encodings for the side stream and keeps a deeper level
only when sampling proves it wins on the same size, predicate-pushdown,
random-access, and Arrow-decode constraints. A deeper level SHALL NOT be applied
on a static rule or a per-column-name assignment.

Recursion SHALL be bounded: the format SHALL declare a small maximum cascade
depth, and the encoder SHALL NOT exceed it. Every level of the cascade SHALL
preserve random-access decode — a reader SHALL still be able to decode a single
granule without decoding unrelated granules — and SHALL preserve software-parity
decode, so that any optional accelerator used at any level can be replaced by the
software path with byte-identical results (see "Optional acceleration with
mandatory software parity"). The full chosen cascade, level by level, SHALL be
recorded in the block's page header and column marks so that a reader decodes it
deterministically from the recorded description alone, without re-deriving the
cascade from the data.

#### Scenario: Cascade an encoding's exception stream
- **WHEN** the encoder selects ALP for an f64 metric block and sampling shows the
  ALP exception stream compresses further under a lightweight inner encoding
- **THEN** the encoder applies that inner encoding to the exception stream as a
  second cascade level and records both levels in the block's marks and page
  metadata

#### Scenario: Bounded recursion depth
- **WHEN** the encoder considers adding a cascade level that would exceed the
  declared maximum cascade depth
- **THEN** it does not add that level, and the encoded block's recorded cascade is
  no deeper than the declared maximum

#### Scenario: Deeper level kept only when sampling wins
- **WHEN** sampling a candidate inner encoding for a side stream does not improve
  the block against the size, predicate-pushdown, random-access, and Arrow-decode
  constraints
- **THEN** the encoder does not add that cascade level and leaves the side stream
  at the shallower encoding

#### Scenario: Reader decodes the recorded cascade deterministically
- **WHEN** a reader opens a block whose marks record a multi-level cascade
- **THEN** it decodes each recorded level in order from the recorded description
  alone, reproduces the original values exactly, and reads no unrelated granule to
  do so

#### Scenario: Software parity holds at every cascade level
- **WHEN** any level of a cascade was produced with an optional accelerator
- **THEN** decoding that level on the software path yields byte-identical results,
  so the cascade never makes correctness depend on an accelerator

### Requirement: Lifecycle-selected cascade strategies
The encoder SHALL support two named cascade strategies, selected by where a part
sits in its lifecycle rather than by any caller-supplied switch:

- `DecodeOptimized` — applied to hot and freshly published parts, biased toward
  fast decode: it prefers shallow cascades and encodings that decode quickly even
  when a deeper or heavier cascade would shrink the bytes a little more.
- `SizeOptimized` — applied when a part is rewritten or compacted, biased toward
  smaller stored bytes: it admits deeper cascades and heavier inner encodings (up
  to the declared maximum cascade depth) when sampling shows they shrink the part,
  accepting somewhat slower decode in exchange.

The active strategy SHALL be selected by the part's lifecycle stage — fresh
publication selects `DecodeOptimized`; rewrite and compaction select
`SizeOptimized` — and that selection, together with the per-block benchmarks the
sampling produces, SHALL be the only inputs to the choice. No per-query,
per-operator, or otherwise caller-facing key SHALL select or override the
strategy; the strategy is not an operator-facing switch. This keeps cascade
selection a governed, best-effort maintenance property of the part's lifecycle,
consistent with the droppable-acceleration discipline, and never a query-time
tuning knob.

Both strategies SHALL ride on the same sampling mechanism described in "Adaptive
per-block encoding selection" — they bias which sampled pipeline wins, they do not
replace sampling with a fixed codec assignment. Selection SHALL be deterministic:
the same part content at the same lifecycle stage SHALL select the same strategy
and produce the same encoded bytes on any node that encodes it. Both strategies
SHALL produce correct, parity-tested round-trips: decoding a block encoded under
either strategy SHALL reproduce the original values exactly, and SHALL match the
software-path decode byte for byte.

#### Scenario: Fresh part uses the decode-optimized strategy
- **WHEN** a part is freshly published from the write path
- **THEN** its blocks are encoded under the `DecodeOptimized` strategy, biased
  toward fast decode, selected without any caller-supplied input

#### Scenario: Rewrite uses the size-optimized strategy
- **WHEN** a part is rewritten or compacted
- **THEN** its blocks are re-encoded under the `SizeOptimized` strategy, which may
  choose a deeper cascade than the fresh part had, bounded by the declared maximum
  cascade depth

#### Scenario: No operator knob selects the strategy
- **WHEN** a query or operator attempts to request a particular cascade strategy
- **THEN** there is no key that selects or overrides it; the strategy follows only
  the part's lifecycle stage and the sampling benchmarks

#### Scenario: Strategy selection is deterministic
- **WHEN** two nodes encode the same part content at the same lifecycle stage
- **THEN** both select the same strategy and produce byte-identical encoded blocks

#### Scenario: Both strategies round-trip correctly
- **WHEN** a block is encoded under either the `DecodeOptimized` or the
  `SizeOptimized` strategy and then decoded
- **THEN** the decoded values equal the originals exactly and equal the
  software-path decode byte for byte

### Requirement: Compressed-data numeric predicates
The reader SHALL provide a numeric-predicate evaluator that answers common
filters directly from a numeric block's compressed form, without decoding every
row first. For equality (`=`), inequality (`!=`), set membership (`IN`), and
range (`<`, `<=`, `>`, `>=`, open or closed on either side):

- For a FastLanes bit-packed, frame-of-reference (FOR), or DELTA block, it SHALL
  answer by comparing the packed lanes against the block's base — equality as a
  lane-equality test and range as an order-respecting lane comparison against the
  bound — without fully unpacking the block.
- For a decimal128 measure block (money is decimal128 per the "Mandatory
  representations for money and floats" requirement), it SHALL answer by rescaling
  the comparison value(s) to the column's fixed scale and comparing the stored
  integer mantissa.
- For an ALP or ALP-RD block, it SHALL answer by evaluating the predicate against
  the encoded integer representation to exclude rows, then decoding only the
  surviving rows to confirm each match, so it never reconstructs a float it is
  about to reject.

The evaluator SHALL be an optimisation, never a correctness dependency: a reader
SHALL always be able to decode the block and filter the values, and SHALL do so
whenever the block's encoding or the predicate is unsupported. Any optional
accelerator used for a fast path (for example SIMD over the packed lanes) SHALL
have a software path that produces the identical result, consistent with the
"Optional acceleration with mandatory software parity" requirement. The
evaluator's result SHALL be exact — every selected row truly satisfies the
predicate — and SHALL equal a full decode-then-filter row for row, with null rows
never selected.

#### Scenario: Range answered over FastLanes packed lanes
- **WHEN** a range filter runs on a FastLanes FOR, DELTA, or bit-packed numeric column
- **THEN** the evaluator compares the packed lanes against the block's base and the rescaled bound and selects rows without fully unpacking the block

#### Scenario: decimal128 equality answered from the integer mantissa
- **WHEN** an `=`, `IN`, or range filter runs on a decimal128 money column
- **THEN** the evaluator rescales the comparison value(s) to the column's fixed scale and selects rows by comparing the stored integer mantissa, decoding no value

#### Scenario: ALP predicate decodes only survivors
- **WHEN** an `=`, `IN`, or range filter runs on an ALP or ALP-RD column
- **THEN** the evaluator evaluates the predicate against the encoded integer representation to exclude rows and decodes only the surviving rows to confirm each match, never reconstructing a rejected float

#### Scenario: Unsupported encoding or predicate falls back to decode
- **WHEN** the block's encoding or the predicate is one no kernel can answer on the compressed form
- **THEN** the reader decodes the block and filters the values, so the answer is always available

#### Scenario: Compressed-data numeric filter equals full decode
- **WHEN** the evaluator answers any numeric predicate from the compressed form
- **THEN** the selected rows equal those of a full decode-then-filter, and null rows are never selected

### Requirement: Portable decoder reference for optional encoding blocks
OE SHALL let a new encoding for an **optional** shredded or payload block — such as `variant_shredded_field_blocks`, `variant_dictionary_blocks`, or another optional encoding family — ship before every reader in the fleet has gained the native code to decode it, so the format can adopt a better encoding without waiting for a fleet-wide native-reader upgrade. To make this safe, the writer MAY advertise a portable decoder for such a block: when it writes an optional block in an encoding that older readers may lack, it MAY record a `min_reader_version` and a `portable_decoder_ref` (a versioned, fleet-resolvable decoder identity) for that block, so a reader without the native encoding can resolve a conformance-passing portable decoder rather than only skipping the block.

This advertisement SHALL apply to optional blocks only and SHALL NOT relax the mandatory software-parity rule: the encoding SHALL still have a software path that produces identical results, and the portable decoder SHALL produce byte-for-byte the rows a native decoder would. Required encodings, and any encoding marked mandatory for correctness, SHALL NOT depend on a `portable_decoder_ref`; a reader that cannot decode them natively SHALL fail the file rather than read it. The reader-side resolution, conformance gate, and skip-to-scan fallback are governed by `oe-reader-compatibility` "Optional-block forward-compatibility escape hatch".

#### Scenario: New shredded encoding ships ahead of native readers
- **WHEN** the writer encodes a `variant_shredded_field_blocks` block in a new optional encoding that older readers lack native code for
- **THEN** it MAY record a `min_reader_version` and a `portable_decoder_ref` for the block so an older reader can resolve a conformance-passing portable decoder, while a reader that resolves neither still skips the block to a correct scan

#### Scenario: Portable decoder reference does not weaken software parity
- **WHEN** an optional block advertises a `portable_decoder_ref`
- **THEN** the encoding still exposes a software path with identical results, and the portable decoder produces byte-for-byte the rows a native decoder would, so no required encoding or correctness-mandatory encoding relies on the reference

### Requirement: Shredded blocks use the adaptive encoder
`variant_shredded_field_blocks` SHALL use the same adaptive per-block encoder and
stored block format as every other column: the writer samples the mandated
candidate pipelines for the block's type and records the chosen pipeline id in
the column marks and page metadata, exactly as "Adaptive per-block encoding
selection" requires, rather than a second serialization private to shredded
blocks. The encoded form SHALL preserve random access in compressed form: a
reader SHALL be able to decode a single requested granule of a shredded field
block without decoding the rest of the block and without a whole-block
heavyweight decompression pass. Encoding SHALL be deterministic — the same
shredded content SHALL produce byte-identical blocks on any node that encodes
it. Transform id 11, which named the removed Vortex shredded-block
serialization, SHALL stay retired: it SHALL NOT be reassigned, and a block whose
recorded pipeline claims it SHALL be rejected as structural corruption rather
than decoded.

#### Scenario: Shredded cascade uses the adaptive encoder
- **WHEN** the writer encodes a `variant_shredded_field_blocks` cascade
- **THEN** the block's bytes are produced by the same adaptive encoder as every
  other column, and its recorded mark carries an adaptive transform, not the
  retired Vortex transform id

#### Scenario: Random access preserved in compressed form
- **WHEN** a query needs a single granule of a shredded field block
- **THEN** the reader decodes only that granule from the stored form, without
  decoding the rest of the block or running a whole-block heavyweight
  decompression

#### Scenario: Serialization round-trips under conformance
- **WHEN** a shredded field block of any carried kind is written with the
  adaptive encoder and then read back
- **THEN** the decoded values, their logical types, and their row order equal the
  pre-serialization input exactly, and this round-trip is exercised by a
  conformance test

#### Scenario: Shredded bytes are deterministic
- **WHEN** two nodes encode the same shredded block content
- **THEN** both produce byte-identical encoded blocks and identical recorded
  pipeline ids

#### Scenario: Retired transform id is rejected
- **WHEN** a block's recorded pipeline claims the retired transform id 11
- **THEN** the reader rejects the block as structural corruption instead of
  decoding it

### Requirement: Self-describing per-page encoding descriptor
Beyond the selected pipeline id already recorded in the page header and column marks, each page SHALL record a compact, self-describing encoding descriptor that exposes the parameters a consumer needs to evaluate predicates and aggregates directly on the encoded representation without a full decode: for frame-of-reference and delta codecs the reference/base value and bit-width; for dictionary codecs a reference to the governing dictionary block; for run-length codecs the run table or run offsets; and for adaptive floating-point (ALP) codecs the exponent/factor parameters. The descriptor SHALL be sufficient for a consumer to translate a literal into the encoded domain once and then compare codes, count or skip runs, and range-compare against the frame-of-reference base, consistent with the compressed-data string and numeric predicate requirements. Surfacing the descriptor SHALL NOT change decoded values: the mandatory software decode path SHALL remain authoritative and SHALL produce identical results whether or not a consumer used the descriptor to compute on the encoded form.

#### Scenario: Predicate evaluated on dictionary codes via the descriptor
- **WHEN** a consumer evaluates `status = 'won'` over a dictionary-encoded page
- **THEN** it reads the dictionary reference from the descriptor, maps `'won'` to its code once, and compares codes without decoding the column, returning the same matches as a full decode

#### Scenario: Decode-cost planning from the descriptor
- **WHEN** the planner orders evaluation of several predicates over a granule
- **THEN** it uses each page's encoding descriptor to estimate decode cost and selectivity and evaluates the cheapest, most selective predicate first

### Requirement: QPL deflate as an IAA-accelerated, software-parity compression family
OE MAY add an RFC-1951 **deflate** compression family to the adaptive per-block candidate set for scan-path payload pages and for the cold/rewritten residual pages that today choose Zstd, so that pages a query will later decompress-and-filter can be produced and read through Intel IAA via QPL. Deflate SHALL be a sampled candidate like every other family — selected only when the adaptive sampler proves it wins on size and decode speed under the existing selection rules (Requirement: "Adaptive per-block encoding selection"), never statically assigned and never selected by an operator config key — and it SHALL NOT displace the mandatory representations or the FastLanes/ALP/FSST primary column encodings, which remain governed by their own requirements. The on-disk bytes of a deflate page SHALL be a **canonical, portable RFC-1951 deflate stream** that any conforming reader decodes with a pure-software inflater (`miniz_oxide`), independent of whether IAA produced it or will read it; to preserve compressed-form random access the writer SHALL constrain deflate to the IAA-compatible history window (≤ 4 KiB) and record per-granule mini-block/index offsets so a reader decodes one requested granule without inflating the whole page, consistent with "Random access preserved in compressed form". The deflate family SHALL be the on-disk substrate the IAA decompress-and-filter read path consumes (query-execution, Requirement: "IAA decompress-and-filter fast path behind the scan interface"); the QPL hardware path is invoked only as the local, droppable acceleration governed by INV-HARDWARE-ACCEL. Producing or reading a deflate page through QPL/IAA SHALL be observably equivalent to the software path: the software deflate/inflate path SHALL be the byte-for-byte correctness oracle, query results, ordering, visibility, checksums, security policy, and public output SHALL be unchanged whether or not the accelerator was used, and a reader with no accelerator SHALL always decode the page correctly. This requirement extends "Optional acceleration with mandatory software parity" with the concrete deflate family; it changes no existing family's selection or bytes.

#### Scenario: Deflate page decodes in software without an accelerator
- **WHEN** a deflate-family page produced on an IAA host is read on a host with no accelerator
- **THEN** the pure-software `miniz_oxide` inflater decodes it correctly to byte-identical values, because the on-disk bytes are a canonical RFC-1951 deflate stream within the ≤ 4 KiB history window, not an accelerator-specific format

#### Scenario: Deflate chosen only by the sampler, never by config
- **WHEN** the writer selects an encoding pipeline for a scan-path payload page
- **THEN** deflate is selected only if the adaptive sampler proves it wins under the existing size/speed/random-access/Arrow-decode constraints, never by an operator-facing config key and never as a static column-to-codec assignment

#### Scenario: Single-granule random access on a deflate page
- **WHEN** a query needs one granule of a deflate-family page
- **THEN** the reader uses the recorded per-granule mini-block/index offsets to decode only that granule, without inflating the whole page, on either the software or the IAA path with identical bytes

### Requirement: FSST write path reuses compression buffers
When encoding an FSST string block, the writer SHALL NOT allocate a fresh output buffer per value. Per-value compression SHALL go through the pinned `fsst` crate's `compress_into` interface, writing into one scratch buffer that is cleared and re-reserved for each value and reused across the whole block, with each value's compressed bytes appended to one block-level arena and its length recorded for the block's offset table. Before every `compress_into` call the scratch buffer SHALL hold capacity of at least twice the plaintext length — the crate's documented worst case, every input byte an escape — so the call's capacity contract is established locally at the call site; an empty value SHALL short-circuit to zero compressed bytes exactly as the crate's allocating `compress` does. The encoded block bytes SHALL be byte-identical to those produced by per-value allocation through the crate's `compress` API, so buffer reuse is invisible in the on-disk format and to every reader. The `unsafe` call the capacity contract requires SHALL live only inside the documented `storage::oe::encoding` module allow (`implementation-toolchain` — "Unsafe code is confined to dedicated crates and documented"), each such block SHALL carry a safety comment naming the capacity guarantee, and the FSST buffer-reuse call sites SHALL be the module's only unsafe blocks.

#### Scenario: Reused buffers produce byte-identical blocks
- **WHEN** the same values — including empty, shorter-than-a-word, escape-heavy, and multi-kilobyte strings — are compressed through the reused scratch buffer and through the crate's per-value allocating `compress`
- **THEN** the compressed bytes are identical for every value, and the encoded FSST block is byte-for-byte identical

#### Scenario: Scratch capacity meets the worst-case bound before every call
- **WHEN** a value of length L is about to be compressed into the reused scratch buffer
- **THEN** the buffer has been cleared and reserved to a capacity of at least 2 × L — the all-escapes worst case — before `compress_into` runs, and the safety comment on the call names that guarantee

#### Scenario: The unsafe surface stays confined to the documented module
- **WHEN** the storage crate's OE sources are audited for `unsafe`
- **THEN** every unsafe block sits in the `storage::oe::encoding` module under its documented allow with a safety comment, and no other OE module contains unsafe
