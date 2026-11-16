Status: Proposed (2026-11-16)

## Why

Issue open-event/open-event-format#98 lists costs in the vector segment read path. Rescore opened the whole
4096-row granule block of the full-precision representation for every candidate's granule, so a rescore of a few
hundred candidates spread over a segment decrypted almost the whole fine column. The scoring kernels decoded one
element at a time through a fallible conversion and summed into a single accumulator, which the compiler does not
vectorise.

## What Changes

- Vector segment version 3: every vector column (coarse and fine) is sealed in sub-blocks of
  `VECTOR_SUBBLOCK_ROWS` (128) rows, each with its own AEAD block index and nonce. The footer records, per vector
  column and granule, a sub-block count (`ceil(rows / 128)`, at least 1) followed by that many block references;
  string and integer columns keep one block per granule. Readers check the count against the granule's rows.
- `rescore`, `vector_row` and `vector_prefix` open only the sub-block holding each row; `top_k` iterates sub-blocks
  and skips those the row filter does not touch; granule reads (`vector_granule`, `read_live`) concatenate them.
- Versions 1 (shared nonce) and 2 (one vector block per granule) still read; writers emit only version 3 and
  `merge_segments` rewrites older segments as version 3. `rewrap_segment` keeps the version.
- Kernels: f32 and f16 dot, and L2 over f32, f16 and int8, are `multiversion` functions over `as_chunks` /
  `chunks_exact` with eight lane accumulators. Scores now differ from a sequential sum only by float reassociation.
- Int8 quantisation stays per-row min/max (scale and offset in the row trailer); no new element encoding. Cosine
  segments SHALL hold unit-normalised vectors: the writer refuses any row whose L2 norm differs from 1 by more than
  `NORMALIZATION_TOLERANCE` (1e-3), and cosine scores are computed as dot products on that basis.

- Vector segment version 4 (issue #98 items 3, 4, 7): U1 columns become RaBitQ codes (deterministic rotation seeded
  from the model, centred on the segment mean, per-row norm and alignment, derived error bound); I8 columns become
  segment-level per-dimension int8 whose trailer stores `|x̂|²` and the reconstruction error. The footer appends the
  rotation seed, the RaBitQ centre, the segment radius and the per-dimension int8 parameters. Searches rerank by
  bounds instead of a fixed `k × oversample` (which becomes a cap), visit segments nearest centroid first and
  pass the running k-th exact score into each segment. Versions 1–3 read unchanged (Hamming U1, per-row I8).

- Partition index (issue #98 item 2): an external, droppable IVF artifact per segment, following the manifest-
  referenced artifact rule (incorporate-lance-learnings design §2). Deterministic seeded Lloyd k-means (fixed
  iterations, `⌈√rows⌉` partitions by default) over the segment's fine vectors; the object (`OEVI` v1) holds the
  segment `content_hash`, model, per-partition centroid, radius and delta-varint sorted row ids, optionally sealed
  with XChaCha20-Poly1305 under a key derived from the segment's wrapping key. `VectorSegmentEntry` gains
  `index: Option<SegmentIndexRef>` (serde default) with the object's BLAKE3, the content hash and model digest it is
  bound to. Search probes the `nprobes` closest partitions, then every partition whose bound (`centroid score +
  |q|·radius`, or the L2 ball gap) still reaches the k-th score; the probed rows become the row filter, so the
  scan opens only their sub-blocks. Without an index (or after it is dropped or rejected) the segment is scanned,
  with the same result.
- Filters (issue #98 item 5): allowed values are byte hash sets compared against borrowed string bytes (no per-row
  `String`); granules with no statistics may match; an adaptive strategy post-filters (scan, then filter the
  candidates, widen once, then fall back to pre-filtering) when granule statistics estimate the filter keeps at
  least 75% of rows. Results equal the pre-filter path.

- Range search (issue #98 item 8a): `search_segments_in_range` with `ScoreRange { min, max }` in score order (L2
  score is `-distance²`; `ScoreRange::l2_radius`, `l2_annulus`). `limit: None` returns every row in range, capped at
  `MAX_RANGE_RESULTS` with `SearchStats::range_truncated`. The range minimum joins the running threshold; segments
  (new segment lower bound from the version 4 centre and radius), partitions (new `PartitionIndex::lower_bound`) and
  coarse rows whose lower bound exceeds the maximum are skipped; a row's lower bound raises the bar only if its upper
  bound is within the maximum. Hits are filtered on the exact fine score. Version 1–3 coarse codes have no bounds, so
  ranged searches score those segments on the fine column.
- Multivector (issue #98 item 8b): vector segment version 5 = version 4 plus a footer tail of document offsets
  (`u64` document count, then count + 1 first-token rows, starting at 0, strictly increasing, ending at the row
  count). Each token is one row and repeats its document's event id and metadata, so filters, deletes and row reads
  are unchanged. Only `write_multivector_segment` writes version 5; single-vector segments stay version 4 and
  versions 1–4 read unchanged. `search_multivector_segments` scores `Σ_q max_t sim(q, t)` (MaxSim): WARP-style
  candidate generation keeps the best `token_candidates` coarse token rows per query token by upper bound, imputes
  the best dropped upper bound for a document missing from a query token's candidates, reranks documents exactly on
  the fine column in decreasing imputed bound and, if the never-seen bound can still reach the k-th score, scores
  the rest, so results equal brute-force MaxSim. `max_rerank` caps reranking (approximate). `merge_segments` refuses
  multivector segments.
- Hybrid (issue #98 item 8c): `reciprocal_rank_fusion` over weighted ranked id lists and `hybrid_rrf` (vector
  `SearchHit`s + a ranked text id list), `RrfParams { k = 60, weights, limit }`. OE's text-token index is a pruning
  filter, not a ranked text search, so the text list comes from the caller. Only the first occurrence in a list
  counts; ties break by best rank, then list order, then event id.

## Impact

- `oe-vector-columns`: segment layout version 3; older segments remain readable.
- More AEAD block opens for a full scan (one per 128 rows instead of per 4096), the same bytes decrypted; a rescore
  decrypts at most one 128-row sub-block per distinct candidate.
