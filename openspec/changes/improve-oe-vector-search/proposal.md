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

## Impact

- `oe-vector-columns`: segment layout version 3; older segments remain readable.
- More AEAD block opens for a full scan (one per 128 rows instead of per 4096), the same bytes decrypted; a rescore
  decrypts at most one 128-row sub-block per distinct candidate.
