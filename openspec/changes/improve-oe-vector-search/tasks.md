# Tasks — improve-oe-vector-search

- [x] Seal vector columns in 128-row sub-blocks (`VECTOR_SUBBLOCK_ROWS`), segment version 3; footer records the
      sub-block count per granule and readers verify it.
- [x] `rescore`, `vector_row` and `vector_prefix` open one sub-block per touched row; test counts `blocks_opened`.
- [x] `top_k` skips sub-blocks outside the row filter.
- [x] Versions 1 and 2 still read; a version 2 segment scores and decodes the same as version 3.
- [x] `multiversion` lane-accumulator kernels for f32/f16 dot and f32/f16/i8 L2, no per-element `try_into`.
- [x] Document that cosine requires unit-normalised rows (enforced at write with `NORMALIZATION_TOLERANCE`).
- [ ] Optional: per-row squared norm so L2 = |q|² + |x|² − 2q·x (not done).
- [ ] Optional: per-dimension or segment-level int8 bounds as a new element encoding (not done).
