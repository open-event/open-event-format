# Tasks — improve-oe-vector-search

- [x] Seal vector columns in 128-row sub-blocks (`VECTOR_SUBBLOCK_ROWS`), segment version 3; footer records the
      sub-block count per granule and readers verify it.
- [x] `rescore`, `vector_row` and `vector_prefix` open one sub-block per touched row; test counts `blocks_opened`.
- [x] `top_k` skips sub-blocks outside the row filter.
- [x] Versions 1 and 2 still read; a version 2 segment scores and decodes the same as version 3.
- [x] `multiversion` lane-accumulator kernels for f32/f16 dot and f32/f16/i8 L2, no per-element `try_into`.
- [x] Document that cosine requires unit-normalised rows (enforced at write with `NORMALIZATION_TOLERANCE`).
- [x] Segment version 4: U1 columns hold RaBitQ codes — rows centred on the segment mean, rotated by a deterministic
      sign-flip + fast Walsh–Hadamard rotation (head/tail power-of-two windows for other dimensions, seed derived
      from the model and stored in the footer), sign bits plus `norm = |o − c|` and `alignment = <ō, u>`. The
      per-row error bound `ε₀·√(1−a²)/(a·√(d−1))` is derived from those factors (`RABITQ_EPSILON` = 2.5).
- [x] Query side quantised to 4 bits as four bit planes; the estimator is five popcounts per 64 dimensions
      (`plane_counts`, multiversion). Metric-aware: dot/cosine use `q·c + <o−c, q>`, L2 `|q−c|² + |o−c|² − 2<o−c, q−c>`.
- [x] Bound-driven rerank (`search_bounded`): candidates are rows whose upper bound reaches the k-th best lower
      bound (and the global threshold); rerank in decreasing upper bound until it falls below the k-th exact score.
      `oversample` is now a cap (`k × oversample`, 0 = none) for version 4 and the fixed multiplier for v1–v3.
- [x] Cross-segment: `search_segments*` visit segments nearest centroid first and pass the running global k-th exact
      score; a segment whose bound (`centre`, stored `radius`) cannot reach it is skipped unopened, and rows whose
      upper bound cannot reach it are dropped during the scan. `search_segments_with_stats` reports the work.
- [x] Segment-level per-dimension int8 (version 4 I8): footer stores `(scale, offset)` per dimension; the row trailer
      holds `|x̂|²` and `|x − x̂|`, giving L2 via `|q|² + |x̂|² − 2q·x̂` and a deterministic bound, so I8 can also be
      a bounded coarse stage. v1–v3 I8 keeps per-row min/max decoding.
- [x] Tests: rotation orthonormality, bound coverage (RaBitQ <1% misses, I8 none), recall vs brute force on
      20 000 × 128 clustered rows in 4 segments (recall@10 = 1.0 for cosine/dot/L2, ~56 rows reranked per query
      over 4 segments ≈ 14 per segment vs 80 fixed), byte-identical vector blocks for equal input, v3 back-compat
      (hamming + per-row I8) and merge into v4, far-segment skip.
- [ ] Per-row squared norm for F32/F16 — not done, deliberately: the direct `(x − q)²` kernel costs the same one
      subtract + FMA per element as a dot product and both are bound by decrypting the sub-block, so storing `|x|²`
      would not speed scoring measurably, while it would change the F32/F16 row width (breaking the Arrow
      `FixedSizeList` granule view and prefix reads). The norm is stored where it pays: the v4 I8 trailer.
- [ ] Not done: sub-block-level skipping in the coarse scan (needs per-sub-block centres/radii in the footer);
      RaBitQ for very small dimensions is noisy (bound ∝ 1/√d), so tiny-dimension segments rerank many rows.
