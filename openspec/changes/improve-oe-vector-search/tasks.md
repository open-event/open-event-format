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
- [x] Sub-block balls: version 4+ footers end with an optional tail (tag 1) holding, per 128-row vector sub-block,
      the centre and radius of its finest-representation rows; segments without the tail still read, unknown tags
      are rejected. The bounded coarse scan visits sub-blocks best ball bound first and skips those whose ball cannot
      reach the bar (`SearchStats::subblocks_skipped`); when the balls separate, the best sub-blocks seed the bar
      with exact scores. Tests: stripped-tail segments answer identically without skipping.
- [x] Small dimensions: `default_representations` / `SegmentSpec::new` pick per-dimension I8 + F32 below
      `RABITQ_MIN_DIMENSION` (64) and U1 + F32 from there. Extended multi-bit RaBitQ is deliberately not added: I8
      already has tight per-row bounds at any dimension and at small `d` costs only a few bytes more per row than
      U1 (d = 16: recall@10 1.0 with 12–16 rows reranked vs U1 0.98–0.99 with ~217), so a second multi-bit code
      would add a format element without a measurable win where it matters.
- [x] Default recall: `oversample` is a rerank patience for version 4 segments (stop after `k × oversample`
      consecutive reranked rows that did not enter the top k) instead of a hard cap. Defaults now give
      recall@10 ≥ 0.98 on tight clusters written in row order (1.0 at d ≥ 32; was 0.61–0.89 with the cap) with ~125
      rows reranked of 8 000 (the query's cluster), test `default_settings_keep_recall_on_tight_clusters`.
- [x] Partition index (IVF) as an external, droppable artifact: `PartitionIndex::build` (seeded k-means,
      `PartitionParams { partitions, iterations, seed }`), `encode`/`decode` (`OEVI` v1, optional seal under the
      segment wrapping key), `publish_segment_index` / `load_segment_index` / `drop_segment_index`, and
      `VectorSegmentEntry::index` (serde default). Loading checks the object BLAKE3, segment content hash, model
      digest, row count and partition count.
- [x] `search_segments_indexed` / `search_catalogued_segments_indexed`: probe `nprobes` (default 8) closest
      partitions, then widen to every partition whose bound reaches the k-th score; probed rows ∩ filter ∖ deletes
      is the row filter. A rejected index is counted (`SearchStats::indexes_rejected`) and the segment scanned.
- [x] Tests: indexed answer equals the scan on clustered data (8 000 × 64, 64 clusters in row order; recall@10 = 1.0
      cosine and L2, blocks opened per query 66 → 16–18, ~8.3 of 85 partitions probed); digest, model, content and
      key mismatches rejected; a dropped or rejected index still answers identically; filters and deletes combine.
- [x] Filters: byte hash sets over borrowed strings; absent granule stats mean "may match"; adaptive pre/post
      filtering (`FilterStrategy`, `POST_FILTER_SELECTIVITY` = 0.75) with tests that post, pre and adaptive agree.
- [x] Clustered compaction: `merge_segments_clustered` writes live rows grouped by k-means partition (partitions
      chained nearest centroid), event ids and every column moving with their rows. Time-ordered 50 clusters × 160
      rows (d = 64), blocks opened per query: scan 123.5 → 36.1 (Cosine, 30 sub-blocks skipped) / 49.1 (L2);
      indexed 123.4 → 12.4 / 13.4; same answers by event id. Granule timestamp stats get less selective in exchange.
- [x] Probe rounds of one segment share a cache of opened blocks (`PartCache`), so an indexed search never decrypts a
      block twice. 200 natural clusters / 90 partitions (~35 probed Cosine, ~63 L2): indexed opens 62.3 vs scan 66.1
      blocks in cluster order (was 76.9) and the same 94.9 in time order; answers equal.
- [ ] Still open by nature: with far more natural clusters than partitions the partition radii stay loose, so many
      partitions are probed; and when natural clusters are smaller than a 128-row sub-block, no row order makes
      sub-blocks homogeneous, so ball skipping does not apply. Filter selectivity is still estimated from min/max
      granule stats only.
- [x] Index GC: `vector_object_keys` / `dropped_vector_object_keys`; `sweep_dropped_objects` notes dropped or
      replaced partition indexes, superseded segment deletes and both for segments removed from the catalogue, and
      deletes them after the safety window unless a live generation names them.
- [x] `range_truncated` tested through the internal result-cap hook (`search_in_range_capped`).
- [x] `RoaringRangeBitmap::union` back to private (only `BitmapIndex::insert` uses it).
- [x] Range search (`search_segments_in_range`, `ScoreRange`, `MAX_RANGE_RESULTS`, `SearchStats::range_truncated`):
      segment lower/upper bounds (`SegmentCodec::segment_bounds`), partition lower bounds and row bounds skip work
      outside the range. Tests: band ranges for Dot and L2 with I8 and U1 coarse codes equal brute force (recall 1.0;
      I8 reranks ~690 rows of 28 800 scanned over 12 queries), the partition index gives the same answer, `Some(limit)`
      returns the best of the range, an L2 radius skips the far segment, inverted ranges are rejected.
- [x] Multivector segments (version 5 document offsets, `write_multivector_segment`, `document_offsets` /
      `document_of` / `document_rows`) and MaxSim search (`search_multivector_segments`, `MultivectorSearch`,
      `max_sim`). Tests: 700 documents × 2–10 tokens × 128 dims in 2 segments, Dot and L2, U1 and I8 coarse, default
      and 3 token candidates: top-10 equals brute-force MaxSim (200/200); I8 scores ~2 300 of 42 000 tokens exactly.
      Version 5 header, plain segments stay version 4, filters, deletes, `max_rerank`, invalid input.
- [x] RRF (`reciprocal_rank_fusion`, `hybrid_rrf`, `RrfParams`, `DEFAULT_RRF_K`) with tests for weights, ties,
      duplicates, limit and determinism.
- [x] Multivector merge: `merge_segments(_with_deletes)` merges version 5 segments (documents keep their tokens, a
      document with no live token is dropped, metadata travels with it; mixing with plain segments is refused);
      `merge_segments_clustered` groups documents by their mean token's partition. `read_live_documents`.
- [x] MaxSim with partition indexes (`search_multivector_segments_indexed`): per query token the `nprobes` best
      partitions (more while under 2 × `token_candidates` rows), unprobed partitions' bounds imputed, widening ×4
      until unseen documents cannot reach the k-th score. Segments are visited by MaxSim bound then centroid
      closeness with the cross-segment threshold. 600 documents over 64 topics: token rows scanned 2 400 → ~1 050
      per query, same answers; a far segment is skipped.
- [ ] Not done: RaBitQ (U1) bounds at small sizes are loose, so U1 range searches rerank a large share of rows
      (still exact); range search over version 1–3 segments scans the fine column. No ranked text search exists in
      OE to wire into `hybrid_rrf`.
- [x] Recall and latency benchmark (`benchmark/vector`, `oe-vector-bench`, `just bench-vector`): seeded clustered
      datasets (10k/100k, d = 128/768, time and cluster order; 1M behind `--large`), top-10, 1% and 50% room filters,
      range and MaxSim workloads; exact, scan, indexed and clustered-merge configurations; recall@10, p50/p99, blocks
      opened, rows reranked, bytes per vector, build time to `benchmark/results/vector.{json,md}`. Gate test in
      `cargo test --release` (10k × 128, 5k × 768, MaxSim): recall@10 ≥ 0.95 and indexed opens fewer blocks than scan
      on clustered data; spec delta `specs/oe-benchmarks-and-acceptance-gates`.
- [ ] Default recall at 100k rows: top-10 recall@10 is 0.87–0.90 with `DEFAULT_OVERSAMPLE` (8) on topics of ~630 rows
      (patience 32 or 0 gives 1.000 at the same latency); the 100k gate test is ignored until the default changes.
      A 1% room filter opens 700–3,200 blocks and reranks far more rows than the room holds.
