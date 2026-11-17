## ADDED Requirements

### Requirement: Vector search recall and latency gate
The benchmark crate SHALL carry a deterministic (seeded) vector search benchmark (`oe-vector-bench`) over clustered
unit vectors at 128 and 768 dimensions and 10,000 and 100,000 rows (1,000,000 behind a flag), each written in time
order and in cluster order, with top-10, room-filtered (about 1% and about 50% selectivity), range and small
multivector (MaxSim) workloads. It SHALL compare exact f32 brute force (the ground truth), a scan (coarse U1 or I8
code plus F32 rerank), a partition-indexed (IVF) search and, for time-ordered data, a clustered merge followed by a scan
and by an indexed search, and SHALL report per configuration and workload recall@10 (recall of the range for range
queries), p50 and p99 latency in milliseconds, sealed blocks opened and rows reranked per query, segment and index
bytes per vector, and segment and index build time, as JSON and a Markdown table under the benchmark results.

An acceptance test run by `cargo test --release` in the benchmark crate, on sizes small enough to finish in about a
minute, SHALL fail when default settings (`SegmentSpec::new`, `DEFAULT_OVERSAMPLE`, `SearchOptions::default`) give
recall@10 below 0.95 on any of these workloads, or when on clustered data (cluster-ordered rows, or time-ordered rows
after a clustered merge) the indexed search does not open fewer blocks per query than a scan of the same data.

#### Scenario: Recall regression fails the gate
- **WHEN** a change to the coarse codes, bounds or rerank lowers default recall@10 below 0.95 on the gate datasets
- **THEN** `cargo test --release` in the benchmark crate fails and names the dataset, configuration and workload

#### Scenario: Index that stops saving block reads fails the gate
- **WHEN** an indexed search on cluster-ordered data opens as many blocks per query as a scan
- **THEN** the gate fails, so the partition index is not kept as a default path without a measured benefit
