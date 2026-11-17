## ADDED Requirements

### Requirement: Compaction writes vector segments in clustered row order
`compact_segments` SHALL be the compaction entry point for vector segments: it SHALL merge the live rows of its inputs
(deletes applied) into one segment in clustered row order (rows grouped by k-means partition with default
`PartitionParams`, partitions chained nearest centroid next), moving event ids and every column with their rows, and
SHALL give the same search answers by event id as an in-order merge. `merge_segments` keeps rows in input order for
callers that need it. Partition indexes and sub-block ball bounds only save block reads when similar rows share sealed
blocks, so time-ordered segments compacted in order keep opening almost every block per query.

#### Scenario: Compaction defaults to clustered order
- **WHEN** time-ordered segments are compacted with `compact_segments`
- **THEN** the result holds the same rows as `merge_segments_clustered` with default parameters, in the same order,
  and an indexed search over it opens a fraction of the blocks the in-order merge needs
