# OE Write Path — Ingest to Durable Acknowledgement, Safe Retry, Publish, and Rewrite

Companion artifact for the `oe-write-path` capability — the concrete formats, schemas, byte-layouts, and tables referenced by `spec.md`.


### Ingest to durable acknowledgement

```text
1. Ingest worker validates event.
2. Ingest worker serializes the event into a pending `oe_oej_compact_batch_v1` record descriptor.
3. The pending descriptor is appended to the worker's lock-free serialized commit queue.
4. The event is in READY state. It is not durable, not queryable, and not public.
5. The worker checks whether its unclaimed pending bytes have reached the selected flush target.
6. The default flush target is 16 KiB; latency-critical and low-load force-commit flushes use 4 KiB.
7. The worker may topology-locally steal same-tenant/same-epoch pending records from peer queues using the clean-cursor CAS rule.
8. The claiming worker reserves one contiguous `(epoch, sequence)` range for the final OEJ frame.
9. The worker writes `first_sequence`, `last_sequence`, and per-row sequence order into `OEJFrameHeaderV1` and the compact batch.
10. The worker submits the aligned OEJ frame through io_uring: io_uring_cmd NVMe passthrough on the journal char device, or filesystem io_uring in the non-NVMe dev fallback.
11. Completion is polled by the submitting worker.
12. Header CRC-64/NVME and frame BLAKE3 are verified.
13. Event becomes HARDENED in OEJ after the selected durability policy completes.
14. Minimal safe-retry metadata is committed or made reconstructable.
15. For ordinary append-only event ingest, HARDENED becomes COMMITTED immediately.
16. For routes with dependent mutable state, autonomous acknowledgement checks the route dependency condition before COMMITTED.
17. The durable OEJ frame is decoded into the fixed LiveOverlay Arrow RecordBatch schema.
18. Event is published into LiveOverlay as part of an immutable Arrow RecordBatch segment.
19. Client is acknowledged according to the API durability/freshness contract.
20. Event becomes eligible for OE publication.
```

The latency paper’s core recommendation is exactly this direction: workers independently flush logs when their local threshold is reached, avoid large SSD writes, avoid centralized group commit, and decouple log flush from commit acknowledgement. OEJ applies that to event ingest by treating append-only event dependencies as trivially satisfied after journal durability, while retaining the autonomous acknowledgement machinery only for routes that truly have dependent mutable state.

LiveOverlay publication must not redefine durability. Durability is OEJ completion. LiveOverlay publication defines query visibility for OEJ-backed rows not yet OE-covered.

Reference: <https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/latency.pdf>

### OEJ and safe retry

OEJ is the protected event payload record. The system must not require both a full KV buffer payload and a OEJ payload before acknowledgement.

Safe retry uses:

```text
client_request_id hash or connector delivery identity;
tenant/account scope;
commit receipt;
OEJ frame cursor;
response status class;
expiry;
minimal dedupe metadata.
```

After restart/takeover, safe-retry state is reconstructed from retained replay-guard rows plus OEJ coverage. Replaying a retained OEJ frame must not enqueue duplicate events when the original acknowledgement is still within the replay-guard window.

### Publishing OE from OEJ

```text
1. Vended durable OEJ ranges are read by the OE publisher.
2. OEJ frames are validated with header CRC-64/NVME, frame BLAKE3, and segment BLAKE3 chain.
3. oe_oej_compact_batch_v1 payloads are decoded using the public deterministic mapping.
4. Events are decoded into OE column builders.
5. Granules are formed using index_granularity and index_granularity_bytes.
6. Dictionaries are built.
7. Promoted attributes and shredded payload paths are selected from per-path statistics.
8. Adaptive encoding samples are collected and encoding pipelines are chosen per block.
9. OE stripe, granule, marks, and page data are written.
10. Required SkipIndex, exact aggregate, and checksum blocks are written.
11. Optional feature blocks are written only when enabled by the OE publication policy and benchmark gates.
12. Optional projections are written when benchmark-justified.
13. File-level directory and footer are written.
14. BLAKE3 checks are finalized.
15. Manifest entry, projection metadata, deletion-vector generation, and coverage watermarks are atomically published.
16. OE journal coverage is advanced in the manifest.
17. LiveOverlay segments fully covered by the new published OE range become evictable.
18. OEJ retention cursor and segment recycling eligibility advance only after publication and the configured recovery safety window.
```

The publisher uploads the sealed file through the application-supplied object-store interface under its content-addressed key (`oe_object_key(tenant, file_id)`) after staged verification and before any manifest generation is written. A file larger than the provider's minimum part size (5 MiB) goes up as a multipart upload whose parts are cut only where a stripe begins (`oe_upload_segments`), so no stripe straddles two parts; a smaller file is one create-only PUT, and multipart completion is create-only too. The publisher then checks the provider's recorded size and whole-object CRC-64/NVME against the build, and only a matching object may be named by a generation. Any upload failure aborts the multipart upload and rolls the attempt back without writing a generation. A key that already holds a matching object (an earlier attempt for the same content) is verified rather than re-uploaded. An attempt that loses the range after uploading never deletes the object — a stalled generation may still name it — and leaves it unreferenced for an application sweep of unreferenced keys; an incomplete upload orphaned by a crashed publisher is reaped by provider lifecycle policy.

The OE publisher must be idempotent. Re-publishing the same journal range must produce either the same file identity or a safely replaceable file.

### OE rewrite and repacking

```text
1. Select manifest-published Active OE files and visible OE-native deletion-vector/correction metadata.
2. Mark selected source files Outdated only after the replacement generation is published.
3. Choose target layout_class and projection set based on workload and policy; select rewrite scope incrementally using per-granule clustering_quality (see the oe-layout-and-clustering capability) rather than whole-file resorts by default.
4. Merge and sort selected rows into granules.
5. Apply safe deletion-vector removal and correction folding when allowed.
6. Rebuild dictionaries, promoted columns, variant_shredded_field_blocks, variant dictionary blocks, and adaptive encodings.
7. Build required marks, granule directories, SkipIndexes, and aggregates.
8. Build optional indexes, rollups, sparse cubes, sketches, context blocks, and optional internal vector blocks when benchmark-justified.
9. Write replacement OE file(s) using the same OE format.
10. Verify BLAKE3 checksums, aggregate coverage, marks coverage, and deletion-vector accounting.
11. Publish new generation manifest atomically.
12. Move superseded files to DeleteOnDestroy only after the safety window and in-flight query horizon expire.
```

OE rewrite is an optimization and cleanup mechanism. It does not create a new file format and does not create separate freshness, historical, cold, or archive OE classes.

