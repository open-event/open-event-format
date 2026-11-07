# Migration from harana/hef

This repository was migrated from `harana/hef` at commit `343c6710313adf27a0a06fca88cd3f54a2903114`. HEF became OE and
HEJ became OEJ. Anything specific to Matrix, Pulse or Harana was left out or reworded. Everything removed is still in
that commit of `harana/hef`.

## Matrix code removed

Matrix federation provenance: re-verifying a Matrix event offline from its original JSON, its room version and the
signing servers' public keys.

| Removed | What it did |
|---|---|
| `src/events/matrix.rs` (340 lines) | `MatrixProvenance` (`new`, `room_version`, `signatures`, `verify`), plus `canonical_json`, `signing_bytes`, `reference_hash`, `event_id` and `content_hash`. Per-room-version redaction rules for versions 1 to 11, canonical JSON, content hash, and the event id (`$` plus reference hash). |
| `src/events/test/matrix.rs` (204 lines) | Nine tests: canonical JSON, two-server signature re-verification, tamper detection, room-version id rules and redaction, unknown room versions. Also the shared helpers `server_key`, `federate` and `message_event`. |
| `benches/matrix.rs` (1828 lines) and its `[[bench]]` entry | The Matrix room workload benchmark against SQLite. |
| `MATRIX_MAX_SAFE_INTEGER`, `MATRIX_ROOM_VERSIONS` in `src/events/constant.rs` | Limits used by the verifier. |
| `column_ids::MATRIX_ROOM_VERSION`, `column_ids::SIGNER_SIGNATURES` and their `SOURCE_FORM_COLUMNS` entries in `src/columns.rs` | The `matrix_room_version` and `signer_signatures` columns (ids 6006 and 6007). |
| `BuildRow.matrix_provenance` in `src/writer/build.rs` | The row field, with its use in the build, update-equality, audit-row check and source-form writer (`src/writer/source_form.rs`, `src/writer/audit.rs`). |
| `matrix_provenance()` in `src/layout/source_form.rs` | The reader that rebuilt the provenance from a sealed file. |
| Tests in `src/writer/test/source_form.rs` | The federation-signed event re-verification test and the signer-signature block test. The canonical JSON round-trip test stays, without the Matrix event. |
| Spec text in `openspec/specs/oe-logical-event-model/spec.md` | The multi-signer sentence and the "Federated event re-verifies with several server signatures" scenario. |
| `openspec/changes/replace-nostr-with-matrix/` | The whole change. |

Kept: `SignerSignature` and the Ed25519 and BIP-340 signature schemes in `src/events/provenance.rs`, and the generic
external-id and relationship features. Their Matrix examples are reworded as protocol ids.

## Other differences

- `openspec/changes/incorporate-liquid-cache-learnings/` was left out because it is about Pulse integration.
- The format identifiers changed: `harana_variant_v1` is now `oe_variant_v1`, `harana_hej_compact_batch_v1` is now `oe_oej_compact_batch_v1`, and the hashing and key context strings now start with `open-event`. The pinned audit chain hash was updated to match. Files written by the old format will not verify.
- The 4-byte file magics became `OEF1`, `OEFT`, `OEFL`, `OEFC` and `OEFV`.
