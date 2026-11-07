//! Checks that the rebuildable hybrid local OE cache honours tenant isolation and data-class requirements, and that single-subject encrypted blocks are cached only in their encrypted form so crypto-shredding remains effective.

/// conformance: oe-hardware-deployment/rebuildable-hybrid-local-oe-cache/encrypted-block-stays-encrypted-in-cache
#[test]
fn encrypted_block_stays_encrypted_in_cache() {
    // stub: blocked on the foyer-style hybrid cache implementation and the OE block encryption layer (D6); lands with the local-OE-cache and per-subject encryption changes.
}
