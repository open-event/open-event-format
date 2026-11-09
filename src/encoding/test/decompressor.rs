use super::*;
use crate::encoding::{
    CascadeStrategy, ColumnData, Compression, ReplayCapture, Transform, decode_block, decode_block_range,
    encode_block_replayed,
};
use std::cell::Cell;
use std::sync::{Arc, Mutex};

static INSTALL_LOCK: Mutex<()> = Mutex::new(());

thread_local! {
    static UNDONE_ON_THIS_THREAD: Cell<usize> = const { Cell::new(0) };
}

#[derive(Debug, Clone, Copy, Default)]
struct CountingDecompressor;

impl Decompressor for CountingDecompressor {
    fn decompress(&self, compression: Compression, bytes: &[u8]) -> Result<Vec<u8>, crate::error::FormatError> {
        UNDONE_ON_THIS_THREAD.with(|count| count.set(count.get() + 1));
        SoftwareDecompressor.decompress(compression, bytes)
    }

    fn decompress_into(
        &self,
        compression: Compression,
        bytes: &[u8],
        plain: &mut Vec<u8>,
    ) -> Result<(), crate::error::FormatError> {
        UNDONE_ON_THIS_THREAD.with(|count| count.set(count.get() + 1));
        SoftwareDecompressor.decompress_into(compression, bytes, plain)
    }

    fn backend(&self) -> DecompressorBackend {
        DecompressorBackend::Software
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct DelegatingDecompressor;

impl Decompressor for DelegatingDecompressor {
    fn decompress(&self, compression: Compression, bytes: &[u8]) -> Result<Vec<u8>, crate::error::FormatError> {
        SoftwareDecompressor.decompress(compression, bytes)
    }

    fn backend(&self) -> DecompressorBackend {
        DecompressorBackend::Qatzip
    }
}

fn lz4_block(raw: &[u8]) -> Vec<u8> {
    lz4_flex::compress_prepend_size(raw)
}

fn zstd_block(raw: &[u8]) -> Vec<u8> {
    let body = zstd::bulk::compress(raw, 1).unwrap();
    let mut framed = (raw.len() as u32).to_le_bytes().to_vec();
    framed.extend_from_slice(&body);
    framed
}

#[test]
fn software_round_trips_every_compression() {
    let raw = b"the quick brown fox jumps over the lazy dog, repeatedly and repeatedly".repeat(8);
    let sw = SoftwareDecompressor;

    assert_eq!(sw.decompress(Compression::None, &raw).unwrap(), raw);
    assert_eq!(sw.decompress(Compression::Lz4, &lz4_block(&raw)).unwrap(), raw);
    assert_eq!(sw.decompress(Compression::Zstd1, &zstd_block(&raw)).unwrap(), raw);
    assert_eq!(sw.decompress(Compression::Zstd3, &zstd_block(&raw)).unwrap(), raw);
    assert_eq!(
        sw.decompress(
            Compression::SeekableZstd,
            &crate::encoding::seekable_zstd::compress(&raw).expect("the block compresses")
        )
        .unwrap(),
        raw
    );
    assert_eq!(sw.backend(), DecompressorBackend::Software);
}

#[test]
fn a_reused_zstd_context_decodes_every_block_independently() {
    let sw = SoftwareDecompressor;
    let blocks: Vec<Vec<u8>> = (0..32)
        .map(|i| {
            let len = 1 + i * 37 % 4096;
            (0..len).map(|byte| ((byte * 7 + i) % 251) as u8).collect()
        })
        .collect();

    for round in 0..3 {
        for (index, raw) in blocks.iter().enumerate() {
            let level = if index.is_multiple_of(2) { 1 } else { 3 };
            let body = crate::encoding::compress_zstd(raw, level);
            let mut framed = (raw.len() as u32).to_le_bytes().to_vec();
            framed.extend_from_slice(&body);
            let compression = if level == 1 {
                Compression::Zstd1
            } else {
                Compression::Zstd3
            };
            assert_eq!(
                &sw.decompress(compression, &framed).unwrap(),
                raw,
                "block {index} in round {round}"
            );
            assert_eq!(&sw.decompress(Compression::Lz4, &lz4_block(raw)).unwrap(), raw);
        }
    }
}

#[test]
fn a_reused_zstd_context_encodes_exactly_as_a_fresh_one() {
    let blocks: Vec<Vec<u8>> = (0..16)
        .map(|i| {
            let len = 1 + i * 211 % 8192;
            (0..len).map(|byte| ((byte / (1 + i)) % 251) as u8).collect()
        })
        .collect();

    for (index, raw) in blocks.iter().enumerate() {
        for level in [1, 3, 1] {
            assert_eq!(
                crate::encoding::compress_zstd(raw, level),
                zstd::bulk::compress(raw, level).unwrap(),
                "block {index} at level {level}"
            );
        }
    }
}

#[test]
fn blocks_written_by_the_c_zstd_library_still_decode() {
    let raw = b"the quick brown fox jumps over the lazy dog, repeatedly and repeatedly".repeat(64);
    for (level, compression) in [(1, Compression::Zstd1), (3, Compression::Zstd3)] {
        let body = zstd::bulk::compress(&raw, level).unwrap();
        let mut framed = (raw.len() as u32).to_le_bytes().to_vec();
        framed.extend_from_slice(&body);
        assert_eq!(
            SoftwareDecompressor.decompress(compression, &framed).unwrap(),
            raw,
            "a level-{level} block from the C library must still decode"
        );
    }
}

#[test]
fn software_rejects_forged_zstd_uncompressed_length() {
    let body = zstd::bulk::compress(b"tiny", 1).unwrap();
    let mut framed = u32::MAX.to_le_bytes().to_vec();
    framed.extend_from_slice(&body);

    let err = SoftwareDecompressor
        .decompress(Compression::Zstd1, &framed)
        .unwrap_err();
    assert!(matches!(err, crate::error::FormatError::Structural { .. }));
}

#[test]
fn software_rejects_forged_lz4_uncompressed_length() {
    let body = lz4_flex::compress_prepend_size(b"tiny");
    let mut forged = u32::MAX.to_le_bytes().to_vec();
    forged.extend_from_slice(&body[4..]);

    let err = SoftwareDecompressor.decompress(Compression::Lz4, &forged).unwrap_err();
    assert!(matches!(err, crate::error::FormatError::Structural { .. }));
}

#[test]
fn software_rejects_an_lz4_block_that_inflates_short_of_its_size_prefix() {
    let raw = b"short but genuine payload".repeat(4);
    let block = lz4_flex::compress_prepend_size(&raw);
    let mut forged = ((raw.len() + 10) as u32).to_le_bytes().to_vec();
    forged.extend_from_slice(&block[4..]);

    let err = SoftwareDecompressor.decompress(Compression::Lz4, &forged).unwrap_err();
    assert!(matches!(err, crate::error::FormatError::Structural { .. }));
}

#[test]
fn active_decompressor_defaults_to_software_and_swaps_when_installed() {
    let _installing = INSTALL_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(active_decompressor().backend(), DecompressorBackend::Software);

    let raw = b"cold block bytes that an accelerator would decompress".repeat(4);
    let framed = zstd_block(&raw);

    let oracle = SoftwareDecompressor.decompress(Compression::Zstd1, &framed).unwrap();

    install_decompressor(Arc::new(DelegatingDecompressor));
    let active = active_decompressor();
    assert_eq!(active.backend(), DecompressorBackend::Qatzip);
    assert_eq!(active.decompress(Compression::Zstd1, &framed).unwrap(), oracle);
    let mut plain = Vec::new();
    active.decompress_into(Compression::Zstd1, &framed, &mut plain).unwrap();
    assert_eq!(plain, oracle);

    install_decompressor(Arc::new(SoftwareDecompressor));
    assert_eq!(active_decompressor().backend(), DecompressorBackend::Software);
}

#[test]
fn decompress_into_reuses_the_callers_buffer_and_matches_decompress() {
    let sw = SoftwareDecompressor;
    let large = b"a larger block whose bytes must not survive into the next decode".repeat(64);
    let small = b"small".repeat(3);
    let mut plain = Vec::new();
    sw.decompress_into(Compression::Zstd3, &zstd_block(&large), &mut plain)
        .unwrap();
    assert_eq!(plain, large);
    let capacity = plain.capacity();
    let pointer = plain.as_ptr();
    for (compression, stored) in [
        (Compression::Zstd1, zstd_block(&small)),
        (Compression::Lz4, lz4_block(&small)),
        (Compression::None, small.clone()),
        (Compression::Zstd3, zstd_block(&small)),
    ] {
        sw.decompress_into(compression, &stored, &mut plain).unwrap();
        assert_eq!(plain, small, "{compression:?}");
        assert_eq!(plain, sw.decompress(compression, &stored).unwrap());
        assert_eq!(plain.as_ptr(), pointer, "{compression:?} must reuse the buffer");
        assert_eq!(plain.capacity(), capacity, "{compression:?} must keep the allocation");
    }
}

#[test]
fn software_rejects_a_zstd_block_that_inflates_short_of_its_length_prefix() {
    let raw = b"short but genuine payload".repeat(4);
    let block = zstd_block(&raw);
    let mut forged = ((raw.len() + 10) as u32).to_le_bytes().to_vec();
    forged.extend_from_slice(&block[4..]);

    let err = SoftwareDecompressor
        .decompress(Compression::Zstd3, &forged)
        .unwrap_err();
    assert!(matches!(err, crate::error::FormatError::Structural { .. }));
    let mut plain = Vec::with_capacity(1 << 16);
    let err = SoftwareDecompressor
        .decompress_into(Compression::Zstd3, &forged, &mut plain)
        .unwrap_err();
    assert!(matches!(err, crate::error::FormatError::Structural { .. }));
}

#[test]
fn software_rejects_a_zstd_block_that_inflates_past_its_length_prefix() {
    let raw = b"a genuine payload longer than its forged prefix claims".repeat(4);
    let block = zstd_block(&raw);
    let mut forged = ((raw.len() - 10) as u32).to_le_bytes().to_vec();
    forged.extend_from_slice(&block[4..]);

    let err = SoftwareDecompressor
        .decompress(Compression::Zstd3, &forged)
        .unwrap_err();
    assert!(matches!(err, crate::error::FormatError::Structural { .. }));
    let mut plain = Vec::with_capacity(1 << 16);
    let err = SoftwareDecompressor
        .decompress_into(Compression::Zstd3, &forged, &mut plain)
        .unwrap_err();
    assert!(matches!(err, crate::error::FormatError::Structural { .. }));
}

#[test]
fn software_rejects_a_truncated_zstd_body_on_both_paths() {
    let raw = b"a payload long enough that half its frame is not a frame".repeat(16);
    let block = zstd_block(&raw);
    let truncated = &block[..block.len() / 2];

    let err = SoftwareDecompressor
        .decompress(Compression::Zstd1, truncated)
        .unwrap_err();
    assert!(matches!(err, crate::error::FormatError::Structural { .. }));
    let mut plain = Vec::with_capacity(raw.len());
    let err = SoftwareDecompressor
        .decompress_into(Compression::Zstd1, truncated, &mut plain)
        .unwrap_err();
    assert!(matches!(err, crate::error::FormatError::Structural { .. }));
}

#[test]
fn a_forged_zstd_length_never_grows_the_reused_buffer() {
    let body = zstd::bulk::compress(b"tiny", 1).unwrap();
    let mut framed = u32::MAX.to_le_bytes().to_vec();
    framed.extend_from_slice(&body);

    let mut plain = Vec::with_capacity(64);
    let capacity = plain.capacity();
    let err = SoftwareDecompressor
        .decompress_into(Compression::Zstd1, &framed, &mut plain)
        .unwrap_err();
    assert!(matches!(err, crate::error::FormatError::Structural { .. }));
    assert_eq!(plain.capacity(), capacity);
}

#[test]
fn the_inflate_buffer_is_retained_up_to_its_bound() {
    let pointer = with_inflate_buffer(|buffer| {
        buffer.clear();
        buffer.reserve_exact(1024);
        buffer.as_ptr()
    });
    assert_eq!(with_inflate_buffer(|buffer| buffer.as_ptr()), pointer);
    with_inflate_buffer(|buffer| buffer.reserve_exact(MAX_RETAINED_INFLATE_BYTES + 1));
    assert!(with_inflate_buffer(|buffer| buffer.capacity()) <= MAX_RETAINED_INFLATE_BYTES);
}

#[test]
fn a_compressed_fallback_range_read_decompresses_once() {
    let _installing = INSTALL_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let values: Vec<u64> = (0..8192u64).map(|i| (i % 64) * 7).collect();
    install_decompressor(Arc::new(CountingDecompressor));
    for compression in [Compression::Lz4, Compression::Zstd1, Compression::Zstd3] {
        let capture = ReplayCapture {
            compression,
            decoded_len: 8192 * 8,
            fsst: None,
            raw_len: 8192 * 8,
            transform: Transform::ForBitpack,
        };
        let (block, replayed) = encode_block_replayed(
            &ColumnData::U64(values.clone()),
            false,
            CascadeStrategy::DecodeOptimized,
            Some(&capture),
            None,
        );
        assert!(replayed);
        assert_eq!(block.pipeline.compression().unwrap(), compression);
        assert_eq!(
            decode_block(block.pipeline, &block.bytes).unwrap(),
            ColumnData::U64(values.clone())
        );
        for (start, end) in [(0, 1), (100, 2_000), (0, values.len()), (8_000, values.len())] {
            UNDONE_ON_THIS_THREAD.with(|count| count.set(0));
            let rows = decode_block_range(block.pipeline, &block.bytes, start, end).unwrap();
            assert_eq!(
                UNDONE_ON_THIS_THREAD.with(Cell::get),
                1,
                "{compression:?} {start}..{end}"
            );
            assert_eq!(rows, ColumnData::U64(values[start..end].to_vec()));
        }
    }
    install_decompressor(Arc::new(SoftwareDecompressor));
}
