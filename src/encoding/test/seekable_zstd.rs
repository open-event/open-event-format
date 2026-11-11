use super::*;

fn arena(records: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    for index in 0..records {
        bytes.extend_from_slice(
            format!(r#"{{"amount":{index},"currency":"USD","note":"record number {index} of the arena"}}"#).as_bytes(),
        );
    }
    bytes
}

#[test]
fn frames_span_the_arena_and_every_range_decompresses_to_its_own_bytes() {
    let plain = arena(4096);
    assert!(
        plain.len() > 3 * FRAME_BYTES as usize,
        "the arena must span several frames"
    );
    let stored = compress(&plain).expect("the arena compresses");
    assert!(stored.len() < plain.len(), "a repetitive arena must shrink");

    let table = seek_table(&stored).unwrap();
    assert!(
        table.num_frames() > 3,
        "each frame holds at most FRAME_BYTES of plaintext"
    );
    assert_eq!(table.size_decomp(), plain.len() as u64);

    for frame in 0..table.num_frames() {
        let start = table.frame_start_decomp(frame).unwrap();
        let end = table.frame_end_decomp(frame).unwrap();
        assert_eq!(
            decompress_range(&stored, &table, start, end).unwrap(),
            plain.get(start as usize..end as usize).unwrap(),
            "frame {frame} must decompress to its own span"
        );
    }
}

#[test]
fn a_range_straddling_frames_decompresses_to_the_same_bytes_as_the_plaintext() {
    let plain = arena(4096);
    let stored = compress(&plain).unwrap();
    let table = seek_table(&stored).unwrap();

    let boundary = table.frame_start_decomp(1).unwrap();
    for (start, end) in [
        (boundary - 10, boundary + 10),
        (boundary - 1, table.frame_end_decomp(2).unwrap()),
        (0, plain.len() as u64),
        (boundary, boundary),
    ] {
        assert_eq!(
            decompress_range(&stored, &table, start, end).unwrap(),
            plain.get(start as usize..end as usize).unwrap(),
            "the range {start}..{end} must match the plaintext"
        );
    }
}

#[test]
fn a_plain_zstd_decoder_recovers_the_whole_arena() {
    let plain = arena(4096);
    let stored = compress(&plain).unwrap();

    assert_eq!(zstd::stream::decode_all(stored.as_slice()).unwrap(), plain);
}

#[test]
fn a_single_frame_arena_round_trips() {
    let plain = arena(4);
    let stored = compress(&plain).unwrap();
    let table = seek_table(&stored).unwrap();

    assert_eq!(table.num_frames(), 1);
    assert_eq!(decompress_range(&stored, &table, 0, plain.len() as u64).unwrap(), plain);
}

#[test]
fn a_range_past_the_arena_or_inverted_is_refused_rather_than_padded() {
    let plain = arena(4);
    let stored = compress(&plain).unwrap();
    let table = seek_table(&stored).unwrap();
    let len = plain.len() as u64;

    assert!(decompress_range(&stored, &table, 0, len + 1).is_err());
    assert!(decompress_range(&stored, &table, len - 1, 0).is_err());
    assert!(decompress_range(&stored, &table, 0, MAX_RANGE_BYTES + 1).is_err());
}

#[test]
fn bytes_without_a_seek_table_are_refused() {
    assert!(seek_table(b"not a seekable zstd stream").is_err());
    assert!(seek_table(&zstd::bulk::compress(&arena(4), 3).unwrap()).is_err());
}

#[test]
fn incompressible_bytes_still_round_trip() {
    let plain: Vec<u8> = (0..200_000u32)
        .map(|index| (index.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let stored = compress(&plain).unwrap();
    let table = seek_table(&stored).unwrap();

    assert_eq!(decompress_range(&stored, &table, 0, plain.len() as u64).unwrap(), plain);
}

#[test]
fn the_whole_arena_decompresses_in_one_call_to_its_exact_size() {
    let plain = arena(4096);
    let stored = compress(&plain).unwrap();

    let all = decompress_all(&stored).unwrap();
    assert_eq!(all, plain);
    assert_eq!(all.capacity(), plain.len(), "sized from the seek table, never grown");
    assert_eq!(decompress_all(&compress(&[]).unwrap()).unwrap(), Vec::<u8>::new());
}

#[test]
fn frames_end_at_the_breaks_a_caller_asks_for() {
    let plain = arena(4096);
    let len = plain.len();
    let (first, second) = (len / 3 + 7, len / 2);
    let stored = compress_with_breaks(&plain, &[0, first, 5, second, second, len - 1, len, len + 5]).unwrap();
    let table = seek_table(&stored).unwrap();
    let starts: Vec<usize> = (0..table.num_frames())
        .map(|frame| table.frame_start_decomp(frame).unwrap() as usize)
        .collect();
    for boundary in [first, second, len - 1] {
        assert!(
            starts.contains(&boundary),
            "a frame must start at {boundary}: {starts:?}"
        );
    }
    assert!(!starts.contains(&5), "a break behind the walk is ignored");
    let sections = [0, first, second, len - 1, len];
    let expected: u32 = sections
        .windows(2)
        .map(|section| (section[1] - section[0]).div_ceil(FRAME_BYTES as usize) as u32)
        .sum();
    assert_eq!(
        table.num_frames(),
        expected,
        "each section splits at FRAME_BYTES on its own"
    );
    assert_eq!(table.size_decomp(), len as u64);
    assert_eq!(decompress_all(&stored).unwrap(), plain);
    assert_eq!(zstd::stream::decode_all(stored.as_slice()).unwrap(), plain);
    assert_eq!(
        decompress_range(&stored, &table, (first - 3) as u64, (second + 3) as u64).unwrap(),
        &plain[first - 3..second + 3]
    );

    let mut window = Window::open(&stored).unwrap();
    assert_eq!(window.read(first, second - first).unwrap(), &plain[first..second]);
    assert_eq!(
        window.decompressed_frames(),
        (second - first).div_ceil(FRAME_BYTES as usize),
        "the section's own frames and nothing else"
    );
    assert_eq!(window.first_frame_len(), FRAME_BYTES as usize);

    assert_eq!(compress_with_breaks(&plain, &[]).unwrap(), compress(&plain).unwrap());
    assert_eq!(
        compress_with_breaks(&plain, &[0, len]).unwrap(),
        compress(&plain).unwrap()
    );
    assert_eq!(compress_with_breaks(&[], &[0]).unwrap(), compress(&[]).unwrap());
}

#[test]
fn the_whole_arena_decompresses_into_a_buffer_the_caller_keeps() {
    let plain = arena(4096);
    let stored = compress(&plain).unwrap();
    let mut kept = Vec::with_capacity(plain.len() * 2);
    kept.extend_from_slice(b"left over from the block before");
    let capacity = kept.capacity();
    decompress_all_into(&stored, &mut kept).unwrap();
    assert_eq!(kept, plain);
    assert_eq!(kept.capacity(), capacity, "a buffer with the room is kept as it is");

    let mut small = Vec::new();
    decompress_all_into(&stored, &mut small).unwrap();
    assert_eq!(small, plain);
    assert_eq!(small.capacity(), plain.len(), "sized from the seek table, never grown");

    decompress_all_into(&compress(&[]).unwrap(), &mut kept).unwrap();
    assert!(kept.is_empty());
    assert!(decompress_all_into(b"not a seekable zstd stream", &mut kept).is_err());
}

#[test]
fn a_forward_walk_keeps_one_frame_inflated_and_agrees_with_the_plaintext() {
    let plain = arena(4096);
    let stored = compress(&plain).unwrap();
    let table = seek_table(&stored).unwrap();
    let boundary = table.frame_start_decomp(1).unwrap() as usize;
    let mut window = Window::open(&stored).unwrap();

    let inside = window.read_forward(10, 20).unwrap();
    assert!(
        matches!(inside, Cow::Borrowed(_)),
        "a range inside one frame is borrowed"
    );
    assert_eq!(&*inside, &plain[10..30]);
    assert_eq!(window.decompressed_frames(), 1);

    let straddling = window.read_forward(boundary - 5, 10).unwrap();
    assert!(
        matches!(straddling, Cow::Owned(_)),
        "a range across a boundary is gathered"
    );
    assert_eq!(&*straddling, &plain[boundary - 5..boundary + 5]);
    assert_eq!(window.decompressed_frames(), 2);

    let after = window.read_forward(boundary + 5, 10).unwrap();
    assert_eq!(&*after, &plain[boundary + 5..boundary + 15]);
    assert_eq!(window.decompressed_frames(), 1, "the frame behind the walk is let go");

    assert_eq!(window.read(0, 30).unwrap(), &plain[..30]);

    let mut window = Window::open(&stored).unwrap();
    let mut walked = Vec::new();
    for start in (0..plain.len()).step_by(100) {
        let len = 100.min(plain.len() - start);
        walked.extend_from_slice(&window.read_forward(start, len).unwrap());
    }
    assert_eq!(walked, plain, "walked 100 bytes at a time, every byte comes back");
    assert_eq!(window.decompressed_frames(), 1);
    assert_eq!(window.read_forward(plain.len(), 0).unwrap().len(), 0);
    assert!(window.read_forward(plain.len() - 4, 5).is_err());
}

#[test]
fn a_window_caches_only_inflated_frames_and_releases_those_the_walk_passes() {
    let plain = arena(8192);
    let stored = compress(&plain).unwrap();
    let table = seek_table(&stored).unwrap();
    let last = table.num_frames() - 1;
    assert!(last >= 5, "the arena spans enough frames to skip over");
    let start_of = |index: u32| table.frame_start_decomp(index).unwrap() as usize;
    take_frames_inflated();

    let mut window = Window::open(&stored).unwrap();
    assert!(window.decompressed_frame_indexes().is_empty());
    let late = window.read_forward(start_of(last) + 3, 7).unwrap();
    assert!(matches!(late, Cow::Borrowed(_)));
    assert_eq!(&*late, &plain[start_of(last) + 3..start_of(last) + 10]);
    assert_eq!(take_frames_inflated(), 1);
    assert_eq!(window.decompressed_frame_indexes(), [last]);

    assert_eq!(window.read(5, 10).unwrap(), &plain[5..15]);
    let below = window.read_forward(start_of(1) + 1, 4).unwrap();
    assert_eq!(&*below, &plain[start_of(1) + 1..start_of(1) + 5]);
    assert_eq!(take_frames_inflated(), 2);
    assert_eq!(
        window.decompressed_frame_indexes(),
        [0, 1, last],
        "a forward read below the walk's high-water mark releases nothing"
    );
    let again = window.read_forward(start_of(last), 4).unwrap();
    assert_eq!(&*again, &plain[start_of(last)..start_of(last) + 4]);
    assert_eq!(take_frames_inflated(), 0, "a cached frame is not inflated again");
    assert_eq!(window.decompressed_frame_indexes(), [0, 1, last]);
    let lowest = window.frames[0].1.clone();
    drop(window);
    assert_eq!(
        SPARE_FRAME.with(|slot| slot.borrow().clone()),
        lowest,
        "a window without a spare hands its lowest cached frame back to the thread"
    );

    let mut window = Window::open(&stored).unwrap();
    window.read(0, 4).unwrap();
    window.read(start_of(2), 4).unwrap();
    let reused = window.frames[0].1.as_ptr();
    let gathered = window.read_forward(start_of(4) - 3, 6).unwrap();
    assert!(matches!(gathered, Cow::Owned(_)));
    assert_eq!(&*gathered, &plain[start_of(4) - 3..start_of(4) + 3]);
    assert_eq!(window.decompressed_frame_indexes(), [3, 4]);
    assert_eq!(
        window.frames[0].1.as_ptr(),
        reused,
        "the lowest released frame's buffer is the spare the next frame inflates into"
    );
    take_frames_inflated();
    let inside = window.read_forward(start_of(4) + 3, 4).unwrap();
    assert_eq!(&*inside, &plain[start_of(4) + 3..start_of(4) + 7]);
    assert_eq!(take_frames_inflated(), 0);
    assert_eq!(window.decompressed_frame_indexes(), [4]);
    assert_eq!(window.read(0, plain.len()).unwrap(), plain);
    assert_eq!(window.decompressed_frames() as u32, last + 1);

    let mut zeroed = stored.clone();
    zeroed[..table.size_comp() as usize].fill(0);
    let mut window = Window::open(&zeroed).unwrap();
    assert!(window.read_forward(start_of(last), 4).is_err());
    assert!(window.read(0, 4).is_err());
    assert!(window.decompressed_frame_indexes().is_empty());
}

#[test]
fn frames_disagreeing_with_their_seek_table_are_refused() {
    let plain = arena(4096);
    let stored = compress(&plain).unwrap();
    let table = seek_table(&stored).unwrap();
    let frames_len = table.size_comp() as usize;
    let first_frame_end = table.frame_end_decomp(0).unwrap();

    let mut forged = SeekTable::new();
    for index in 0..table.num_frames() {
        forged
            .log_frame(
                table.frame_size_comp(index).unwrap() as u32,
                table.frame_size_decomp(index).unwrap() as u32 + 1,
            )
            .unwrap();
    }
    let mut serializer = forged.into_serializer();
    let mut lying = stored[..frames_len].to_vec();
    lying.resize(frames_len + serializer.encoded_len(), 0);
    assert_eq!(
        serializer.write_into(&mut lying[frames_len..]),
        lying.len() - frames_len
    );
    let lying_table = seek_table(&lying).unwrap();
    assert!(decompress_all(&lying).is_err());
    assert!(decompress_all_into(&lying, &mut Vec::new()).is_err());
    assert!(decompress_range(&lying, &lying_table, 0, first_frame_end + 1).is_err());
    assert!(Window::open(&lying).unwrap().read_forward(0, 16).is_err());

    let mut zeroed = stored.clone();
    zeroed[..frames_len].fill(0);
    assert!(decompress_all(&zeroed).is_err());
    assert!(decompress_all_into(&zeroed, &mut Vec::new()).is_err());
    assert!(decompress_range(&zeroed, &table, 0, 16).is_err());
    assert!(Window::open(&zeroed).unwrap().read(0, 16).is_err());

    let mut cut = stored[..frames_len - 10].to_vec();
    cut.extend_from_slice(&stored[frames_len..]);
    let cut_table = seek_table(&cut).unwrap();
    let last = cut_table.num_frames() - 1;
    let last_start = cut_table.frame_start_decomp(last).unwrap();
    assert!(decompress_all(&cut).is_err());
    assert!(decompress_all_into(&cut, &mut Vec::new()).is_err());
    assert!(decompress_range(&cut, &cut_table, last_start, plain.len() as u64).is_err());
    assert!(
        Window::open(&cut)
            .unwrap()
            .read_forward(last_start as usize, 16)
            .is_err()
    );
}

#[test]
#[ignore]
fn whole_stream_and_per_frame_inflate_timing() {
    let values: Vec<Option<String>> = (0..8192)
        .map(|i| {
            Some(if i % 997 == 0 {
                format!("row {i} raised on the escalation-path after a threshold breach")
            } else {
                format!("row {i} settled without incident on the standard path")
            })
        })
        .collect();
    let block = super::super::encode_block(&super::super::ColumnData::Strings(values.into()), true);
    assert_eq!(
        block.pipeline.compression().unwrap(),
        super::super::Compression::SeekableZstd
    );
    let stored = &block.bytes;
    let table = seek_table(stored).unwrap();
    let rounds = 2_000;
    let best_of = |run: &mut dyn FnMut()| {
        (0..5)
            .map(|_| {
                let started = std::time::Instant::now();
                for _ in 0..rounds {
                    run();
                }
                started.elapsed() / rounds
            })
            .min()
            .unwrap()
    };
    let parse = best_of(&mut || {
        std::hint::black_box(seek_table(stored).unwrap());
    });
    let fresh = best_of(&mut || {
        std::hint::black_box(decompress_all(stored).unwrap());
    });
    let mut kept = Vec::new();
    let reused = best_of(&mut || {
        decompress_all_into(stored, &mut kept).unwrap();
        std::hint::black_box(&kept);
    });
    let mut frame = Vec::new();
    let mut out = Vec::new();
    let per_frame = best_of(&mut || {
        out.clear();
        for index in 0..table.num_frames() {
            decompress_frame(stored, &table, index, &mut frame).unwrap();
            out.extend_from_slice(&frame);
        }
        std::hint::black_box(&out);
    });
    assert_eq!(out, kept);
    println!(
        "stored {} B, plain {} B, {} frames: seek table parse {parse:?}, decompress_all {fresh:?}, \
         decompress_all_into {reused:?}, frame by frame {per_frame:?}",
        stored.len(),
        table.size_decomp(),
        table.num_frames()
    );
    for index in 0..table.num_frames() {
        let one = best_of(&mut || {
            decompress_frame(stored, &table, index, &mut frame).unwrap();
            std::hint::black_box(&frame);
        });
        println!(
            "  frame {index}: {} plain B from {} stored B in {one:?}",
            table.frame_size_decomp(index).unwrap(),
            table.frame_size_comp(index).unwrap()
        );
    }
}

fn noise(len: usize) -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

#[test]
fn frames_that_do_not_pay_for_inflating_are_stored_raw() {
    let mut plain = noise(3 * FRAME_BYTES as usize + 123);
    let break_at = plain.len();
    plain.extend_from_slice(&arena(4096));
    let stored = compress_with_breaks_keeping(&plain, &[break_at], (1, 2)).unwrap();
    let table = seek_table(&stored).unwrap();
    let raw_frames = table.frame_index_decomp(break_at as u64 - 1) + 1;
    for frame in 0..table.num_frames() {
        let comp = table.frame_size_comp(frame).unwrap();
        let decomp = table.frame_size_decomp(frame).unwrap();
        if frame < raw_frames {
            assert_eq!(comp, decomp + 4 + 1 + 4 + 3, "frame {frame} is stored raw");
        } else {
            assert!(comp * 2 <= decomp, "frame {frame} keeps its compression");
        }
    }
    assert_eq!(decompress_all(&stored).unwrap(), plain);
    assert_eq!(zstd::stream::decode_all(stored.as_slice()).unwrap(), plain);
    let (start, end) = (break_at as u64 - 70_000, break_at as u64 + 9_000);
    assert_eq!(
        decompress_range(&stored, &table, start, end).unwrap(),
        &plain[start as usize..end as usize]
    );
    let mut window = Window::open(&stored).unwrap();
    assert_eq!(window.read(10, 100_000).unwrap(), &plain[10..100_010]);
}

#[test]
fn raw_frames_of_any_length_decode() {
    for len in [
        0usize,
        1,
        200,
        MAX_RAW_BLOCK_BYTES,
        MAX_RAW_BLOCK_BYTES + 1,
        3 * MAX_RAW_BLOCK_BYTES + 7,
    ] {
        let plain = noise(len);
        let mut stored = Vec::new();
        push_raw_frame(&plain, &mut stored);
        assert_eq!(
            zstd::stream::decode_all(stored.as_slice()).unwrap(),
            plain,
            "{len} bytes"
        );
    }
}

#[test]
fn the_frame_bar_decides_which_frames_stay_compressed() {
    let plain = arena(8192);
    let all_raw = compress_with_breaks_keeping(&plain, &[], (0, 1)).unwrap();
    let all_compressed = compress_with_breaks_keeping(&plain, &[], (1, 1)).unwrap();
    assert!(all_raw.len() > plain.len());
    assert!(all_compressed.len() * 4 < plain.len());
    assert_eq!(all_compressed, compress_with_breaks(&plain, &[]).unwrap());
    for stored in [all_raw, all_compressed] {
        assert_eq!(decompress_all(&stored).unwrap(), plain);
    }
}

#[test]
fn a_prefix_read_inflates_only_the_frames_it_needs() {
    let plain = arena(8192);
    let stored = compress(&plain).unwrap();
    let table = seek_table(&stored).unwrap();
    assert!(table.num_frames() > 4);
    let mut out = Vec::new();
    for want in [
        1usize,
        FRAME_BYTES as usize,
        FRAME_BYTES as usize + 1,
        3 * FRAME_BYTES as usize - 5,
    ] {
        let _ = take_frames_inflated();
        decompress_prefix_into(&stored, &mut out, |head| {
            assert_eq!(head, &plain[..FRAME_BYTES as usize]);
            Some(want)
        })
        .unwrap();
        assert!(out.len() >= want);
        assert_eq!(out, &plain[..out.len()]);
        assert_eq!(
            take_frames_inflated(),
            want.div_ceil(FRAME_BYTES as usize),
            "frames inflated for a {want}-byte prefix"
        );
    }
    decompress_prefix_into(&stored, &mut out, |_| None).unwrap();
    assert_eq!(out, plain);
    assert!(decompress_prefix_into(&stored, &mut out, |_| Some(plain.len() + 1)).is_err());
    let empty = compress(&[]).unwrap();
    decompress_prefix_into(&empty, &mut out, |_| Some(0)).unwrap();
    assert!(out.is_empty());
}

fn frames_from_a_fresh_encoder(bytes: &[u8], breaks: &[usize]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut consumed = 0;
    for end in breaks.iter().copied().chain([bytes.len()]) {
        if end <= consumed || end > bytes.len() {
            continue;
        }
        for frame in bytes[consumed..end].chunks(FRAME_BYTES as usize) {
            out.extend_from_slice(&zstd::bulk::compress(frame, LEVEL).unwrap());
        }
        consumed = end;
    }
    out
}

#[test]
fn a_reused_encoder_writes_the_bytes_a_fresh_one_does() {
    let frame = FRAME_BYTES as usize;
    let mut noise = Vec::with_capacity(3 * frame + 16);
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    while noise.len() < 3 * frame + 11 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        noise.extend_from_slice(&state.to_le_bytes());
    }
    let text = arena(4096);
    let arenas: Vec<(&[u8], Vec<usize>)> = vec![
        (&text, vec![]),
        (&[], vec![]),
        (&text[..1], vec![]),
        (&text[..frame], vec![]),
        (&text[..2 * frame], vec![frame]),
        (&text[..2 * frame + 1], vec![17, frame, frame + 5]),
        (&noise, vec![]),
        (&noise, vec![1, frame - 1, 2 * frame, 3 * frame + 10]),
        (&text[..300], vec![100, 200]),
        (&text, vec![text.len() / 3, text.len() / 2]),
        (&text[..1], vec![]),
    ];
    for round in 0..2 {
        for (bytes, breaks) in &arenas {
            let stored = compress_with_breaks(bytes, breaks).unwrap();
            let frames = frames_from_a_fresh_encoder(bytes, breaks);
            let table = seek_table(&stored).unwrap();
            assert_eq!(table.size_comp(), frames.len() as u64, "round {round}: {breaks:?}");
            assert_eq!(
                &stored[..frames.len()],
                frames.as_slice(),
                "round {round}: {} bytes with breaks {breaks:?}",
                bytes.len()
            );
            assert_eq!(decompress_all(&stored).unwrap(), *bytes);
        }
    }
    let on_another_thread = std::thread::spawn({
        let text = text.clone();
        move || compress(&text).unwrap()
    })
    .join()
    .unwrap();
    assert_eq!(on_another_thread, compress(&text).unwrap());
}
