
set shell := ["bash", "-euo", "pipefail", "-c"]

target := justfile_directory() / "target"

default:
    @just --list --unsorted

check: fmt-check clippy test

build:
    cargo build --all-features

test *args:
    cargo test --all-features {{ args }}

clippy:
    cargo clippy --all-features --all-targets -- -D warnings

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

fuzz target seconds="60":
    cd fuzz && cargo +nightly fuzz run {{ target }} -- -max_total_time={{ seconds }}

bench-build:
    cd benchmark && cargo bench --no-run --target-dir {{ target }}
    cd benchmark && cargo build --release --target-dir {{ target }}

bench-test:
    cd benchmark && cargo test --release --target-dir {{ target }}

bench-micro bench="" *filter:
    cd benchmark && cargo bench --target-dir {{ target }} {{ if bench == "" { "" } else { "--bench " + bench } }} {{ if filter == "" { "" } else { "-- " + filter } }}

bench-compare *args="--sizes 10000,100000,1000000 --reps 5 --write-reps 3":
    cd benchmark && cargo run --release --target-dir {{ target }} --bin oe-benchmark -- --out results {{ args }}

bench-ops dir=(target / "bench-ops") *args="":
    cd benchmark && cargo run --release --target-dir {{ target }} --bin oe-vs-parquet -- {{ dir }} {{ args }}

bench-smoke:
    cd benchmark && cargo bench --target-dir {{ target }} --benches -- --quick --warm-up-time 0.1 --measurement-time 0.2
    cd benchmark && cargo run --release --target-dir {{ target }} --bin oe-benchmark -- --sizes 10000 --reps 1 --write-reps 1 --skip-ingest yes --out {{ target }}/bench-smoke
    cd benchmark && cargo run --release --target-dir {{ target }} --bin oe-vs-parquet -- {{ target }}/bench-smoke-ops

bench: bench-micro bench-compare bench-ops
