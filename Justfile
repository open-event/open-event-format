# OE task runner. `just` lists every recipe.
#
# Benchmarks come in three suites that share one target directory (`target/`):
#   - micro:      Criterion kernel benches in `benchmark/micro/` (encoding, vector)
#   - compare:    `benchmark/`, OE vs Parquet, Vortex and JSON (size, write, scans, lookups, ingest)
#   - operations: `benchmark/operations/`, OE vs Parquet and Vortex per operation (create, read, update, delete, search, diff)
# The two comparison harnesses are standalone crates (they pin different Vortex versions), so each keeps its own
# lockfile; `--target-dir` points them at the shared `target/` so dependencies are built once.

set shell := ["bash", "-euo", "pipefail", "-c"]

target := justfile_directory() / "target"

default:
    @just --list --unsorted

# Format, lint, test.
check: fmt-check clippy test

# Build the library with every feature.
build:
    cargo build --all-features

# Run the test suite (tests need the `write` feature).
test *args:
    cargo test --all-features {{ args }}

clippy:
    cargo clippy --all-features --all-targets -- -D warnings

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

# Run a fuzz target for N seconds (needs cargo-fuzz and nightly).
fuzz target seconds="60":
    cd fuzz && cargo +nightly fuzz run {{ target }} -- -max_total_time={{ seconds }}

# ---- benchmarks ---------------------------------------------------------------------------------------------------

# Compile every benchmark suite without running anything.
bench-build:
    cargo bench --no-run
    cd benchmark && cargo build --release --target-dir {{ target }}
    cd benchmark/operations && cargo build --release --target-dir {{ target }}

# Criterion micro benches. `just bench-micro encoding filter/for` runs one bench, filtered.
bench-micro bench="" *filter:
    cargo bench {{ if bench == "" { "" } else { "--bench " + bench } }} {{ if filter == "" { "" } else { "-- " + filter } }}

# OE vs Parquet/Vortex/JSON. Writes `benchmark/results/`. Extra flags pass through, e.g. `--profiles chat`.
bench-compare *args="--sizes 10000,100000,1000000 --reps 5 --write-reps 3":
    cd benchmark && cargo run --release --target-dir {{ target }} -- --out results {{ args }}

# Per-operation OE vs Parquet/Vortex into a work directory. Extra flags pass through, e.g. `--format oe --toml`.
bench-ops dir=(target / "bench-ops") *args="":
    cd benchmark/operations && cargo run --release --target-dir {{ target }} --bin oe-vs-parquet -- {{ dir }} {{ args }}

# Quick pass through every suite on small inputs; checks they all run, numbers are not meaningful.
bench-smoke:
    cargo bench -- --quick --warm-up-time 0.1 --measurement-time 0.2
    cd benchmark && cargo run --release --target-dir {{ target }} -- --sizes 10000 --reps 1 --write-reps 1 --skip-ingest yes --out {{ target }}/bench-smoke
    cd benchmark/operations && cargo run --release --target-dir {{ target }} --bin oe-vs-parquet -- {{ target }}/bench-smoke-ops

# Every suite at full size.
bench: bench-micro bench-compare bench-ops
