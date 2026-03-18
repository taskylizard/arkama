# Autoresearch: download path perf + behavior correctness

## Objective
Optimize download runtime in `arkama_core` while preserving behavior correctness (resume/range semantics and event flow).

## Metrics
- **Primary**: `download_e2e_ms` (ms, lower is better)
- **Secondary**: `run_experiment_duration_s`, `cargo_check_pass`, `cargo_test_pass`, `cargo_clippy_fix_pass`, `cargo_fmt_pass`

## How to Run
`./autoresearch.sh` — prints `METRIC download_e2e_ms=<number>`.

## Files in Scope
- `crates/arkama_core/src/downloader.rs` — hot download loops, segment scheduling, resume state updates.
- `crates/arkama_core/src/segment.rs` — segmentation utilities (only if needed).
- `crates/arkama_core/src/http.rs` — range probing/fetch behavior (only if needed).
- `crates/arkama_core/tests/download_e2e.rs` — integration correctness/perf signal.

## Off Limits
- Database/UI crates unless required for a correctness fix tied directly to download behavior.
- Public API shape changes in `arkama_core`.

## Constraints
- Preserve user-visible behavior.
- Keep changes minimal and maintainable.
- Required checks sequence: `cargo check` -> `cargo test` -> `cargo clippy --fix --allow-dirty` -> `cargo fmt`.

## What's Been Tried
- Baseline pending.
