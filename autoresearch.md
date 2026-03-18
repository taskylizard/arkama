# Autoresearch: download path perf + behavior correctness

## Objective
Optimize download runtime in `arkama_core` while preserving behavior correctness (resume/range semantics and event flow).

## Metrics
- **Primary**: `download_e2e_ms` (ms, lower is better)
- **Secondary**: `run_experiment_duration_s`, `cargo_check_pass`, `cargo_test_pass`, `cargo_clippy_fix_pass`, `cargo_fmt_pass`

## How to Run
`./autoresearch.sh` — builds `download_e2e` test binary, performs one warm-up, runs `download_segmented_large_payload` 5 measured times directly via the test binary, prints median `METRIC download_e2e_ms=<number>`.

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
- Initial setup issue: `arkama` integration benchmark path failed due missing OpenSSL headers; fixed by switching workspace `reqwest` to `default-features = false` + `rustls-tls`.
- Added local integration workload in `crates/arkama_core/tests/download_e2e.rs`.
- Benchmark evolved to reduce overfitting/noise:
  - cargo-test driven median-of-3
  - then direct test-binary median-of-3
  - now direct test-binary **warm-up + median-of-5** (`autoresearch.sh`).
- Current segment baseline (warm-up + median-of-5): **153ms**.
- Kept code optimizations in `crates/arkama_core/src/downloader.rs`:
  - Batched segmented state sync + id-index fast path in `update_state`.
  - Removed redundant hot-loop `ensure_parent_dir` calls; create parent once before loops.
  - Skipped redundant immediate periodic-save tick (initial explicit `save_state` already done).
  - Tuned `STATE_SYNC_BATCH_BYTES`: 64KiB -> 96KiB -> 88KiB -> **84KiB**.
  - Removed unnecessary `.read(true)` flags from output file open options.
- Current head: `4d5966c` on `autoresearch/perf-correctness-2026-03-18`.
- Observed high ambient jitter (roughly 120–155ms band on calibration runs). Treat sub-3ms differences as noise; prefer larger deltas or repeated confirmation.
- Discarded ideas so far:
  - One-time seek per segment stream (large regression)
  - `Response::chunk()` refactor (no gain)
  - Progress-event shortcut when no subscribers (regressed)
  - Compact JSON state serialization (`to_vec`) (no gain)
  - `try_lock` opportunistic state updates (no gain)
  - Further batch-size sweeps not beating 84KiB (e.g., 80/82/92/112KiB)
