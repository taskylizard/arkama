#!/usr/bin/env bash
set -euo pipefail

TEST_NAME="download_segmented_large_payload"

# Fast pre-check/build step outside timed region.
cargo test -p arkama_core --test download_e2e "$TEST_NAME" --no-run --quiet

start_ns=$(date +%s%N)
cargo test -p arkama_core --test download_e2e "$TEST_NAME" -- --exact
end_ns=$(date +%s%N)

elapsed_ms=$(((end_ns - start_ns) / 1000000))
echo "METRIC download_e2e_ms=${elapsed_ms}"
