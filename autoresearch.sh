#!/usr/bin/env bash
set -euo pipefail

TEST_NAME="download_segmented_large_payload"
RUNS=3

# Fast pre-check/build step outside timed region.
cargo test -p arkama_core --test download_e2e "$TEST_NAME" --no-run --quiet

measurements=()
for _ in $(seq 1 "$RUNS"); do
  start_ns=$(date +%s%N)
  cargo test -p arkama_core --test download_e2e "$TEST_NAME" -- --exact
  end_ns=$(date +%s%N)
  elapsed_ms=$(((end_ns - start_ns) / 1000000))
  measurements+=("$elapsed_ms")
done

readarray -t sorted < <(printf '%s\n' "${measurements[@]}" | sort -n)
median_index=$((RUNS / 2))
median_ms=${sorted[$median_index]}

echo "METRIC download_e2e_ms=${median_ms}"
