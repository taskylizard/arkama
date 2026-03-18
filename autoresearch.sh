#!/usr/bin/env bash
set -euo pipefail

TEST_NAME="download_segmented_large_payload"
RUNS=5

# Build test binary outside timed region.
cargo test -p arkama_core --test download_e2e "$TEST_NAME" --no-run --quiet

TEST_BIN=$(find "target/debug/deps" -maxdepth 1 -type f -name "download_e2e-*" -perm -111 -printf '%T@ %p\n' | sort -nr | head -n1 | cut -d' ' -f2-)
if [[ -z "${TEST_BIN}" ]]; then
  echo "failed to locate download_e2e test binary" >&2
  exit 1
fi

# Warm-up run to stabilize runtime effects.
"$TEST_BIN" --exact "$TEST_NAME" > /dev/null

measurements=()
for _ in $(seq 1 "$RUNS"); do
  start_ns=$(date +%s%N)
  "$TEST_BIN" --exact "$TEST_NAME"
  end_ns=$(date +%s%N)
  elapsed_ms=$(((end_ns - start_ns) / 1000000))
  measurements+=("$elapsed_ms")
done

readarray -t sorted < <(printf '%s\n' "${measurements[@]}" | sort -n)
median_index=$((RUNS / 2))
median_ms=${sorted[$median_index]}

echo "METRIC download_e2e_ms=${median_ms}"
