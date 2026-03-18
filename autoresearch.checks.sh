#!/usr/bin/env bash
set -euo pipefail

cargo check -p arkama_core --quiet
cargo test -p arkama_core --quiet
cargo clippy -p arkama_core --fix --allow-dirty --quiet
cargo fmt
