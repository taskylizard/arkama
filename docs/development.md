# Development

Arkama is a small Rust workspace. Keep the download engine, persistence, and process-facing code
separate; new modules should represent responsibilities rather than mirror a larger project's crate count.

## Build and check

Use current stable Rust for the workspace. `arkama_core` separately declares Rust 1.85 as its minimum.

```sh
cargo build --workspace --locked
cargo check --workspace --all-targets --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all -- --check
```

The CI matrix runs these checks and release-mode CLI builds on Linux, macOS, and Windows. Validate
core changes against the supported feature combinations and minimum Rust version as well:

```sh
cargo +1.85.0 test -p arkama_core --locked
cargo test -p arkama_core --no-default-features --locked
cargo check -p arkama_core --all-features --all-targets --locked
cargo test -p arkama_core --doc --locked
cargo doc --workspace --no-deps --locked
```

Install the 1.85 toolchain separately if it is not present. Core examples live in
[`crates/arkama_core/examples`](../crates/arkama_core/examples); they perform real network downloads,
so choose the URL and output path intentionally when running them.

## Crate boundaries

| Crate | Owns | Does not own |
| --- | --- | --- |
| [`arkama_core`](../crates/arkama_core) | HTTP identity/ranges, transfer workers, partial files, checkpoints, controls, completion. | SQLite, a listener, runtime creation, Ctrl+C registration, or tracing setup. |
| [`arkama_data`](../crates/arkama_data) | Synchronous SQLite settings/history/queued requests and application paths. | Transfer scheduling, checkpoint validity, or transport. |
| [`arkama_manager`](../crates/arkama_manager) | Dedicated-thread persistence, scheduling, admission, output reservations, joined controls, snapshots. | Listener, runtime creation, signals, or tracing setup. |
| [`arkama`](../crates/arkama) | Command parsing, command handlers, terminal/JSON output, signals, and the existing local daemon. | A second download engine or the planned secure server contract. |

Dependencies point from `arkama` to the manager/data/core, from the manager to data/core, and from
data to core request/result types.
The core does not depend on the application or database. Its `forbid(unsafe_code)` policy stays at
the crate root; safe dependency APIs may have unsafe internals.

## Source organization

- **Crate roots:** `lib.rs` documents the boundary and re-exports the existing consumer-facing API.
  Binary entry points only supply the runtime and process exit handling.
- **Core:** `request.rs` owns configuration/builders; `task.rs` owns events, completion, and controls;
  `downloader.rs` owns entry points and its private orchestration modules. `http.rs` handles representation
  and response validation; `segment.rs` owns byte-range geometry; `error.rs` owns public errors.
- **CLI:** `cli.rs` parses and dispatches each binary's command tree; `args.rs` supplies shared download
  arguments. `commands/` contains the command handlers, with terminal rendering under `commands/download/`.
- **Manager:** `model.rs` owns shared commands and snapshots; `actor.rs` owns serialized scheduling,
  persistence and joined state changes. The crate root owns bounded handles and thread lifecycle.
- **Daemon:** `daemon.rs` owns startup, signals and bounded connections. `daemon/client.rs` sends
  requests; `daemon/protocol.rs` owns the current wire representation. Scheduling belongs to the manager.
- **Data:** `db.rs` owns connection/query access; `db/schema.rs` owns initialization and compatibility
  migrations. `model.rs` defines stored records and `paths.rs` resolves application locations.

Use explicit modules, descriptive names, and private implementation details. Prefer the existing safe
APIs and ordinary structs/enums over new wrappers, traits, crates, or generic layers without a consumer
that needs them. Preserve the root exports and serialized formats during layout-only changes.
Local formatting is controlled by rustfmt; the reference projects do not share one indentation style.

## Tests and documentation

Public HTTP behavior is covered in `arkama_core/tests/download_e2e.rs` using the server in `tests/support/`.
CLI integration tests exercise binary downloads; data tests cover queue claims and restart recovery.
Extend these fixtures when changing a contract. A structure-only refactor should pass the existing
tests unchanged, not add assertions tied to module names or private call order.

Keep the root README focused on what works, installation, and useful entry points. The CLI guide owns
command details; the core README owns the runtime/resume/control contract. Core rustdoc includes that
README directly so its example is checked by doctests instead of maintaining a second introduction.
Public item docs explain individual types/functions; module docs explain ownership and important invariants.
Document limitations and actual defaults alongside features. Do not present planned behavior as shipped.

## Reference projects

The organization follows patterns observed in these repositories, adapted to Arkama rather than copying code:

- [Oxc](https://github.com/oxc-project/oxc): composable libraries, explicit parser submodules, and a README that points users to focused guides.
- [rust-analyzer](https://github.com/rust-lang/rust-analyzer): documented component boundaries, selective root exports, and separate user, architecture, and contributor documentation.
- [Ruff](https://github.com/astral-sh/ruff): argument definitions and focused command modules, with setup/usage examples before the detailed references.
- [Biome](https://github.com/biomejs/biome): command/service separation and a crate README included in rustdoc.
- [SWC](https://github.com/swc-project/swc): distinct component responsibilities, Rust API documentation, and a contributor architecture guide.
- [Rolldown](https://github.com/rolldown/rolldown): private implementation modules with selected root exports and named processing stages.
- [Turborepo](https://github.com/vercel/turborepo): command dispatch separated from daemon/run responsibilities, with build/test instructions in contributor documentation.

These projects have different sizes, APIs, and optimization needs. Arkama does not inherit their
unsafe internals, global allocators, feature matrices, branding, performance claims, or dependency stacks.

[Back to the README](../README.md)
