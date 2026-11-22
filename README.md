# Arkama

[![CI](https://github.com/taskylizard/arkama/actions/workflows/ci.yml/badge.svg?branch=trunk)](https://github.com/taskylizard/arkama/actions/workflows/ci.yml)

A download manager written in Rust, with a reusable HTTP engine, a CLI, and a local background daemon.

[CLI guide](docs/cli.md) · [Rust API](crates/arkama_core/README.md) · [Server API](docs/server.md) · [Client examples](docs/clients.md) · [Development](docs/development.md)

> [!WARNING]
> Arkama is in early development. The ordinary-file core/server foundation is implemented, not a
> published production release. Server API keys are enabled by default; remote authenticated access
> requires an owner-configured TLS proxy. A release license has not been added yet.

## Getting started

Build and install both binaries from this checkout using stable Rust:

```sh
git clone https://github.com/taskylizard/arkama.git
cd arkama
cargo install --path crates/arkama --locked
```

Run a download directly, without a daemon:

```sh
arkama-cli download https://example.com/file.bin -o ./file.bin
arkama-cli download https://example.com/file.bin --connections 8 --limit 2MB
```

`arkama` and `arkama-cli` expose the same commands. Run `arkama-cli --help` for the command list,
or `arkama-cli download --help` for download options.

## What works today

- HTTP/HTTPS downloads, redirects, and filename inference.
- Segmented transfers when the server supports byte ranges, a known size, and a strong ETag.
- Validated resume checkpoints, pause/cancel controls, and verified final-file publication.
- Single-stream fallback when safe segmentation or resume is unavailable.
- Retry/backoff and an aggregate speed limit across the workers of one download.
- Terminal progress, silent mode, and JSON events. Progress is sampled at most once per 500 ms, not every chunk.
- SQLite settings and history, search, sequential links-file input, and shell completions.
- A persistent manager shared by the local daemon and explicit embedded/no-server use, with joined
  controls, output reservations, live settings, and bounded paginated history.
- Named queues, priority/order controls, shared host/bandwidth limits, folder rules, batches and
  timezone-aware download windows. See [download management](docs/management.md).
- Ordinary GET handoffs with protected headers/cookies/referrer, credential-scoped redirects,
  and durable duplicate-safe client IDs. See [client inputs and examples](docs/clients.md).

The daemon exposes a bounded [single-owner HTTP/JSON API](docs/server.md), with safe local defaults.
The experimental connection-entropy option is opt-in for bonded LACP/ECMP setups, not a general
flaky-link optimization. See the [CLI guide](docs/cli.md).

## Resume and completion

Unfinished bytes and checkpoints use `<output>.arkama.part` and
`<output>.arkama.part.arkama.state`. Restart with the same URL, output path, and compatible settings
to reuse validated progress. Missing, corrupt, legacy, or incompatible checkpoints restart from zero.
Weak/missing validators, unavailable ranges, and unknown sizes use full streams that restart after interruption.

Existing final files are never treated as proof of completion or silently overwritten. Move old
final-name partials aside or choose a new output path. The final filename appears only after verified
coverage and synchronized data; the destination filesystem must support hard links. A reusable,
zero-byte `<output>.arkama.lock` remains after completion and does not indicate a running job.

HTTP requests use identity encoding; compressed responses are rejected to avoid ambiguous byte offsets.
These checks protect resume and transfer integrity, not against a server lying about its bytes or ETag.
See the [core contract](crates/arkama_core/README.md) for shutdown, durability, and feature limits.

## Use the Rust core

`arkama_core` provides awaited and background downloads without CLI, database, or daemon dependencies.
The consumer supplies a Tokio runtime and owns signals, tracing, and output. The core enforces
`#![forbid(unsafe_code)]`; public request, control, event, and result types are exported at the crate root.

Start with the [Rust API guide and examples](crates/arkama_core/README.md). The core supports Rust 1.85;
use current stable Rust for the whole workspace.

## Development

```sh
cargo build --workspace --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo fmt --all -- --check
```

CI runs workspace checks, tests, Clippy, formatting, and release builds on Linux, macOS, and Windows.
The [development guide](docs/development.md) explains crate boundaries, source organization,
documentation, and feature/MSRV checks.

## Foundation status

The reliability/safe-core API, bounded observations, configurable recovery, shared-manager,
secure-server, download-management and protected-client-input milestones are implemented.
Combined validation and its limits are documented in the [foundation validation guide](docs/validation.md).
See the [manager contract](crates/arkama_manager/README.md) for host ownership and shutdown.
Optional diagnostics use a dedicated OS thread; slow observers cannot back up the transfer path.

Browser extensions, desktop/Android clients, video support, and measured acceleration remain later work.
