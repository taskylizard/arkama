# arkama_core

Library API for Arkama's HTTP/HTTPS downloader.

`arkama_core` is the reusable download engine used by the Arkama CLI. It is intended for Rust
applications that want file downloads with progress reporting, resume support, segmented transfers,
pause/cancel controls, and optional speed limiting.

## Status

Arkama is still early and the API is being prepared for crates.io publication. The public request
fields remain available for compatibility, but new code should prefer `DownloadRequest::new`, the
chainable setters, or `DownloadRequest::builder`.

## Quick start

```rust,no_run
use arkama_core::{DownloadRequest, download};

#[tokio::main]
async fn main() -> arkama_core::Result<()> {
    let request = DownloadRequest::new("https://example.com/file.bin")
        .output("./file.bin")
        .connections(8)
        .speed_limit(2_000_000);

    let summary = download(request).await?;
    println!("saved {} bytes to {:?}", summary.downloaded_bytes, summary.output);
    Ok(())
}
```

## Runtime requirements

All public download APIs run on the `tokio` async runtime:

- `download(request).await` runs the download on the current Tokio runtime task.
- `start_download(request)` requires an existing Tokio runtime and spawns a background download.
- `start_download_with_handle(handle, request)` lets callers choose the Tokio runtime handle used
  for spawning.

Background downloads return a `DownloadHandle` with:

- `events`: an unbounded stream of `DownloadEvent` values.
- `join`: the spawned task that resolves to `arkama_core::Result<DownloadSummary>`.
- `control`: a `DownloadControl` for pause/cancel requests.

## Progress events

`DownloadEvent` currently reports:

- `Started { output, total_bytes, resumed_bytes }`
- `Progress { downloaded_bytes, total_bytes }`
- `Finished { summary }`
- `Failed { message }`

Pause and cancellation are surfaced as typed completion errors from `DownloadHandle::join`:
`Error::Paused` and `Error::Cancelled`.

## Resume behavior

For resumable segmented downloads, arkama writes state next to the output file as:

```text
<output file name>.arkama.state
```

Start a later download with the same URL and output path to resume from saved state when possible.
State files are removed after successful completion. If a server does not support HTTP byte ranges,
arkama falls back to a single-stream download internally.

## Segmented downloads

`DownloadRequest::connections` controls the requested number of segmented workers. The default is
`DEFAULT_CONNECTIONS` (`4`). Segmented downloading requires a known content length and HTTP byte
range support; unsupported servers transparently use the single-stream path.

## Pause and cancel

`DownloadControl::pause()` asks the background task to stop while keeping resumable state when
possible. The completion task returns `Error::Paused`.

`DownloadControl::cancel()` asks the background task to stop and remove partial output/state when
possible. The completion task returns `Error::Cancelled`.

Use `Error::is_paused()` and `Error::is_cancelled()` to branch on those outcomes without matching
the enum directly.

## Speed limiting

Use `DownloadRequest::speed_limit(bytes_per_second)` or the builder's `speed_limit` method to set an
approximate global byte-per-second limit for a download.

## Feature flags

`arkama_core` currently has no crate-defined optional feature flags. Dependency feature selection is
still inherited from the workspace, including Reqwest with Rustls TLS and Tokio's full feature set.
Dependency and feature cleanup is planned for a later publication phase.

## Examples

See the crate examples for library-oriented usage:

- `simple.rs`
- `progress.rs`
- `pause_resume.rs`
- `speed_limit.rs`
