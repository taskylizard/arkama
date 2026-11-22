# arkama_core

Library API for arkama's HTTP/HTTPS downloader.

`arkama_core` is the reusable download engine used by the arkama CLI. It is intended for Rust
applications that want file downloads with progress reporting, resume support, segmented transfers,
pause/cancel controls, and optional speed limiting.

## Status

arkama is still early and the API is being prepared for crates.io publication. The public request
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

The crate enforces `#![forbid(unsafe_code)]`. Its dependencies expose safe interfaces; this is not a
claim that every dependency is implemented without unsafe code. The library never creates a runtime,
registers Ctrl+C, initializes tracing, or chooses a logging destination. Those belong to the host.
The CLI translates Ctrl+C into a pause request.

## Progress events

`DownloadEvent` currently reports:

- `Started { output, total_bytes, resumed_bytes }`
- `Progress { downloaded_bytes, total_bytes }`
- `Finished { summary }`
- `Failed { message }`

Progress is sampled at most once every 500 ms per download, shared across all workers, so the CLI
does not queue or render an update for every chunk. The first progress sample and lifecycle events
are immediate; intermediate samples are skipped without making workers wait. Completion still
reports the exact final byte count through `join`. The event channel remains unbounded, so an
indefinitely stalled reader can still accumulate these low-rate updates.

Pause and cancellation are surfaced as typed completion errors from `DownloadHandle::join`:
`Error::Paused` and `Error::Cancelled`.

## Resume behavior

Unfinished bytes are stored separately from the requested final output:

```text
<output file name>.arkama.part
<output file name>.arkama.part.arkama.state
<output file name>.arkama.lock
```

Start a later download with the same URL and output path to resume. Only version-1 checkpoints with
matching source/final URL, strong ETag, total size, output identity, entropy settings, and valid
non-overlapping segment coverage can supply reusable positions. Missing, corrupt, old, incompatible,
or out-of-bounds checkpoints cause a conservative restart. File length alone never proves progress.
Weak ETags and Last-Modified alone are insufficient: these servers use a full stream and restart
after interruption instead of mixing responses. Unknown-size streams also restart from zero.

Checkpoints include a SHA-256 checksum of their serialized payload, so corruption that still parses
as valid JSON cannot silently change saved progress. This detects corruption, not malicious rewriting
by someone who can also replace the checksum or modify the partial file.

Progress events count bytes written by this attempt plus accepted resumed bytes, not wire bytes or
durable bytes. A checkpoint captures a written-position snapshot, synchronizes file data, then
atomically replaces the checkpoint with a synchronized temporary file. Serialization and disk I/O
do not hold the state lock. Bytes written since that snapshot may be redownloaded after a crash.

Exact range headers, representation identity, body bounds, and complete coverage are checked before
publication. Automatic decompression is disabled and requests specify `Accept-Encoding: identity`;
servers returning non-identity Content-Encoding are rejected, not decoded into ambiguous offsets.
No external content hash is fetched or inferred: verification covers HTTP framing/identity and
coverage, not protection from a server that lies about its ETag or bytes.

Only a verified, synchronized partial is published at the final name, atomically without overwriting.
Publication requires a filesystem supporting hard links in the destination directory; on an
unsupported filesystem it fails and retains the partial. Parent directories are synchronized on
Unix; power-loss durability on other filesystems/platforms is not claimed. Completed partial/state
files are removed. The zero-byte lock file stays intentionally: removing it could let concurrent
attempts lock different files. It is reusable and does not indicate a running job.

**Compatibility change:** an existing final output is always a collision error, not assumed complete
and never silently resumed or overwritten. Legacy final-name partials and `*.arkama.state` files are
left untouched; move them aside or choose a new output path and restart. There is no unsafe migration
of their unverified progress. Arkama artifact suffixes are reserved and sidecar symlinks are refused.

## Segmented downloads

`DownloadRequest::connections` controls the requested number of segmented workers. The default is
`DEFAULT_CONNECTIONS` (`4`). Segmented downloading requires a known content length and HTTP byte
range support and a strong ETag; unsupported servers use the single-stream path.

## Pause and cancel

`DownloadControl::pause()` asks the background task to stop while keeping resumable state when
possible. The completion task returns `Error::Paused`.

`DownloadControl::cancel()` asks the background task to stop and remove partial output/state when
possible. The completion task returns `Error::Cancelled`.

Await `DownloadHandle::join` to acknowledge shutdown before resuming/retrying. All transfer workers
are joined before an error, pause, fallback, or cancel can release output ownership. Controls interrupt
network setup, streaming, and retry/rate-limit waits; in-flight disk operations finish first.
Cancel cannot be downgraded by a later pause. It cleans reserved partial/checkpoint artifacts, never
an existing final file. Dropping a background handle detaches its task; aborting `join` or dropping
an awaited `download` future is not a coordinated shutdown. Keep the runtime alive and use controls
plus `join` when progress must be retained safely.

Use `Error::is_paused()` and `Error::is_cancelled()` to branch on those outcomes without matching
the enum directly.

## Speed limiting

Use `DownloadRequest::speed_limit(bytes_per_second)` or the builder's `speed_limit` method to set an
approximate global byte-per-second limit for a download.

## Feature flags

Default features:

- `rustls-tls`: enables Reqwest's Rustls TLS backend.
- `serde`: enables `Serialize`/`Deserialize` for public request types and the internal persisted
  resume-state format.

Optional features:

- `native-tls`: enables Reqwest's native TLS backend instead or in addition.

If `serde` is disabled with `default-features = false`, the downloader still compiles, but persisted
resume state is unavailable.

Pause/resume therefore restarts from zero without `serde`, even on a range-capable server. Fresh
segmented transfers and coverage verification still work without that feature. TLS features can be
disabled for HTTP-only embedded use.

## Examples

See the crate examples for library-oriented usage inside `examples/`.
