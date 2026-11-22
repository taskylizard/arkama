# arkama_core

Arkama's reusable HTTP/HTTPS download engine, implemented behind a safe Rust API.

Use it for direct or background downloads with validated resume, segmented transfers, progress,
pause/cancel controls, and per-download speed limits. No CLI, database, or daemon is required.
The host owns its Tokio runtime, process signals, tracing, and logging destinations.

## Status

Arkama is in early development; the core/server foundation is not a finished release. The core
supports Rust 1.85 and enforces `#![forbid(unsafe_code)]`. Public types are exported at the crate root.
Request fields remain public for compatibility; new code can use `DownloadRequest::new`, its
chainable setters, or `DownloadRequest::builder`. Use this workspace checkout for development.

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

- `events`: an eight-event, best-effort stream of `DownloadEvent` values.
- `observation`: current counters independent of event/collector availability.
- `join`: the spawned task that resolves to `arkama_core::Result<DownloadSummary>`.
- `control`: a `DownloadControl` for pause/cancel requests.

The crate enforces `#![forbid(unsafe_code)]`. Its dependencies expose safe interfaces; this is not a
claim that every dependency is implemented without unsafe code. The library never creates a runtime,
registers Ctrl+C, initializes tracing, or chooses a logging destination. Those belong to the host.
The CLI translates Ctrl+C into a pause request.

## Progress events

`DownloadEvent` currently reports:

- `Metadata { etag, last_modified, mime_type, final_url }`
- `Started { output, total_bytes, resumed_bytes }`
- `Progress { downloaded_bytes, total_bytes }`
- `Finished { summary }`
- `Failed { message }`

Progress is sampled at most once every 500 ms per download, shared across all workers, so the CLI
does not queue or render an update for every chunk. Samples and lifecycle events are offered immediately
without waiting for buffer capacity; overload drops events and increments `dropped_events`.
Use `join` for authoritative completion and `observation.snapshot()` for current counters.
Consumers explicitly naming `UnboundedReceiver` must change to `mpsc::Receiver`; `recv`/`try_recv`
usage remains the same. Lifecycle notifications are optional, not acknowledgements.

Pause and cancellation are surfaced as typed completion errors from `DownloadHandle::join`:
`Error::Paused` and `Error::Cancelled`.

## Bounded diagnostics

`Diagnostics::start(handle.observation.clone(), DiagnosticsConfig::default())` starts an optional
dedicated OS thread. It aggregates bounded speed history, samples process CPU/memory and optional
disk space, and emits numeric, credential-free tracing observations using the host's dispatcher.
Workers never invoke a tracing subscriber, collector sink, OS sampler, or export writer.

`Diagnostics::with_sink` runs a host-provided sink on that same thread. Sink failure, blocked sinks,
and slow/absent event readers cannot delay transfers or replace the `join` result. Collector snapshots
expose lag, sink errors, history evictions, and gaps; interval speeds are unsmoothed and gaps/resets
have no estimate. Defaults retain 120 samples at 500 ms; intervals must be 50 ms–60 s and retention
1–10,000 samples. Dropping the collector requests stop without waiting; a blocked host sink must
eventually return for its thread to exit.

Measurements distinguish received HTTP **body** bytes, accepted written coverage, disk-write traffic,
and checkpointed bytes. Disk-write traffic includes overwritten retry data; received counts exclude
headers and do not claim wire-level accounting. Timings are inclusive; disk/throttle/retry durations
sum worker time rather than exclusive wall-clock phases. Pending bytes cover known body chunks,
not all HTTP/TLS/OS buffers. Process measurements are process-wide, never exact per-job usage.
Allocated disk bytes are currently available on Unix only; unavailable OS measurements are `None`.
Queue and paused durations belong to the manager that owns those states, not a stopped core attempt.

Snapshots contain no request URLs, headers, cookies, filenames, or error strings. Hosts control export
destinations and should keep exported snapshots bounded. Instrumentation has measurable resource cost;
non-waiting producers do not imply zero CPU, memory, disk, or throughput overhead.

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
approximate byte-per-second cap shared by the workers of one download. This is not an
application-wide cap across multiple jobs.

## Recovery policy

Every request has a `RecoveryPolicy`. Omitted fields in persisted JSON use defaults; Rust struct
literals must now include `recovery: Default::default()` or use the request constructor/builder.
Invalid policies return `Error::InvalidRequest` before an attempt is spawned or touches the network.

| Setting | Default |
| --- | --- |
| `connect_timeout_ms` | 10,000; includes connection establishment and TLS |
| `setup_timeout_ms` | 30,000; connection, redirects, and response headers |
| `read_idle_timeout_ms` | 30,000; waiting for the next HTTP body chunk only |
| `max_retries` | 5 per probe or transfer worker; 0 disables retries |
| `base_delay_ms`, `max_delay_ms` | 500, 10,000; exponential delay starts at twice the base |
| `jitter_ms` | 250; uniform additional delay, capped at maximum wait |
| `max_host_retries`, `host_window_ms` | 30 retries admitted per origin over a rolling 60,000 ms |

Deadlines, maximum wait, and host windows must be 1 ms–1 hour. Base delay and jitter must not exceed
maximum wait; retry limits are at most 1,000 per worker and 10,000 per host. Zero host retries refuses
all recovery. Retries include waits admitted but interrupted by controls; successful requests do not
consume the host retry budget.

Connection/header/read-idle failures, interrupted HTTP framing, short bodies, and HTTP 408, 429,
500, 502, 503, 504 can recover. Other HTTP errors, invalid ranges, changed identity, excess body
coverage, encoding errors, and filesystem errors stop instead of repeatedly replaying invalid work.
401/403/404 errors suggest checking credentials or the link; automatic expired-link refresh is not
provided. Only replayable HTTP GET downloads are supported. Experimental entropy remains opt-in.

Valid numeric or HTTP-date `Retry-After` values set a minimum wait. If that exceeds `max_delay_ms`,
the attempt fails with an actionable retry-later error rather than retrying before the server permits.
Malformed values are ignored. Network-idle timers do not run during rate limiting or disk writes.
Pause/cancel interrupt setup and backoff; workers still finish in-flight disk work before acknowledging.

```rust,no_run
use arkama_core::{DownloadRequest, DownloadSession, RecoveryPolicy};

#[tokio::main]
async fn main() -> arkama_core::Result<()> {
    let session = DownloadSession::default(); // Keep and share this across the application's jobs.
    let policy = RecoveryPolicy { read_idle_timeout_ms: 60_000, ..Default::default() };
    let handle = session.start(DownloadRequest::new("https://example.com/file.bin").recovery(policy))?;
    let summary = handle.join.await.expect("host kept runtime alive")?;
    println!("saved {} bytes", summary.downloaded_bytes);
    Ok(())
}
```

Cloned sessions share per-origin retry budgets. Standalone download functions create a fresh budget
per attempt; applications managing several jobs should reuse a session, as the daemon does. Budgets
are process-local and reset with a new session/restart; they are not a connection or bandwidth cap.
HTTP status failures use the response origin; connection/body failures use the known attempt origin.
Retry history is bounded to 1,024 origins and 10,000 entries per origin, with entries retained for at
most an hour to support different request windows. Capacity exhaustion fails recovery explicitly.

Observation snapshots expose the latest admitted recovery reason and delay, retry count, and admitted
read-idle stalls without URL/header/error strings. Final errors and acknowledgements remain available
through `join`, not optional events. Shared persistent job-state integration follows in the manager.

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

The `examples/` directory contains a simple awaited download, progress reporting, speed limiting,
and pause/resume. `cargo test -p arkama_core --doc --locked` checks the Rust examples in the API docs,
including this README's quick start. The README is also the crate's rustdoc introduction.
