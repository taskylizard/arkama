# CLI guide

Both `arkama` and `arkama-cli` expose the same commands. Direct downloads run in the current process;
daemon job commands talk to the existing local background process. Neither mode requires a GUI.

## Download a file

```sh
arkama-cli download https://example.com/file.bin
arkama-cli download https://example.com/file.bin -o ./file.bin --connections 8
arkama-cli download https://example.com/file.bin --limit 2MB
arkama-cli download --links-file ./links.txt
```

When no output path is given, Arkama infers a filename from HTTP headers or the URL and uses the
configured download folder. Links files contain one URL per line and run sequentially in direct mode;
`--output` cannot be combined with `--links-file`.

Use `arkama-cli download --help` for the full argument reference:

| Option | Behavior |
| --- | --- |
| `-o, --output <PATH>` | Explicit final output path. Existing final files are refused. |
| `--links-file <PATH>` | Read URLs from a text file instead of the positional URL. |
| `--connections <N>` | Workers per download; configured value, otherwise 4. |
| `--user-agent <UA>` | Override the HTTP User-Agent. |
| `--limit <BYTES_PER_SEC>` | Per-download speed cap; accepts values such as `2MB` or `500KB`. |
| `--recovery-policy <JSON>` | Validated deadlines/retry policy; omitted fields keep core defaults. |
| `--daemon` | Enqueue instead of downloading in this process. |
| `--silent` | Suppress progress and summary output. Errors still fail the command. |
| `--json` | Emit machine-readable events to stdout. Cannot be combined with `--silent`. |
| `--experimental-entropy` | Opt-in connection recycling for LACP/ECMP environments. |
| `-y, --yes` | Currently a no-op; does not authorize overwrite or change collision behavior. |

The request speed cap is shared across the workers of **one download**. The manager's additional
`global_speed_limit` cap is shared across all active jobs; see [download management](management.md).
JSON progress and terminal updates are sampled at most once per 500 ms. Optional core notifications
use a bounded buffer and may be dropped under overload; the CLI obtains the exact final result from
the independent completion handle.

### Network recovery

```sh
arkama-cli download https://example.com/file.bin --recovery-policy '{"read_idle_timeout_ms":60000,"max_retries":8}'
arkama-cli add https://example.com/file.bin --recovery-policy '{"max_retries":0}'
```

The same policy is used for direct and queued downloads and persists with queued requests. Defaults
are a 10 s connection deadline, 30 s connection/header deadline, 30 s network-read idle deadline,
five retries, exponential backoff capped at 10 s, and up to 250 ms jitter. The daemon shares a rolling
30-retry budget per origin per minute across its jobs. Independent direct processes do not share it.
Permanent HTTP errors and invalid transfer data are not retried. `Retry-After` is honored unless it
exceeds the maximum wait, in which case Arkama stops and asks you to retry later. Limiter and disk
waits do not count as network idle. See [all settings and bounds](../crates/arkama_core/README.md#recovery-policy).

## Pause and resume a direct download

Press Ctrl+C to request pause. The CLI waits for the core to finish shutdown, then returns an
interrupted result. Run the same download again to resume validated checkpointed bytes:

```sh
arkama-cli download https://example.com/file.bin -o ./file.bin
```

Reuse depends on matching source/redirect identity, a strong ETag, size, compatible settings, and
valid saved coverage. Keep the same entropy setting when resuming. Without safe identity or a valid
checkpoint, the transfer restarts rather than trusting file length. See [resume details](../crates/arkama_core/README.md#resume-behavior).

## Settings and history

Both binaries share the same SQLite settings and history:

```sh
arkama-cli config show
arkama-cli config get download_dir
arkama-cli config set download_dir ~/Downloads
arkama-cli config set connections 8
arkama-cli config set speed_limit 2MB
arkama-cli config set max_concurrent 3
arkama-cli history
arkama-cli history -n 50
arkama-cli history -q "example.com"
```

- `download_dir` supplies the inferred-output folder. An explicit `--output` takes precedence.
- `connections` and `speed_limit` supply defaults for newly resolved download requests.
- `max_concurrent` controls active daemon jobs; the daemon reads it at startup, so restart it to apply changes.
- `history -q` searches URL, output path, status, and error text. Query results do not apply the `-n` limit.

The offline `config` commands edit string values; use `manage set_setting` for validated live changes.
Do not edit the store while a manager owns it. The global `--reset-db` option deletes the settings/history database;
it is not a download-cancel command and does not delete completed files.

## Local background jobs

> [!WARNING]
> The HTTP/JSON daemon enables API keys and loopback binding by default. See the
> [server guide](server.md) before selecting private sources, browser origins or remote access.

```sh
arkama-cli daemon start
arkama-cli daemon status
arkama-cli add https://example.com/file.bin
arkama-cli download https://example.com/other.bin --daemon
arkama-cli list
arkama-cli list -n 50 -q "example.com"
arkama-cli show 1
arkama-cli pause 1
arkama-cli resume 1
arkama-cli cancel 1
arkama-cli retry 1
arkama-cli daemon stop
```

Replace `1` with the ID returned when adding a job. `daemon run` runs the daemon in the foreground.
By default, it binds to an available loopback port and writes its address to the application data directory.
Queued requests survive daemon restarts; interrupted active requests are requeued on startup.
Pause/cancel responses acknowledge joined workers and durable state. `pausing`/`cancelling` mean a
request is still being completed. A transfer that has already published may finish instead.
Named queues enforce priority, order and per-queue admission. Shared host/bandwidth caps, folder
rules, batches and timezone windows use `manage`; see [download management](management.md).

### Shared manager and no-server operation

```sh
arkama-cli manage '{"op":"status"}'
arkama-cli manage '{"op":"list","limit":20,"offset":0,"query":"example.com"}'
arkama-cli manage '{"op":"set_setting","key":"max_concurrent","value":"2"}'
arkama-cli manage --no-server '{"op":"status"}'
arkama-cli daemon run --no-server
```

`manage` emits JSON and shares the exact local/server manager commands. `--no-server` opens no
control listener, requires exclusive store ownership, and stops/checkpoints before the command
exits; it cannot control a manager owned by another process. `daemon run --no-server` keeps that
local host running until Ctrl+C. See the [lifecycle contract](../crates/arkama_manager/README.md).
Managed jobs reserve output paths at admission and use URL basename inference; direct core downloads
still prefer HTTP filenames. Search pagination is bounded through `manage`, including filtered pages.

`ARKAMA_DATA_DIR` overrides the application data directory containing `arkama.sqlite`, `daemon.addr`,
and `daemon.log`. `ARKAMA_DAEMON_ADDR` overrides the daemon bind/client address. Local CLI discovery
requires loopback. Authenticated remote access requires an owner-configured local TLS proxy; plain
authenticated non-loopback binding is refused. See [keys, roots and explicit no-auth](server.md).

## Shell completions

```sh
arkama-cli completions bash
arkama-cli completions zsh
arkama-cli completions fish
arkama-cli completions powershell
```

Completions are printed to stdout for the shell you select. Substitute `arkama` to generate
completions for that binary's name. The parser also accepts `nushell`, but its completion generator
currently panics; Nushell completions are unavailable.

## Experimental connection entropy

`--experimental-entropy` targets bonded networks that distribute connections by their 5-tuple
(LACP/ECMP). It disables idle pooling, uses 4 MiB segments, and recycles slow segment connections.

It adds connection churn and TCP/TLS handshakes, can be slower on small files or restrictive servers,
and is not a default remedy for a flaky connection. Changes to entropy mode invalidate incompatible
resume settings. No universal speedup is claimed.

[Back to the README](../README.md)
