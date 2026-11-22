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
| `--daemon` | Enqueue instead of downloading in this process. |
| `--silent` | Suppress progress and summary output. Errors still fail the command. |
| `--json` | Emit machine-readable events to stdout. Cannot be combined with `--silent`. |
| `--experimental-entropy` | Opt-in connection recycling for LACP/ECMP environments. |
| `-y, --yes` | Currently a no-op; does not authorize overwrite or change collision behavior. |

The speed cap is shared across the workers of **one download**, not across all active daemon jobs.
JSON progress and terminal updates are sampled at most once per 500 ms. The first sample and lifecycle
events are immediate; the final completion result contains the exact byte count.

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

Configuration currently stores string values; it is not the validated, live-update management contract
planned for later milestones. The global `--reset-db` option deletes the settings/history database;
it is not a download-cancel command and does not delete completed files.

## Local background jobs

> [!WARNING]
> The current daemon is unauthenticated and uses line-delimited JSON over TCP. Keep it on loopback.
> It is not ready for remote exposure or browser-origin access.

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
Current job control messages describe requested actions, not a new acknowledged lifecycle contract.
Queue names and priority fields exist, but scheduling currently claims queued jobs by insertion order.

`ARKAMA_DATA_DIR` overrides the application data directory containing `arkama.sqlite`, `daemon.addr`,
and `daemon.log`. `ARKAMA_DAEMON_ADDR` overrides the daemon bind/client address. An override can expose
the listener beyond loopback without adding authentication, so do not use a public bind address.

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
