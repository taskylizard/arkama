# arkama

Very, _very_, _very_, work in progress, proceed with fire!

Fast, practical download manager. Ships as `arkama-cli` and the combined `arkama` binary.

## 🧭 Quick start (CLI)

```bash
arkama-cli download <url>
arkama-cli download <url> -o ./file.bin
arkama-cli download <url> --connections 8
arkama-cli daemon start
arkama-cli download <url> --daemon
```

You can replace `arkama-cli` with `arkama` in the examples below if you prefer the combined binary.

## ✨ Features

CLI-first for now.

- HTTP/HTTPS downloads with redirects.
- Filename inference from `Content-Disposition` or URL path.
- Resume via HTTP Range.
- Segmented downloads when size and range support are available (default 4 connections).
- Falls back to single stream when Range is unavailable.
- Progress bar with total bytes, speed, and ETA.
- Retries with exponential backoff on transient failures.
- Ctrl+C saves state for resume.
- Optional global speed limit.
- Experimental connection entropy mode for LACP/ECMP environments.
- Download history backed by SQLite.
- Background daemon with local TCP control and persisted queue recovery.
- Search across past downloads.
- Configurable download folder (persisted in app settings).
- JSON output for automation.

## 🔄 Shared configuration

Both binaries share the same SQLite database for settings and download history:

```bash
# Set download folder (used by both binaries)
arkama-cli config set download_dir ~/Downloads
arkama-cli config set connections 8
arkama-cli config set speed_limit 2MB

# View current config
arkama-cli config show

# View download history
arkama-cli history
arkama-cli history -n 50
arkama-cli history -q "example.com"
```

## 🎛️ CLI commands

### `arkama-cli download [OPTIONS] [URL]`

Download a file from a URL.

- `-o, --output <PATH>`: output file path.
- `--links-file <PATH>`: text file with one URL per line (downloads run sequentially).
- `--connections <N>`: concurrent connections for segmented downloads (defaults to config, otherwise `4`).
- `--user-agent <UA>`: override HTTP User-Agent.
- `--limit <BYTES_PER_SEC>`: global speed limit (defaults to config if unset; ex: `2MB`, `500KB`).
- `--experimental-entropy`: experimental; disables idle pooling, uses 4MB segments, and recycles slow segment connections.
- `--daemon`: enqueue the download on the local Arkama daemon.
- `--silent`: suppress progress and summary output.
- `--json`: emit JSON events to stdout for automation.

### `arkama-cli daemon <COMMAND>`

Manage the background daemon.

- `arkama-cli daemon start`: launch the daemon in the background.
- `arkama-cli daemon run`: run the daemon in the foreground.
- `arkama-cli daemon status`: show daemon status.
- `arkama-cli daemon stop`: stop the daemon.

Queued daemon jobs are stored in the shared SQLite database, survive daemon restarts, and are
recovered automatically when the daemon starts again.

### `arkama-cli history [OPTIONS]`

Show download history.

- `-n, --limit <N>`: number of records to show (default 20).
- `-q, --query <QUERY>`: filter by URL, path, or status.

### `arkama-cli config <COMMAND>`

View or modify configuration.

- `arkama-cli config show`: show all settings.
- `arkama-cli config get <KEY>`: get a setting value.
- `arkama-cli config set <KEY> <VALUE>`: set a setting value.

Common keys:

- `download_dir`
- `connections`
- `max_concurrent`
- `speed_limit`

### `arkama <COMMAND>`

The `arkama` binary exposes the same CLI commands as `arkama-cli`.

### Global options

- `--reset-db`: reset the application database.

## 🧪 Experimental entropy mode

`--experimental-entropy` is for bonded network setups that load-balance by hashing the 5‑tuple (LACP/ECMP).
It pushes connection diversity so traffic spreads across links.

What changes:

- Disables idle pooling so segments prefer fresh connections.
- Uses fixed 4MB segments across your `--connections` worker count.
- Recycles slow segment connections for the remaining bytes.

Tradeoffs:

- More connection churn and TCP/TLS handshakes.
- Slightly higher CPU and latency overhead.
- Can be slower on small files or servers with strict connection limits.

Resume notes:

- Resume state includes the segment size and entropy mode flag.
- If you resume without the same entropy setting, arkama starts fresh instead of reusing incompatible state.

Should I use this? Nope.

## 💾 Resume and state

- If the output file exists, arkama resumes when possible.
- For segmented downloads, state is stored in `*.arkama.state` next to the output file.
- On successful completion, the state file is removed.

## 📌 Examples

```bash
# Download files
arkama-cli download https://example.com/file
arkama-cli download https://example.com/file -o ./file.bin
arkama-cli download https://example.com/file --connections 8
arkama-cli download https://example.com/file --limit 2MB
arkama-cli download --links-file ./links.txt
arkama-cli download https://example.com/file --experimental-entropy
arkama-cli daemon start
arkama-cli download https://example.com/file --daemon
arkama-cli download https://example.com/file --silent
arkama-cli download https://example.com/file --json

# Configuration and history
arkama-cli config show
arkama-cli config set download_dir ~/Downloads
arkama-cli history
arkama-cli history -n 10 -q "zip"
```
