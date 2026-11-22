# Client inputs and examples

Browser, desktop and remote Android clients use the same running server and v1 manager commands.
No-server is local/embedded use, not a way for a web page or remote phone to download on its host.
These are ordinary-file backend examples, not an extension, GUI, Android service or video worker.
Video/extraction is disabled; optional media support requires a later explicit opt-in implementation.

## Submit safely

Send this command to authenticated `POST /v1/commands` or `arkama manage -` via stdin. Replace only
the synthetic download context with the context the browser is permitted to hand off. Keep the
control API key in the HTTP Authorization header, never inside the download context.

```json
{"op":"submit","submission":{"handoff_id":"device_A-request_1","request":{"url":"https://example.com/export?token=download-token","connections":4,"experimental_entropy":false,"context":{"method":"GET","headers":{"authorization":"Bearer download-credential","accept":"application/octet-stream"},"cookies":"session=download-cookie","referrer":"https://example.com/exports","filename":"report.bin","source":"https://example.com/exports"}}}}
```

`accepted` contains a durable job ID, not proof the download will succeed. Keep the original
submission and ID until acknowledgement. On an ambiguous disconnect, resubmit identical input
with the **same** ID; it returns the original job across restart/completion/credential removal.
Any changed input conflicts. IDs are 1–128 ASCII letters/digits/`_`/`-`, unique within the store,
and retain with history as keyed fingerprints. This compares the parsed input, including defaults
and original unresolved paths, not whitespace/key order. Changing explicit header spelling is a
different input. Use a new ID for a genuinely new request. Plain `enqueue` is not duplicate-safe.
Future extensions cancel the browser's download only after this durable acknowledgement; actual
browser interception/cancellation remains unimplemented and unverified here.

## Supported context

- Only GET is supported. HEAD is an internal probe, not a client method. POST, bodies and unknown
  fields are refused so a future extension can leave the original browser transfer running.
- Headers allow `authorization`, `accept`, `accept-language`, and `x-*`: at most 16 entries and
  8,192 combined name/value bytes. Names are case-insensitive; duplicate normalized names, invalid
  values, transport/range/host fields and direct Cookie/Referer injection are refused.
- `cookies` is one explicit Cookie value, at most 4,096 bytes; there is no browser cookie jar or
  Set-Cookie persistence. `referrer` and metadata-only `source` are HTTP/HTTPS URLs, at most 8,192
  bytes, without userinfo. Referrer fragments are removed. Download URLs are at most 8,192 bytes
  in the manager. Explicit filename is a plain name, at most 240 bytes, under portable path checks.
- Explicit output wins over filename inference. Source is retained as protected metadata, never
  forwarded; folder source rules match the download hostname, not this metadata URL.
- Every request applies context only to the original scheme/host/port. Origin-changing redirects
  strip all context and custom user-agent values, including a return to the original origin.
  HTTPS downgrade is refused. Every redirect still follows the host's destination policy.

## Secret storage, display and retention

Replay JSON is authenticated-encrypted with AES-256-GCM and a random nonce. The independent random
key is at `<database stem>.credentials/request.key` beside the database (normally
`<data>/arkama.credentials/request.key`). Unix uses 0700/0600; Windows uses the owner-only credential
directory ACL. Missing/wrong keys and damaged ciphertext fail closed. Back up and restore the
database **and matching key** together, privately. API-key rotation does not rotate this storage key.
There is no OS keyring or password recovery. A database alone cannot recover replay credentials;
the owner, administrators or malware able to read both files can. TLS and local filesystem trust
are still operator responsibilities. Direct core callers own any persistence they add.

Queued/paused/active requests retain restart context. Successful completion drops replay JSON;
failed/cancelled jobs retain it for retry until `forget_request` is explicitly called. That command
accepts terminal jobs only, removes replay but not history/handoff IDs/output reservations/files,
and makes subsequent retry unavailable. There is no time-based expiry or secure memory-erasure
guarantee. API keys retain until rotation/revocation; they are separate owner-readable credentials.

Request Debug, errors, history, checkpoints, numeric diagnostics and exports do not expose replay
values. Display URLs contain only origin plus `/[redacted]`; ETags are SHA-256 fingerprints.
Checkpoint identity hashes include credential context, preventing reuse across changed accounts.
Serialize intentionally includes secrets; never log request JSON. Filenames, paths, MIME types
and Last-Modified are ordinary visible metadata: do not put credentials there. A malicious source
can reflect secrets into file contents/metadata; this is not a content-redaction service.

Startup encrypts legacy queued JSON, redacts URL/history metadata and clears old error/ETag text.
Finished legacy replay is discarded. SQLite secure deletion and migration compaction clear current
pages, not old backups, captured logs or historical WAL/checkpoint copies. Version-1 checkpoints
restart, rather than reuse plaintext identities. Stop old executables before upgrading; do not
downgrade against a migrated store. Move aside legacy final-name partials to restart safely.

## Executable examples

The standard-library [headless Python client](../scripts/server_client.py) reads a command from stdin,
checks `/v1/info`, sends it and optionally long-polls/resnapshots a job. It never follows control-server
redirects or sends keys over remote plaintext HTTP. HTTPS uses normal certificate verification.

```sh
python3 scripts/server_client.py https://downloads.example --key-file /private/api.key --watch < command.json
python3 scripts/server_client.py http://127.0.0.1:PORT --no-auth --watch < command.json
arkama manage - < command.json
```

The credential-free example requires an explicitly `--no-auth` loopback server. `command.json`
contains secrets: protect it, or supply stdin from an in-memory client rather than saving a file.
Neither credentials nor secret commands belong in shell history. The same script accepts `show`,
`pause`, `resume`, `cancel`, paginated `list`, settings and `batch` commands. Every batch item should
have its own handoff ID. A transport failure exits: reconnect, fetch info/state, then reconcile
acceptance using the stable ID. Updates are resync invalidations, never a lossless progress log.

The safe [embedded Rust example](../crates/arkama_manager/examples/handoff.rs) accepts a Submission
(the contents of the command's `submission` field) on stdin, owns its runtime, starts no listener
and explicitly joins the manager before exit:

```sh
cargo run -p arkama_manager --example handoff -- ./state/arkama.sqlite < submission.json
```

Call the asynchronous manager API from a future GUI host, not SQLite/parsing/transfer work on its
frame thread. Linux/Windows filesystem behavior is covered by workspace platform checks; Android
storage, foreground/background services, lifecycle restrictions and packaging remain later work.

[Server limits and startup](server.md) · [Management commands](management.md) · [Core API](../crates/arkama_core/README.md)
