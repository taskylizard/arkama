# Server API

Arkama's HTTP/JSON v1 API controls the same persistent manager used by embedded Rust hosts.
There are no accounts. One owner selects API keys or explicitly disables them at startup.

## Start safely

```sh
arkama daemon run --download-root ~/Downloads
arkama daemon status
arkama manage '{"op":"list","limit":20,"offset":0,"query":""}'
arkama daemon stop
```

The default is an available loopback port, API keys enabled, public HTTP/HTTPS download sources,
and one authorized Downloads root. `ARKAMA_DATA_DIR` contains the store, discovery address and logs.
The store and its credential directory must be disjoint from all output roots. `--download-root`
is repeatable; directories are created at owner-selected startup. Paths refer to the **server's**
filesystem. Embedded `Manager::open` and direct core calls are trusted local use; server hosts use
`Manager::open_with_policy`. Client settings and persisted jobs cannot relax that host policy.

`daemon start` launches defaults in the background. Use foreground `daemon run` under your own
process supervisor for custom options. A disconnected client does not cancel accepted jobs.
`daemon run --no-server` creates no listener and uses the trusted local manager until host shutdown.

### Keys

The first authenticated start generates a 256-bit random key in
`<data>/credentials/api.key`. Local CLI requests read it automatically. Unix uses directory mode
0700 and key mode 0600; Windows restricts the application directory ACL to the process owner.
These are owner-readable plaintext credentials, not encrypted storage or protection against that
owner, administrators, or malware running as them. Keep backups and exported copies private.

```sh
arkama daemon key rotate
arkama daemon key revoke
```

Rotation atomically replaces the key; old keys fail new requests immediately, without restarting.
Revocation persists until explicit rotation, including across restarts. Previously admitted commands
and long polls may finish; rotation does not undo durable acceptance. These commands print the
credential **path**, never the key. Remote clients read a securely transferred copy into their
`Authorization: Bearer <key>` header. Never put API keys in URLs, browser local-storage examples,
command-line history, or public diagnostics.

### Explicit credential-free use

```sh
arkama daemon run --no-auth --download-root ~/Downloads
```

This emits a warning: anyone who can reach the API can control downloads. Path, destination,
origin, request-size and admission boundaries remain enabled. Wider credential-free binding also
requires `--allow-remote-no-auth` and an explicit `ARKAMA_DAEMON_ADDR`; this is intentionally unsafe
for untrusted reachable callers. The local CLI requires loopback discovery. Other clients supply
the server address themselves.

### Remote access and browser origins

Authenticated non-loopback binding is refused. Terminate TLS in an owner-configured reverse proxy
on the same host and forward to the loopback listener. Use `--allow-origin https://downloads.example`
to authorize that exact HTTPS origin/Host. The proxy must expose **HTTPS only**, validate certificates,
preserve that Host and Authorization, and must not add permissive CORS headers or forward untrusted
requests into an unauthenticated listener. Do not trust forwarded scheme/client-address headers;
Arkama does not use them as authorization. TLS termination is the operator's responsibility, not
built-in Arkama TLS. No production proxy or deployment is supplied by this milestone.

Browser origins are denied unless explicitly listed with repeatable `--allow-origin`. Cross-origin
responses echo only an approved exact origin; no wildcard or ambient cookie authentication is used.
OPTIONS preflight grants no data or command access. Every actual route uses the selected key policy.
Native/headless clients may omit Origin but must supply an authorized Host and, by default, a key.

## Destination and filesystem boundaries

Server transfers disable environment HTTP proxies. Literal IPs, each redirect URL, and actual DNS
answers are checked. Checked DNS addresses are passed directly to the HTTP connector without a
second lookup. Private, loopback, link-local, multicast, reserved and documentation networks are
refused. Mixed public/private DNS answers are refused as a set. IPv4-mapped IPv6 is not an escape.

For an intended private source, the owner can select a bounded exact exception:

```sh
arkama daemon run --download-root ~/Downloads --allow-private-source http://192.168.1.20:8080
```

Exceptions authorize an exact scheme/hostname/port, not arbitrary redirects or other ports.
They are host configuration, never request fields. URL user/password credentials and unknown
handoff fields are refused. Credential-bearing browser context follows in the next input milestone.

Outputs must stay inside authorized roots. Directory traversal and paths through symlinks are
refused. Managed names reject Windows devices, alternate streams, control characters, trailing
dots/spaces and case-insensitive Arkama artifact suffixes. Existing final files are never silently
overwritten; jobs retain output reservations. The filesystem and other programs running as the
owner are trusted: portable path checks are **not** a race-proof sandbox against concurrent local
symlink, mount, junction or hard-link replacement. Use directories that untrusted local users cannot
modify. Changing host roots can make older queued/paused jobs ineligible; they fail safely rather
than write outside the new policy. See [resume and migration](../crates/arkama_core/README.md).

## HTTP/JSON v1 contract

All routes require the same Host/Origin/authentication policy. Send `application/json` for POST.
Unknown routes, methods and command fields are refused. Unsupported API versions have no fallback.

| Route | Contract |
| --- | --- |
| GET `/v1/info` | API version, instance ID, capabilities and fixed resource limits. |
| POST `/v1/commands` | Shared `arkama_manager::Command` JSON; returns `Response` JSON. |
| GET `/v1/updates?instance=…&revision=…` | One bounded coalesced invalidation, with a 20-second long-poll timeout. |
| GET `/v1/diagnostics?id=…` | On-demand bounded numeric diagnostics for one job. |
| POST `/v1/shutdown` | Body `{}`; joins/checkpoints before acknowledgement. |
| POST `/v1/legacy` | Compatibility command shapes for the CLI, under exactly the same protections. |

The old raw line-delimited TCP endpoint is removed; clients must upgrade with the server.
Use `/v1/commands` for new clients. Current capabilities are jobs, controls, settings, paginated
history/search, diagnostics and updates; queues/rules/batches/limits/schedules arrive separately.

Commands include `status`, `enqueue`, `list`, `show`, `pause`, `resume`, `cancel`, `retry`,
`get_setting`, `set_setting`, and `diagnostics`. For example:

```json
{"op":"enqueue","request":{"url":"https://example.com/file.bin","output":null,"output_dir":null,"connections":4,"user_agent":null,"limit":null,"experimental_entropy":false}}
```

Acceptance returns `{"type":"accepted","id":1}` only after persistence. A transport timeout is
ambiguous: fetch state before repeating a mutating command. Duplicate-safe handoff IDs are the
next milestone. `pause`/`cancel` replies describe joined, persisted state, not merely a request.

List requires `limit` (1–100), `offset` (0–1,000,000) and `query` (up to 1,024 bytes). It returns
`jobs`, `offset`, `has_more` and `revision`. Show returns an authoritative job `record` and optional
last/current numeric `observation`. Active progress may be newer than its stored history. Errors
are `{"error":{"code":"…","message":"…"}}`: invalid 400, unauthorized 401, forbidden 403,
not_found 404, conflict 409, busy/stopped 503, storage 500. Legacy compatibility responses retain
their old JSON error shape; new clients should use the structured command route.

### Snapshots and reconnects

Read `/v1/info` for the random server `instance`, then `status`/`list`/`show` for current state.
Long-poll updates using that instance and the snapshot's revision. The response contains instance,
revision, changed, resync, and current status. Every changed revision is a **resync invalidation**,
not an event log: fetch snapshots again. Progress is coalesced, not delivered per chunk. Revisions
are ordered within one instance; restart changes the instance. Missed updates, disconnected clients,
future/stale revisions and instance mismatches cannot be used to infer terminal state. Multi-page
views should restart pagination if revisions change while reading. No subscriber queue is retained.
Terminal state always remains in snapshots even if the client missed the invalidation.

### Diagnostics and bounds

The first diagnostic read of an active attempt starts an optional dedicated collector thread.
It samples every 500 ms and retains 120 unsmoothed speed samples; at most one collector per active
job. It is independent of download producers and completion. An inactive job returns its last
persisted export, or `snapshot: null` if collection was never requested or unavailable. Collector
or persistence failure never changes a download result. Gaps, lag, evictions, received/written/
crash-safe bytes and process-wide memory/CPU are labeled in the export. Resume starts a new attempt
with a new collector on demand; exports describe an attempt, not complete lifetime accounting.

Requests are capped at 64 KiB and replies at 2 MiB; header parsing uses at most 32 KiB. Admission
allows 32 concurrent connections and 64 accepted connections per second globally. One HTTP
request per connection avoids pipelining queues. Excess connections are closed without admission;
clients must back off, not blindly replay accepted commands. Headers/body each have a 5-second
read deadline and connections a 30-second total deadline, including commands and reply writing.
The manager separately bounds pending commands to 128 and pending controls to 16 per active job.
Slow/no subscribers cannot enqueue observations or block transfers. Authentication is checked
before body decoding, command admission, snapshots, updates and diagnostic access.

[Back to the README](../README.md)
