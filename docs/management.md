# Download management

Queues, limits, folder rules, windows and batch submission use the same persistent manager in
embedded Rust, `manage --no-server`, and authenticated HTTP `/v1/commands`. Server paths always
refer to the server's filesystem and must satisfy its owner-selected roots.

## Queues and order

```sh
arkama manage '{"op":"put_queue","queue":{"name":"work","paused":true,"max_active":2,"priority":5,"newest_first":false,"window":null}}'
arkama manage '{"op":"queues"}'
arkama manage '{"op":"submit","submission":{"queue":"work","priority":3,"request":{"url":"https://example.com/file.bin","connections":4,"experimental_entropy":false}}}'
arkama manage '{"op":"move_job","id":1,"queue":"work","priority":7,"position":-10}'
```

`put_queue` creates or replaces a queue. Set `paused:false` to start admission. Pausing a queue
prevents **new starts**; active jobs continue. Explicit job pause/cancel still joins their workers.
Queue names contain 1–64 ASCII letters/digits, `_` or `-`; at most 32 queues exist. The default
queue cannot be deleted. Delete another queue with `delete_queue` only after its pending jobs
have been moved or made terminal. Retry of a terminal job in a deleted queue requires moving it first.

Higher queue priority is considered first, then higher job priority within that queue. Both
priorities are -1,000,000 through 1,000,000. Equal queue priorities choose the oldest eligible
candidate ID. Within a queue, FIFO uses ascending position (the job ID unless explicitly moved);
`newest_first:true` reverses position order. Ties use ascending ID. This is strict priority, not
a starvation-free fairness guarantee. Moves/reordering affect inactive queued/paused/failed/cancelled
jobs only; they never preempt active work or change a resolved output.

`max_active` is 1–64 per queue, also bounded by `max_concurrent` (default 3). The original default
queue has a limit of 64 so the application limit governs old stores; `Queue::new` defaults to 3.
Lowered limits prevent new admission until active jobs drain. Queue policies and positions persist.

## Shared limits

```sh
arkama manage '{"op":"set_setting","key":"max_host_connections","value":"8"}'
arkama manage '{"op":"set_setting","key":"global_speed_limit","value":"5MB"}'
arkama manage '{"op":"set_setting","key":"global_speed_limit","value":""}'
```

The manager defaults to eight active HTTP responses per normalized hostname, across **all jobs**,
ports and schemes. A permit covers setup and the response body; redirects release it and acquire
the actual destination's permit. Retries/probes use the same admission. Idle HTTP pooling is disabled
so finished responses do not leave unbudgeted idle connections. HTTP/2 streams each use admission,
which is more conservative than counting sockets. Changing the 1–256 host limit is live; existing
responses drain without interruption. Waiting for admission does not consume network deadlines.

`global_speed_limit` is one application-wide bytes/second cap, initially unlimited. An empty value
disables it; positive bytefmt values enable it. The setting persists and changes wake blocked
throttles immediately. Every worker reserves the same aggregate budget. Optional request `limit`
caps remain per job and are reserved concurrently with the aggregate budget, not added as two
delays. Both caps must be satisfied. Limits govern file-byte processing, not TLS/HTTP overhead,
probe bodies, or all bytes already buffered by HTTP/OS layers. Brief buffered bursts are possible.
Received, written and retry/disk traffic remain distinct diagnostics.

Direct independent downloads/processes do not share a manager cap. Embedded applications can share
`arkama_core::DownloadLimits` through one `DownloadSession`. Per-job request caps/defaults apply to
new attempts; changing `speed_limit` does not rewrite existing requests.

## Folders and collisions

`set_rules` replaces an ordered list of at most 64 rules. `rules` reads it. A rule specifies an
optional category, optional exact source hostname and directory; at least one match field is
required. Both fields must match when supplied. Host matching is case-insensitive; category
matching is exact. The first matching rule wins. Categories are at most 64 non-control UTF-8 bytes.

```json
{"op":"set_rules","rules":[{"category":"documents","source_host":null,"directory":"/home/owner/Downloads/documents"}]}
```

Use `submission.category` to select a category. Explicit `output`, then explicit `output_dir`, take
precedence over rules. Otherwise rules precede `download_dir`, the first authorized root and the
local default folder. The resolved output is persisted at acceptance; later rule changes do not
move existing jobs. Rules and directories use the same authorized-root checks as manual paths.

Submission `collision` defaults to `fail`: existing final files, unowned partials and another job's
reservation are refused before acceptance. `unique` chooses `name.1` through `name.100`, bounded by
the same portable name checks. It never overwrites files. Cancellation deletes only an attempted
job's retained partials, not completed files; deleting records/files is not a queue control.

## Download windows

Set a queue's optional `window` with an IANA `timezone`, `weekdays` (Monday 0–Sunday 6),
`start_minute` and `end_minute` (0–1439). Start is inclusive; end is exclusive. Equal endpoints
are refused; use `window:null` for unrestricted time. Overnight windows belong to their starting
weekday. For example Monday 23:30–Tuesday 00:30 uses `[0]`, 1410, 30.

Windows govern **new starts only**, not active jobs. The host's current UTC instant is converted
to the declared timezone, including after restart. Repeated DST minutes are eligible twice;
skipped local minutes do not occur. Jobs retain one execution owner through either transition.
The server, not an attached client, evaluates persisted eligibility.

## Batches, history and automation

`batch` takes 1–32 `submissions` under the ordinary 64 KiB HTTP limit. It returns ordered per-item
`accepted` job IDs or `rejected` structured errors. Each accepted item is already durable; a later
item's failure does not roll it back. Invalid collection/field sizes reject admission of the entire
batch. Transport interruption remains ambiguous; duplicate-safe handoff IDs follow in milestone 7.
Use `list` with bounded limit/offset/query for history/search and `show` for authoritative snapshots.
The CLI `manage` command prints JSON, including batch failures, without a GUI or progress stream.

[Server startup and security](server.md) · [Embedded manager](../crates/arkama_manager/README.md)
