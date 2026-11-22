# Foundation validation

The eight milestones provide a safe ordinary-file core, bounded diagnostics/recovery, a shared
persistent manager, secure HTTP access, queues/limits/rules/windows, and protected duplicate-safe
handoff. This is implementation and controlled validation, not deployment or a published release.
Check the exact-head [CI results](https://github.com/taskylizard/arkama/actions/workflows/ci.yml)
before relying on a new checkout. The owner still supplies the release license.

## Repeatable regression gates

`cargo test --workspace --locked` currently runs 84 tests/doctests. Expected HTTP bytes are generated
independently with asymmetric content; valid length alone cannot satisfy the integrity checks.

| Gate | Existing public-contract coverage |
| --- | --- |
| 1. Reliable safe core | [Core transfer tests](../crates/arkama_core/tests/download_e2e.rs): missing/corrupt/out-of-bounds checkpoints, replaced same-size resources, invalid ranges/encoding, full/unknown-length fallback, collisions, joined controls and independent byte equality. Core/manager enforce `forbid(unsafe_code)`. |
| 2. Bounded observations | Same tests: saturated eight-event buffers, blocked/failed collector sinks, independent final results, lag/gaps, fixed retention, useful/received/disk/checkpoint counters. |
| 3. Predictable recovery | Same tests: connection/read stalls, temporary/permanent statuses, Retry-After, shared budgets, redirects, intentional throttling, and responsive setup/backoff controls. |
| 4. Shared persistent manager | [Manager/real-daemon tests](../crates/arkama/tests/manager_e2e.rs): identical controls, store/output ownership, queue/pause/terminal restart, process kill/recovery, joined shutdown and storage failure. |
| 5. Secure bounded server | [Real server tests](../crates/arkama/tests/server_e2e.rs): current/missing/wrong/rotated/revoked keys, deliberate no-auth, hostile Host/Origin/path/DNS/redirect/body input, deadlines/admission, diagnostics and snapshot resync. |
| 6. Download management | Manager tests plus core shared-limit tests: actual priority/order/per-queue admission, shared host/bandwidth caps, live changes, portable collisions/rules, batch results, persisted windows and independent timezone/DST cases. |
| 7. Client inputs | Cookie/header/referrer-dependent files through core, manager and daemon; concurrent/restarted duplicate IDs, conflict/unsupported input, context-sensitive resume, same/cross/back-origin redirect checks, secret-free history/checkpoints and [key/migration failures](../crates/arkama_data/src/db.rs). |
| 8. Combined checks | Workspace checks and all-feature Clippy on Linux/macOS/Windows; Rust 1.85 library/examples and four feature jobs; strict rustdoc; executable clients and controlled faults/measurements below. |

CI exercises Windows filename/path/ACL behavior, partial publication/checkpoint replacement and
real daemon lifecycle. Linux/macOS local runs supplement it. The Unix process/syscall fault tools
below were run on Linux only; passing Windows suites do not prove Windows power-loss durability.
Tests use explicit body gates and bounded observation waits where a fixed delay cannot establish
concurrency. No test is skipped to hide platform failures.

## Executed client and fault checks

- The [headless client and embedded example](clients.md) were executed against a real
  cookie/header/referrer-dependent server with API-key and explicit local no-auth access.
  Repeated submission returned one durable ID; CLI stdin pause, host restart, reconnect/resume,
  updates/list/batch/settings all worked. Each final file matched 262,147 independent bytes.
  Synthetic secrets were absent from output, daemon logs and raw SQLite pages.
- A separate safe Rust 1.85 consumer controlled downloads with its own runtime and tracing
  destination, without global signal interception. Process SIGKILL at three checkpoint ages and
  SIGINT retained only verified reusable bytes. Every restarted 4,194,307-byte final file had
  SHA-256 `008fd62e93c4968247b73a1daf3d3da0017009bd920e606e798af5f9099d2779`.
  Console/silent/JSON CLI Ctrl+C acknowledged pause and resumed the same content.
- Linux fault interposition delayed real partial-file writes by 300 ms each. A 20 ms network-idle
  policy did not mistake that delay for a network stall: exactly one intended 503 retry and 32,771
  correct bytes. Injected ENOSPC failed without a finished event or final name; removing the fault
  and restarting produced all 4,194,307 independent bytes.
- Actual daemon SIGKILL immediately before and after no-clobber publication left SQLite nonterminal.
  Before publication, restart resumed and completed. After publication, restart failed safely on
  the existing final rather than inferring completion. Duplicate handoff still returned the same
  job ID, and the published bytes were independently correct. Inspect that final and submit to a
  new path if the history is ambiguous; there is no automatic completion inference.
- An actual TLS proxy checked its certificate and returned 401 for missing/wrong keys and 200
  for the current key. Discovery/authenticated CLI shutdown passed. Separate safe Rust 1.85
  consumers fetched a pinned public HTTPS README using Rustls and native TLS independently,
  with 5,336 bytes equal to `git show` of the pinned source, not a hash derived from downloaded data.

These syscall shims are external validation tools, not shipped unsafe Arkama backends. Scratch
reproduction commands, inputs and output remain in the implementation thread's
`.amp/in/orb-backup/`; the [milestone evidence](https://ampcode.com/artifacts/core-milestone-evidence-bec206ff-9a6f-46de-8ee9-9ace017e8825)
records delivery and the full commands. The permanent public-contract tests above are in the repo.

## Diagnostics measurements have limits

Four release-mode repetitions per condition were interleaved in forward/reverse order, with no
client/diagnostics, diagnostics plus host tracing, and an unread event client plus a 500 ms sink.
The collector used an aggressive 50 ms interval and four samples; shipped defaults are 500 ms/120.

| Fixture | Off median completion | Diagnostics/tracing median | Off → on median process RSS |
| --- | --- | --- | --- |
| Healthy, 8,388,611 bytes | 41.987 ms | 38.472 ms | 60.926 → 61.041 MiB |
| Four interrupted ranges/chunk delay, 8,388,611 bytes | 2,314.615 ms | 2,314.754 ms | 68.865 → 76.717 MiB |
| 300 ms actual-write delay, 65,539 bytes | 612.327 ms | 611.769 ms | 6.092 → 6.256 MiB |

Unread-client/slow-sink medians were 34.843, 2,277.516 and 610.111 ms respectively. These differences
are noise, not acceleration. Healthy completion ranged 30.945–46.769 ms, interrupted runs
2,220.349–2,356.043 ms, and disk-delay runs 607.998–909.305 ms. Disk runs with a ninth write took
one additional 300 ms delay; no run was discarded. Four interrupted ranges incurred four retries
and retained verified in-attempt progress, with no extra file-body/disk bytes; the non-range
regression separately checks duplicate traffic after restarting a full stream. All buffers drained
to zero pending bytes/workers, history stayed at most four, and unread event buffers stayed at most eight.

Measurements include the HTTP fixture, runtime, allocator and process-wide OS samples. CPU time
and filesystem block I/O were recorded, not attributed as exact per-job cost. A provisional local
regression envelope is ≤10% median completion regression and ≤16 MiB median process RSS growth
against diagnostics off for this fixture; the runs stayed inside it. It is not a portable SLA or
an owner-agreed production budget. There is no zero-overhead promise or measured universal fastest
connection count.

## Remaining limits and later work

Controlled disconnects/stalls are not tasky's real ISP/link. Hardware power cuts, malicious local
filesystem races, inaccessible Windows syscall fault injection, Android lifecycle/storage and
actual extension interception/cancellation remain unproven. The filesystem must support hard links;
checks trust the owner-controlled filesystem. Restricted DNS/redirect checks and TLS do not create
a race-proof local sandbox. Secret-at-rest protection requires safeguarding the matching storage
key; owners/admins/malware able to read both key/database are outside that threat model.

Read [migration/retention](clients.md), [safe startup](server.md), [queue/window/limit semantics](management.md)
and [core lifecycle/features](../crates/arkama_core/README.md) before integrating clients. Browser
extensions, Linux/Windows desktop clients, Android apps, opt-in video/extraction and measured
acceleration are later phases. The pre-existing Nushell completion panic remains unrelated.
No license, package release, production proxy or deployment was added.
