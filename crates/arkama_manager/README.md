# arkama_manager

Persistent download management shared by server and embedded hosts. The host owns its Tokio
runtime, signals, tracing, and lifetime; this library opens no control listener.

```rust,no_run
use arkama_core::DownloadRequest;
use arkama_manager::{Command, Manager, Response};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = Manager::open("./state/arkama.sqlite", tokio::runtime::Handle::current()).await?;
    let Response::Accepted { id } = manager.execute(Command::Enqueue {
        request: DownloadRequest::new("https://example.com/file.bin").output("./downloads/file.bin"),
    }).await? else { unreachable!() };
    // Call Show/List to observe, and Pause/Resume/Cancel/Retry to control this job.
    let _snapshot = manager.execute(Command::Show { id }).await?;
    manager.shutdown().await?; // Keep the host runtime alive through checkpointing and joining.
    Ok(())
}
```

## Ownership and lifecycle

- One retained, never-unlinked `*.manager.lock` refuses simultaneous execution owners of a store.
  Clones are clients of the same owner, not independent managers. Use one canonical database path;
  do not hard-link a database into another directory or mutate it directly while owned.
- SQLite and scheduling run on a dedicated OS thread, never a transfer task or GUI frame thread.
  Admission holds at most 128 commands, 16 pending controls per active job, and 64 active jobs.
  Oversized fields and full admission fail explicitly. History/search pages contain at most 100 jobs.
- Jobs reserve canonical output paths at durable acceptance, including across pauses/restarts.
  Retained output locks identify case aliases on case-insensitive filesystems; do not unlink these
  locks. Managed paths must be UTF-8 for the shared JSON contract.
  Managed filename inference uses the URL basename (or `download`) **before** starting the transfer;
  set an explicit output for another name. The single-file core still supports HTTP-header inference.
  No silent overwrite occurs; failed/paused jobs retain reservations until cancelled. Finished jobs
  retain their history/output reservation. Reserved sidecar suffixes are refused by the core.
- `pausing`/`cancelling` are requests, not stopped states. Pause/cancel responses wait for joined
  workers and persisted outcomes. Cancel wins over a concurrent pause. A transfer already published
  may finish instead; a conflicting control reports the actual state, not false acknowledgement.
- Progress/metadata come from the observation handle, not best-effort events. Stored progress is
  approximate history, never evidence for reusing bytes. Numeric snapshots round-trip with serde;
  `last_recovery_reason` is now an owned string to support transport/persistence.
  Job records are authoritative; an inactive job's observation describes its last attempt. Controls
  require a managed request; standalone-download history is visible but cannot control another host.
- Queued and paused durations use persisted wall-clock milliseconds, include host downtime, and
  clamp backward clock movement to zero. They are separate from inclusive core worker timings.
- Shutdown refuses new work, checkpoints/joins, requeues interrupted running jobs, and preserves
  intentional pauses. Explicit shutdown is required before dropping the runtime. Dropping the last
  handle requests shutdown but cannot await it; admission already accepted survives a lost caller.
  Shutdown returns a storage error if required state persistence failed, even after workers joined.
  Cancelling a requeued attempt removes its retained partials; optional diagnostics never decide
  whether an attempt started or whether its controls succeeded.
- Startup requeues interrupted `starting`/`running` jobs and preserves paused/terminal jobs. A crash
  during an unacknowledged pause/cancel leaves the job paused for explicit reconciliation, not
  automatically running. Repeat cancel to clean retained artifacts.
- Publication and SQLite terminal persistence are different durable operations. If a crash leaves
  a final file but no terminal record, the next attempt refuses the existing file and fails safely.
  Inspect the file independently and choose a new output; never infer completion from length.

## Commands

`Command`/`Response` are shared, typed serde contracts. The CLI's `manage` command uses the same
operations via the daemon, or `manage --no-server` owns a local manager for one command and shuts
it down before exit. A local enqueue therefore persists a queued/resumable job; it does not detach
a background download. `daemon run --no-server` owns the manager until host shutdown, with no listener.

Settings `max_concurrent` and `connections` accept 1–64; `download_dir` is a nonempty path;
`speed_limit` is an optional bytefmt per-request default. `max_concurrent` changes future admission
live without preempting workers. The legacy standalone CLI settings remain an offline store editor;
use `manage` for live shared settings. Named queues and shared host/bandwidth limits follow separately.

The current daemon transport is local and unauthenticated. The secure/versioned HTTP contract is
the next milestone, not a guarantee supplied by this crate's local command handle.
