//! Serialized state transitions; only joined attempts can become terminal or resumable.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arkama_core::{
    DownloadControl, DownloadObservation, DownloadRequest, DownloadSession, DownloadSummary,
};
use arkama_data::{Db, default_download_dir};
use fs2::FileExt;
use tokio::runtime::Handle;

use crate::{
    BatchResult, Collision, Command, Error, FolderRule, JobSnapshot, ManagerPolicy, Message, Queue,
    Reply, Response, Result, Status, Submission,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Intent {
    Pause,
    Cancel,
    Shutdown,
}
struct Active {
    request: DownloadRequest,
    queue: String,
    observation: DownloadObservation,
    control: DownloadControl,
    done: mpsc::Receiver<arkama_core::Result<DownloadSummary>>,
    intent: Option<Intent>,
    pending: Vec<(Intent, Reply)>,
    metadata_saved: bool,
    last_save: Instant,
    diagnostics: Option<arkama_core::Diagnostics>,
}

pub(crate) struct Actor {
    _ownership: File,
    runtime: Handle,
    session: DownloadSession,
    policy: ManagerPolicy,
    db: Db,
    active: BTreeMap<i64, Active>,
    max_concurrent: usize,
    queues: BTreeMap<String, Queue>,
    rules: Vec<FolderRule>,
    revision: u64,
    stopping: bool,
}

impl Actor {
    pub(crate) fn open(path: PathBuf, runtime: Handle, policy: ManagerPolicy) -> Result<Self> {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::create_dir_all(parent)
            .map_err(|_| Error::new("storage", "cannot create store directory"))?;
        let path = if path.exists() {
            path.canonicalize()
        } else {
            parent
                .canonicalize()
                .map(|parent| parent.join(path.file_name().unwrap_or_default()))
        }
        .map_err(|_| Error::new("storage", "cannot resolve store path"))?;
        let store = path.parent().expect("resolved store parent");
        if policy
            .roots
            .iter()
            .any(|root| root.starts_with(store) || store.starts_with(root))
        {
            return Err(Error::new(
                "invalid",
                "store and output roots must be disjoint",
            ));
        }
        let mut lock_name = path.file_name().unwrap_or_default().to_os_string();
        lock_name.push(".manager.lock");
        let lock_path = path.with_file_name(lock_name);
        if std::fs::symlink_metadata(&lock_path)
            .is_ok_and(|metadata| !metadata.is_file() || metadata.file_type().is_symlink())
        {
            return Err(Error::new("storage", "store lock must be a regular file"));
        }
        let ownership = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(|_| Error::new("storage", "cannot open store lock"))?;
        ownership
            .try_lock_exclusive()
            .map_err(|_| Error::new("conflict", "store already has an execution owner"))?;
        let db = Db::open_path(&path)?;
        db.refresh_output_keys()?;
        db.recover_daemon_downloads()?;
        let max_concurrent = db
            .get_setting("max_concurrent")?
            .unwrap_or_else(|| "3".into());
        validate_setting("max_concurrent", &max_concurrent)?;
        let host_limit = db
            .get_setting("max_host_connections")?
            .unwrap_or_else(|| "8".into());
        let speed = db.get_setting("global_speed_limit")?.unwrap_or_default();
        validate_setting("max_host_connections", &host_limit)?;
        validate_setting("global_speed_limit", &speed)?;
        let limits = arkama_core::DownloadLimits::default();
        limits
            .set_connections_per_host(host_limit.parse().expect("validated host limit"))
            .map_err(|_| Error::new("invalid", "invalid host connection limit"))?;
        limits.set_bytes_per_second(if speed.is_empty() {
            0
        } else {
            bytefmt::parse(&speed).expect("validated speed")
        });
        let queues: BTreeMap<String, Queue> = match db.get_setting("queues")? {
            Some(value) => serde_json::from_str(&value)
                .map_err(|_| Error::new("storage", "invalid persisted queues"))?,
            None => {
                let mut queue = Queue::new("default");
                queue.max_active = 64;
                [(queue.name.clone(), queue)].into_iter().collect()
            }
        };
        if queues.len() > 32 || !queues.contains_key("default") {
            return Err(Error::new("storage", "invalid persisted queues"));
        }
        for (name, queue) in &queues {
            queue.validate()?;
            if name != &queue.name {
                return Err(Error::new("storage", "queue identity mismatch"));
            }
        }
        let rules: Vec<FolderRule> = db
            .get_setting("folder_rules")?
            .map(|value| serde_json::from_str(&value))
            .transpose()
            .map_err(|_| Error::new("storage", "invalid persisted folder rules"))?
            .unwrap_or_default();
        validate_rules(&rules, &policy)?;
        Ok(Self {
            _ownership: ownership,
            runtime,
            session: DownloadSession::default()
                .destination_policy(policy.destination.clone())
                .limits(limits),
            policy,
            db,
            active: BTreeMap::new(),
            max_concurrent: max_concurrent.parse().expect("validated limit"),
            queues,
            rules,
            revision: 1,
            stopping: false,
        })
    }

    pub(crate) fn run(
        &mut self,
        receiver: mpsc::Receiver<Message>,
        stop: Arc<AtomicBool>,
    ) -> Result<()> {
        let mut failure = None;
        loop {
            if stop.load(Ordering::Acquire) {
                self.begin_shutdown();
            }
            if let Err(error) = self.poll().and_then(|_| self.schedule()) {
                failure.get_or_insert(error);
                self.begin_shutdown();
            }
            if self.stopping && self.active.is_empty() {
                return failure.map_or(Ok(()), Err);
            }
            match receiver.recv_timeout(Duration::from_millis(25)) {
                Ok(message) if self.stopping => {
                    let _ = message
                        .reply
                        .send(Err(Error::new("stopped", "manager is stopping")));
                }
                Ok(message) => self.command(message),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.begin_shutdown();
                    std::thread::sleep(Duration::from_millis(25));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
        }
    }

    fn begin_shutdown(&mut self) {
        if self.stopping {
            return;
        }
        self.stopping = true;
        for active in self.active.values_mut() {
            if active.intent.is_none() {
                active.intent = Some(Intent::Shutdown);
                active.control.pause();
            }
        }
    }

    fn command(&mut self, Message { command, reply }: Message) {
        let intent = match command {
            Command::Pause { id } => Some((id, Intent::Pause)),
            Command::Cancel { id } => Some((id, Intent::Cancel)),
            _ => None,
        };
        if let Some((id, intent)) = intent {
            if let Some(active) = self.active.get_mut(&id) {
                if active.pending.len() >= 16 {
                    let _ = reply.send(Err(Error::new("busy", "job control admission is full")));
                    return;
                }
                let status = if intent == Intent::Cancel {
                    "cancelling"
                } else {
                    "pausing"
                };
                if intent == Intent::Cancel || active.intent.is_none() {
                    if let Err(error) = self.db.transition_download(id, status) {
                        let _ = reply.send(Err(error.into()));
                        return;
                    }
                    active.intent = Some(intent);
                    if intent == Intent::Cancel {
                        active.control.cancel();
                    } else {
                        active.control.pause();
                    }
                    self.revision += 1;
                }
                active.pending.push((intent, reply));
                return;
            }
        }
        let result = self.execute(command);
        let _ = reply.send(result);
    }

    fn execute(&mut self, command: Command) -> Result<Response> {
        match command {
            Command::Status => Ok(Response::Status(Status {
                pid: std::process::id(),
                active: self.active.len(),
                queued: self.db.count_queued_daemon_downloads()?,
                revision: self.revision,
            })),
            Command::Enqueue { request } => self.submit(request.into()),
            Command::Submit { submission } => self.submit(submission),
            Command::Batch { submissions } => {
                let results = submissions
                    .into_iter()
                    .map(|submission| match self.submit(submission) {
                        Ok(Response::Accepted { id }) => BatchResult::Accepted { id },
                        Err(error) => BatchResult::Rejected { error },
                        _ => unreachable!("submission response"),
                    })
                    .collect();
                Ok(Response::Batch { results })
            }
            Command::Queues => Ok(Response::Queues {
                queues: self.queues.values().cloned().collect(),
            }),
            Command::PutQueue { queue } => {
                queue.validate()?;
                if !self.queues.contains_key(&queue.name) && self.queues.len() >= 32 {
                    return Err(Error::new("invalid", "at most 32 queues"));
                }
                let mut queues = self.queues.clone();
                queues.insert(queue.name.clone(), queue);
                self.db.set_setting(
                    "queues",
                    &serde_json::to_string(&queues).expect("queue JSON"),
                )?;
                self.queues = queues;
                self.revision += 1;
                Ok(Response::Ok)
            }
            Command::DeleteQueue { name } => {
                if name == "default" || self.db.queue_has_pending(&name)? {
                    return Err(Error::new(
                        "conflict",
                        "queue is default or has pending jobs",
                    ));
                }
                let mut queues = self.queues.clone();
                if queues.remove(&name).is_none() {
                    return Err(Error::new("not_found", "queue not found"));
                }
                self.db.set_setting(
                    "queues",
                    &serde_json::to_string(&queues).expect("queue JSON"),
                )?;
                self.queues = queues;
                self.revision += 1;
                Ok(Response::Ok)
            }
            Command::MoveJob {
                id,
                queue,
                priority,
                position,
            } => {
                self.require_status(id, &["queued", "paused", "failed", "cancelled"])?;
                if !self.queues.contains_key(&queue) || priority.abs_diff(0) > 1_000_000 {
                    return Err(Error::new("invalid", "unknown queue or invalid priority"));
                }
                self.db
                    .move_queued_download(id, &queue, priority, position)?;
                self.revision += 1;
                Ok(Response::Job(Box::new(self.job(id)?)))
            }
            Command::Rules => Ok(Response::Rules {
                rules: self.rules.clone(),
            }),
            Command::SetRules { rules } => {
                validate_rules(&rules, &self.policy)?;
                self.db.set_setting(
                    "folder_rules",
                    &serde_json::to_string(&rules).expect("rule JSON"),
                )?;
                self.rules = rules;
                self.revision += 1;
                Ok(Response::Ok)
            }
            Command::List {
                limit,
                offset,
                query,
            } => {
                if !(1..=100).contains(&limit) || offset > 1_000_000 || query.len() > 1024 {
                    return Err(Error::new("invalid", "invalid history page"));
                }
                let mut records = self.db.load_downloads_page(limit + 1, offset, &query)?;
                let has_more = records.len() > limit;
                records.truncate(limit);
                let jobs = records
                    .into_iter()
                    .map(|record| self.snapshot(record))
                    .collect::<Result<_>>()?;
                Ok(Response::Jobs {
                    jobs,
                    offset,
                    has_more,
                    revision: self.revision,
                })
            }
            Command::Show { id } => Ok(Response::Job(Box::new(self.job(id)?))),
            Command::Diagnostics { id } => {
                self.job(id)?;
                let snapshot = if let Some(active) = self.active.get_mut(&id) {
                    if active.diagnostics.is_none() {
                        active.diagnostics = arkama_core::Diagnostics::start(
                            active.observation.clone(),
                            arkama_core::DiagnosticsConfig {
                                disk_path: active.request.output.as_ref().map(|output| {
                                    let mut path = output.as_os_str().to_os_string();
                                    path.push(".arkama.part");
                                    PathBuf::from(path)
                                }),
                                ..Default::default()
                            },
                        )
                        .ok();
                    }
                    active.diagnostics.as_ref().map(|diagnostics| {
                        let mut snapshot = diagnostics.snapshot();
                        snapshot.observation = active.observation.snapshot();
                        snapshot
                    })
                } else {
                    self.db.load_diagnostics(id)?
                };
                Ok(Response::Diagnostics {
                    id,
                    snapshot: snapshot.map(Box::new),
                })
            }
            Command::Pause { id } => {
                self.require_status(id, &["queued", "paused"])?;
                self.db.transition_download(id, "paused")?;
                self.revision += 1;
                Ok(Response::Job(Box::new(self.job(id)?)))
            }
            Command::Cancel { id } => {
                let status =
                    self.require_status(id, &["queued", "paused", "failed", "cancelled"])?;
                let retained = self.db.attempt_started(id)?;
                if retained && status != "cancelled" {
                    if let Some(request) = self.db.load_daemon_request(id)? {
                        if let Some(output) = request.output {
                            self.policy.directory(
                                output
                                    .parent()
                                    .ok_or_else(|| Error::new("invalid", "output has no parent"))?,
                            )?;
                            arkama_core::discard_partial(&output).map_err(|_| {
                                Error::new("conflict", "partial output cannot be safely discarded")
                            })?;
                        }
                    }
                }
                self.db.transition_download(id, "cancelled")?;
                self.db.update_download_cancelled(id)?;
                self.revision += 1;
                Ok(Response::Job(Box::new(self.job(id)?)))
            }
            Command::Resume { id } => self.resume(id, false),
            Command::Retry { id } => self.resume(id, true),
            Command::GetSetting { key } => {
                if key.len() > 128 {
                    return Err(Error::new("invalid", "invalid setting key"));
                }
                Ok(Response::Setting {
                    value: self.db.get_setting(&key)?,
                    key,
                })
            }
            Command::SetSetting { key, value } => {
                validate_setting(&key, &value)?;
                if key == "download_dir" {
                    self.policy.directory(Path::new(&value))?;
                }
                self.db.set_setting(&key, &value)?;
                if key == "max_concurrent" {
                    self.max_concurrent = value.parse().expect("validated limit");
                }
                if key == "max_host_connections" {
                    self.session
                        .limits_handle()
                        .set_connections_per_host(value.parse().expect("validated host limit"))
                        .map_err(|_| Error::new("invalid", "invalid host connection limit"))?;
                }
                if key == "global_speed_limit" {
                    self.session
                        .limits_handle()
                        .set_bytes_per_second(if value.is_empty() {
                            0
                        } else {
                            bytefmt::parse(&value).expect("validated speed")
                        });
                }
                self.revision += 1;
                Ok(Response::Ok)
            }
        }
    }

    fn resume(&mut self, id: i64, retry: bool) -> Result<Response> {
        if !self.queues.contains_key(&self.job(id)?.record.queue_name) {
            return Err(Error::new(
                "invalid",
                "move the job to an existing queue before retrying",
            ));
        }
        self.require_status(
            id,
            if retry {
                &["failed", "cancelled"]
            } else {
                &["paused"]
            },
        )?;
        let mut request = self
            .db
            .load_daemon_request(id)?
            .ok_or_else(|| Error::new("invalid", "job has no managed request"))?;
        validate_request(&request)?;
        self.resolve_output(&mut request, id)?;
        self.db.update_daemon_request(id, &request)?;
        let bytes = self.job(id)?.record.downloaded_bytes.max(0) as u64;
        self.db.transition_download(id, "queued")?;
        self.db.requeue_download(id, bytes)?;
        self.revision += 1;
        Ok(Response::Job(Box::new(self.job(id)?)))
    }

    fn require_status(&self, id: i64, allowed: &[&str]) -> Result<String> {
        let status = self
            .db
            .download_status(id)?
            .ok_or_else(|| Error::new("not_found", "job not found"))?;
        if self.db.load_daemon_request(id)?.is_none() {
            return Err(Error::new("invalid", "job has no managed request"));
        }
        if self.active.contains_key(&id) || !allowed.contains(&status.as_str()) {
            return Err(Error::new(
                "conflict",
                format!("job cannot change from {status}"),
            ));
        }
        Ok(status)
    }

    fn job(&self, id: i64) -> Result<JobSnapshot> {
        self.snapshot(
            self.db
                .load_download(id)?
                .ok_or_else(|| Error::new("not_found", "job not found"))?,
        )
    }

    fn snapshot(&self, mut record: arkama_data::DownloadRecord) -> Result<JobSnapshot> {
        let observation = match self.active.get(&record.id) {
            Some(active) => Some(active.observation.snapshot()),
            None => self.db.load_observation(record.id)?,
        };
        if let Some(snapshot) = &observation {
            if self.active.contains_key(&record.id) {
                record.downloaded_bytes = snapshot.written_bytes.min(i64::MAX as u64) as i64;
            }
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64;
        let elapsed = now.saturating_sub(record.state_since_ms).max(0);
        if record.status == "queued" {
            record.queue_wait_ms = record.queue_wait_ms.saturating_add(elapsed);
        }
        if record.status == "paused" {
            record.paused_ms = record.paused_ms.saturating_add(elapsed);
        }
        Ok(JobSnapshot {
            record,
            observation,
        })
    }

    fn resolve_output(&self, request: &mut DownloadRequest, id: i64) -> Result<()> {
        let output = match request.output.clone() {
            Some(output) => output,
            None => {
                let url = url::Url::parse(&request.url)
                    .map_err(|_| Error::new("invalid", "invalid download URL"))?;
                let name = url
                    .path_segments()
                    .and_then(|mut segments| segments.next_back())
                    .filter(|name| !name.is_empty())
                    .unwrap_or("download");
                if name == "." || name == ".." || name.contains(['/', '\\', ':']) {
                    return Err(Error::new("invalid", "filename is not a plain file name"));
                }
                let dir = request
                    .output_dir
                    .clone()
                    .or(self.db.get_setting("download_dir")?.map(PathBuf::from))
                    .or_else(|| self.policy.roots.first().cloned())
                    .unwrap_or_else(default_download_dir);
                dir.join(name)
            }
        };
        let filename = output
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::new("invalid", "output filename must be UTF-8"))?;
        crate::policy::validate_filename(filename)?;
        let parent = output
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = self.policy.directory(parent)?;
        std::fs::create_dir_all(&parent)
            .map_err(|_| Error::new("storage", "cannot create output directory"))?;
        let output = parent
            .canonicalize()
            .map_err(|_| Error::new("storage", "cannot resolve output directory"))?
            .join(filename);
        request.output = Some(output.clone());
        let occupied = std::fs::symlink_metadata(&output).is_ok()
            || (id < 0
                && [".arkama.part", ".arkama.part.arkama.state"]
                    .iter()
                    .any(|suffix| {
                        std::fs::symlink_metadata(
                            output.with_file_name(format!("{filename}{suffix}")),
                        )
                        .is_ok()
                    }));
        if occupied {
            return Err(Error::new(
                "conflict",
                "output or unowned partial already exists",
            ));
        }
        // The retained lock gives case aliases one filesystem identity, even while queued.
        let lock_path = output.with_file_name(format!("{filename}.arkama.lock"));
        if std::fs::symlink_metadata(&lock_path)
            .is_ok_and(|metadata| !metadata.is_file() || metadata.file_type().is_symlink())
        {
            return Err(Error::new("invalid", "output lock must be a regular file"));
        }
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|_| Error::new("storage", "cannot reserve output identity"))?;
        if self.db.output_owner(&output, id)?.is_some() {
            return Err(Error::new("conflict", "output is reserved by another job"));
        }
        request.output = Some(output);
        Ok(())
    }

    fn submit(&mut self, submission: Submission) -> Result<Response> {
        let Submission {
            mut request,
            queue,
            priority,
            category,
            collision,
        } = submission;
        if !self.queues.contains_key(&queue)
            || priority.abs_diff(0) > 1_000_000
            || category
                .as_ref()
                .is_some_and(|value| value.len() > 64 || value.chars().any(char::is_control))
        {
            return Err(Error::new(
                "invalid",
                "unknown queue or invalid submission metadata",
            ));
        }
        validate_request(&request)?;
        let url = url::Url::parse(&request.url).expect("validated URL");
        self.policy
            .destination
            .validate(&url)
            .map_err(|_| Error::new("forbidden", "download destination is not authorized"))?;
        if request.output.is_none() && request.output_dir.is_none() {
            if let Some(rule) = self.rules.iter().find(|rule| {
                rule.category
                    .as_ref()
                    .is_none_or(|expected| category.as_ref() == Some(expected))
                    && rule.source_host.as_ref().is_none_or(|host| {
                        url.host_str()
                            .is_some_and(|source| source.eq_ignore_ascii_case(host))
                    })
            }) {
                request.output_dir = Some(rule.directory.clone());
            }
        }
        match self.resolve_output(&mut request, -1) {
            Ok(()) => {}
            Err(error) if error.code == "conflict" && matches!(collision, Collision::Unique) => {
                let output = request.output.clone().expect("resolved collision path");
                let filename = output
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| Error::new("invalid", "invalid filename"))?;
                let mut found = false;
                for suffix in 1..=100 {
                    request.output = Some(output.with_file_name(format!("{filename}.{suffix}")));
                    match self.resolve_output(&mut request, -1) {
                        Ok(()) => {
                            found = true;
                            break;
                        }
                        Err(error) if error.code == "conflict" => {}
                        Err(error) => return Err(error),
                    }
                }
                if !found {
                    return Err(Error::new("conflict", "no free collision suffix"));
                }
            }
            Err(error) => return Err(error),
        }
        let id = self.db.insert_queued_download(&request, &queue, priority)?;
        self.revision += 1;
        Ok(Response::Accepted { id })
    }

    fn schedule(&mut self) -> Result<()> {
        if self.stopping {
            return Ok(());
        }
        while self.active.len() < self.max_concurrent {
            let mut selected: Option<(&Queue, i64)> = None;
            for queue in self.queues.values() {
                if !queue.eligible()
                    || self
                        .active
                        .values()
                        .filter(|active| active.queue == queue.name)
                        .count()
                        >= queue.max_active
                {
                    continue;
                }
                if let Some(id) = self.db.queue_candidate(&queue.name, queue.newest_first)? {
                    if selected.is_none_or(|(chosen, candidate)| {
                        queue.priority > chosen.priority
                            || (queue.priority == chosen.priority && id < candidate)
                    }) {
                        selected = Some((queue, id));
                    }
                }
            }
            let Some((queue, _)) = selected else {
                break;
            };
            let queue = queue.clone();
            let Some((id, mut request)) = self
                .db
                .claim_queue_download(&queue.name, queue.newest_first)?
            else {
                continue;
            };
            if let Err(error) =
                validate_request(&request).and_then(|_| self.resolve_output(&mut request, id))
            {
                self.db.update_download_failed(id, &error.message)?;
                self.revision += 1;
                continue;
            }
            self.db.update_daemon_request(id, &request)?;
            match self
                .session
                .start_with_handle(self.runtime.clone(), request.clone())
            {
                Ok(handle) => {
                    let (done_tx, done) = mpsc::sync_channel(1);
                    self.runtime.spawn(async move {
                        let result = handle.join.await.unwrap_or_else(|_| {
                            Err(arkama_core::Error::RuntimeUnavailable {
                                message: "download task stopped".into(),
                            })
                        });
                        let _ = done_tx.send(result);
                    });
                    drop(handle.events);
                    self.active.insert(
                        id,
                        Active {
                            request,
                            queue: queue.name.clone(),
                            observation: handle.observation,
                            control: handle.control,
                            done,
                            intent: None,
                            pending: Vec::new(),
                            metadata_saved: false,
                            last_save: Instant::now(),
                            diagnostics: None,
                        },
                    );
                }
                Err(_) => self
                    .db
                    .update_download_failed(id, "invalid download request")?,
            }
            self.revision += 1;
        }
        Ok(())
    }

    fn poll(&mut self) -> Result<()> {
        let mut completed = Vec::new();
        let mut storage_error = None;
        for (&id, active) in &mut self.active {
            let snapshot = active.observation.snapshot();
            let mut save = || -> eyre::Result<()> {
                if !active.metadata_saved {
                    if let Some(metadata) = active.observation.metadata() {
                        active.request.output = Some(metadata.output.clone());
                        self.db.update_daemon_request(id, &active.request)?;
                        self.db.update_download_started(
                            id,
                            &metadata.output,
                            metadata.total_bytes,
                            snapshot.written_bytes,
                        )?;
                        self.db.update_download_metadata(
                            id,
                            metadata.etag.as_deref(),
                            metadata.last_modified.as_deref(),
                            metadata.mime_type.as_deref(),
                            &metadata.final_url,
                        )?;
                        if let Some(intent) = active.intent {
                            self.db.transition_download(
                                id,
                                if intent == Intent::Cancel {
                                    "cancelling"
                                } else if intent == Intent::Pause {
                                    "pausing"
                                } else {
                                    "running"
                                },
                            )?;
                        }
                        active.metadata_saved = true;
                        self.revision += 1;
                    }
                }
                if active.last_save.elapsed() >= Duration::from_millis(500) {
                    self.db.update_download_progress(
                        id,
                        active
                            .observation
                            .metadata()
                            .and_then(|metadata| metadata.total_bytes),
                        snapshot.written_bytes,
                    )?;
                    let _ = self.db.save_observation(id, &snapshot);
                    if let Some(diagnostics) = &active.diagnostics {
                        let _ = self.db.save_diagnostics(id, &diagnostics.snapshot());
                    }
                    active.last_save = Instant::now();
                    self.revision += 1;
                }
                Ok(())
            };
            if let Err(error) = save() {
                storage_error = Some(error);
            }
            match active.done.try_recv() {
                Ok(result) => completed.push((id, result)),
                Err(mpsc::TryRecvError::Disconnected) => completed.push((
                    id,
                    Err(arkama_core::Error::RuntimeUnavailable {
                        message: "download host runtime stopped".into(),
                    }),
                )),
                Err(mpsc::TryRecvError::Empty) => {}
            }
        }
        for (id, result) in completed {
            let active = self.active.remove(&id).expect("joined active job");
            let snapshot = active.observation.snapshot();
            let _ = self.db.save_observation(id, &snapshot);
            if let Some(diagnostics) = &active.diagnostics {
                let mut diagnostics = diagnostics.snapshot();
                diagnostics.observation = snapshot.clone();
                let _ = self.db.save_diagnostics(id, &diagnostics);
            }
            let persisted: eyre::Result<()> = (|| {
                self.db.update_download_progress(
                    id,
                    active
                        .observation
                        .metadata()
                        .and_then(|metadata| metadata.total_bytes),
                    snapshot.written_bytes,
                )?;
                match result {
                    Ok(summary) => {
                        self.db.transition_download(id, "finished")?;
                        self.db.update_download_finished(id, &summary)?;
                    }
                    Err(arkama_core::Error::Paused) if active.intent == Some(Intent::Shutdown) => {
                        self.db.transition_download(id, "queued")?;
                        self.db.requeue_download(id, snapshot.written_bytes)?;
                    }
                    Err(arkama_core::Error::Paused) => {
                        self.db.transition_download(id, "paused")?;
                        self.db.update_download_paused(id, snapshot.written_bytes)?;
                    }
                    Err(arkama_core::Error::Cancelled) => {
                        self.db.transition_download(id, "cancelled")?;
                        self.db.update_download_cancelled(id)?;
                    }
                    Err(error) => self.db.update_download_failed(id, &error.to_string())?,
                }
                Ok(())
            })();
            self.revision += 1;
            for (intent, reply) in active.pending {
                let response = match &persisted {
                    Err(_) => Err(Error::new("storage", "failed to persist joined job")),
                    Ok(()) => self.job(id).and_then(|job| {
                        if (intent == Intent::Pause && job.record.status != "paused")
                            || (intent == Intent::Cancel && job.record.status != "cancelled")
                        {
                            Err(Error::new(
                                "conflict",
                                format!("joined job is {}", job.record.status),
                            ))
                        } else {
                            Ok(Response::Job(Box::new(job)))
                        }
                    }),
                };
                let _ = reply.send(response);
            }
            if let Err(error) = persisted {
                storage_error = Some(error);
            }
        }
        if let Some(error) = storage_error {
            return Err(error.into());
        }
        Ok(())
    }
}

pub fn validate_setting(key: &str, value: &str) -> Result<()> {
    let valid = match key {
        "max_concurrent" | "connections" => value
            .parse::<usize>()
            .is_ok_and(|value| (1..=64).contains(&value)),
        "max_host_connections" => value
            .parse::<usize>()
            .is_ok_and(|value| (1..=256).contains(&value)),
        "download_dir" => !value.is_empty() && value.len() <= 4096,
        "speed_limit" | "global_speed_limit" => {
            value.is_empty() || bytefmt::parse(value).is_ok_and(|limit| limit > 0)
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(Error::new("invalid", "unknown setting or invalid value"))
    }
}

fn validate_request(request: &DownloadRequest) -> Result<()> {
    let url =
        url::Url::parse(&request.url).map_err(|_| Error::new("invalid", "invalid download URL"))?;
    if request.url.len() > 8192
        || !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || request.connections > 64
        || request
            .user_agent
            .as_ref()
            .is_some_and(|value| value.len() > 1024 || value.contains(['\r', '\n']))
        || request.limit == Some(0)
    {
        return Err(Error::new("invalid", "unsupported download request"));
    }
    request
        .recovery
        .validate()
        .map_err(|_| Error::new("invalid", "invalid recovery policy"))
}

fn validate_rules(rules: &[FolderRule], policy: &ManagerPolicy) -> Result<()> {
    if rules.len() > 64 {
        return Err(Error::new("invalid", "at most 64 folder rules"));
    }
    for rule in rules {
        if (rule.category.is_none() && rule.source_host.is_none())
            || rule.category.as_ref().is_some_and(|value| {
                value.is_empty() || value.len() > 64 || value.chars().any(char::is_control)
            })
            || rule.source_host.as_ref().is_some_and(|host| {
                host.is_empty()
                    || host.len() > 253
                    || url::Url::parse(&format!("https://{host}/"))
                        .ok()
                        .is_none_or(|url| {
                            url.host_str()
                                .is_none_or(|parsed| !parsed.eq_ignore_ascii_case(host))
                        })
            })
        {
            return Err(Error::new("invalid", "invalid category/source folder rule"));
        }
        policy.directory(&rule.directory)?;
    }
    Ok(())
}
