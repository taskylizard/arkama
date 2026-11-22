//! The same lifecycle scenarios through a local manager and the actual daemon process.

use std::path::{Path, PathBuf};
use std::time::Duration;

use arkama_core::DownloadRequest;
use arkama_data::Db;
use arkama_manager::{Command, JobSnapshot, Manager, Response};
use tokio::net::TcpStream;
use tokio::process::Child;
use tokio::runtime::Handle;

#[path = "../../arkama_core/tests/support/mod.rs"]
mod support;
use support::{FlakyServer, FlakyServerConfig};

enum Host {
    Local(Manager),
    Server {
        child: Box<Child>,
        addr: String,
        key: String,
    },
}

impl Host {
    async fn open(dir: &Path, server: bool, source: &str) -> Self {
        std::fs::create_dir_all(dir.join("out")).unwrap();
        if !server {
            return Self::Local(
                Manager::open(dir.join("state/arkama.sqlite"), Handle::current())
                    .await
                    .unwrap(),
            );
        }
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_arkama"))
            .args(["daemon", "run"])
            .arg("--download-root")
            .arg(dir.join("out"))
            .arg("--allow-private-source")
            .arg(
                url::Url::parse(source)
                    .unwrap()
                    .origin()
                    .ascii_serialization(),
            )
            .env("ARKAMA_DATA_DIR", dir.join("state"))
            .env_remove("ARKAMA_DAEMON_ADDR")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let addr = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "daemon exited before ready"
                );
                match tokio::fs::read_to_string(dir.join("state/daemon.addr")).await {
                    Ok(addr) if TcpStream::connect(&addr).await.is_ok() => break addr,
                    _ => {}
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        Self::Server {
            child: Box::new(child),
            addr,
            key: std::fs::read_to_string(dir.join("state/credentials/api.key")).unwrap(),
        }
    }

    async fn execute(&self, command: Command) -> Result<Response, String> {
        match self {
            Self::Local(manager) => manager
                .execute(command)
                .await
                .map_err(|error| error.to_string()),
            Self::Server { addr, key, .. } => {
                let response: serde_json::Value = reqwest::Client::builder()
                    .no_proxy()
                    .build()
                    .unwrap()
                    .post(format!("http://{addr}/v1/commands"))
                    .bearer_auth(key)
                    .json(&command)
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                if let Some(error) = response.get("error") {
                    return Err(error.to_string());
                }
                Ok(serde_json::from_value(response).unwrap())
            }
        }
    }

    async fn stop(&mut self) {
        match self {
            Self::Local(manager) => manager.shutdown().await.unwrap(),
            Self::Server { child, addr, key } => {
                assert_eq!(
                    exchange(addr, key, serde_json::json!("Shutdown")).await,
                    "ShuttingDown"
                );
                assert!(
                    tokio::time::timeout(Duration::from_secs(5), child.wait())
                        .await
                        .unwrap()
                        .unwrap()
                        .success()
                );
            }
        }
    }

    async fn enqueue(&self, request: DownloadRequest) -> i64 {
        let Response::Accepted { id } = self.execute(Command::Enqueue { request }).await.unwrap()
        else {
            panic!("expected durable acceptance");
        };
        id
    }

    async fn job(&self, id: i64) -> JobSnapshot {
        let Response::Job(job) = self.execute(Command::Show { id }).await.unwrap() else {
            panic!("expected snapshot");
        };
        *job
    }

    async fn wait(&self, id: i64, predicate: impl Fn(&JobSnapshot) -> bool) -> JobSnapshot {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let job = self.job(id).await;
                if predicate(&job) {
                    return job;
                }
                assert_ne!(
                    job.record.status,
                    "failed",
                    "{}",
                    job.record.error_message.unwrap_or_default()
                );
                // Normal observation stays below the HTTP connection admission rate.
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
        })
        .await
        .unwrap()
    }
}

async fn exchange(addr: &str, key: &str, value: serde_json::Value) -> serde_json::Value {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("http://{addr}/v1/legacy"))
        .bearer_auth(key)
        .json(&value)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

fn partial(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.arkama.part", path.display()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_controls_preserve_bytes_ownership_and_restart_state() {
    let data: Vec<u8> = (0..524_291)
        .map(|i| ((i * 31 + i / 251 + i / 65537) % 256) as u8)
        .collect();
    let mut config = FlakyServerConfig::new(data.clone());
    config.chunk_size = 1024;
    config.chunk_delay = Duration::from_millis(2);
    let source = FlakyServer::spawn(config.clone()).await;
    for server in [false, true] {
        source.configure(config.clone());
        let dir = tempfile::tempdir().unwrap();
        let mut host = Host::open(dir.path(), server, &source.url()).await;
        host.execute(Command::SetSetting {
            key: "max_concurrent".into(),
            value: "1".into(),
        })
        .await
        .unwrap();
        assert!(
            host.execute(Command::SetSetting {
                key: "max_concurrent".into(),
                value: "0".into()
            })
            .await
            .is_err()
        );
        let path = dir.path().join("out/complete.bin");
        let request = DownloadRequest::new(source.url())
            .output(&path)
            .connections(2);
        let id = host.enqueue(request.clone()).await;
        host.wait(id, |job| job.record.downloaded_bytes > 8192)
            .await;
        let Response::Job(paused) = host.execute(Command::Pause { id }).await.unwrap() else {
            panic!("expected paused snapshot");
        };
        assert_eq!(paused.record.status, "paused");
        assert_eq!(paused.observation.as_ref().unwrap().active_workers, 0);
        assert!(!path.exists());
        let retained = tokio::fs::read(partial(&path)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(70)).await;
        assert_eq!(
            tokio::fs::read(partial(&path)).await.unwrap(),
            retained,
            "writes continued after pause acknowledgement"
        );
        assert!(host.job(id).await.record.paused_ms >= 60);
        assert!(
            host.execute(Command::Enqueue {
                request: request.clone()
            })
            .await
            .is_err(),
            "paused output lost reservation"
        );
        let alias = dir.path().join("out/COMPLETE.bin");
        let alias_lock = dir.path().join("out/COMPLETE.bin.arkama.lock");
        let case_insensitive = alias_lock.exists();
        if case_insensitive {
            assert!(
                host.execute(Command::Enqueue {
                    request: request.clone().output(alias),
                })
                .await
                .is_err(),
                "case alias lost output reservation"
            );
        }
        host.stop().await;
        host = Host::open(dir.path(), server, &source.url()).await;
        assert_eq!(host.job(id).await.record.status, "paused");
        host.execute(Command::Resume { id }).await.unwrap();
        let finished = host.wait(id, |job| job.record.status == "finished").await;
        assert_eq!(finished.record.downloaded_bytes, data.len() as i64);
        assert_eq!(tokio::fs::read(&path).await.unwrap(), data);
        assert!(host.execute(Command::Cancel { id }).await.is_err());
        assert_eq!(tokio::fs::read(&path).await.unwrap(), data);

        let second_path = dir.path().join("out/retry.bin");
        let second = host
            .enqueue(
                DownloadRequest::new(source.url())
                    .output(&second_path)
                    .connections(1),
            )
            .await;
        host.wait(second, |job| job.record.downloaded_bytes > 8192)
            .await;
        host.execute(Command::Pause { id: second }).await.unwrap();
        let cancel_blocker = host
            .enqueue(
                DownloadRequest::new(source.url())
                    .output(dir.path().join("out/cancel-blocker.bin"))
                    .connections(1),
            )
            .await;
        host.wait(cancel_blocker, |job| job.record.downloaded_bytes > 8192)
            .await;
        host.execute(Command::Resume { id: second }).await.unwrap();
        assert_eq!(host.job(second).await.record.status, "queued");
        host.execute(Command::Cancel { id: second }).await.unwrap();
        assert_eq!(host.job(second).await.record.downloaded_bytes, 0);
        assert!(!partial(&second_path).exists());
        assert!(
            !PathBuf::from(format!("{}.arkama.state", partial(&second_path).display())).exists()
        );
        host.execute(Command::Cancel { id: cancel_blocker })
            .await
            .unwrap();
        assert!(!partial(&dir.path().join("out/cancel-blocker.bin")).exists());
        host.execute(Command::Retry { id: second }).await.unwrap();
        host.wait(second, |job| job.record.status == "finished")
            .await;
        assert_eq!(tokio::fs::read(&second_path).await.unwrap(), data);

        let blocker = host
            .enqueue(
                DownloadRequest::new(source.url())
                    .output(dir.path().join("out/blocker.bin"))
                    .connections(1),
            )
            .await;
        host.wait(blocker, |job| job.record.downloaded_bytes > 8192)
            .await;
        let queued = host
            .enqueue(DownloadRequest::new(source.url()).output(dir.path().join("out/queued.bin")))
            .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        host.execute(Command::Pause { id: queued }).await.unwrap();
        assert!(host.job(queued).await.record.queue_wait_ms >= 40);
        let Response::Jobs { jobs, has_more, .. } = host
            .execute(Command::List {
                limit: 1,
                offset: 0,
                query: "127.0.0.1".into(),
            })
            .await
            .unwrap()
        else {
            panic!("expected page");
        };
        assert_eq!(jobs.len(), 1);
        assert!(has_more, "search ignored pagination");
        host.stop().await;
        host = Host::open(dir.path(), server, &source.url()).await;
        assert_eq!(host.job(queued).await.record.status, "paused");
        host.execute(Command::Cancel { id: blocker }).await.unwrap();
        host.execute(Command::Cancel { id: queued }).await.unwrap();
        assert_eq!(host.job(id).await.record.status, "finished");
        let mut full_stream = FlakyServerConfig::new(data.clone());
        full_stream.accept_ranges = false;
        source.configure(full_stream);
        let stream_path = dir.path().join("out/full.bin");
        let stream_id = host
            .enqueue(DownloadRequest::new(source.url()).output(&stream_path))
            .await;
        host.wait(stream_id, |job| job.record.status == "finished")
            .await;
        assert_eq!(tokio::fs::read(stream_path).await.unwrap(), data);
        let stats = source.stats();
        assert!(
            stats.head_requests > 0 && stats.range_get_requests > 0 && stats.full_get_requests > 0
        );
        host.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_owner_and_publication_crash_never_infer_completion() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("arkama.sqlite");
    let path = dir.path().join("published.bin");
    let data = vec![8, 31, 2, 99, 0, 23, 1];
    let source = FlakyServer::spawn(FlakyServerConfig::new(data.clone())).await;
    let db = Db::open_path(&db_path).unwrap();
    let id = db
        .insert_daemon_download(&DownloadRequest::new(source.url()).output(&path))
        .unwrap();
    db.transition_download(id, "running").unwrap();
    tokio::fs::write(&path, &data).await.unwrap(); // Publication succeeded, DB terminal persistence did not.
    drop(db);
    let manager = Manager::open(&db_path, Handle::current()).await.unwrap();
    assert_eq!(
        Manager::open(&db_path, Handle::current())
            .await
            .err()
            .unwrap()
            .code,
        "conflict"
    );
    let host = Host::Local(manager.clone());
    tokio::time::timeout(Duration::from_secs(5), async {
        while host.job(id).await.record.status != "failed" {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(tokio::fs::read(path).await.unwrap(), data);
    manager.shutdown().await.unwrap();
    assert_eq!(
        manager.execute(Command::Status).await.unwrap_err().code,
        "stopped"
    );
    let reopened = Manager::open(db_path, Handle::current()).await.unwrap();
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abrupt_daemon_restart_reuses_only_verified_checkpoint_bytes() {
    let data: Vec<u8> = (0..262_151)
        .map(|i| ((i * 17 + i / 257) % 256) as u8)
        .collect();
    let mut config = FlakyServerConfig::new(data.clone());
    config.chunk_size = 1024;
    config.chunk_delay = Duration::from_millis(8);
    let source = FlakyServer::spawn(config).await;
    let dir = tempfile::tempdir().unwrap();
    let mut host = Host::open(dir.path(), true, &source.url()).await;
    let path = dir.path().join("out/crash.bin");
    let id = host
        .enqueue(
            DownloadRequest::new(source.url())
                .output(&path)
                .connections(1),
        )
        .await;
    host.wait(id, |job| {
        job.observation
            .as_ref()
            .is_some_and(|snapshot| snapshot.crash_safe_bytes > 4096)
    })
    .await;
    let Host::Server { child, .. } = &mut host else {
        unreachable!()
    };
    child.kill().await.unwrap();
    tokio::fs::remove_file(dir.path().join("state/daemon.addr"))
        .await
        .unwrap();
    host = Host::open(dir.path(), true, &source.url()).await;
    host.wait(id, |job| job.record.status == "finished").await;
    assert_eq!(tokio::fs::read(path).await.unwrap(), data);
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_reports_failed_persistence_and_releases_joined_workers() {
    let data: Vec<u8> = (0..65_539)
        .map(|i| ((i * 17 + i / 127) % 256) as u8)
        .collect();
    let mut config = FlakyServerConfig::new(data.clone());
    config.chunk_size = 1024;
    config.chunk_delay = Duration::from_millis(8);
    // Hold the first attempt open while SQLite acquires its schema lock, even on slow CI hosts.
    config.stall_gets = 1;
    config.stall_after_bytes = 16_384;
    config.stall_duration = Duration::from_secs(60);
    let source = FlakyServer::spawn(config.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("state/arkama.sqlite");
    let path = dir.path().join("persist.bin");
    let manager = Manager::open(&db_path, Handle::current()).await.unwrap();
    let host = Host::Local(manager.clone());
    let id = host
        .enqueue(
            DownloadRequest::new(source.url())
                .output(&path)
                .connections(1),
        )
        .await;
    host.wait(id, |job| job.record.downloaded_bytes > 8192)
        .await;
    // Fault injection into an isolated real store, never a supported concurrent editor.
    let connection = rusqlite::Connection::open(&db_path).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_status BEFORE UPDATE OF status ON downloads BEGIN SELECT RAISE(FAIL, 'injected storage failure'); END").unwrap();
    assert_eq!(manager.shutdown().await.unwrap_err().code, "storage");
    assert_eq!(
        manager.execute(Command::Status).await.unwrap_err().code,
        "stopped"
    );
    let retained = tokio::fs::read(partial(&path)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(tokio::fs::read(partial(&path)).await.unwrap(), retained);
    assert!(!path.exists());
    connection
        .execute_batch("DROP TRIGGER reject_status")
        .unwrap();
    drop(connection);
    config.stall_gets = 0;
    source.configure(config);
    let mut reopened = Host::open(dir.path(), false, &source.url()).await;
    reopened
        .wait(id, |job| job.record.status == "finished")
        .await;
    assert_eq!(tokio::fs::read(path).await.unwrap(), data);
    reopened.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queues_rules_batches_and_limits_match_across_hosts_and_restart() {
    use arkama_manager::{BatchResult, Collision, DownloadWindow, FolderRule, Queue, Submission};
    let data: Vec<u8> = (0..262_151)
        .map(|i| ((i * 23 + i / 241) % 256) as u8)
        .collect();
    let mut config = FlakyServerConfig::new(data.clone());
    config.chunk_size = 1024;
    config.chunk_delay = Duration::from_millis(8);
    let source = FlakyServer::spawn(config.clone()).await;
    for server in [false, true] {
        source.configure(config.clone());
        let dir = tempfile::tempdir().unwrap();
        let mut host = Host::open(dir.path(), server, &source.url()).await;
        host.execute(Command::SetSetting {
            key: "max_concurrent".into(),
            value: "1".into(),
        })
        .await
        .unwrap();
        let mut default = Queue::new("default");
        default.paused = true;
        let mut high = Queue::new("high");
        high.paused = true;
        high.max_active = 1;
        high.priority = 4;
        let mut low = Queue::new("low");
        low.paused = true;
        low.max_active = 1;
        low.priority = -2;
        for queue in [&default, &high, &low] {
            host.execute(Command::PutQueue {
                queue: queue.clone(),
            })
            .await
            .unwrap();
        }
        let make = |name: &str, queue: &str, priority: i64| {
            let mut submission = Submission::from(
                DownloadRequest::new(source.url())
                    .output(dir.path().join(format!("out/{name}.bin")))
                    .connections(2),
            );
            submission.queue = queue.into();
            submission.priority = priority;
            submission
        };
        let a = make("a", "high", 0);
        let mut unique = a.clone();
        unique.collision = Collision::Unique;
        let Response::Batch { results } = host
            .execute(Command::Batch {
                submissions: vec![
                    a.clone(),
                    make("b", "high", 0),
                    make("c", "low", 100),
                    make("invalid", "missing", 0),
                    a,
                    unique,
                ],
            })
            .await
            .unwrap()
        else {
            panic!("batch response");
        };
        let accepted = |i: usize| match &results[i] {
            BatchResult::Accepted { id } => *id,
            other => panic!("{other:?}"),
        };
        let (a, b, c, d) = (accepted(0), accepted(1), accepted(2), accepted(5));
        assert!(matches!(&results[3], BatchResult::Rejected { error } if error.code == "invalid"));
        assert!(matches!(&results[4], BatchResult::Rejected { error } if error.code == "conflict"));
        assert_eq!(
            host.job(d).await.record.output_path,
            dir.path()
                .join("out/a.bin.1")
                .canonicalize()
                .unwrap_or_else(|_| dir
                    .path()
                    .join("out")
                    .canonicalize()
                    .unwrap()
                    .join("a.bin.1"))
                .to_string_lossy()
        );
        assert!(
            host.execute(Command::DeleteQueue {
                name: "high".into()
            })
            .await
            .is_err()
        );
        host.execute(Command::MoveJob {
            id: b,
            queue: "high".into(),
            priority: 5,
            position: -10,
        })
        .await
        .unwrap();
        high.paused = false;
        host.execute(Command::PutQueue {
            queue: high.clone(),
        })
        .await
        .unwrap();
        host.wait(b, |job| job.record.downloaded_bytes > 4096).await;
        assert_eq!(host.job(a).await.record.status, "queued");
        assert_eq!(
            host.job(c).await.record.status,
            "queued",
            "job priority bypassed queue priority"
        );
        assert!(
            host.execute(Command::MoveJob {
                id: b,
                queue: "low".into(),
                priority: 0,
                position: 0
            })
            .await
            .is_err()
        );
        host.execute(Command::SetSetting {
            key: "max_concurrent".into(),
            value: "2".into(),
        })
        .await
        .unwrap();
        low.paused = false;
        host.execute(Command::PutQueue { queue: low.clone() })
            .await
            .unwrap();
        host.wait(c, |job| job.record.downloaded_bytes > 4096).await;
        assert_eq!(
            host.job(a).await.record.status,
            "queued",
            "per-queue active limit ignored"
        );
        high.paused = true;
        host.execute(Command::PutQueue {
            queue: high.clone(),
        })
        .await
        .unwrap();
        let closed = DownloadWindow {
            timezone: "UTC".into(),
            weekdays: vec![0, 1, 2, 3, 4, 5, 6],
            start_minute: ((chrono_minute() + 10) % 1440),
            end_minute: ((chrono_minute() + 11) % 1440),
        };
        low.window = Some(closed);
        host.execute(Command::PutQueue { queue: low.clone() })
            .await
            .unwrap();
        host.wait(b, |job| job.record.status == "finished").await;
        host.wait(c, |job| job.record.status == "finished").await;
        assert_eq!(std::fs::read(dir.path().join("out/b.bin")).unwrap(), data);
        assert_eq!(std::fs::read(dir.path().join("out/c.bin")).unwrap(), data);
        let Response::Accepted { id: scheduled } = host
            .execute(Command::Submit {
                submission: make("scheduled", "low", 0),
            })
            .await
            .unwrap()
        else {
            panic!("acceptance");
        };
        let rules = vec![
            FolderRule {
                category: Some("docs".into()),
                source_host: Some("127.0.0.1".into()),
                directory: dir.path().join("out/docs"),
            },
            FolderRule {
                category: None,
                source_host: Some("127.0.0.1".into()),
                directory: dir.path().join("out/source"),
            },
        ];
        host.execute(Command::SetRules { rules }).await.unwrap();
        host.execute(Command::SetSetting {
            key: "download_dir".into(),
            value: dir
                .path()
                .join("out/default")
                .to_string_lossy()
                .into_owned(),
        })
        .await
        .unwrap();
        let mut inferred = Submission::from(DownloadRequest::new(source.url()));
        inferred.category = Some("docs".into());
        let Response::Accepted { id: document } = host
            .execute(Command::Submit {
                submission: inferred.clone(),
            })
            .await
            .unwrap()
        else {
            panic!("document acceptance");
        };
        assert!(Path::new(&host.job(document).await.record.output_path).ends_with("docs/file.bin"));
        inferred.request.output_dir = Some(dir.path().join("out/explicit"));
        let Response::Accepted { id: explicit } = host
            .execute(Command::Submit {
                submission: inferred,
            })
            .await
            .unwrap()
        else {
            panic!("explicit acceptance");
        };
        assert!(
            Path::new(&host.job(explicit).await.record.output_path).ends_with("explicit/file.bin")
        );
        let Response::Accepted { id: by_source } = host
            .execute(Command::Enqueue {
                request: DownloadRequest::new(source.url()),
            })
            .await
            .unwrap()
        else {
            panic!("source acceptance");
        };
        assert!(
            Path::new(&host.job(by_source).await.record.output_path).ends_with("source/file.bin")
        );
        let Response::Accepted { id: fallback } = host
            .execute(Command::Enqueue {
                request: DownloadRequest::new("https://example.com/fallback.bin"),
            })
            .await
            .unwrap()
        else {
            panic!("default folder acceptance");
        };
        assert!(
            Path::new(&host.job(fallback).await.record.output_path)
                .ends_with("default/fallback.bin")
        );
        host.execute(Command::Cancel { id: fallback })
            .await
            .unwrap(); // Never fetch an external fixture.
        assert!(
            host.execute(Command::Submit {
                submission: make("c", "default", 0)
            })
            .await
            .is_err()
        );
        let mut collision = make("c", "default", 0);
        collision.collision = Collision::Unique;
        let Response::Accepted { id: collision } = host
            .execute(Command::Submit {
                submission: collision,
            })
            .await
            .unwrap()
        else {
            panic!("unique file acceptance");
        };
        assert!(Path::new(&host.job(collision).await.record.output_path).ends_with("c.bin.1"));
        host.execute(Command::Cancel { id: collision })
            .await
            .unwrap();
        assert_eq!(std::fs::read(dir.path().join("out/c.bin")).unwrap(), data);
        if server {
            assert!(
                host.execute(Command::SetRules {
                    rules: vec![FolderRule {
                        category: Some("unsafe".into()),
                        source_host: None,
                        directory: dir.path().join("state")
                    }]
                })
                .await
                .is_err()
            );
        }
        host.execute(Command::SetSetting {
            key: "max_host_connections".into(),
            value: "1".into(),
        })
        .await
        .unwrap();
        assert!(
            host.execute(Command::SetSetting {
                key: "global_speed_limit".into(),
                value: "0".into()
            })
            .await
            .is_err()
        );
        host.execute(Command::SetSetting {
            key: "global_speed_limit".into(),
            value: "524288".into(),
        })
        .await
        .unwrap();
        host.stop().await;
        host = Host::open(dir.path(), server, &source.url()).await;
        for id in [a, d, scheduled, document, explicit, by_source] {
            assert_eq!(host.job(id).await.record.status, "queued");
        }
        assert_eq!(host.job(b).await.record.status, "finished");
        let Response::Queues { queues } = host.execute(Command::Queues).await.unwrap() else {
            panic!("queues");
        };
        assert!(
            queues
                .iter()
                .any(|queue| queue.name == "high" && queue.paused)
        );
        assert!(
            queues
                .iter()
                .any(|queue| queue.name == "low" && queue.window.is_some())
        );
        let Response::Rules { rules } = host.execute(Command::Rules).await.unwrap() else {
            panic!("rules");
        };
        assert_eq!(rules.len(), 2);
        let Response::Setting { value, .. } = host
            .execute(Command::GetSetting {
                key: "max_host_connections".into(),
            })
            .await
            .unwrap()
        else {
            panic!("limits");
        };
        assert_eq!(value.as_deref(), Some("1"));
        high.paused = false;
        high.newest_first = true;
        host.execute(Command::MoveJob {
            id: d,
            queue: "high".into(),
            priority: 0,
            position: 100,
        })
        .await
        .unwrap();
        host.execute(Command::PutQueue { queue: high })
            .await
            .unwrap();
        host.wait(d, |job| job.record.downloaded_bytes > 4096).await;
        assert_eq!(
            host.job(a).await.record.status,
            "queued",
            "newest-first/custom order ignored"
        );
        host.execute(Command::Cancel { id: d }).await.unwrap();
        host.wait(a, |job| job.record.status == "finished").await;
        assert_eq!(std::fs::read(dir.path().join("out/a.bin")).unwrap(), data);
        low.window = None;
        host.execute(Command::PutQueue { queue: low })
            .await
            .unwrap();
        host.wait(scheduled, |job| job.record.status == "finished")
            .await;
        assert_eq!(
            std::fs::read(dir.path().join("out/scheduled.bin")).unwrap(),
            data
        );
        // Start three ordinary files under one aggregate cap, not one cap per job.
        source.configure(FlakyServerConfig::new(data.clone()));
        host.execute(Command::SetSetting {
            key: "max_host_connections".into(),
            value: "8".into(),
        })
        .await
        .unwrap();
        let started = std::time::Instant::now();
        default.paused = false;
        host.execute(Command::PutQueue { queue: default })
            .await
            .unwrap();
        for id in [document, explicit, by_source] {
            host.wait(id, |job| job.record.status == "finished").await;
            assert_eq!(
                std::fs::read(host.job(id).await.record.output_path).unwrap(),
                data
            );
        }
        assert!(
            started.elapsed() >= Duration::from_millis(1400),
            "application cap was not shared"
        );
        host.stop().await;
    }
}

fn chrono_minute() -> u32 {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    ((seconds / 60) % 1440) as u32
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protected_handoffs_are_durable_unique_and_reconnectable() {
    use arkama_core::RequestContext;
    use arkama_manager::{Queue, Submission};
    let data: Vec<u8> = (0..262_147)
        .map(|i| ((i * 41 + i / 257) % 256) as u8)
        .collect();
    let mut config = FlakyServerConfig::new(data.clone());
    config.chunk_size = 1024;
    config.chunk_delay = Duration::from_millis(3);
    config.required_headers = vec![
        ("cookie".into(), "session=cookie-secret".into()),
        ("x-token".into(), "header-secret".into()),
        (
            "referer".into(),
            "https://example.com/page?token=referrer-secret".into(),
        ),
    ];
    let source = FlakyServer::spawn(config).await;
    for server in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut host = Host::open(dir.path(), server, &source.url()).await;
        let mut queue = Queue::new("browser");
        queue.paused = true;
        host.execute(Command::PutQueue {
            queue: queue.clone(),
        })
        .await
        .unwrap();
        let context = RequestContext {
            headers: [("x-token".into(), "header-secret".into())].into(),
            cookies: Some("session=cookie-secret".into()),
            referrer: Some("https://example.com/page?token=referrer-secret".into()),
            filename: Some("chosen.bin".into()),
            source: Some("https://example.com/source?token=source-secret".into()),
            ..Default::default()
        };
        let mut submission = Submission::from(
            DownloadRequest::new(format!("{}?token=query-secret", source.url()))
                .output_dir(dir.path().join("out"))
                .context(context),
        );
        submission.queue = "browser".into();
        submission.handoff_id = Some("device_A-request_1".into());
        let command = Command::Submit {
            submission: submission.clone(),
        };
        let (a, b, c) = tokio::join!(
            host.execute(command.clone()),
            host.execute(command.clone()),
            host.execute(command.clone())
        );
        let Response::Accepted { id } = a.unwrap() else {
            panic!("missing acceptance")
        };
        for reply in [b.unwrap(), c.unwrap()] {
            assert!(matches!(reply, Response::Accepted { id: other } if other == id));
        }
        let mut changed = submission.clone();
        changed.request.context.cookies = Some("session=different".into());
        assert!(
            host.execute(Command::Submit {
                submission: changed
            })
            .await
            .unwrap_err()
            .contains("conflict")
        );
        let mut unsupported = submission.clone();
        unsupported.handoff_id = Some("unsupported".into());
        unsupported.request.context.method = "POST".into();
        assert!(
            host.execute(Command::Submit {
                submission: unsupported
            })
            .await
            .unwrap_err()
            .contains("GET")
        );
        let db_path = dir.path().join("state/arkama.sqlite");
        let secrets = [
            "cookie-secret",
            "header-secret",
            "referrer-secret",
            "source-secret",
            "query-secret",
        ];
        let db = Db::open_path(&db_path).unwrap();
        assert_eq!(db.load_downloads("").unwrap().len(), 1);
        assert_eq!(
            db.load_daemon_request(id).unwrap().unwrap().context.cookies,
            submission.request.context.cookies
        );
        for secret in secrets {
            assert!(!String::from_utf8_lossy(&std::fs::read(&db_path).unwrap()).contains(secret));
        }
        drop(db);
        assert!(host.execute(Command::ForgetRequest { id }).await.is_err());
        host.stop().await;
        host = Host::open(dir.path(), server, &source.url()).await;
        assert!(
            matches!(host.execute(command.clone()).await.unwrap(), Response::Accepted { id: other } if other == id)
        );
        queue.paused = false;
        host.execute(Command::PutQueue { queue }).await.unwrap();
        host.wait(id, |job| job.record.downloaded_bytes > 8192)
            .await;
        host.execute(Command::Pause { id }).await.unwrap();
        let path = dir.path().join("out/chosen.bin");
        let checkpoint =
            std::fs::read_to_string(format!("{}.arkama.state", partial(&path).display())).unwrap();
        let record = format!("{:?}", host.job(id).await);
        for secret in secrets {
            assert!(!checkpoint.contains(secret));
            assert!(!record.contains(secret));
        }
        let Response::Diagnostics { .. } = host.execute(Command::Diagnostics { id }).await.unwrap()
        else {
            panic!("no diagnostic reply")
        };
        host.stop().await;
        host = Host::open(dir.path(), server, &source.url()).await;
        assert!(
            matches!(host.execute(command.clone()).await.unwrap(), Response::Accepted { id: other } if other == id)
        );
        host.execute(Command::Resume { id }).await.unwrap();
        host.wait(id, |job| job.record.status == "finished").await;
        assert_eq!(std::fs::read(&path).unwrap(), data);
        assert!(
            Db::open_path(&db_path)
                .unwrap()
                .load_daemon_request(id)
                .unwrap()
                .is_none()
        );
        assert!(
            matches!(host.execute(command).await.unwrap(), Response::Accepted { id: other } if other == id)
        );

        // Dropping completed replay credentials must not drop the output reservation.
        std::fs::rename(&path, path.with_extension("moved")).unwrap();
        host.stop().await;
        host = Host::open(dir.path(), server, &source.url()).await;
        let mut repeated = submission.clone();
        repeated.handoff_id = Some("another-request".into());
        assert!(
            host.execute(Command::Submit {
                submission: repeated
            })
            .await
            .unwrap_err()
            .contains("reserved")
        );

        let mut failed = submission.clone();
        failed.handoff_id = Some("device_A-failed".into());
        failed.request.context.cookies = Some("session=expired".into());
        failed.request.output = Some(dir.path().join("out/failed.bin"));
        let Response::Accepted { id: failed_id } = host
            .execute(Command::Submit { submission: failed })
            .await
            .unwrap()
        else {
            panic!("no failed-job acceptance")
        };
        let job = host
            .wait(failed_id, |job| job.record.status == "failed")
            .await;
        assert!(job.record.error_message.unwrap().contains("403"));
        assert!(
            Db::open_path(&db_path)
                .unwrap()
                .load_daemon_request(failed_id)
                .unwrap()
                .is_some()
        );
        host.execute(Command::ForgetRequest { id: failed_id })
            .await
            .unwrap();
        assert!(
            host.execute(Command::Retry { id: failed_id })
                .await
                .unwrap_err()
                .contains("no managed request")
        );
        let mut cancelled = submission;
        cancelled.handoff_id = Some("device_A-cancelled".into());
        cancelled.request.output = Some(dir.path().join("out/cancelled.bin"));
        let Response::Accepted { id: cancelled_id } = host
            .execute(Command::Submit {
                submission: cancelled,
            })
            .await
            .unwrap()
        else {
            panic!("no cancellation acceptance")
        };
        host.execute(Command::Cancel { id: cancelled_id })
            .await
            .unwrap();
        host.execute(Command::ForgetRequest { id: cancelled_id })
            .await
            .unwrap();
        assert!(
            Db::open_path(&db_path)
                .unwrap()
                .load_daemon_request(cancelled_id)
                .unwrap()
                .is_none()
        );
        let Response::Jobs { jobs, .. } = host
            .execute(Command::List {
                limit: 20,
                offset: 0,
                query: "".into(),
            })
            .await
            .unwrap()
        else {
            panic!("no history")
        };
        let history = serde_json::to_string(&jobs).unwrap();
        for secret in secrets {
            assert!(!history.contains(secret));
        }
        host.stop().await;
    }
}
