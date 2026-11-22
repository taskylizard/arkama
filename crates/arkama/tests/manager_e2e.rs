//! The same lifecycle scenarios through a local manager and the actual daemon process.

use std::path::{Path, PathBuf};
use std::time::Duration;

use arkama_core::DownloadRequest;
use arkama_data::Db;
use arkama_manager::{Command, JobSnapshot, Manager, Response};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::Child;
use tokio::runtime::Handle;

#[path = "../../arkama_core/tests/support/mod.rs"]
mod support;
use support::{FlakyServer, FlakyServerConfig};

enum Host {
    Local(Manager),
    Server { child: Child, addr: String },
}

impl Host {
    async fn open(dir: &Path, server: bool) -> Self {
        if !server {
            return Self::Local(
                Manager::open(dir.join("arkama.sqlite"), Handle::current())
                    .await
                    .unwrap(),
            );
        }
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_arkama"))
            .args(["daemon", "run"])
            .env("ARKAMA_DATA_DIR", dir)
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
                match tokio::fs::read_to_string(dir.join("daemon.addr")).await {
                    Ok(addr) if TcpStream::connect(&addr).await.is_ok() => break addr,
                    _ => {}
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        Self::Server { child, addr }
    }

    async fn execute(&self, command: Command) -> Result<Response, String> {
        match self {
            Self::Local(manager) => manager
                .execute(command)
                .await
                .map_err(|error| error.to_string()),
            Self::Server { addr, .. } => {
                let response =
                    exchange(addr, serde_json::json!({"Manage": {"command": command}})).await;
                if let Some(error) = response.get("Error") {
                    return Err(error.to_string());
                }
                Ok(serde_json::from_value(response["Managed"].clone()).unwrap())
            }
        }
    }

    async fn stop(&mut self) {
        match self {
            Self::Local(manager) => manager.shutdown().await.unwrap(),
            Self::Server { child, addr } => {
                assert_eq!(
                    exchange(addr, serde_json::json!("Shutdown")).await,
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
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
}

async fn exchange(addr: &str, value: serde_json::Value) -> serde_json::Value {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(format!("{value}\n").as_bytes())
        .await
        .unwrap();
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).await.unwrap();
    serde_json::from_str(&line).unwrap()
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
        let mut host = Host::open(dir.path(), server).await;
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
        let path = dir.path().join("complete.bin");
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
        let alias = dir.path().join("COMPLETE.bin");
        let alias_lock = dir.path().join("COMPLETE.bin.arkama.lock");
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
        host = Host::open(dir.path(), server).await;
        assert_eq!(host.job(id).await.record.status, "paused");
        host.execute(Command::Resume { id }).await.unwrap();
        let finished = host.wait(id, |job| job.record.status == "finished").await;
        assert_eq!(finished.record.downloaded_bytes, data.len() as i64);
        assert_eq!(tokio::fs::read(&path).await.unwrap(), data);
        assert!(host.execute(Command::Cancel { id }).await.is_err());
        assert_eq!(tokio::fs::read(&path).await.unwrap(), data);

        let second_path = dir.path().join("retry.bin");
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
                    .output(dir.path().join("cancel-blocker.bin"))
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
        assert!(!partial(&dir.path().join("cancel-blocker.bin")).exists());
        host.execute(Command::Retry { id: second }).await.unwrap();
        host.wait(second, |job| job.record.status == "finished")
            .await;
        assert_eq!(tokio::fs::read(&second_path).await.unwrap(), data);

        let blocker = host
            .enqueue(
                DownloadRequest::new(source.url())
                    .output(dir.path().join("blocker.bin"))
                    .connections(1),
            )
            .await;
        host.wait(blocker, |job| job.record.downloaded_bytes > 8192)
            .await;
        let queued = host
            .enqueue(DownloadRequest::new(source.url()).output(dir.path().join("queued.bin")))
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
        host = Host::open(dir.path(), server).await;
        assert_eq!(host.job(queued).await.record.status, "paused");
        host.execute(Command::Cancel { id: blocker }).await.unwrap();
        host.execute(Command::Cancel { id: queued }).await.unwrap();
        assert_eq!(host.job(id).await.record.status, "finished");
        let mut full_stream = FlakyServerConfig::new(data.clone());
        full_stream.accept_ranges = false;
        source.configure(full_stream);
        let stream_path = dir.path().join("full.bin");
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
    let mut host = Host::open(dir.path(), true).await;
    let path = dir.path().join("crash.bin");
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
    tokio::fs::remove_file(dir.path().join("daemon.addr"))
        .await
        .unwrap();
    host = Host::open(dir.path(), true).await;
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
    let source = FlakyServer::spawn(config).await;
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("arkama.sqlite");
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
    let mut reopened = Host::open(dir.path(), false).await;
    reopened
        .wait(id, |job| job.record.status == "finished")
        .await;
    assert_eq!(tokio::fs::read(path).await.unwrap(), data);
    reopened.stop().await;
}
