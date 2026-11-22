use std::path::{Path, PathBuf};
use std::time::Duration;

use arkama_core::{DownloadEvent, DownloadRequest, download, start_download};
use tokio::sync::mpsc;
use tokio::time::timeout;

mod support;

use support::{FlakyServer, FlakyServerConfig};

fn request(url: String, path: PathBuf, connections: usize) -> DownloadRequest {
    DownloadRequest {
        url,
        output: Some(path),
        output_dir: None,
        connections,
        user_agent: None,
        limit: None,
        experimental_entropy: false,
        recovery: Default::default(),
    }
}

fn state_path(path: &Path) -> PathBuf {
    PathBuf::from(format!(
        "{}.arkama.part.arkama.state",
        path.to_string_lossy()
    ))
}

fn partial_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.arkama.part", path.to_string_lossy()))
}

async fn wait_for_progress(events: &mut mpsc::Receiver<DownloadEvent>) -> u64 {
    loop {
        let next = timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("progress event timeout");
        let event = next.expect("download event");
        match event {
            DownloadEvent::Metadata { .. } => {}
            DownloadEvent::Started { resumed_bytes, .. } => {
                if resumed_bytes > 0 {
                    return resumed_bytes;
                }
            }
            DownloadEvent::Progress {
                downloaded_bytes, ..
            } => {
                if downloaded_bytes > 0 {
                    return downloaded_bytes;
                }
            }
            DownloadEvent::Finished { .. } => {
                panic!("download finished before progress was observed");
            }
            DownloadEvent::Failed { message } => {
                panic!("download failed before progress was observed: {message}");
            }
        }
    }
}

async fn assert_downloaded(path: &PathBuf, data: &[u8]) {
    let saved = tokio::fs::read(path).await.expect("read downloaded file");
    assert_eq!(saved, data);
}

fn asymmetric_data(length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| ((index * 31 + index / 251 + index / 65537) % 256) as u8)
        .collect()
}

fn quick_recovery() -> arkama_core::RecoveryPolicy {
    arkama_core::RecoveryPolicy {
        base_delay_ms: 5,
        jitter_ms: 0,
        ..Default::default()
    }
}

#[tokio::test]
async fn recovery_retries_temporary_statuses_not_permanent_failures() {
    let data = asymmetric_data(16_387);
    for status in [401, 403, 404, 416, 501, 505, 408, 429, 500, 502, 503, 504] {
        let mut config = FlakyServerConfig::new(data.clone());
        config.status = status;
        config.status_gets = 1;
        let server = FlakyServer::spawn(config).await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("status.bin");
        let handle =
            start_download(request(server.url(), path.clone(), 1).recovery(quick_recovery()))
                .unwrap();
        let observation = handle.observation;
        let result = timeout(Duration::from_secs(3), handle.join)
            .await
            .unwrap()
            .unwrap();
        let temporary = matches!(status, 408 | 429 | 500 | 502 | 503 | 504);
        if temporary {
            result.unwrap();
            assert_downloaded(&path, &data).await;
            assert_eq!(observation.snapshot().retries, 1);
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains(&status.to_string())
            );
            assert!(!path.exists());
            assert_eq!(observation.snapshot().retries, 0);
        }
        assert_eq!(
            server.stats().range_get_requests,
            if temporary { 2 } else { 1 }
        );
    }
    let mut config = FlakyServerConfig::new(data.clone());
    config.status_heads = 2;
    let server = FlakyServer::spawn(config).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("probe.bin");
    download(request(server.url(), path.clone(), 1).recovery(quick_recovery()))
        .await
        .unwrap();
    assert_downloaded(&path, &data).await;
    assert_eq!(server.stats().head_requests, 3);
}

#[tokio::test]
async fn recovery_honors_retry_after_and_refuses_an_excessive_wait() {
    let data = asymmetric_data(16_387);
    for excessive in [false, true] {
        let mut config = FlakyServerConfig::new(data.clone());
        config.status = 429;
        config.status_gets = 1;
        config.retry_after = Some("1".into());
        let server = FlakyServer::spawn(config).await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("throttle.bin");
        let mut policy = quick_recovery();
        policy.max_delay_ms = if excessive { 50 } else { 2000 };
        let started = std::time::Instant::now();
        let handle =
            start_download(request(server.url(), path.clone(), 1).recovery(policy)).unwrap();
        let observation = handle.observation;
        let result = timeout(Duration::from_secs(4), handle.join)
            .await
            .unwrap()
            .unwrap();
        if excessive {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("Retry-After exceeds")
            );
            assert_eq!(server.stats().range_get_requests, 1);
            assert!(!path.exists());
        } else {
            result.unwrap();
            assert!(
                started.elapsed() >= Duration::from_secs(1),
                "must not retry before the server's minimum wait"
            );
            assert_eq!(observation.snapshot().last_retry_delay_ms, 1000);
            assert_eq!(
                observation.snapshot().last_recovery_reason,
                Some("server throttling".into())
            );
            assert_downloaded(&path, &data).await;
        }
    }
}

#[tokio::test]
async fn recovery_preserves_correct_bytes_after_disconnects_and_idle_reads() {
    let data = asymmetric_data(16_387);
    for ranges in [false, true] {
        for stall in [false, true] {
            let mut config = FlakyServerConfig::new(data.clone());
            config.accept_ranges = ranges;
            config.chunk_size = 4096;
            if stall {
                config.stall_gets = 1;
            } else {
                config.interrupt_range_gets = 1;
                config.interrupt_full_gets = 1;
                config.interrupt_after_bytes = 4096;
            }
            let server = FlakyServer::spawn(config).await;
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("recover.bin");
            let mut policy = quick_recovery();
            policy.read_idle_timeout_ms = 30;
            let handle =
                start_download(request(server.url(), path.clone(), 1).recovery(policy)).unwrap();
            let observation = handle.observation;
            timeout(Duration::from_secs(3), handle.join)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_downloaded(&path, &data).await;
            assert_eq!(observation.snapshot().retries, 1);
            assert_eq!(observation.snapshot().network_stalls, u64::from(stall));
        }
    }
}

#[tokio::test]
async fn recovery_deadlines_are_bounded_and_exclude_intentional_throttling() {
    let data = asymmetric_data(8195);
    let mut config = FlakyServerConfig::new(data.clone());
    config.header_delay = Duration::from_secs(1);
    let server = FlakyServer::spawn(config).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("deadline.bin");
    let mut policy = quick_recovery();
    policy.setup_timeout_ms = 30;
    policy.max_retries = 1;
    let result = timeout(
        Duration::from_secs(1),
        download(request(server.url(), path.clone(), 1).recovery(policy)),
    )
    .await
    .unwrap();
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("deadline") && message.contains("retry limit"),
        "{message}"
    );
    assert_eq!(server.stats().head_requests, 2);
    assert!(!partial_path(&path).exists());

    let mut config = FlakyServerConfig::new(data.clone());
    config.chunk_size = 4096;
    let server = FlakyServer::spawn(config).await;
    let mut policy = quick_recovery();
    policy.read_idle_timeout_ms = 20;
    let handle = start_download(
        request(server.url(), path.clone(), 1)
            .speed_limit(8192)
            .recovery(policy),
    )
    .unwrap();
    let observation = handle.observation;
    timeout(Duration::from_secs(3), handle.join)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_downloaded(&path, &data).await;
    assert_eq!(observation.snapshot().retries, 0);
    assert!(observation.snapshot().durations_ms["throttle_wait"] > 100);

    let mut invalid = request(server.url(), dir.path().join("invalid.bin"), 1);
    invalid.recovery.connect_timeout_ms = 0;
    assert!(matches!(
        start_download(invalid),
        Err(arkama_core::Error::InvalidRequest { .. })
    ));
}

#[tokio::test]
async fn recovery_sessions_share_host_budgets_not_other_origins() {
    let mut config = FlakyServerConfig::new(asymmetric_data(8195));
    config.status_heads = usize::MAX;
    let server = FlakyServer::spawn(config.clone()).await;
    let other = FlakyServer::spawn(config).await;
    let dir = tempfile::tempdir().unwrap();
    let session = arkama_core::DownloadSession::default();
    let mut policy = quick_recovery();
    policy.max_host_retries = 1;
    let first = session
        .start(request(server.url(), dir.path().join("first.bin"), 1).recovery(policy.clone()))
        .unwrap();
    let second = session
        .clone()
        .start(request(server.url(), dir.path().join("second.bin"), 1).recovery(policy.clone()))
        .unwrap();
    let (first, second) = tokio::join!(first.join, second.join);
    let errors = [
        first.unwrap().unwrap_err().to_string(),
        second.unwrap().unwrap_err().to_string(),
    ];
    assert!(
        errors
            .iter()
            .all(|message| message.contains("host retry budget exhausted"))
    );
    assert_eq!(
        server.stats().head_requests,
        3,
        "two initial requests and one shared retry"
    );
    let result = session
        .start(request(other.url(), dir.path().join("other.bin"), 1).recovery(policy.clone()))
        .unwrap()
        .join
        .await
        .unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("host retry budget exhausted")
    );
    assert_eq!(other.stats().head_requests, 2);
    policy.host_window_ms = 1;
    policy.max_retries = 1;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let result = session
        .start(request(server.url(), dir.path().join("later.bin"), 1).recovery(policy))
        .unwrap()
        .join
        .await
        .unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("retry limit exhausted")
    );
    assert_eq!(
        server.stats().head_requests,
        5,
        "expired debt allows one new retry"
    );

    let mut config = FlakyServerConfig::new(asymmetric_data(16_387));
    config.interrupt_range_gets = 100;
    config.interrupt_after_bytes = 4096;
    let target = FlakyServer::spawn(config.clone()).await;
    config.redirect = Some(target.url());
    let source = FlakyServer::spawn(config.clone()).await;
    let alias = FlakyServer::spawn(config).await;
    let mut policy = quick_recovery();
    policy.max_host_retries = 1;
    let first = session
        .start(request(source.url(), dir.path().join("redirect.bin"), 1).recovery(policy.clone()))
        .unwrap();
    let second = session
        .start(request(alias.url(), dir.path().join("alias.bin"), 1).recovery(policy))
        .unwrap();
    let (first, second) = tokio::join!(first.join, second.join);
    for result in [first, second] {
        assert!(
            result
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("host retry budget exhausted")
        );
    }
    assert_eq!(
        target.stats().range_get_requests,
        3,
        "redirect aliases must share the final origin's body retry budget"
    );
}

#[tokio::test]
async fn existing_preallocated_file_is_not_completion_evidence() {
    let data = asymmetric_data(262_147);
    let server = FlakyServer::spawn(FlakyServerConfig::new(data.clone())).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("preallocated.bin");
    let file = std::fs::File::create(&path).unwrap();
    file.set_len(data.len() as u64).unwrap();
    let before = tokio::fs::read(&path).await.unwrap();

    let result = download(request(server.url(), path.clone(), 4)).await;
    assert!(
        result.is_err(),
        "unverified existing output must not report success"
    );
    assert_downloaded(&path, &before).await;
}

#[tokio::test]
async fn invalid_range_is_not_published() {
    let data = asymmetric_data(262_147);
    for header in [
        "bytes 1-65537/262147",
        "bytes 0-65534/262147",
        "bytes 0-65535/262148",
        "bytes 0-65535/*",
        "bytes nonsense",
    ] {
        let mut config = FlakyServerConfig::new(data.clone());
        config.range_header = Some(header.into());
        let server = FlakyServer::spawn(config).await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("invalid.bin");
        let result = download(request(server.url(), path.clone(), 4)).await;
        assert!(result.is_err(), "invalid range must be rejected: {header}");
        assert!(
            !path.exists(),
            "invalid transfer must not publish final output"
        );
    }
}

#[tokio::test]
async fn worker_failure_joins_other_writers_before_returning() {
    let data = asymmetric_data(524_291);
    let mut config = FlakyServerConfig::new(data.clone());
    config.range_header = Some("bytes 1-131071/524291".into());
    config.range_header_start = Some(0);
    config.chunk_size = 4096;
    config.chunk_delay = Duration::from_millis(5);
    let server = FlakyServer::spawn(config).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("workers.bin");
    let req = request(server.url(), path.clone(), 4);
    assert!(download(req.clone()).await.is_err());
    let before = tokio::fs::read(partial_path(&path)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_downloaded(&partial_path(&path), &before).await;
    server.configure(FlakyServerConfig::new(data.clone()));
    download(req).await.unwrap();
    assert_downloaded(&path, &data).await;
}

#[tokio::test]
async fn controls_interrupt_setup_and_retry_waits() {
    for setup in [true, false] {
        for cancel in [false, true] {
            let mut config = FlakyServerConfig::new(asymmetric_data(262_147));
            if setup {
                config.header_delay = Duration::from_secs(2);
            } else {
                config.interrupt_range_gets = 100;
                config.interrupt_after_bytes = 4096;
                config.chunk_size = 4096;
            }
            let server = FlakyServer::spawn(config).await;
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("control.bin");
            let mut handle = start_download(request(server.url(), path.clone(), 4)).unwrap();
            if setup {
                timeout(Duration::from_secs(1), async {
                    while server.stats().head_requests == 0 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .unwrap();
            } else {
                wait_for_progress(&mut handle.events).await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            if cancel {
                handle.control.cancel();
                handle.control.pause();
            } else {
                handle.control.pause();
            }
            let error = timeout(Duration::from_millis(250), handle.join)
                .await
                .expect("control acknowledgement must not wait for network/backoff")
                .unwrap()
                .unwrap_err();
            assert_eq!(error.is_cancelled(), cancel);
            assert_eq!(error.is_paused(), !cancel);
            assert!(!path.exists());
            if cancel {
                assert!(!partial_path(&path).exists());
            }
        }
    }
}

#[cfg(feature = "serde")]
#[tokio::test]
async fn rejected_checkpoints_restart_without_reusing_bytes() {
    let data = asymmetric_data(262_147);
    let mut config = FlakyServerConfig::new(data.clone());
    config.chunk_size = 4096;
    config.chunk_delay = Duration::from_millis(3);
    let server = FlakyServer::spawn(config).await;
    let dir = tempfile::tempdir().unwrap();
    for case in [
        "missing",
        "corrupt",
        "version",
        "url",
        "output",
        "redirect",
        "etag",
        "size",
        "entropy",
        "segment_size",
        "id",
        "overlap",
        "gap",
        "bounds",
        "progress",
        "in_bounds_corrupt",
        "truncated",
    ] {
        let path = dir.path().join(format!("{case}.bin"));
        let req = request(server.url(), path.clone(), 4);
        let mut handle = start_download(req.clone()).unwrap();
        wait_for_progress(&mut handle.events).await;
        handle.control.pause();
        assert!(handle.join.await.unwrap().unwrap_err().is_paused());
        let checkpoint = state_path(&path);
        let mut envelope: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&checkpoint).await.unwrap()).unwrap();
        let mut saved: serde_json::Value =
            serde_json::from_str(envelope["data"].as_str().unwrap()).unwrap();
        match case {
            "missing" => {
                tokio::fs::remove_file(&checkpoint).await.unwrap();
            }
            "corrupt" => {
                tokio::fs::write(&checkpoint, b"{truncated").await.unwrap();
            }
            "truncated" => {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(partial_path(&path))
                    .unwrap()
                    .set_len(1)
                    .unwrap();
            }
            _ => {
                match case {
                    "version" => saved["version"] = 0.into(),
                    "url" => saved["url"] = "http://other/file".into(),
                    "output" => saved["output"] = "other.bin".into(),
                    "redirect" => saved["final_url"] = "http://other/file".into(),
                    "etag" => saved["etag"] = "\"old\"".into(),
                    "size" => saved["total_size"] = 262146.into(),
                    "entropy" => saved["experimental_entropy"] = true.into(),
                    "segment_size" => saved["segment_size"] = 4096.into(),
                    "id" => saved["segments"][0]["id"] = 1.into(),
                    "overlap" => saved["segments"][1]["start"] = 0.into(),
                    "gap" => saved["segments"][0]["start"] = 1.into(),
                    "bounds" => saved["segments"][0]["end"] = u64::MAX.into(),
                    "progress" => saved["segments"][0]["downloaded"] = u64::MAX.into(),
                    "in_bounds_corrupt" => saved["segments"][0]["downloaded"] = 65536.into(),
                    _ => unreachable!(),
                }
                let payload = serde_json::to_string(&saved).unwrap();
                if case != "in_bounds_corrupt" {
                    // Exercise identity/geometry rejection even with an intact checksum.
                    use sha2::{Digest, Sha256};
                    envelope["sha256"] = format!("{:x}", Sha256::digest(payload.as_bytes())).into();
                }
                envelope["data"] = payload.into();
                tokio::fs::write(&checkpoint, serde_json::to_vec(&envelope).unwrap())
                    .await
                    .unwrap();
            }
        }
        let summary = download(req).await.unwrap();
        assert_eq!(summary.resumed_bytes, 0, "checkpoint case {case}");
        assert_downloaded(&path, &data).await;
    }
}

#[cfg(feature = "serde")]
#[tokio::test]
async fn same_size_resource_change_restarts_and_never_mixes_versions() {
    let old = asymmetric_data(524_291);
    let replacement: Vec<_> = old.iter().map(|byte| byte ^ 0xa7).collect();
    let mut config = FlakyServerConfig::new(old);
    config.chunk_size = 4096;
    config.chunk_delay = Duration::from_millis(5);
    let server = FlakyServer::spawn(config).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("changed.bin");
    let req = request(server.url(), path.clone(), 4);
    let mut handle = start_download(req.clone()).unwrap();
    wait_for_progress(&mut handle.events).await;
    handle.control.pause();
    assert!(handle.join.await.unwrap().unwrap_err().is_paused());
    server.configure(FlakyServerConfig::new(replacement.clone()));
    let summary = download(req).await.unwrap();
    assert_eq!(summary.resumed_bytes, 0);
    assert_downloaded(&path, &replacement).await;
}

#[tokio::test]
async fn unknown_length_and_ignored_ranges_download_correctly() {
    let data = asymmetric_data(262_147);
    for unknown in [false, true] {
        let mut config = FlakyServerConfig::new(data.clone());
        config.ignore_ranges = true;
        config.omit_length = unknown;
        let server = FlakyServer::spawn(config).await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fallback.bin");
        let summary = download(request(server.url(), path.clone(), 4))
            .await
            .unwrap();
        assert_eq!(summary.downloaded_bytes, data.len() as u64);
        assert_downloaded(&path, &data).await;
    }
}

#[tokio::test]
async fn absent_or_weak_validators_restart_paused_transfers() {
    let data = asymmetric_data(262_147);
    for etag in [None, Some("W/\"weak\"".into())] {
        let mut config = FlakyServerConfig::new(data.clone());
        config.etag = etag;
        config.chunk_size = 4096;
        config.chunk_delay = Duration::from_millis(3);
        let server = FlakyServer::spawn(config).await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unvalidated.bin");
        let req = request(server.url(), path.clone(), 4);
        let mut handle = start_download(req.clone()).unwrap();
        wait_for_progress(&mut handle.events).await;
        handle.control.pause();
        assert!(handle.join.await.unwrap().unwrap_err().is_paused());
        let summary = download(req).await.unwrap();
        assert_eq!(summary.resumed_bytes, 0);
        assert_downloaded(&path, &data).await;
    }
}

#[tokio::test]
async fn oversized_ranges_changed_identity_and_encoding_are_rejected() {
    let data = asymmetric_data(262_147);
    for case in ["oversize", "identity", "gzip", "br", "deflate"] {
        let mut config = FlakyServerConfig::new(data.clone());
        match case {
            "oversize" => config.extra_range_bytes = 7,
            "identity" => config.get_etag = Some("\"replacement\"".into()),
            encoding => config.encoding = Some(encoding.into()),
        }
        let server = FlakyServer::spawn(config).await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.bin");
        assert!(
            download(request(server.url(), path.clone(), 4))
                .await
                .is_err(),
            "{case}"
        );
        assert!(!path.exists(), "{case}");
        if partial_path(&path).exists() {
            assert!(partial_path(&path).metadata().unwrap().len() <= data.len() as u64);
        }
    }
}

#[tokio::test]
async fn concurrent_attempt_and_publication_collision_preserve_existing_output() {
    let data = asymmetric_data(524_291);
    let mut config = FlakyServerConfig::new(data);
    config.chunk_size = 4096;
    config.chunk_delay = Duration::from_millis(5);
    let server = FlakyServer::spawn(config).await;
    let dir = tempfile::tempdir().unwrap();
    for cancel in [false, true] {
        let path = dir.path().join(format!("owned-{cancel}.bin"));
        let req = request(server.url(), path.clone(), 4);
        let mut first = start_download(req.clone()).unwrap();
        wait_for_progress(&mut first.events).await;
        assert!(download(req).await.is_err());
        tokio::fs::write(&path, b"unrelated existing file")
            .await
            .unwrap();
        if cancel {
            first.control.cancel();
        }
        let error = first.join.await.unwrap().unwrap_err();
        assert_eq!(error.is_cancelled(), cancel);
        assert_downloaded(&path, b"unrelated existing file").await;
        assert_eq!(partial_path(&path).exists(), !cancel);
    }
}

#[tokio::test]
async fn optional_event_reader_does_not_determine_completion() {
    let data = asymmetric_data(262_147);
    let server = FlakyServer::spawn(FlakyServerConfig::new(data.clone())).await;
    let dir = tempfile::tempdir().unwrap();
    for absent in [false, true] {
        let path = dir.path().join(format!("{absent}.bin"));
        let handle = start_download(request(server.url(), path.clone(), 4)).unwrap();
        let events = if absent {
            drop(handle.events);
            None
        } else {
            Some(handle.events)
        };
        let summary = handle.join.await.unwrap().unwrap();
        assert_eq!(summary.downloaded_bytes, data.len() as u64);
        assert_downloaded(&path, &data).await;
        let metadata = handle
            .observation
            .metadata()
            .expect("metadata survives absent events");
        assert_eq!(metadata.output, path);
        assert_eq!(metadata.total_bytes, Some(data.len() as u64));
        drop(events);
    }
}

#[tokio::test]
async fn progress_is_sampled_across_workers_without_losing_completion() {
    let data = asymmetric_data(1_048_579);
    for connections in [1, 4] {
        let mut config = FlakyServerConfig::new(data.clone());
        config.chunk_size = 1024;
        config.chunk_delay = Duration::from_millis(5);
        let server = FlakyServer::spawn(config).await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sampled.bin");
        let mut handle = start_download(request(server.url(), path.clone(), connections)).unwrap();
        let mut progress_count = 0;
        let mut finished_bytes = None;
        while let Some(event) = handle.events.recv().await {
            match event {
                DownloadEvent::Progress { .. } => progress_count += 1,
                DownloadEvent::Finished { summary } => {
                    finished_bytes = Some(summary.downloaded_bytes);
                }
                _ => {}
            }
        }
        let summary = handle.join.await.unwrap().unwrap();
        assert!(
            progress_count >= 2,
            "progress must continue during a transfer"
        );
        assert!(
            progress_count <= 1 + summary.elapsed.as_millis() / 500,
            "progress must be limited per download, not per chunk or worker: {progress_count}"
        );
        assert_eq!(finished_bytes, Some(data.len() as u64));
        assert_eq!(summary.downloaded_bytes, data.len() as u64);
        assert_downloaded(&path, &data).await;
    }
}

#[tokio::test]
async fn saturated_observations_and_stalled_diagnostics_do_not_block_completion() {
    use arkama_core::{Diagnostics, DiagnosticsConfig};
    let data = asymmetric_data(1_048_579);
    let mut config = FlakyServerConfig::new(data.clone());
    config.chunk_size = 1024;
    config.chunk_delay = Duration::from_millis(5);
    let server = FlakyServer::spawn(config).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stalled-reader.bin");
    let handle = start_download(request(server.url(), path.clone(), 1)).unwrap();
    let observation = handle.observation.clone();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let mut entered_tx = Some(entered_tx);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let collector = Diagnostics::with_sink(
        observation.clone(),
        DiagnosticsConfig {
            interval: Duration::from_millis(50),
            history_capacity: 2,
            disk_path: Some(dir.path().into()),
        },
        move |_| {
            if let Some(entered) = entered_tx.take() {
                let _ = entered.send(());
                let _ = release_rx.recv();
            }
            Ok(())
        },
    )
    .unwrap();
    timeout(Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    // Keep events unread while the dedicated sink is blocked. Waiting sends would deadlock here.
    let result = timeout(Duration::from_secs(15), handle.join).await;
    let lag = collector.snapshot().collector_lag_ms;
    let _ = release_tx.send(());
    let summary = result
        .expect("download must not wait for observations")
        .unwrap()
        .unwrap();
    assert_eq!(summary.downloaded_bytes, data.len() as u64);
    assert_downloaded(&path, &data).await;
    assert_eq!(handle.events.len(), 8);
    let snapshot = observation.snapshot();
    assert!(snapshot.dropped_events > 0);
    assert!(lag > 1000);
    assert_eq!(snapshot.outcome, "complete");
    assert_eq!(snapshot.written_bytes, data.len() as u64);
    assert_eq!(snapshot.received_bytes, data.len() as u64);
    assert_eq!(snapshot.disk_written_bytes, data.len() as u64);
    assert_eq!(
        snapshot.crash_safe_bytes,
        if cfg!(feature = "serde") {
            data.len() as u64
        } else {
            0
        }
    );
    assert_eq!(snapshot.active_workers, 0);
    assert_eq!(snapshot.pending_bytes, 0);
    timeout(Duration::from_secs(2), async {
        loop {
            if let Some(gap) = collector
                .snapshot()
                .history
                .iter()
                .find(|sample| sample.gap)
            {
                assert_eq!(gap.bytes_per_second, None);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("collector must expose its gap after resuming");
    tokio::time::sleep(Duration::from_millis(250)).await;
    let retained = collector.snapshot();
    assert_eq!(retained.history.len(), 2);
    assert!(retained.evicted_samples > 0);
    assert!(retained.process_memory_bytes.is_some());
    assert!(retained.disk_available_bytes.is_some());
}

#[tokio::test]
async fn failed_diagnostic_sink_retains_bounded_history_and_correct_bytes() {
    use arkama_core::{Diagnostics, DiagnosticsConfig};
    let data = asymmetric_data(131_075);
    let mut config = FlakyServerConfig::new(data.clone());
    config.chunk_size = 1024;
    config.chunk_delay = Duration::from_millis(5);
    let server = FlakyServer::spawn(config).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("failed-sink.bin");
    let handle = start_download(request(server.url(), path.clone(), 1)).unwrap();
    let diagnostics = Diagnostics::with_sink(
        handle.observation.clone(),
        DiagnosticsConfig {
            interval: Duration::from_millis(50),
            history_capacity: 2,
            disk_path: None,
        },
        |_| Err(std::io::Error::other("sink unavailable")),
    )
    .unwrap();
    drop(handle.events);
    timeout(Duration::from_secs(5), handle.join)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_downloaded(&path, &data).await;
    let snapshot = diagnostics.snapshot();
    assert_eq!(snapshot.history.len(), 2);
    assert!(snapshot.evicted_samples > 0);
    assert!(snapshot.sink_errors > 0);
}

#[tokio::test]
async fn download_single_throttled_without_ranges() {
    let data = vec![42u8; 512 * 1024];
    let mut config = FlakyServerConfig::new(data.clone());
    config.accept_ranges = false;
    config.chunk_size = 16 * 1024;
    config.chunk_delay = Duration::from_millis(2);
    let server = FlakyServer::spawn(config).await;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join("file.bin");

    let req = request(server.url(), path.clone(), 4);
    let summary = download(req).await.expect("single throttled download");
    assert_eq!(summary.total_bytes, data.len() as u64);
    assert_eq!(summary.downloaded_bytes, data.len() as u64);
    assert_downloaded(&path, &data).await;

    let stats = server.stats();
    assert_eq!(stats.head_requests, 1);
    assert_eq!(stats.full_get_requests, 1);
    assert_eq!(stats.range_get_requests, 1);
}

#[tokio::test]
async fn download_single_restarts_after_interrupted_non_range_stream() {
    let data = asymmetric_data(262_147);
    let mut config = FlakyServerConfig::new(data.clone());
    config.accept_ranges = false;
    config.chunk_size = 32 * 1024;
    config.interrupt_full_gets = 1;
    config.interrupt_after_bytes = 64 * 1024;
    let server = FlakyServer::spawn(config).await;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join("restart.bin");

    let req = request(server.url(), path.clone(), 4);
    let handle = start_download(req).unwrap();
    let observation = handle.observation.clone();
    drop(handle.events);
    let summary = handle.join.await.unwrap().expect("single restart download");
    assert_eq!(summary.total_bytes, data.len() as u64);
    assert_eq!(summary.downloaded_bytes, data.len() as u64);
    assert_downloaded(&path, &data).await;
    let observed = observation.snapshot();
    assert_eq!(observed.written_bytes, data.len() as u64);
    assert!(observed.received_bytes > observed.written_bytes);
    assert_eq!(observed.disk_written_bytes, observed.received_bytes);
    assert_eq!(observed.crash_safe_bytes, 0);
    assert_eq!(observed.retries, 1);
    assert!(observed.durations_ms["retry_wait"] >= 900);

    let stats = server.stats();
    assert_eq!(stats.head_requests, 1);
    assert_eq!(stats.range_get_requests, 1);
    assert!(stats.full_get_requests >= 2);
}

#[tokio::test]
async fn download_single_and_segmented_resume() {
    let data = asymmetric_data(1_048_579);
    let server = FlakyServer::spawn(FlakyServerConfig::new(data.clone())).await;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join("file.bin");

    let req = request(server.url(), path.clone(), 1);

    let summary = download(req.clone()).await.expect("single download");
    assert_eq!(summary.total_bytes, data.len() as u64);
    assert_eq!(summary.downloaded_bytes, data.len() as u64);

    assert!(
        download(req.clone()).await.is_err(),
        "existing final output is never assumed complete"
    );
    assert_downloaded(&path, &data).await;
    tokio::fs::remove_file(&path).await.unwrap();

    let half = data.len() / 2;
    tokio::fs::write(partial_path(&path), &data[..half])
        .await
        .expect("write partial");
    let state_path = state_path(&path);
    if state_path.exists() {
        tokio::fs::remove_file(&state_path)
            .await
            .expect("remove state");
    }

    let mut resume = req;
    resume.connections = 4;
    let summary = download(resume).await.expect("segmented resume");
    assert_eq!(summary.total_bytes, data.len() as u64);
    assert_eq!(summary.downloaded_bytes, data.len() as u64);
    assert_eq!(
        summary.resumed_bytes, 0,
        "bytes without a checkpoint are not reusable"
    );
    assert_downloaded(&path, &data).await;
}

#[tokio::test]
async fn download_segmented_retries_after_range_interrupts() {
    let data = asymmetric_data(1_048_579);
    let mut config = FlakyServerConfig::new(data.clone());
    config.chunk_size = 16 * 1024;
    config.interrupt_range_gets = 4;
    config.interrupt_after_bytes = 64 * 1024;
    let server = FlakyServer::spawn(config).await;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join("segmented.bin");

    let req = request(server.url(), path.clone(), 4);
    let summary = download(req).await.expect("segmented retry download");
    assert_eq!(summary.total_bytes, data.len() as u64);
    assert_eq!(summary.downloaded_bytes, data.len() as u64);
    assert_downloaded(&path, &data).await;

    let stats = server.stats();
    assert_eq!(stats.head_requests, 1);
    assert!(stats.range_get_requests > 4);
}

#[tokio::test]
async fn pause_and_resume_segmented_download() {
    let data = asymmetric_data(1_048_579);
    let mut config = FlakyServerConfig::new(data.clone());
    config.chunk_size = 16 * 1024;
    config.chunk_delay = Duration::from_millis(10);
    let server = FlakyServer::spawn(config).await;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join("paused.bin");
    let state_path = state_path(&path);

    let req = request(server.url(), path.clone(), 4);
    let mut handle = start_download(req.clone()).expect("start download");
    let paused_bytes = wait_for_progress(&mut handle.events).await;
    handle.control.pause();

    let err = handle
        .join
        .await
        .expect("join paused download")
        .expect_err("pause should interrupt download");
    assert_eq!(err.to_string(), "interrupted");
    assert_eq!(state_path.exists(), cfg!(feature = "serde"));
    assert!(!path.exists(), "paused output stays under the partial name");

    let summary = download(req).await.expect("resume paused download");
    assert_eq!(summary.total_bytes, data.len() as u64);
    assert_eq!(summary.downloaded_bytes, data.len() as u64);
    if cfg!(feature = "serde") {
        assert!(summary.resumed_bytes >= paused_bytes);
    } else {
        assert_eq!(summary.resumed_bytes, 0);
    }
    assert_downloaded(&path, &data).await;
    assert!(!state_path.exists());
}

#[tokio::test]
async fn cancel_removes_partial_output_and_state() {
    let data = vec![3u8; 1_048_576];
    let mut config = FlakyServerConfig::new(data);
    config.chunk_size = 16 * 1024;
    config.chunk_delay = Duration::from_millis(10);
    let server = FlakyServer::spawn(config).await;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join("cancelled.bin");
    let state_path = state_path(&path);

    let req = request(server.url(), path.clone(), 4);
    let mut handle = start_download(req).expect("start cancellable download");
    let _ = wait_for_progress(&mut handle.events).await;
    handle.control.cancel();

    let err = handle
        .join
        .await
        .expect("join cancelled download")
        .expect_err("cancel should interrupt download");
    assert_eq!(err.to_string(), "cancelled");
    assert!(!path.exists());
    assert!(!state_path.exists());
    assert!(!partial_path(&path).exists());
}

#[tokio::test]
async fn download_segmented_large_payload() {
    let data = asymmetric_data(8 * 1024 * 1024 + 3);
    let server = FlakyServer::spawn(FlakyServerConfig::new(data.clone())).await;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let mut run = 0usize;
    while run < 6 {
        let path = temp_dir.path().join(format!("large-{run}.bin"));
        let mut req = request(server.url(), path.clone(), 8);
        req.experimental_entropy = run % 2 == 1;

        let summary = download(req).await.expect("segmented download");
        assert_eq!(summary.total_bytes, data.len() as u64);
        assert_eq!(summary.downloaded_bytes, data.len() as u64);
        assert_downloaded(&path, &data).await;
        run += 1;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_limits_cover_bodies_redirects_bandwidth_and_live_changes() {
    use arkama_core::{DownloadLimits, DownloadSession};
    let data = asymmetric_data(65_539);
    let mut config = FlakyServerConfig::new(data.clone());
    config.chunk_size = 1024;
    config.chunk_delay = Duration::from_millis(4);
    let server = FlakyServer::spawn(config).await;
    let destination = server.url().replace("127.0.0.1", "localhost");
    let mut redirect_config = FlakyServerConfig::new(Vec::new());
    redirect_config.redirect = Some(destination.clone());
    let redirect = FlakyServer::spawn(redirect_config).await;
    let dir = tempfile::tempdir().unwrap();
    let limits = DownloadLimits::default();
    assert!(limits.set_connections_per_host(257).is_err());
    limits.set_connections_per_host(1).unwrap();
    let session = DownloadSession::default().limits(limits.clone());
    let first = session
        .start(request(destination, dir.path().join("direct.bin"), 4))
        .unwrap();
    let second = session
        .start(request(redirect.url(), dir.path().join("redirect.bin"), 3))
        .unwrap();
    let (a, b) = timeout(Duration::from_secs(10), async {
        tokio::join!(first.join, second.join)
    })
    .await
    .unwrap();
    a.unwrap().unwrap();
    b.unwrap().unwrap();
    assert_eq!(
        server.peak_bodies(),
        1,
        "host admission released before response bodies ended"
    );
    assert_downloaded(&dir.path().join("direct.bin"), &data).await;
    assert_downloaded(&dir.path().join("redirect.bin"), &data).await;

    let source = FlakyServer::spawn(FlakyServerConfig::new(data.clone())).await;
    limits.set_connections_per_host(8).unwrap();
    limits.set_bytes_per_second(131_072);
    let started = std::time::Instant::now();
    let mut joins = Vec::new();
    for i in 0..3 {
        joins.push(
            session
                .start(request(
                    source.url(),
                    dir.path().join(format!("capped-{i}.bin")),
                    2,
                ))
                .unwrap()
                .join,
        );
    }
    for join in joins {
        timeout(Duration::from_secs(10), join)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    assert!(
        started.elapsed() >= Duration::from_millis(1400),
        "three jobs received three independent caps"
    );
    for i in 0..3 {
        assert_downloaded(&dir.path().join(format!("capped-{i}.bin")), &data).await;
    }

    limits.set_bytes_per_second(1);
    let job = session
        .start(request(source.url(), dir.path().join("live.bin"), 1))
        .unwrap();
    timeout(Duration::from_secs(3), async {
        while job.observation.snapshot().received_bytes == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    limits.set_bytes_per_second(0);
    timeout(Duration::from_secs(3), job.join)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_downloaded(&dir.path().join("live.bin"), &data).await;
}
