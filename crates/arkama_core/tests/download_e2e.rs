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
    }
}

fn state_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.arkama.state", path.to_string_lossy()))
}

async fn wait_for_progress(events: &mut mpsc::UnboundedReceiver<DownloadEvent>) -> u64 {
    loop {
        let next = timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("progress event timeout");
        let event = next.expect("download event");
        match event {
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
    let data = vec![7u8; 256 * 1024];
    let mut config = FlakyServerConfig::new(data.clone());
    config.accept_ranges = false;
    config.chunk_size = 32 * 1024;
    config.interrupt_full_gets = 1;
    config.interrupt_after_bytes = 64 * 1024;
    let server = FlakyServer::spawn(config).await;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join("restart.bin");

    let req = request(server.url(), path.clone(), 4);
    let summary = download(req).await.expect("single restart download");
    assert_eq!(summary.total_bytes, data.len() as u64);
    assert_eq!(summary.downloaded_bytes, data.len() as u64);
    assert_downloaded(&path, &data).await;

    let stats = server.stats();
    assert_eq!(stats.head_requests, 1);
    assert_eq!(stats.range_get_requests, 1);
    assert!(stats.full_get_requests >= 2);
}

#[tokio::test]
async fn download_single_and_segmented_resume() {
    let data = vec![42u8; 1_048_576];
    let server = FlakyServer::spawn(FlakyServerConfig::new(data.clone())).await;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join("file.bin");

    let req = request(server.url(), path.clone(), 1);

    let summary = download(req.clone()).await.expect("single download");
    assert_eq!(summary.total_bytes, data.len() as u64);
    assert_eq!(summary.downloaded_bytes, data.len() as u64);

    let summary = download(req.clone()).await.expect("already complete");
    assert_eq!(summary.total_bytes, data.len() as u64);
    assert_eq!(summary.downloaded_bytes, data.len() as u64);

    let half = data.len() / 2;
    tokio::fs::write(&path, &data[..half])
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
    assert_downloaded(&path, &data).await;
}

#[tokio::test]
async fn download_segmented_retries_after_range_interrupts() {
    let data = vec![19u8; 1_048_576];
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
    let data = vec![5u8; 1_048_576];
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
    assert!(state_path.exists());

    let summary = download(req).await.expect("resume paused download");
    assert_eq!(summary.total_bytes, data.len() as u64);
    assert_eq!(summary.downloaded_bytes, data.len() as u64);
    assert!(summary.resumed_bytes >= paused_bytes);
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
}

#[tokio::test]
async fn download_segmented_large_payload() {
    let data = vec![7u8; 8 * 1024 * 1024];
    let server = FlakyServer::spawn(FlakyServerConfig::new(data.clone())).await;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let mut run = 0usize;
    while run < 6 {
        let path = temp_dir.path().join(format!("large-{run}.bin"));
        let req = request(server.url(), path.clone(), 8);

        let summary = download(req).await.expect("segmented download");
        assert_eq!(summary.total_bytes, data.len() as u64);
        assert_eq!(summary.downloaded_bytes, data.len() as u64);
        assert_downloaded(&path, &data).await;
        run += 1;
    }
}
