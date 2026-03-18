use std::net::SocketAddr;
use std::path::PathBuf;

use arkama_core::{DownloadRequest, download};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use tokio::net::TcpListener;

async fn serve_file(req: Request<hyper::body::Incoming>, data: Bytes) -> Response<Full<Bytes>> {
    let mut builder = Response::builder().status(StatusCode::OK);
    if req.method() == Method::HEAD {
        builder = builder.header("content-length", data.len());
        return builder
            .body(Full::new(Bytes::new()))
            .expect("head response");
    }

    let mut body = data.clone();
    if let Some(range) = req.headers().get(hyper::header::RANGE)
        && let Ok(range) = range.to_str()
        && let Some(start) = range.strip_prefix("bytes=")
        && let Some((start, end)) = start.split_once('-')
        && let Ok(start) = start.parse::<usize>()
    {
        let end = end
            .parse::<usize>()
            .ok()
            .map(|end| end + 1)
            .unwrap_or(data.len());
        let start = start.min(data.len());
        let end = end.min(data.len());
        body = data.slice(start..end);
        builder = builder.status(StatusCode::PARTIAL_CONTENT);
        builder = builder.header(
            "content-range",
            format!("bytes {}-{}/{}", start, end - 1, data.len()),
        );
    }

    builder = builder.header("accept-ranges", "bytes");
    builder = builder.header("content-length", body.len());
    builder.body(Full::new(body)).expect("body response")
}

async fn spawn_server(data: Bytes) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let handle = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let data = data.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let data = data.clone();
                    async move { Ok::<_, hyper::Error>(serve_file(req, data).await) }
                });
                let io = hyper_util::rt::tokio::TokioIo::new(stream);
                let _ = http1::Builder::new().serve_connection(io, service).await;
            });
        }
    });

    (addr, handle)
}

#[tokio::test]
async fn download_single_and_segmented_resume() {
    let data = Bytes::from(vec![42u8; 1_048_576]);
    let (addr, server_handle) = spawn_server(data.clone()).await;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join("file.bin");

    let req = DownloadRequest {
        url: format!("http://{addr}/file.bin"),
        output: Some(path.clone()),
        output_dir: None,
        connections: 1,
        user_agent: None,
        limit: None,
        experimental_entropy: false,
    };

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
    let state_path = PathBuf::from(format!("{}.arkama.state", path.to_string_lossy()));
    if state_path.exists() {
        tokio::fs::remove_file(&state_path)
            .await
            .expect("remove state");
    }

    let mut resume = req;
    resume.connections = 4;
    let summary = download(resume).await.expect("segmented resume");
    assert_eq!(summary.total_bytes, data.len() as u64);

    server_handle.abort();
    let _ = server_handle.await;
}

#[tokio::test]
async fn download_segmented_large_payload() {
    let data = Bytes::from(vec![7u8; 8 * 1024 * 1024]);
    let (addr, server_handle) = spawn_server(data.clone()).await;

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join("large.bin");
    let req = DownloadRequest {
        url: format!("http://{addr}/large.bin"),
        output: Some(path.clone()),
        output_dir: None,
        connections: 8,
        user_agent: None,
        limit: None,
        experimental_entropy: false,
    };

    let summary = download(req).await.expect("segmented download");
    assert_eq!(summary.total_bytes, data.len() as u64);
    assert_eq!(summary.downloaded_bytes, data.len() as u64);

    server_handle.abort();
    let _ = server_handle.await;
}
