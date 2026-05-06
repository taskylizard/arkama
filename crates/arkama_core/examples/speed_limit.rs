use arkama_core::{DownloadRequest, download};

#[tokio::main]
async fn main() -> arkama_core::Result<()> {
    let request = DownloadRequest::builder("https://example.com/file.bin")
        .output("./file.bin")
        .speed_limit(1_000_000)
        .build();

    let summary = download(request).await?;
    println!(
        "downloaded {} bytes to {:?} with an approximate 1 MB/s limit",
        summary.downloaded_bytes, summary.output
    );

    Ok(())
}
