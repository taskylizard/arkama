use arkama_core::{DownloadRequest, download};

#[tokio::main]
async fn main() -> arkama_core::Result<()> {
    let request = DownloadRequest::new("https://example.com/file.bin").output("./file.bin");

    let summary = download(request).await?;
    println!(
        "downloaded {} bytes to {:?} in {:?}",
        summary.downloaded_bytes, summary.output, summary.elapsed
    );

    Ok(())
}
