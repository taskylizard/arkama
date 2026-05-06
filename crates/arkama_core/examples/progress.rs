use std::error::Error;

use arkama_core::{DownloadEvent, DownloadRequest, start_download};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let request = DownloadRequest::new("https://example.com/file.bin").output("./file.bin");

    let mut handle = start_download(request)?;

    while let Some(event) = handle.events.recv().await {
        match event {
            DownloadEvent::Started {
                output,
                total_bytes,
                resumed_bytes,
            } => {
                println!(
                    "started {:?}; total: {:?}; resumed: {} bytes",
                    output, total_bytes, resumed_bytes
                );
            }
            DownloadEvent::Progress {
                downloaded_bytes,
                total_bytes,
            } => {
                println!("progress: {downloaded_bytes} / {total_bytes:?} bytes");
            }
            DownloadEvent::Finished { summary } => {
                println!("finished {:?}", summary.output);
                break;
            }
            DownloadEvent::Failed { message } => {
                eprintln!("download failed: {message}");
                break;
            }
        }
    }

    let summary = handle.join.await??;
    println!("saved {} bytes", summary.downloaded_bytes);

    Ok(())
}
