use std::error::Error;

use arkama_core::{DownloadEvent, DownloadRequest, download, start_download};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let request = DownloadRequest::new("https://example.com/large-file.bin")
        .output("./large-file.bin")
        .connections(4);

    let mut handle = start_download(request.clone())?;

    while let Some(event) = handle.events.recv().await {
        match event {
            DownloadEvent::Metadata { .. } => {}
            DownloadEvent::Progress {
                downloaded_bytes, ..
            } if downloaded_bytes > 0 => {
                println!("pausing after {downloaded_bytes} bytes");
                handle.control.pause();
                break;
            }
            DownloadEvent::Started { resumed_bytes, .. } => {
                println!("started with {resumed_bytes} resumed bytes");
            }
            DownloadEvent::Finished { summary } => {
                println!("finished before pause took effect: {:?}", summary.output);
                break;
            }
            DownloadEvent::Failed { message } => {
                eprintln!("download failed before pause request completed: {message}");
                break;
            }
            DownloadEvent::Progress { .. } => {}
        }
    }

    match handle.join.await? {
        Ok(summary) => {
            println!("download already completed: {:?}", summary.output);
        }
        Err(err) if err.is_paused() => {
            println!("download paused; resuming with the same request/output");
            let summary = download(request).await?;
            println!("resumed and saved {} bytes", summary.downloaded_bytes);
        }
        Err(err) => return Err(err.into()),
    }

    Ok(())
}
