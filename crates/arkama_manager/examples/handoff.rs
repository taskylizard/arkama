//! A local embedded host: stdin contains a submission, never credentials in command-line args.
#![forbid(unsafe_code)]

use std::io::Read;
use std::time::Duration;

use arkama_manager::{Command, Manager, Response, Submission};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args().nth(1).ok_or("provide a database path")?;
    let mut data = String::new();
    std::io::stdin().take(65_537).read_to_string(&mut data)?;
    if data.len() > 65_536 {
        return Err("submission exceeds 64 KiB".into());
    }
    let submission: Submission =
        serde_json::from_str(&data).map_err(|_| "invalid submission JSON")?;
    if submission.handoff_id.is_none() {
        return Err("provide a stable handoff ID".into());
    }
    let manager = Manager::open(database, tokio::runtime::Handle::current()).await?;
    let result = async {
        let Response::Accepted { id } = manager.execute(Command::Submit { submission }).await?
        else {
            return Err("expected durable acceptance".into());
        };
        println!("accepted job {id}");
        loop {
            let Response::Job(job) = manager.execute(Command::Show { id }).await? else {
                return Err("expected a job snapshot".into());
            };
            if matches!(
                job.record.status.as_str(),
                "finished" | "paused" | "cancelled"
            ) {
                println!("job {id}: {}", job.record.status);
                return Ok(());
            }
            if job.record.status == "failed" {
                return Err("job failed; inspect its redacted snapshot".into());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    .await;
    // The host keeps its runtime alive while the manager joins/checkpoints workers.
    manager.shutdown().await?;
    result
}
