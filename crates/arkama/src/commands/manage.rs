//! Machine-readable access to the shared manager, including explicit local/no-listener use.

use arkama_manager::{Command, Manager};
use clap::Args;
use eyre::{Context, Result};

#[derive(Args, Debug)]
pub struct ManageArgs {
    #[arg(
        long,
        help = "Own the local manager for this command, without a listener. Stops/checkpoints on exit."
    )]
    no_server: bool,
    #[arg(help = "JSON manager command, e.g. '{\"op\":\"status\"}'.")]
    command: String,
}

pub async fn run(args: ManageArgs) -> Result<()> {
    if args.command.len() > 65_536 {
        return Err(eyre::eyre!("manager command exceeds size limit"));
    }
    let command: Command =
        serde_json::from_str(&args.command).context("invalid manager command")?;
    let response = if args.no_server {
        let manager =
            Manager::open(arkama_data::db_path()?, tokio::runtime::Handle::current()).await?;
        let response = manager.execute(command).await;
        manager.shutdown().await?;
        response?
    } else {
        crate::daemon::manager_command(command).await?
    };
    println!("{}", serde_json::to_string(&response)?);
    Ok(())
}
