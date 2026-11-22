//! Machine-readable access to the shared manager, including explicit local/no-listener use.

use arkama_manager::{Command, Manager};
use clap::Args;
use eyre::{Context, Result};
use std::io::Read;

#[derive(Args, Debug)]
pub struct ManageArgs {
    #[arg(
        long,
        help = "Own the local manager for this command, without a listener. Stops/checkpoints on exit."
    )]
    no_server: bool,
    #[arg(help = "JSON manager command or '-' for bounded stdin (use stdin for credentials).")]
    command: String,
}

pub async fn run(args: ManageArgs) -> Result<()> {
    let data = if args.command == "-" {
        let mut data = String::new();
        std::io::stdin()
            .take(65_537)
            .read_to_string(&mut data)
            .context("cannot read manager command")?;
        data
    } else {
        args.command
    };
    if data.len() > 65_536 {
        return Err(eyre::eyre!("manager command exceeds size limit"));
    }
    let command: Command = serde_json::from_str(&data).map_err(|_| {
        eyre::eyre!("invalid manager command; only supported GET context can be submitted")
    })?;
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
