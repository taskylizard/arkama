use arkama_data::{Db, default_download_dir};
use clap::{Args, Subcommand};
use eyre::Result;

#[derive(Args, Debug)]
pub struct ConfigArgs {
    #[command(subcommand)]
    pub command: ConfigCommand,
}

#[derive(Subcommand, Debug)]
pub enum ConfigCommand {
    #[command(about = "Get a configuration value.")]
    Get {
        #[arg(help = "Configuration key (e.g., download_dir).")]
        key: String,
    },

    #[command(about = "Set a configuration value.")]
    Set {
        #[arg(help = "Configuration key (e.g., download_dir).")]
        key: String,

        #[arg(help = "Configuration value.")]
        value: String,
    },

    #[command(about = "Show all configuration values.")]
    Show,
}

pub fn run(args: ConfigArgs) -> Result<()> {
    let db = Db::open()?;

    match args.command {
        ConfigCommand::Get { key } => {
            let value = db.get_setting(&key)?;
            match value {
                Some(v) => println!("{v}"),
                None => println!("(not set)"),
            }
        }
        ConfigCommand::Set { key, value } => {
            db.set_setting(&key, &value)?;
            println!("{key} = {value}");
        }
        ConfigCommand::Show => {
            let download_dir = db
                .get_setting("download_dir")?
                .unwrap_or_else(|| default_download_dir().display().to_string());
            let connections = db
                .get_setting("connections")?
                .unwrap_or_else(|| "4".to_string());
            let max_concurrent = db
                .get_setting("max_concurrent")?
                .unwrap_or_else(|| "3".to_string());
            let speed_limit = db
                .get_setting("speed_limit")?
                .filter(|value| !value.is_empty());

            println!("download_dir = {download_dir}");
            println!("connections = {connections}");
            println!("max_concurrent = {max_concurrent}");
            match speed_limit {
                Some(speed_limit) => println!("speed_limit = {speed_limit}"),
                None => println!("speed_limit = (not set)"),
            }
        }
    }

    Ok(())
}
