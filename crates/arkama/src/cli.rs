use clap::Args;
use std::path::PathBuf;

fn parse_rate(src: &str) -> Result<u64, String> {
    let Ok(bytes) = bytefmt::parse(src) else {
        return Err("invalid byte size".to_string());
    };
    Ok(bytes)
}

#[derive(Args, Debug)]
pub struct DownloadArgs {
    #[arg(
        value_name = "URL",
        required_unless_present = "links_file",
        help = "Download URL."
    )]
    pub url: Option<String>,

    #[arg(
        long = "links-file",
        value_name = "PATH",
        conflicts_with = "url",
        help = "Text file with one download URL per line."
    )]
    pub links_file: Option<PathBuf>,

    #[arg(short = 'o', long = "output", help = "Output file path.")]
    pub output: Option<PathBuf>,

    #[arg(
        long = "connections",
        help = "Concurrent connections for segmented downloads (defaults to config, otherwise 4)."
    )]
    pub connections: Option<usize>,

    #[arg(long = "user-agent", help = "Override HTTP User-Agent.")]
    pub user_agent: Option<String>,

    #[arg(
        long = "limit",
        value_name = "BYTES_PER_SEC",
        value_parser = parse_rate,
        help = "Global download speed limit (defaults to config if unset; ex: 2MB, 500KB)."
    )]
    pub limit: Option<u64>,

    #[arg(
        long = "experimental-entropy",
        help = "Experimental: rotate source ports and recycle slow segment connections."
    )]
    pub experimental_entropy: bool,

    #[arg(
        long = "silent",
        conflicts_with = "json",
        help = "Suppress progress and summary output."
    )]
    pub silent: bool,

    #[arg(
        long = "json",
        conflicts_with = "silent",
        help = "Emit JSON events to stdout for automation."
    )]
    pub json: bool,

    #[arg(
        short = 'y',
        long = "yes",
        help = "Assume yes to all prompts (non-interactive)."
    )]
    pub yes: bool,
}
