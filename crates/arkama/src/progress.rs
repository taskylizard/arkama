use indicatif::{ProgressBar, ProgressStyle};
use std::path::Path;

pub fn build_progress(total: Option<u64>, output: &Path) -> ProgressBar {
    let progress = match total {
        Some(size) => ProgressBar::new(size),
        None => ProgressBar::new_spinner(),
    };

    let style = ProgressStyle::with_template(
        "{spinner:.green} {msg}\n{bytes}/{total_bytes} {bar:40.cyan/blue} {bytes_per_sec} ETA {eta}\n",
    );
    let style = match style {
        Ok(style) => style,
        Err(err) => {
            progress.println(format!("progress style error: {err}"));
            ProgressStyle::default_bar()
        }
    };

    let file_name = output
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("download");
    progress.set_message(format!("file {file_name}"));
    progress.set_style(style);
    progress
}
