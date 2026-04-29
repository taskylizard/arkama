use std::path::Path;

use indicatif::{ProgressBar, ProgressState, ProgressStyle};

use crate::util::{format_duration, format_speed};

pub fn build_progress(total: Option<u64>, output: &Path) -> ProgressBar {
    let progress = match total {
        Some(size) => ProgressBar::new(size),
        None => ProgressBar::new_spinner(),
    };

    let style = match total {
        Some(_) => ProgressStyle::with_template(
            "{spinner:.green} {msg}\n{bytes}/{total_bytes} {bar:40.cyan/blue} {speed_human} ETA {eta_human}\n",
        ),
        None => ProgressStyle::with_template("{spinner:.green} {msg}\n{bytes} {speed_human}\n"),
    };
    let style = match style {
        Ok(style) => style
            .with_key(
                "eta_human",
                |state: &ProgressState, writer: &mut dyn std::fmt::Write| {
                    let _ = write!(writer, "{}", format_duration(state.eta()));
                },
            )
            .with_key(
                "speed_human",
                |state: &ProgressState, writer: &mut dyn std::fmt::Write| {
                    let speed = state.per_sec();
                    let speed = if speed.is_finite() && speed > 0.0 {
                        speed.round() as u64
                    } else {
                        0
                    };
                    let _ = write!(writer, "{}", format_speed(speed));
                },
            ),
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
