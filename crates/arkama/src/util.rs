use std::path::Path;
use std::time::Instant;

pub fn active_label(output: &Path) -> String {
    if output.as_os_str().is_empty() {
        return "Downloading".to_string();
    }
    format!("Downloading: {}", output.display())
}

pub fn progress_fraction(downloaded_bytes: u64, total_bytes: Option<u64>) -> f32 {
    let Some(total_bytes) = total_bytes else {
        return 0.0;
    };
    if total_bytes == 0 {
        return 0.0;
    }
    downloaded_bytes as f32 / total_bytes as f32
}

pub fn progress_text(downloaded_bytes: u64, total_bytes: Option<u64>) -> String {
    match total_bytes {
        Some(total_bytes) => format!(
            "{}/{}",
            format_bytes(downloaded_bytes as i64),
            format_bytes(total_bytes as i64)
        ),
        None => format_bytes(downloaded_bytes as i64),
    }
}

pub fn history_progress_text(downloaded_bytes: i64, total_bytes: Option<i64>) -> String {
    match total_bytes {
        Some(total_bytes) => format!(
            "{}/{}",
            format_bytes(downloaded_bytes),
            format_bytes(total_bytes)
        ),
        None => format_bytes(downloaded_bytes),
    }
}

pub fn should_persist_progress(
    last_update: &Instant,
    last_bytes: u64,
    downloaded_bytes: u64,
) -> bool {
    if last_update.elapsed() >= std::time::Duration::from_millis(500) {
        return true;
    }
    downloaded_bytes.saturating_sub(last_bytes) >= 512 * 1024
}

pub fn format_bytes(bytes: i64) -> String {
    let mut value = bytes as f64;
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut unit_index = 0usize;
    while value >= 1024.0 && unit_index + 1 < units.len() {
        value /= 1024.0;
        unit_index += 1;
    }
    format!("{value:.1} {}", units[unit_index])
}
