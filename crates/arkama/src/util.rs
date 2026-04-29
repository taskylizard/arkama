use std::path::Path;
use std::time::{Duration, Instant};

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

pub fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs == 0 {
        let millis = duration.subsec_millis();
        if millis == 0 {
            return "0s".to_string();
        }
        return format!("{millis}ms");
    }

    let days = secs / 86_400;
    let hours = (secs % 86_400) / 3_600;
    let minutes = (secs % 3_600) / 60;
    let seconds = secs % 60;

    let units = [(days, "d"), (hours, "h"), (minutes, "m"), (seconds, "s")];
    let mut parts = Vec::new();
    for (value, suffix) in units {
        if value == 0 {
            continue;
        }
        parts.push(format!("{value}{suffix}"));
        if parts.len() == 2 {
            break;
        }
    }

    parts.join(" ")
}

pub fn format_speed(bytes_per_sec: u64) -> String {
    format!("{}/s", format_bytes(bytes_per_sec as i64))
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{format_duration, format_speed};

    #[test]
    fn test_format_duration_uses_compact_human_units() {
        assert_eq!(format_duration(Duration::from_millis(850)), "850ms");
        assert_eq!(format_duration(Duration::from_secs(65)), "1m 5s");
        assert_eq!(format_duration(Duration::from_secs(7_381)), "2h 3m");
    }

    #[test]
    fn test_format_speed_humanizes_bytes_per_second() {
        assert_eq!(format_speed(1_536), "1.5 KB/s");
    }
}
