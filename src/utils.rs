use time::Duration;

/// Convert a Duration into a friendly string like "1h 30m left"
pub fn format_friendly_duration(d: Duration) -> String {
    let total_seconds = d.whole_seconds();
    if total_seconds <= 0 {
        return "expired".to_string();
    }
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;

    match (hours, minutes) {
        (0, m) => format!("{m}m left"),
        (h, 0) => format!("{h}h left"),
        (h, m) => format!("{h}h {m}m left"),
    }
}
