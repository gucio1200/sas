use time::OffsetDateTime;
use tracing::warn;

pub fn format_rfc3339(dt: OffsetDateTime) -> String {
    dt.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|e| {
            warn!(?dt, ?e, "Failed to format OffsetDateTime to RFC3339");
            String::new()
        })
}
