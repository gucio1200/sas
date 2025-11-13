use crate::crd::{ContextData, SasGenerator, SasGeneratorStatus};
use crate::sas::{generate_container_sas, SasTokenInfo};
use crate::secret::ensure_secret;
use crate::status::update_crd_status;
use kube::runtime::controller::Action;
use kube::ResourceExt;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use time::{Duration, OffsetDateTime};
use tracing::{error, info, instrument};

#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("Azure SAS generation error: {0}")]
    Azure(String),

    #[error("CRD apply failed: {0}")]
    CrdApply(String),
}

/// Returns true if the SAS token should be regenerated
fn should_regenerate(
    now: OffsetDateTime,
    status: &Option<SasGeneratorStatus>,
    renewal_hours: i64,
) -> bool {
    status
        .as_ref()
        .and_then(|s| s.expiry.as_ref())
        .map_or(true, |expiry| {
            let parsed =
                OffsetDateTime::parse(expiry, &time::format_description::well_known::Rfc3339)
                    .unwrap_or(now);
            now >= (parsed - Duration::hours(renewal_hours))
        })
}

/// Safely format OffsetDateTime to RFC3339 string
fn format_rfc3339(dt: OffsetDateTime) -> String {
    dt.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

/// Build a new SasGeneratorStatus from SAS token info and CRD
fn build_status(token_info: SasTokenInfo, sasgen: &SasGenerator) -> SasGeneratorStatus {
    SasGeneratorStatus {
        token: Some(token_info.token),
        target_secret: Some(sasgen.target_secret_name()), // centralized single source
        generated: Some(format_rfc3339(token_info.generated)),
        expiry: Some(format_rfc3339(token_info.expiry)),
    }
}

#[instrument(skip_all, fields(cr_name = %sasgen.name_any()))]
pub async fn reconcile(
    sasgen: Arc<SasGenerator>,
    ctx: Arc<ContextData>,
) -> Result<Action, ReconcileError> {
    let renewal_hours = sasgen
        .spec
        .sas_renewal_hours
        .unwrap_or(ctx.sas_renewal_hours);
    let ttl_hours = sasgen.spec.sas_ttl_hours.unwrap_or(ctx.sas_ttl_hours);
    let now = OffsetDateTime::now_utc();

    if should_regenerate(now, &sasgen.status, renewal_hours) {
        let token_info = generate_container_sas(
            &sasgen.spec.storage_account,
            &sasgen.spec.container_name,
            ttl_hours,
            now,
        )
        .await
        .map_err(|e| ReconcileError::Azure(e.to_string()))?;

        info!(new_expiry = %token_info.expiry, "Generated new SAS token");

        let new_status = build_status(token_info, &sasgen);

        // Apply status and ensure secret
        update_crd_status(&sasgen, &ctx, new_status.clone()).await?;
        ensure_secret(&sasgen, &ctx).await?;
    }

    Ok(Action::requeue(StdDuration::from_secs(15)))
}

pub fn error_policy(
    _obj: Arc<SasGenerator>,
    err: &ReconcileError,
    _ctx: Arc<ContextData>,
) -> Action {
    error!(?err, "Reconcile failed");
    Action::requeue(StdDuration::from_secs(300))
}
