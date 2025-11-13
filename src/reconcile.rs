use crate::crd::{ContextData, SasGenerator, SasGeneratorStatus};
use crate::sas::{generate_container_sas, SasTokenInfo};
use crate::secret::ensure_secret;
use crate::status::update_crd_status;
use crate::utils::format_rfc3339;
use kube::runtime::controller::Action;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use time::{Duration, OffsetDateTime};
use tracing::{error, info, instrument, warn};

#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("Azure SAS generation error: {0}")]
    Azure(String),

    #[error("CRD apply failed: {0}")]
    CrdApply(String),
}

fn should_regenerate(
    now: OffsetDateTime,
    status: &Option<SasGeneratorStatus>,
    renewal_hours: i64,
) -> bool {
    status
        .as_ref()
        .and_then(|s| s.expiry.as_ref())
        .map_or(true, |expiry| {
            match OffsetDateTime::parse(expiry, &time::format_description::well_known::Rfc3339) {
                Ok(parsed) => now >= (parsed - Duration::hours(renewal_hours)),
                Err(e) => {
                    warn!(
                        ?expiry,
                        ?e,
                        "Failed to parse expiry; will regenerate SAS token"
                    );
                    true
                }
            }
        })
}

fn build_status(token_info: SasTokenInfo, secret_name: &str) -> SasGeneratorStatus {
    SasGeneratorStatus {
        token: Some(token_info.token),
        target_secret: Some(secret_name.to_string()),
        generated: Some(format_rfc3339(token_info.generated)),
        expiry: Some(format_rfc3339(token_info.expiry)),
    }
}

pub fn error_policy(
    _obj: Arc<SasGenerator>,
    err: &ReconcileError,
    _ctx: Arc<ContextData>,
) -> Action {
    error!(?err, "Reconcile failed");
    Action::requeue(StdDuration::from_secs(300))
}

#[instrument(skip_all)]
pub async fn reconcile(
    sasgen: Arc<SasGenerator>,
    ctx: Arc<ContextData>,
) -> Result<Action, ReconcileError> {
    sasgen.log_spec();

    let now = OffsetDateTime::now_utc();
    let renewal_hours = sasgen.spec.sas_renewal_hours.unwrap_or(ctx.sas_renewal_hours);
    let ttl_hours = sasgen.spec.sas_ttl_hours.unwrap_or(ctx.sas_ttl_hours);

    if should_regenerate(now, &sasgen.status, renewal_hours) {
        let token_info = generate_container_sas(
            &sasgen.spec.storage_account,
            &sasgen.spec.container_name,
            ttl_hours,
            now,
        )
        .await
        .map_err(|e| ReconcileError::Azure(e.to_string()))?;

        let target_secret = sasgen.target_secret_name(None);
        let new_status = build_status(token_info, &target_secret);

        info!(new_expiry = %new_status.expiry.as_deref().unwrap_or_default(), "Generated new SAS token");

        let labels = sasgen.secret_labels();
        let annotations = sasgen.secret_annotations(Some(&new_status));

        ensure_secret(&sasgen, &ctx, &target_secret, labels, annotations).await?;
        update_crd_status(&sasgen, &ctx, new_status.clone()).await?;
    }

    Ok(Action::requeue(std::time::Duration::from_secs(15)))
}
