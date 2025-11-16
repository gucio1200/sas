use crate::crd::{ContextData, SasGenerator, SasGeneratorStatus};
use crate::reconcile::ReconcileError;
use kube::api::{Api, Patch, PatchParams};
use kube::ResourceExt;
use tracing::{debug, info, instrument, warn};

#[instrument(skip(ctx), fields(cr_name = %sasgen.name_any()))]
pub async fn update_crd_status(
    sasgen: &SasGenerator,
    ctx: &ContextData,
    status: SasGeneratorStatus,
) -> Result<(), ReconcileError> {
    let ns = sasgen.namespace().unwrap_or_else(|| "default".into());
    let name = sasgen.name_any();

    debug!(
        %name,
        %ns,
        has_token = status.token.is_some(),
        has_expiry = status.expiry.is_some(),
        "Patching CRD status"
    );

    let api: Api<SasGenerator> = Api::namespaced(ctx.client.clone(), &ns);

    let patch = serde_json::json!({ "status": status });

    let params = PatchParams::default();

    api.patch_status(&name, &params, &Patch::Merge(&patch))
        .await
        .map(|_| info!(%name, "CRD status successfully updated"))
        .map_err(|e| {
            warn!(%name, ?e, "Failed to patch CRD status");
            ReconcileError::CrdApply(format!("Failed to patch CRD status: {e}"))
        })
}
