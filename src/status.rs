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
    let api: Api<SasGenerator> = Api::namespaced(ctx.client.clone(), &ns);

    debug!(
        %name,
        %ns,
        has_token = status.token.is_some(),
        has_expiry = status.expiry.is_some(),
        "Preparing to patch CRD status"
    );

    let patch = Patch::Apply(&SasGenerator {
        metadata: kube::api::ObjectMeta {
            name: Some(name.clone()),
            namespace: sasgen.namespace(),
            ..Default::default()
        },
        spec: sasgen.spec.clone(),
        status: Some(status),
    });

    let params = PatchParams::apply("sas-operator").force();

    api.patch_status(&name, &params, &patch)
        .await
        .map(|_| info!(%name, "CRD status successfully updated"))
        .map_err(|e| {
            warn!(%name, ?e, "Failed to update CRD status");
            ReconcileError::CrdApply(format!("Failed to patch CRD status: {e}"))
        })
}
