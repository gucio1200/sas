use crate::crd::{ContextData, SasGenerator, SasGeneratorStatus};
use crate::reconcile::ReconcileError;
use kube::api::{Api, Patch, PatchParams};
use kube::CustomResourceExt;
use kube::ResourceExt;

/// Patch the CRD status
pub async fn update_crd_status(
    sasgen: &SasGenerator,
    ctx: &ContextData,
    status: SasGeneratorStatus,
) -> Result<(), ReconcileError> {
    let ns = sasgen.namespace().unwrap_or_else(|| "default".into());
    let api: Api<SasGenerator> = Api::namespaced(ctx.client.clone(), &ns);

    let patch = Patch::Apply(&SasGenerator {
        metadata: kube::api::ObjectMeta {
            name: Some(sasgen.name_any()),
            namespace: sasgen.namespace(),
            ..Default::default()
        },
        status: Some(status),
        spec: sasgen.spec.clone(),
    });

    let params = PatchParams::apply("sas-operator").force();
    api.patch_status(&sasgen.name_any(), &params, &patch)
        .await
        .map_err(|e| ReconcileError::CrdApply(format!("Failed to patch CRD status: {e}")))?;

    Ok(())
}

/// Generate CRD YAML
pub fn generate_crd_yaml() -> Result<(), Box<dyn std::error::Error>> {
    let crd = SasGenerator::crd();
    let yaml = serde_yaml::to_string(&crd)?;
    std::fs::write("crd.yaml", yaml)?;
    println!("CRD YAML generated at crd.yaml");
    Ok(())
}
