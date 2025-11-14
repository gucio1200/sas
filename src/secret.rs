use crate::crd::{ContextData, SasGenerator, SasGeneratorStatus};
use crate::reconcile::ReconcileError;
use k8s_openapi::api::core::v1::Secret;
use kube::api::{Patch, PatchParams};
use kube::{Api, Resource, ResourceExt};
use std::collections::BTreeMap;
use tracing::{debug, info, instrument, warn};

#[instrument(skip(ctx), fields(cr_name = %sasgen.name_any()))]
pub async fn ensure_secret(
    sasgen: &SasGenerator,
    ctx: &ContextData,
    secret_name: &str,
    labels: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
    status_override: Option<&SasGeneratorStatus>,
) -> Result<(), ReconcileError> {
    let ns = sasgen.namespace().unwrap_or_else(|| "default".into());
    info!(%secret_name, %ns, "Ensuring Secret exists or is up to date");

    let api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);

    // Use the override if provided, otherwise fall back to CRD status
    let status = status_override
        .cloned()
        .unwrap_or_else(|| sasgen.status.clone().unwrap_or_default());

    let secret = Secret {
        metadata: kube::api::ObjectMeta {
            name: Some(secret_name.to_string()),
            namespace: Some(ns.clone()),
            labels: Some(labels),
            annotations: Some(annotations),
            owner_references: Some(vec![sasgen.controller_owner_ref(&()).unwrap()]),
            ..Default::default()
        },
        string_data: Some(BTreeMap::from([
            ("sas_token".into(), status.token.clone().unwrap_or_default()),
            ("account".into(), sasgen.spec.storage_account.clone()),
            ("container".into(), sasgen.spec.container_name.clone()),
        ])),
        ..Default::default()
    };

    match api.get(secret_name).await {
        Ok(_) => {
            debug!(%secret_name, "Secret exists; applying patch");
            api.patch(
                secret_name,
                &PatchParams::apply("sas-operator").force(),
                &Patch::Apply(&secret),
            )
            .await?;
            info!(%secret_name, "Secret updated successfully");
        }
        Err(kube::Error::Api(e)) if e.code == 404 => {
            info!(%secret_name, "Secret not found; creating new one");
            api.create(&Default::default(), &secret).await?;
            info!(%secret_name, "Secret created successfully");
        }
        Err(e) => {
            warn!(%secret_name, ?e, "Failed to apply Secret changes");
            return Err(ReconcileError::Kube(e));
        }
    }

    Ok(())
}

