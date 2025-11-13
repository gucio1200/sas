use crate::crd::{ContextData, SasGenerator};
use crate::reconcile::ReconcileError;
use k8s_openapi::api::core::v1::Secret;
use kube::api::{Patch, PatchParams};
use kube::{Api, Resource, ResourceExt};
use std::collections::BTreeMap;
use tracing::{debug, info, instrument, warn};

pub fn secret_labels(spec: &crate::crd::SasGeneratorSpec) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("sas.azure.com/account".into(), spec.storage_account.clone()),
        (
            "sas.azure.com/container".into(),
            spec.container_name.clone(),
        ),
    ])
}

pub fn secret_annotations(status: &crate::crd::SasGeneratorStatus) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "sas.azure.com/generated".into(),
            status.generated.clone().unwrap_or_default(),
        ),
        (
            "sas.azure.com/expires".into(),
            status.expiry.clone().unwrap_or_default(),
        ),
    ])
}

pub fn secret_data(
    spec: &crate::crd::SasGeneratorSpec,
    token: Option<String>,
) -> BTreeMap<String, String> {
    let token = token.unwrap_or_default();
    BTreeMap::from([
        ("sas_token".into(), token),
        ("account".into(), spec.storage_account.clone()),
        ("container".into(), spec.container_name.clone()),
    ])
}

#[instrument(skip(ctx), fields(cr_name = %sasgen.name_any()))]
pub async fn ensure_secret(sasgen: &SasGenerator, ctx: &ContextData) -> Result<(), ReconcileError> {
    let ns = sasgen.namespace().unwrap_or_else(|| "default".into());
    let secret_name = sasgen.target_secret_name();

    info!(%secret_name, %ns, "Ensuring Secret exists or is up to date");

    let api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);

    // Use a default empty status if None (first-run)
    let status = sasgen.status.clone().unwrap_or_default();

    let secret = Secret {
        metadata: kube::api::ObjectMeta {
            name: Some(secret_name.clone()),
            namespace: Some(ns.clone()),
            labels: Some(secret_labels(&sasgen.spec)),
            annotations: Some(secret_annotations(&status)),
            owner_references: Some(vec![sasgen.controller_owner_ref(&()).unwrap()]),
            ..Default::default()
        },
        string_data: Some(secret_data(&sasgen.spec, status.token.clone())),
        ..Default::default()
    };

    match api.get(&secret_name).await {
        Ok(_) => {
            debug!(%secret_name, "Secret exists; applying patch");
            api.patch(
                &secret_name,
                &PatchParams::apply("sas-operator").force(),
                &Patch::Apply(&secret),
            )
            .await?;
            info!(%secret_name, "Secret updated successfully");
        }
        Err(kube::Error::Api(e)) if e.code == 404 => {
            warn!(%secret_name, "Secret not found; creating new one");
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
