use crate::crd::{ContextData, SasGenerator};
use crate::reconcile::ReconcileError;
use k8s_openapi::api::core::v1::Secret;
use kube::api::{Patch, PatchParams};
use kube::{Api, Resource, ResourceExt};
use std::collections::BTreeMap;

/// Labels for the Secret
pub fn secret_labels(spec: &crate::crd::SasGeneratorSpec) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("sas.azure.com/account".into(), spec.storage_account.clone()),
        (
            "sas.azure.com/container".into(),
            spec.container_name.clone(),
        ),
    ])
}

/// Annotations for the Secret
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

/// Secret data containing the SAS token
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

/// Ensure Secret exists or is updated
pub async fn ensure_secret(sasgen: &SasGenerator, ctx: &ContextData) -> Result<(), ReconcileError> {
    let ns = sasgen.namespace().unwrap_or_else(|| "default".into());
    let secret_name = sasgen
        .status
        .as_ref()
        .and_then(|s| s.target_secret.clone())
        .unwrap_or_else(|| sasgen.target_secret_name()); // use centralized method

    let api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);

    let status = sasgen.status.as_ref().ok_or_else(|| {
        ReconcileError::CrdApply("SasGenerator status missing, cannot create secret".into())
    })?;

    let secret = Secret {
        metadata: kube::api::ObjectMeta {
            name: Some(secret_name.clone()),
            namespace: Some(ns),
            labels: Some(secret_labels(&sasgen.spec)),
            annotations: Some(secret_annotations(status)),
            owner_references: Some(vec![sasgen.controller_owner_ref(&()).unwrap()]),
            ..Default::default()
        },
        string_data: Some(secret_data(&sasgen.spec, status.token.clone())),
        ..Default::default()
    };

    match api.get(&secret_name).await {
        Ok(_) => {
            api.patch(
                &secret_name,
                &PatchParams::apply("sas-operator").force(),
                &Patch::Apply(&secret),
            )
            .await?;
        }
        Err(kube::Error::Api(e)) if e.code == 404 => {
            api.create(&Default::default(), &secret).await?;
        }
        Err(e) => return Err(ReconcileError::Kube(e)),
    }

    Ok(())
}
