use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "sas.example.com",
    version = "v1",
    kind = "SasGenerator",
    namespaced,
    status = "SasGeneratorStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct SasGeneratorSpec {
    pub storage_account: String,
    pub container_name: String,
    pub sas_ttl_hours: Option<i64>,
    pub sas_renewal_hours: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct SasGeneratorStatus {
    pub token: Option<String>,
    pub target_secret: Option<String>,
    pub generated: Option<String>,
    pub expiry: Option<String>,
}

#[derive(Clone)]
pub struct ContextData {
    pub client: kube::Client,
    pub sas_renewal_hours: i64,
    pub sas_ttl_hours: i64,
}

impl SasGenerator {
    pub fn target_secret_name(&self) -> String {
        self.status
            .as_ref()
            .and_then(|s| s.target_secret.clone())
            .unwrap_or_else(|| {
                format!(
                    "volsync-{}-{}",
                    self.spec.storage_account, self.spec.container_name
                )
            })
    }
}
