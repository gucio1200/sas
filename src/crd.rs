use kube::{CustomResource, CustomResourceExt, ResourceExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, instrument};

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "sas.azure.com",
    version = "v1alpha1",
    kind = "SasGenerator",
    namespaced,
    status = "SasGeneratorStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct SasGeneratorSpec {
    pub storage_account: String,
    pub container_name: String,
    pub secret_name: Option<String>,
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

impl ContextData {
    pub fn new(client: kube::Client, sas_renewal_hours: i64, sas_ttl_hours: i64) -> Self {
        info!(
            renewal_hours = sas_renewal_hours,
            ttl_hours = sas_ttl_hours,
            "Initialized ContextData"
        );
        Self {
            client,
            sas_renewal_hours,
            sas_ttl_hours,
        }
    }
}

impl SasGenerator {
    #[instrument(skip(self))]
    pub fn target_secret_name(&self, override_name: Option<&str>) -> String {
        let result = match override_name {
            Some(name) => {
                debug!(%name, "Using override name for secret");
                name.to_string()
            }
            None => match &self.spec.secret_name {
                Some(name) => {
                    debug!(%name, "Using secret_name from CR spec");
                    name.clone()
                }
                None => {
                    let default_name =
                        format!("volsync-{}-{}", self.spec.storage_account, self.spec.container_name);
                    debug!(target_secret = %default_name, "Computed default secret name");
                    default_name
                }
            },
        };
        result
    }

    pub fn secret_labels(&self) -> std::collections::BTreeMap<String, String> {
        std::collections::BTreeMap::from([
            (
                "sas.azure.com/account".into(),
                self.spec.storage_account.clone(),
            ),
            (
                "sas.azure.com/container".into(),
                self.spec.container_name.clone(),
            ),
        ])
    }

    pub fn secret_annotations(&self, status_override: Option<&SasGeneratorStatus>) -> std::collections::BTreeMap<String, String> {
        let status = status_override.cloned().unwrap_or_else(|| self.status.clone().unwrap_or_default());
        std::collections::BTreeMap::from([
            (
                "sas.azure.com/generated".into(),
                status.generated.unwrap_or_default(),
            ),
            (
                "sas.azure.com/expires".into(),
                status.expiry.unwrap_or_default(),
            ),
        ])
    }

    pub fn log_spec(&self) {
        let cr_name = self.name_any();
        let target_secret = self.target_secret_name(None);
        let token_present = self
            .status
            .as_ref()
            .and_then(|s| s.token.as_ref())
            .is_some();
        let expiry = self.status.as_ref().and_then(|s| s.expiry.as_ref());

        info!(
            crd = %cr_name,
            account = %self.spec.storage_account,
            container = %self.spec.container_name,
            ttl = ?self.spec.sas_ttl_hours,
            renewal = ?self.spec.sas_renewal_hours,
            target_secret = %target_secret,
            token_present = %token_present,
            expiry = ?expiry,
            "Loaded SasGenerator spec and status"
        );
    }
}

#[instrument]
pub fn generate_crd() -> Result<(), Box<dyn std::error::Error>> {
    let crd = SasGenerator::crd();
    let yaml = serde_yaml::to_string(&crd)?;
    std::fs::write("crd.yaml", yaml)?;
    info!("CRD YAML generated successfully at crd.yaml");
    Ok(())
}
