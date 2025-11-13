use kube::ResourceExt;
use kube::{CustomResource, CustomResourceExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, instrument};

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
    pub fn target_secret_name(&self) -> String {
        let name = self
            .status
            .as_ref()
            .and_then(|s| s.target_secret.clone())
            .unwrap_or_else(|| {
                let generated_name = format!(
                    "volsync-{}-{}",
                    self.spec.storage_account, self.spec.container_name
                );
                debug!(
                    ?generated_name,
                    "Target Secret name not set in status — computed automatically"
                );
                generated_name
            });

        debug!(target_secret = %name, "Resolved target Secret name");
        name
    }

    pub fn log_spec(&self) {
        let target_secret = self.target_secret_name();
        let token_present = self
            .status
            .as_ref()
            .and_then(|s| s.token.as_ref())
            .is_some();
        let expiry = self.status.as_ref().and_then(|s| s.expiry.as_ref());

        info!(
            crd = %self.name_any(),
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
