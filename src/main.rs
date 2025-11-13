mod crd;
mod reconcile;
mod sas;
mod secret;
mod status;

use crate::crd::{ContextData, SasGenerator};
use crate::reconcile::{error_policy, reconcile};
use futures::StreamExt;
use kube::{
    api::Api, runtime::controller::Controller, runtime::watcher::Config as WatcherConfig, Client,
};
use std::sync::Arc;
use tracing_subscriber::{fmt, EnvFilter};

/// Reads an environment variable and parses it into type `T`.
/// Returns `default` if the variable is not set or parsing fails.
fn env_var_or_default<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Holds configuration derived from environment variables
struct Config {
    sas_renewal_hours: i64,
    sas_ttl_hours: i64,
}

impl Config {
    fn from_env() -> Self {
        Self {
            sas_renewal_hours: env_var_or_default("SAS_RENEWAL_HOURS", 24),
            sas_ttl_hours: env_var_or_default("SAS_TTL_HOURS", 48),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing/logging
    fmt::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .with_ansi(true)
        .init();

    // Generate CRD YAML if requested
    if std::env::args().any(|arg| arg == "--crd") {
        crate::status::generate_crd_yaml()?;
        return Ok(());
    }

    // Kubernetes client
    let client = Client::try_default().await?;

    // Load configuration from environment variables
    let config = Config::from_env();

    // Context passed to reconcile
    let context = Arc::new(ContextData {
        client: client.clone(),
        sas_renewal_hours: config.sas_renewal_hours,
        sas_ttl_hours: config.sas_ttl_hours,
    });

    // Controller for the custom resource
    let cr_api = Api::<SasGenerator>::all(client.clone());
    Controller::new(cr_api, WatcherConfig::default())
        .run(reconcile, error_policy, context)
        .for_each(|res| async move {
            match res {
                Ok((_obj_ref, action)) => tracing::info!(?action, "Reconciliation complete"),
                Err(err) => tracing::error!(?err, "Controller error"),
            }
        })
        .await;

    Ok(())
}
