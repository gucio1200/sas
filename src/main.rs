mod crd;
mod reconcile;
mod sas;
mod secret;
mod status;
mod utils;

use crate::crd::{generate_crd, ContextData, SasGenerator};
use crate::reconcile::{error_policy, reconcile};
use futures::StreamExt;
use kube::{
    api::Api, runtime::controller::Controller, runtime::watcher::Config as WatcherConfig, Client,
};
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn env_var_or_default<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

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
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .with_ansi(true)
        .init();

    if std::env::args().any(|arg| arg == "--crd") {
        generate_crd()?;
        return Ok(());
    }

    let client = Client::try_default().await?;
    let config = Config::from_env();

    let context = Arc::new(ContextData::new(
        client.clone(),
        config.sas_renewal_hours,
        config.sas_ttl_hours,
    ));
    let cr_api = Api::<SasGenerator>::all(client.clone());

    let controller = Controller::new(cr_api, WatcherConfig::default())
        .run(reconcile, error_policy, context)
        .for_each(|res| async move {
            match res {
                Ok((_obj_ref, action)) => info!(?action, "Reconciliation complete"),
                Err(err) => error!(?err, "Controller error"),
            }
        });

    info!("Controller started; waiting for Ctrl+C to stop");
    tokio::select! {
        _ = controller => {},
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down gracefully");
        }
    }

    Ok(())
}
