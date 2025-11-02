mod sas;
mod utils;

use crate::sas::generate_container_sas;
use crate::utils::format_friendly_duration;
use anyhow::Result;
use kube::{Api, Client};
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::{Patch, PatchParams};
use std::{collections::BTreeMap, fs};
use time::{OffsetDateTime, Duration, format_description::well_known::Rfc3339};
use tracing::{info, warn, error};
use tracing_subscriber::fmt::SubscriberBuilder;

#[derive(Clone)]
struct Config {
    root_cm_name: String,
    validity_hours: i64,
    recheck_hours: i64,
    pull_interval: u64,
}

impl Config {
    fn from_env() -> Result<Self> {
        fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> T {
            std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
        }

        Ok(Self {
            root_cm_name: std::env::var("SA_MAP")?,
            validity_hours: parse_env("SAS_TTL", 2),
            recheck_hours: parse_env("RECHECK", 1),
            pull_interval: parse_env("PULL_INTERVAL", 10),
        })
    }
}

enum RegenerationCheck {
    Valid(Duration),
    NeedsRegen,
}

#[tokio::main]
async fn main() -> Result<()> {
    SubscriberBuilder::default()
        .with_env_filter("info")
        .with_target(false)
        .init();

    let cfg = Config::from_env()?;
    let client = Client::try_default().await?;
    let namespace = fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")?
        .trim()
        .to_string();
    let api: Api<ConfigMap> = Api::namespaced(client, &namespace);

    info!("SAS Daemon started, pulling every {}s", cfg.pull_interval);

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(cfg.pull_interval));
    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal); // fix for Unpin error

    loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                info!("Shutdown signal received, exiting...");
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = pull_accounts(&api, &cfg).await {
                    error!(?e, "Error pulling accounts");
                }
            }
        }
    }

    info!("Daemon stopped.");
    Ok(())
}

async fn pull_accounts(api: &Api<ConfigMap>, cfg: &Config) -> Result<()> {
    let root_cm = match api.get(&cfg.root_cm_name).await {
        Ok(cm) => cm,
        Err(e) => {
            warn!(?e, "Failed to fetch root ConfigMap '{}'", cfg.root_cm_name);
            return Ok(());
        }
    };

    for (account, container) in root_cm.data.unwrap_or_default() {
        let api = api.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move {
            if let Err(e) = process_storage_account(&api, &account, &container, &cfg).await {
                error!(?e, %account, %container, "Error processing container");
            }
        });
    }

    Ok(())
}

async fn process_storage_account(api: &Api<ConfigMap>, account: &str, container: &str, cfg: &Config) -> Result<()> {
    let cm_name = format!("{account}-{container}");

    match check_ttl(api, &cm_name, cfg.recheck_hours).await? {
        RegenerationCheck::Valid(remaining) => {
            info!(%cm_name, left = %format_friendly_duration(remaining), "Token still valid");
            return Ok(());
        }
        RegenerationCheck::NeedsRegen => info!(%cm_name, "Generating new SAS token..."),
    }

    let (sas_token, expiry) = generate_container_sas(account, container, cfg.validity_hours).await?;

    let mut data = BTreeMap::new();
    data.insert("sas_token".into(), sas_token);
    data.insert("expiry".into(), expiry.format(&Rfc3339)?);
    data.insert("account".into(), account.into());
    data.insert("container".into(), container.into());

    let cm = ConfigMap {
        metadata: kube::api::ObjectMeta { name: Some(cm_name.clone()), ..Default::default() },
        data: Some(data),
        ..Default::default()
    };

    api.patch(&cm_name, &PatchParams::apply("sas-generator"), &Patch::Apply(&cm)).await?;
    info!(%cm_name, expires_at = %expiry.format(&Rfc3339)?, "SAS token updated");

    Ok(())
}

async fn check_ttl(api: &Api<ConfigMap>, cm_name: &str, recheck_hours: i64) -> Result<RegenerationCheck> {
    if let Ok(cm) = api.get(cm_name).await {
        if let Some(expiry_str) = cm.data.as_ref().and_then(|d| d.get("expiry")) {
            if let Ok(expiry) = OffsetDateTime::parse(expiry_str, &Rfc3339) {
                let remaining = expiry - OffsetDateTime::now_utc();
                if remaining.is_positive() && remaining > Duration::hours(recheck_hours) {
                    return Ok(RegenerationCheck::Valid(remaining));
                }
            }
        }
    }
    Ok(RegenerationCheck::NeedsRegen)
}
