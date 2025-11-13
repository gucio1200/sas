use anyhow::{Context, Result};
use azure_identity::{DefaultAzureCredential, TokenCredentialOptions};
use azure_storage::prelude::SasToken;
use azure_storage::shared_access_signature::service_sas::BlobSasPermissions;
use azure_storage_blobs::prelude::*;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;
use tracing::{debug, info, instrument, warn};

pub const SAS_PERMISSIONS: BlobSasPermissions = BlobSasPermissions {
    read: true,
    write: true,
    add: true,
    create: true,
    delete: true,
    delete_version: true,
    permanent_delete: true,
    list: true,
    tags: true,
    move_: true,
    execute: true,
    ownership: true,
    permissions: true,
};

#[derive(Debug, Clone)]
pub struct SasTokenInfo {
    pub token: String,
    pub expiry: OffsetDateTime,
    pub generated: OffsetDateTime,
}

#[instrument(skip_all, fields(account = %account, container = %container, expiry_hours = expiry_hours))]
pub async fn generate_container_sas(
    account: &str,
    container: &str,
    expiry_hours: i64,
    now: OffsetDateTime,
) -> Result<SasTokenInfo> {
    let start = now - Duration::seconds(5);
    let expiry = now + Duration::hours(expiry_hours);

    info!("Starting SAS token generation");

    let credential =
        create_credential().context("Failed to create Azure DefaultAzureCredential")?;
    debug!("Azure DefaultAzureCredential created successfully");

    let storage_credentials = azure_storage::StorageCredentials::token_credential(credential);
    let service_client = BlobServiceClient::new(account.to_string(), storage_credentials);
    let container_client = service_client.container_client(container);

    let retry_strategy = ExponentialBackoff::from_millis(500)
        .factor(2)
        .max_delay(std::time::Duration::from_secs(30))
        .take(5)
        .map(jitter);

    debug!("Starting SAS generation with retry strategy");

    let sas_token = Retry::spawn(retry_strategy, || async {
        match generate_client(&container_client, start, expiry).await {
            Ok(token) => {
                info!("SAS token generated successfully");
                Ok(token)
            }
            Err(e) => {
                warn!(?e, "SAS generation failed; retrying...");
                Err(e)
            }
        }
    })
    .await
    .context("Failed to generate SAS token after retries")?;

    info!(expiry = %expiry, "SAS token generation completed successfully");
    Ok(SasTokenInfo {
        token: sas_token,
        expiry,
        generated: now,
    })
}

#[instrument(skip_all)]
fn create_credential() -> Result<Arc<DefaultAzureCredential>> {
    debug!("Creating DefaultAzureCredential (auto-detects environment, managed identity, or workload identity)");

    let credential = DefaultAzureCredential::create(TokenCredentialOptions::default())
        .context("Failed to initialize DefaultAzureCredential")?;

    debug!("Azure DefaultAzureCredential initialized successfully");
    Ok(Arc::new(credential))
}

#[instrument(skip_all, fields(container = %container_client.container_name()))]
async fn generate_client(
    container_client: &ContainerClient,
    start: OffsetDateTime,
    expiry: OffsetDateTime,
) -> Result<String> {
    debug!("Generating SAS token for container");

    debug!("Fetching user delegation key");
    let user_delegation_key = container_client
        .service_client()
        .get_user_deligation_key(start, expiry)
        .await
        .context("Failed to fetch user delegation key")?;
    debug!("User delegation key fetched successfully");

    let client = container_client
        .user_delegation_shared_access_signature(
            SAS_PERMISSIONS,
            &user_delegation_key.user_deligation_key,
        )
        .await
        .context("Failed to generate SAS token")?;

    info!("SAS token successfully generated");
    Ok(client.token()?)
}
