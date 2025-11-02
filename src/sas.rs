use azure_identity::WorkloadIdentityCredential;
use azure_storage::shared_access_signature::service_sas::BlobSasPermissions;
use azure_storage::prelude::SasToken;
use azure_storage_blobs::prelude::*;
use std::sync::Arc;
use time::{OffsetDateTime, Duration};
use anyhow::{Result, Context};
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tokio_retry::Retry;
use tracing::{info, debug, warn, instrument};

/// Generate a container SAS token with retry and structured tracing
#[instrument(skip_all, fields(account = %account, container = %container, expiry_hours = expiry_hours))]
pub async fn generate_container_sas(
    account: &str,
    container: &str,
    expiry_hours: i64,
) -> Result<(String, OffsetDateTime)> {
    let start = OffsetDateTime::now_utc();
    let expiry = start + Duration::hours(expiry_hours);

    info!(account = %account, container = %container, "Starting SAS token generation");

    // Initialize Azure credential
    let credential = create_credential()
        .context("Failed to create Azure WorkloadIdentityCredential")?;
    debug!(account = %account, container = %container, "Azure credential created successfully");

    let storage_credentials = azure_storage::StorageCredentials::token_credential(credential);
    let service_client = BlobServiceClient::new(account.to_string(), storage_credentials);
    let container_client = service_client.container_client(container);

    // Retry with exponential backoff and jitter
    let retry_strategy = ExponentialBackoff::from_millis(500)
        .factor(2)
        .max_delay(std::time::Duration::from_secs(30))
        .take(5)
        .map(jitter);

    debug!(account = %account, container = %container, "Starting SAS generation with retry strategy");

    // Retry operation without attempt counter
    let sas_token = Retry::spawn(retry_strategy, || async {
        match generate_client(&container_client, start, expiry, account, container).await {
            Ok(token) => {
                info!(account = %account, container = %container, "SAS token generated successfully on this attempt");
                Ok(token)
            },
            Err(e) => {
                warn!(account = %account, container = %container, error = ?e, "SAS generation failed; retrying...");
                Err(e)
            }
        }
    })
    .await
    .context("Failed to generate SAS token after all retries")?;

    info!(account = %account, container = %container, expiry = ?expiry, "SAS token generation completed successfully");
    Ok((sas_token, expiry))
}

/// Create WorkloadIdentityCredential from environment variables
#[instrument(skip_all)]
fn create_credential() -> Result<Arc<WorkloadIdentityCredential>> {
    debug!("Reading environment variables for Azure Workload Identity");

    let client_id = std::env::var("AZURE_CLIENT_ID")
        .context("AZURE_CLIENT_ID env var missing")?;
    let tenant_id = std::env::var("AZURE_TENANT_ID")
        .context("AZURE_TENANT_ID env var missing")?;
    let token_path = std::env::var("AZURE_FEDERATED_TOKEN_FILE")
        .unwrap_or_else(|_| "/var/run/secrets/azure/tokens/sa-token".to_string());

    debug!("Environment variables loaded; creating credential");

    let http_client = azure_core::new_http_client();
    let authority = url::Url::parse("https://login.microsoftonline.com/")
        .context("Failed to parse Azure authority URL")?;

    let credential = WorkloadIdentityCredential::new(
        http_client,
        authority,
        tenant_id,
        client_id,
        token_path,
    );

    debug!("Azure WorkloadIdentityCredential initialized");
    Ok(Arc::new(credential))
}

/// Generate SAS token using an existing container client
#[instrument(skip_all, fields(container = %container_client.container_name()))]
async fn generate_client(
    container_client: &ContainerClient,
    start: OffsetDateTime,
    expiry: OffsetDateTime,
    account: &str,
    container: &str,
) -> Result<String> {
    debug!(account = %account, container = %container, "Fetching user delegation key for SAS token");

    let permissions = BlobSasPermissions {
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

    let user_deligation_key = container_client
        .service_client()
        .get_user_deligation_key(start, expiry)
        .await
        .context("Failed to fetch user delegation key from Azure")?;

    debug!(account = %account, container = %container, "User delegation key fetched successfully");

    let client = container_client
        .user_delegation_shared_access_signature(
            permissions,
            &user_deligation_key.user_deligation_key,
        )
        .await
        .context("Failed to generate SAS token using delegation key")?;

    info!(account = %account, container = %container, "SAS token successfully generated for container");
    Ok(client.token()?)
}
