use futures_util::TryFutureExt;
use kube::Client;
use tracing::info;

use crate::{app_error, error::AppError};

pub async fn run() -> Result<(), AppError> {
    Client::try_default()
        .map_err(|e| app_error!("Can't connect to K8s: {e}"))
        .await?;
    info!("Connected to Kubernetes API...");

    Ok(())
}
