use anyhow::Context;

use kube::Client;
use tracing::info;

pub async fn run() -> anyhow::Result<()> {
    Client::try_default()
        .await
        .context("Can't connect to Kubernetes API")?;
    info!("Connected to Kubernetes API...");
    todo!("rest of the owl")
}
