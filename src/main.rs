use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use concord_client::{
    api_client::{self, ApiClient},
    model::{AgentId, ApiToken, ProcessId, ProcessStatus, SessionToken},
    queue_client::{self, CorrelationIdGenerator, ProcessResponse, QueueClient},
};
use runner::{ApiConfiguration, RunnerConfiguration};
use serde_json::json;
use tokio::{
    fs::{File, create_dir_all},
    time::sleep,
};
use tracing::{debug, error, info, warn};
use url::Url;
use utils::unzip;

mod controller;
mod runner;
mod utils;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt::init();

    let agent_id = AgentId::default();
    debug!("Agent ID: {agent_id}");

    let uri = "ws://localhost:8001/websocket"
        .parse::<http::Uri>()
        .context("Invalid WEBSOCKET_URL")?;

    info!("Connecting to {}", uri);

    let api_token = std::env::var("API_TOKEN")
        .context("API_TOKEN is not set")
        .map(ApiToken::new)?;

    let mode = std::env::var("MODE").context("MODE is not set")?;
    match mode.as_str() {
        "controller" => controller::run().await?,
        "standalone" => {}
        _ => anyhow::bail!("Invalid MODE: {mode}"),
    };

    let max_concurrency = std::env::var("MAX_CONCURRENCY")
        .map_or(Ok(10), |s| s.parse())
        .context("Invalid MAX_CONCURRENCY value")?;

    let queue_client = {
        let api_token = api_token.clone();
        Arc::new(
            QueueClient::connect(queue_client::Config {
                agent_id,
                uri,
                api_token,
                capabilities: json!({"runtime": "rs"}),
                ping_interval: Duration::from_secs(10),
            })
            .await
            .context("Can't connect to the websocket")?,
        )
    };
    info!("Connected to the server");

    let correlation_id_gen = CorrelationIdGenerator::default();

    let process_handlers: Vec<_> = (0..max_concurrency)
        .map(|handler_id| {
            spawn_process_handler(
                handler_id,
                queue_client.clone(),
                correlation_id_gen.clone(),
                agent_id,
            )
        })
        .collect();

    let command_handler = spawn_command_handler(queue_client.clone(), correlation_id_gen.clone());

    tokio::select! {
        _ = async {
            for handler in process_handlers {
                handler.await?;
            }
            Ok::<_, tokio::task::JoinError>(())
        } => {
            error!("Process handler completed unexpectedly")
        }
        _ = command_handler => {
            error!("Command handler exited unexpectedly");
        }
    }

    Ok(())
}

fn spawn_process_handler(
    handler_id: usize,
    queue_client: Arc<QueueClient>,
    correlation_id_gen: CorrelationIdGenerator,
    agent_id: AgentId,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            debug!(handler_id, "Waiting for next process...");
            let correlation_id = correlation_id_gen.next();
            match queue_client.next_process(correlation_id).await {
                Ok(ProcessResponse {
                    process_id,
                    session_token,
                    ..
                }) => {
                    info!(handler_id, "Running {process_id}...");
                    if let Err(e) = handle_process(agent_id, session_token, process_id).await {
                        error!("Process failed: {e}. Continuing in 5 seconds...");
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                }
                Err(e) => {
                    warn!(
                        handler_id,
                        "Failed to get next process: {e}. Retrying in 5 seconds..."
                    );
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    })
}

fn spawn_command_handler(
    queue_client: Arc<QueueClient>,
    correlation_id_gen: CorrelationIdGenerator,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            debug!("Waiting for next command...");
            let correlation_id = correlation_id_gen.next();
            match queue_client.next_command(correlation_id).await {
                Ok(cmd) => {
                    info!("Got command: {cmd:?}");
                }
                Err(e) => {
                    warn!("Failed to get next command: {e}. Retrying in 5 seconds...");
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    })
}

#[tracing::instrument(fields(process_id = display(process_id)), skip(agent_id, session_token))]
async fn handle_process(
    agent_id: AgentId,
    session_token: SessionToken,
    process_id: ProcessId,
) -> anyhow::Result<()> {
    let temp_dir = PathBuf::from(format!("/tmp/{process_id}"));
    create_dir_all(&temp_dir).await?;
    debug!("Temporary directory: {temp_dir:?}");

    let base_url = "http://localhost:8001".to_string();
    let api_client = ApiClient::new(api_client::Config {
        base_url: Url::parse(&base_url)?,
        session_token,
        temp_dir: temp_dir.clone(),
    })?;

    let process_api = api_client.process_api();

    // download state
    let state_file_path = process_api
        .download_state(process_id)
        .await
        .context("Failed to download the process state")?;
    let work_dir = {
        let file = File::open(state_file_path).await?;
        let out_dir = temp_dir.join("state");
        unzip(file, &out_dir).await?;
        debug!("State file extracted to {out_dir:?}");
        out_dir
    };

    // execute process
    let runner_cfg = RunnerConfiguration {
        agent_id,
        api: ApiConfiguration { base_url },
    };
    runner::run(&runner_cfg, process_id, &work_dir, &process_api).await?;

    // TODO upload attachments

    // mark as FINISHED
    process_api
        .update_status(process_id, agent_id, ProcessStatus::Finished)
        .await
        .context("Failed to update process status to FINISHED")?;

    info!(status = ?ProcessStatus::Finished);

    Ok(())
}
