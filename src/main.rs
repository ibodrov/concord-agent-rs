use std::{path::PathBuf, time::Duration};

use anyhow::Context;
use concord_client::{
    api_client::{self, ApiClient},
    model::{AgentId, ApiToken, ProcessId, ProcessStatus, SessionToken},
    queue_client::{self, ProcessResponse, QueueClient},
};
use runner::{ApiConfiguration, RunnerConfiguration};
use serde_json::json;
use tokio::{
    fs::{File, create_dir_all},
    signal,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, info, warn};
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
        "controller" => {
            controller::run().await?;
            // doesn't automatically reconnects
        }
        "standalone" => {
            // automatically reconnects
        }
        _ => anyhow::bail!("Invalid MODE: {mode}"),
    };

    let _max_concurrency = std::env::var("MAX_CONCURRENCY") // TODO
        .map_or(Ok(10), |s| s.parse())
        .context("Invalid MAX_CONCURRENCY value")?;

    let cancellation_token = CancellationToken::new();

    let token = cancellation_token.clone();
    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(_) => {
                debug!("Received a shutdown signal...");
                token.cancel();
            }
            Err(_) => warn!("Unable to listen for shutdown signal."),
        }
    });

    let tracker = TaskTracker::new();

    loop {
        if cancellation_token.is_cancelled() {
            break;
        }

        let queue_client_config = queue_client::Config {
            agent_id,
            uri: uri.clone(),
            api_token: api_token.clone(),
            capabilities: json!({}),
            ping_interval: Duration::from_secs(10),
        };

        match QueueClient::connect(queue_client_config).await {
            Ok(queue_client) => {
                info!("Connected");
                tokio::select! {
                    _ = cancellation_token.cancelled() => break,
                    cmd = queue_client.next_command() => {
                        match cmd {
                            Ok(_) => {
                                warn!("TODO Actually handle commands");
                            },
                            Err(e) => {
                                warn!("Failed to receive a command: {e}")
                            }
                        }
                    },
                    proc = queue_client.next_process() => {
                        match proc {
                            Ok(ProcessResponse { session_token, process_id, .. }) => {
                                tracker.spawn(async move { handle_process(agent_id, session_token, process_id).await });
                            }
                            Err(e) => {
                                warn!("Failed to receive a process: {e}")
                            },
                        }

                    }
                }
            }
            Err(e) => {
                warn!("Queue client error: {e}. Reconnecting in 5 seconds...");
                tokio::select! {
                    _ = cancellation_token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(5)) => continue,
                }
            }
        }
    }

    debug!("Shutting down...");
    tracker.close();
    tracker.wait().await;
    Ok(())
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
