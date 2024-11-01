use std::{path::PathBuf, sync::Arc, time::Duration};

use concord_client::{
    api_client::{self, ApiClient},
    model::{AgentId, ApiToken, ProcessId, ProcessStatus},
    queue_client::{self, CorrelationIdGenerator, ProcessResponse, QueueClient},
};
use error::AppError;
use runner::{ApiConfiguration, RunnerConfiguration};
use serde_json::json;
use tokio::{
    fs::{create_dir_all, File},
    time::sleep,
};
use tracing::{debug, error, info, warn};
use url::Url;
use utils::unzip;

mod controller;
mod error;
mod runner;
mod utils;

#[tokio::main]
async fn main() -> Result<(), AppError> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt::init();

    let agent_id = AgentId::default();
    debug!("Agent ID: {agent_id}");

    let uri = "ws://localhost:8001/websocket"
        .parse::<http::Uri>()
        .map_err(|e| app_error!("Invalid WEBSOCKET_URL: {e}"))?;
    info!("Connecting to {}", uri);

    let api_token = std::env::var("API_TOKEN")
        .map_err(|_| AppError::new("API_TOKEN is not set"))
        .map(ApiToken::new)?;

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
            .map_err(|e| app_error!("Can't connect to the websocket: {e}"))?,
        )
    };
    info!("Connected to the server");

    let mode = std::env::var("MODE").map_err(|_| AppError::new("MODE is not set"))?;
    match mode.as_str() {
        "controller" => controller::run().await?,
        _ => return app_err!("Invalid MODE: {mode}"),
    };

    let correlation_id_gen = CorrelationIdGenerator::default();

    let process_handler = {
        let queue_client = queue_client.clone();
        let correlation_id_gen = correlation_id_gen.clone();
        tokio::spawn(async move {
            loop {
                debug!("Waiting for next process...");
                let correlation_id = correlation_id_gen.next();
                match queue_client.next_process(correlation_id).await {
                    Ok(ProcessResponse { process_id, .. }) => {
                        info!("Running {process_id}...");
                        if let Err(e) =
                            handle_process(agent_id, api_token.clone(), process_id).await
                        {
                            error!("{e}");
                            continue;
                        }
                    }
                    Err(e) => {
                        warn!("Error while getting next process: {e}");
                        warn!("Retrying in 5 seconds...");
                        sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        })
    };

    let command_handler = tokio::spawn(async move {
        loop {
            debug!("Waiting for next command...");
            let correlation_id = correlation_id_gen.next();
            match queue_client.next_command(correlation_id).await {
                Ok(cmd) => {
                    info!("Got command: {cmd:?}");
                }
                Err(e) => {
                    warn!("Error while getting next command: {e}");
                    warn!("Retrying in 5 seconds...");
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    });

    tokio::try_join!(process_handler, command_handler)
        .map_err(|e| app_error!("Error waiting for process and command handlers: {e}"))?;

    Ok(())
}

#[tracing::instrument(fields(process_id = display(process_id)), skip(agent_id, api_token))]
async fn handle_process(
    agent_id: AgentId,
    api_token: ApiToken,
    process_id: ProcessId,
) -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = PathBuf::from(format!("/tmp/{process_id}"));
    create_dir_all(&temp_dir).await?;
    debug!("Temporary directory: {temp_dir:?}");

    let base_url = "http://localhost:8001".to_string();
    let api_client = ApiClient::new(api_client::Config {
        base_url: Url::parse(&base_url)?,
        api_token,
        temp_dir: temp_dir.clone(),
    })?;

    let process_api = api_client.process_api();

    let state_file_path = process_api.download_state(process_id).await?;
    let work_dir = {
        let file = File::open(state_file_path).await?;
        let out_dir = temp_dir.join("state");
        unzip(file, &out_dir).await?;
        debug!("State file extracted to {out_dir:?}");
        out_dir
    };

    let runner_cfg = RunnerConfiguration {
        agent_id,
        api: ApiConfiguration { base_url },
    };
    runner::run(&runner_cfg, process_id, &work_dir, &process_api).await?;

    process_api
        .update_status(process_id, agent_id, ProcessStatus::Finished)
        .await?;

    info!(status = ?ProcessStatus::Finished);

    Ok(())
}
