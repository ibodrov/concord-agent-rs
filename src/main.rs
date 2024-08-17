use std::{path::PathBuf, sync::Arc, time::Duration};

use concord_client::{
    api_client::{self, ApiClient},
    model::{AgentId, ApiToken, ProcessId, ProcessStatus},
    queue_client::{self, CorrelationIdGenerator, ProcessResponse, QueueClient},
};
use error::AppError;
use runner::run;
use serde_json::json;
use tokio::{
    fs::{create_dir_all, File},
    time::sleep,
};
use tracing::{debug, error, info, warn};
use url::Url;
use utils::unzip;
use uuid::Uuid;

mod error;
mod model;
mod runner;
mod utils;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt::init();

    let agent_id = AgentId(Uuid::default());
    debug!("Agent ID: {agent_id}");

    let uri = "ws://localhost:8001/websocket".parse::<http::Uri>()?;
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
            .await?,
        )
    };
    info!("Connected to the server");

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
                        info!("Got process: {process_id}");
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

    tokio::try_join!(process_handler, command_handler)?;

    Ok(())
}

#[tracing::instrument(skip(agent_id, api_token))]
async fn handle_process(
    agent_id: AgentId,
    api_token: ApiToken,
    process_id: ProcessId,
) -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = PathBuf::from(format!("/tmp/{process_id}"));
    create_dir_all(&temp_dir).await?;
    debug!("Temporary directory: {temp_dir:?}");

    let api_client = ApiClient::new(api_client::Config {
        base_url: Url::parse("http://localhost:8001")?,
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

    run(process_id, &work_dir, &process_api).await?;

    process_api
        .update_status(process_id, agent_id, ProcessStatus::Finished)
        .await?;

    Ok(())
}
