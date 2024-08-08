use std::{env, path::PathBuf, sync::Arc, time::Duration};

use async_zip::base::read::seek::ZipFileReader;
use concord_client::{
    api_client::{self, ApiClient},
    model::{
        AgentId, ApiToken, LogSegmentRequest, LogSegmentStatus, LogSegmentUpdateRequest, ProcessId,
        ProcessStatus, SegmentCorrelationId,
    },
    queue_client::{self, CorrelationIdGenerator, ProcessResponse, QueueClient},
};
use dotenvy::dotenv;
use error::AppError;
use http::Uri;
use serde_json::json;
use tokio::{fs::File, io::BufReader, time::sleep};
use tracing::{debug, info, warn};
use url::Url;
use uuid::Uuid;

mod error {
    #[derive(Debug)]
    pub struct AppError {
        message: String,
    }

    impl AppError {
        pub fn new(message: &str) -> Self {
            Self {
                message: message.to_string(),
            }
        }
    }

    impl std::error::Error for AppError {}

    impl std::fmt::Display for AppError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.message)
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    dotenv().ok();

    let agent_id = AgentId(Uuid::default());

    let uri = "ws://localhost:8001/websocket".parse::<Uri>()?;
    info!("Connecting to {}", uri);

    let api_token = env::var("API_TOKEN")
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

    let correlation_id_gen = CorrelationIdGenerator::new();

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
                            warn!("Process error: {e}");
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

async fn handle_process(
    agent_id: AgentId,
    api_token: ApiToken,
    process_id: ProcessId,
) -> Result<(), Box<dyn std::error::Error>> {
    let api_client = ApiClient::new(api_client::Config {
        base_url: Url::parse("http://localhost:8001")?,
        api_token,
        temp_dir: PathBuf::from("/tmp"),
    })?;

    let process_api = api_client.process_api();

    let state_file_path = process_api.download_state(process_id).await?;
    {
        let file = File::open(state_file_path).await?;
        let zip = ZipFileReader::with_tokio(BufReader::new(file)).await?;
        for entry in zip.file().entries() {
            let filename = &entry
                .filename()
                .as_str()
                .map_err(|e| AppError::new(&format!("Process state archive error: {e}")))?;
            info!("Entry: {filename:?}");
        }
    }

    process_api
        .update_status(process_id, agent_id, ProcessStatus::Running)
        .await?;

    let segment_id = process_api
        .create_log_segment(
            process_id,
            &LogSegmentRequest {
                correlation_id: SegmentCorrelationId::new(Uuid::default()),
                name: "test".to_owned(),
            },
        )
        .await?;

    process_api
        .update_log_segment(
            process_id,
            segment_id,
            &LogSegmentUpdateRequest {
                status: Some(LogSegmentStatus::Ok),
                ..Default::default()
            },
        )
        .await?;

    process_api
        .update_status(process_id, agent_id, ProcessStatus::Finished)
        .await?;

    Ok(())
}
