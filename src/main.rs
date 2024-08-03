use std::{env, sync::Arc, time::Duration};

use concord_client::{
    api_client::{self, ApiClient},
    model::{AgentId, ProcessStatus},
    queue_client::{self, CorrelationIdGenerator, ProcessResponse, QueueClient},
};
use dotenvy::dotenv;
use error::AppError;
use http::Uri;
use serde_json::json;
use tokio::time::sleep;
use tracing::{info, warn};
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

    let auth_header = env::var("AUTHORIZATION_HEADER")
        .map_err(|_| AppError::new("AUTHORIZATION_HEADER is not set"))?;

    let queue_client = Arc::new(
        QueueClient::connect(queue_client::Config {
            agent_id,
            uri,
            auth_header: auth_header.clone(),
            capabilities: json!({"runtime": "rs"}),
            ping_interval: Duration::from_secs(10),
        })
        .await?,
    );
    info!("Connected to the server");

    let correlation_id_gen = CorrelationIdGenerator::new();

    let api_client = ApiClient::new(api_client::Config {
        base_url: Url::parse("http://localhost:8001")?,
        auth_header,
    })?;

    let process_handler = {
        let queue_client = queue_client.clone();
        let correlation_id_gen = correlation_id_gen.clone();
        tokio::spawn(async move {
            loop {
                info!("Waiting for next process...");
                let correlation_id = correlation_id_gen.next();
                match queue_client.next_process(correlation_id).await {
                    Ok(ProcessResponse { process_id, .. }) => {
                        info!("Got process: {process_id}");
                        if let Err(e) = api_client
                            .update_status(process_id, agent_id, ProcessStatus::Running)
                            .await
                        {
                            warn!("Error while updating process status: {e}");
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
            info!("Waiting for next command...");
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
