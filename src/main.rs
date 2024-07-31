use std::{env, sync::Arc, time::Duration};

use concord_client::{Config, Message, QueueClient};
use dotenvy::dotenv;
use http::Uri;
use tokio::{sync::Mutex, time::{interval, sleep}};
use tracing::{error, info};

#[derive(Debug)]
struct AppError {
    message: String,
}

impl std::error::Error for AppError {}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    dotenv().ok();

    let uri = "ws://localhost:8001/websocket".parse::<Uri>()?;
    info!("Connecting to {}", uri);

    let auth_header = env::var("AUTHORIZATION_HEADER").map_err(|_| AppError {
        message: "AUTHORIZATION_HEADER is not set".to_string(),
    })?;

    let config = Config { uri, auth_header };
    let queue_client = Arc::new(Mutex::new(QueueClient::connect(config).await?));
    info!("Connected to the server");

    // ping the server every 30 seconds
    let ping_interval = Duration::from_secs(10);
    let ping_client = queue_client.clone();
    tokio::spawn(async move {
        loop {
            sleep(ping_interval).await;
            let mut queue_client = ping_client.lock().await;
            if let Err(err) = queue_client.send_message(Message::Ping).await {
                error!("Failed to send ping message: {}", err);
            }
            info!("Sent ping message");
        }
    });

    loop {
        info!("Waiting for messages");

        let mut queue_client = queue_client.lock().await;
        match queue_client.receive_message().await? {
            Message::CommandResponse { correlation_id } => todo!(),
            Message::ProcessResponse { correlation_id, session_token, process_id } => todo!(),
            Message::Ping => {
                queue_client.send_message(Message::Pong).await?;
                info!("Sent pong message");
            },
            Message::Pong => {},
            _ => {}
        }

        sleep(Duration::from_secs(5)).await;
    }
}
