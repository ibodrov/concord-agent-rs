use http::{Request, Uri};
use tokio_tungstenite::connect_async;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let api_key = std::env::var("SERVER_API_KEY").expect("SERVER_API_KEY must be set");
    let uri = Uri::from_static("ws://localhost:8001/websocket");
    let host = uri.host().expect("Invalid host in WebSocket URL");

    let request = Request::builder()
        .uri(&uri)
        .header("Host", host)
        .header("Authorization", api_key)
        .header("Connection", "upgrade")
        .header("Upgrade", "websocket")
        .header("User-Agent", "concord-agent-rs")
        .header("sec-websocket-key", "concord-agent-rs")
        .header("sec-websocket-version", 13)
        .body(())
        .unwrap();
    let (ws_session, _) = connect_async(request).await.unwrap();

    info!("Connected to {}", uri);
}

