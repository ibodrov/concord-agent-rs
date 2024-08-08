use std::{
    env,
    fs::Permissions,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

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
use tokio::{
    fs::{self, create_dir_all, File, OpenOptions},
    io::BufReader,
    time::sleep,
};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::{debug, error, info, warn};
use url::Url;
use uuid::Uuid;

mod error {
    #[derive(Debug)]
    pub struct AppError {
        pub message: String,
    }

    impl AppError {
        pub fn new(message: &str) -> Self {
            Self {
                message: message.to_string(),
            }
        }
    }

    #[macro_export]
    macro_rules! app_error {
        ($($arg:tt)*) => {
            AppError {
                message: format!($($arg)*),
            }
        };
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
    dotenv().ok();

    tracing_subscriber::fmt::init();

    let agent_id = AgentId(Uuid::default());
    debug!("Agent ID: {agent_id}");

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
    {
        let file = File::open(state_file_path).await?;
        let out_dir = temp_dir.join("state");
        unzip(file, &out_dir).await?;
        debug!("State file extracted to {out_dir:?}");
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
        .append_to_log_segment(process_id, segment_id, "Hello, world!".into())
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

fn sanitize_file_path(path: &str) -> PathBuf {
    path.replace('\\', "/")
        .split('/') // TODO sanitize components
        .collect()
}

async fn unzip(archive_file: File, out_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let archive = BufReader::new(archive_file).compat();

    let mut reader = ZipFileReader::new(archive).await?;

    let entries = reader
        .file()
        .entries()
        .iter()
        .map(|e| (e.filename().clone(), e.unix_permissions()))
        .enumerate()
        .collect::<Box<[_]>>();

    for (index, (filename, permissions)) in entries {
        let filename = sanitize_file_path(filename.as_str()?);
        let entry_is_dir = filename.ends_with("/");
        let path = out_dir.join(filename);

        if entry_is_dir {
            if !path.exists() {
                create_dir_all(&path).await?;
            }
        } else {
            let mut entry_reader = reader.reader_without_entry(index).await?;

            let parent = path.parent().ok_or_else(|| {
                app_error!("Failed to get parent path of the target file: {path:?}")
            })?;
            if !parent.is_dir() {
                create_dir_all(parent)
                    .await
                    .map_err(|e| app_error!("Failed to create parent directories: {e}"))?;
            }

            let writer = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .await
                .map_err(|e| app_error!("Failed to open file for writing: {e}"))?;

            futures_util::io::copy(&mut entry_reader, &mut writer.compat_write())
                .await
                .map_err(|e| app_error!("Failed to copy ZipEntry data: {e}"))?;

            #[cfg(unix)]
            {
                if let Some(permissions) = permissions {
                    let permissions = Permissions::from_mode(permissions as u32);
                    fs::set_permissions(&path, permissions).await?;
                }
            }
        }
    }

    Ok(())
}
