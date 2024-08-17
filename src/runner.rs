use std::{path::Path, str::FromStr};

use bollard::{
    container::{self, CreateContainerOptions, LogOutput, StartContainerOptions},
    secret::{ContainerWaitResponse, HostConfig, Mount},
    Docker,
};
use bytes::Bytes;
use concord_client::{
    api_client::ProcessApiClient,
    model::{AgentId, LogSegmentId, LogSegmentStatus, LogSegmentUpdateRequest, ProcessId},
};
use futures_util::StreamExt;
use serde::Serialize;
use tracing::{debug, warn};

use crate::{app_err, app_error, error::AppError};

impl From<bollard::errors::Error> for AppError {
    fn from(err: bollard::errors::Error) -> Self {
        AppError::new(&format!("{}", err))
    }
}

#[derive(Debug, Serialize)]
pub struct ApiConfiguration {
    #[serde(rename = "baseUrl")]
    pub base_url: String,
}

#[derive(Debug, Serialize)]
pub struct RunnerConfiguration {
    #[serde(rename = "agentId")]
    pub agent_id: AgentId,
    pub api: ApiConfiguration,
}

pub async fn run<'a>(
    runner_cfg: &RunnerConfiguration,
    process_id: ProcessId,
    work_dir: &Path,
    process_api: &'a ProcessApiClient<'a>,
) -> Result<(), AppError> {
    // Prepare the runner.json file
    let runner_json = serde_json::to_string(runner_cfg)
        .map_err(|e| app_error!("Failed to serialize runner.json: {:?}", e))?;

    let runner_json_path = work_dir.join("runner.json");
    tokio::fs::write(&runner_json_path, &runner_json)
        .await
        .map_err(|e| app_error!("Failed to write runner.json: {:?}", e))?;

    // Prepare the _instanceId file
    let instance_id = format!("{}", process_id);
    let instance_id_path = work_dir.join("_instanceId");
    tokio::fs::write(&instance_id_path, &instance_id)
        .await
        .map_err(|e| app_error!("Failed to write _instanceId: {:?}", e))?;

    // Connect to Docker
    let docker = Docker::connect_with_local_defaults()?;

    let container_name = format!("runner-{}", process_id);
    let image = "ibodrov/concord-runner-v2-image:latest";
    let host_path = work_dir.to_string_lossy();
    let container_path = "/work";
    let config_path = "/work/runner.json";

    let concord_version = "2.15.1-SNAPSHOT"; // Replace with your actual version
    let java_args = format!(
        "JAVA_ARGS=-cp /home/concord/.m2/repository/com/walmartlabs/concord/runtime/v2/concord-runner-v2/{0}/concord-runner-v2-{0}-jar-with-dependencies.jar com.walmartlabs.concord.runtime.v2.runner.Main {1}",
        concord_version,
        config_path
    );

    let config = container::Config {
        image: Some(image),
        host_config: Some(HostConfig {
            mounts: Some(vec![Mount {
                target: Some(container_path.to_string()),
                source: Some(host_path.to_string()),
                typ: Some(bollard::models::MountTypeEnum::BIND),
                read_only: Some(false),
                ..Default::default()
            }]),
            auto_remove: Some(true),
            network_mode: Some("host".to_string()),
            ..Default::default()
        }),
        env: Some(vec![&java_args]),
        working_dir: Some(container_path),
        ..Default::default()
    };

    // Create the container
    let create_options = Some(CreateContainerOptions {
        name: container_name.clone(),
        ..Default::default()
    });
    docker.create_container(create_options, config).await?;

    // Start the container
    docker
        .start_container(&container_name, None::<StartContainerOptions<String>>)
        .await?;

    // Attach to the container to capture stdout and stderr separately
    let mut logs = docker.logs::<String>(
        &container_name,
        Some(bollard::container::LogsOptions {
            follow: true,
            stdout: true,
            stderr: true,
            ..Default::default()
        }),
    );

    // Process the logs line by line
    while let Some(log) = logs.next().await {
        match log {
            Ok(LogOutput::StdOut { message } | LogOutput::StdErr { message }) => {
                let line = parse_log_line(message)?;
                match line {
                    LogLine::Segmented {
                        msg_length,
                        segment_id,
                        status,
                        warnings,
                        errors,
                        msg,
                    } => {
                        // TODO retries
                        // TODO validate msg_length
                        if msg_length != 0 {
                            process_api
                                .append_to_log_segment(process_id, segment_id, Bytes::from(msg))
                                .await
                                .map_err(|e| {
                                    app_error!("Error while appending to a log segment: {e}")
                                })?;
                        }

                        process_api
                            .update_log_segment(
                                process_id,
                                segment_id,
                                &LogSegmentUpdateRequest {
                                    status: Some(status),
                                    warnings: Some(warnings),
                                    errors: Some(errors),
                                },
                            )
                            .await
                            .map_err(|e| {
                                app_error!("Error while updating a log segment status: {e}")
                            })?;
                    }
                    LogLine::Regular { msg } => {
                        // TODO retries
                        process_api
                            .append_to_log_segment(
                                process_id,
                                LogSegmentId::default(),
                                Bytes::from(msg),
                            )
                            .await
                            .map_err(|e| {
                                app_error!("Error while appending to a log segment: {e}")
                            })?;
                    }
                }
            }
            Err(err) => {
                warn!("Error while parsing container log output: {}", err);
            }
            _ => {}
        }
    }

    // Wait for the container to finish and get the exit code
    let mut wait_stream = docker.wait_container::<String>(&container_name, None);
    if let Some(result) = wait_stream.next().await {
        match result {
            Ok(ContainerWaitResponse { status_code, .. }) => {
                debug!(status_code);
                return Ok(());
            }
            Err(e) => {
                return app_err!("Error while waiting for container: {:?}", e);
            }
        }
    }

    app_err!("Container exited unexpectedly")
}

#[derive(Debug)]
enum LogLine {
    Segmented {
        msg_length: u16,
        segment_id: LogSegmentId,
        status: LogSegmentStatus,
        warnings: u16,
        errors: u16,
        msg: Vec<u8>,
    },
    Regular {
        msg: Vec<u8>,
    },
}

fn parse_as_utf8_lossy<T: FromStr<Err = impl std::fmt::Display>>(ab: &[u8]) -> Result<T, AppError> {
    let s = String::from_utf8_lossy(ab);
    s.parse().map_err(|e| app_error!("Can't parse {s}: {e}"))
}

fn parse_log_segment_status(ab: &[u8]) -> Result<LogSegmentStatus, AppError> {
    let s = String::from_utf8_lossy(ab);
    let i = s
        .parse::<i32>()
        .map_err(|e| app_error!("Can't parse {s}: {e}"))?;
    match i {
        0 => Ok(LogSegmentStatus::Running),
        1 => Ok(LogSegmentStatus::Ok),
        2 => Ok(LogSegmentStatus::Suspended),
        3 => Ok(LogSegmentStatus::Failed),
        _ => app_err!("Invalid LogSegmentStatus: {i}"),
    }
}

fn parse_log_line(line: Bytes) -> Result<LogLine, AppError> {
    if line.is_empty() || line[0] != b'|' {
        return Ok(LogLine::Regular { msg: line.to_vec() });
    }

    // |msgLength|segmentId|status|warnings|errors|msg
    let mut parts = line.split(|&b| b == b'|');

    let _ = parts.next(); // skip the first empty part

    let msg_length = parts
        .next()
        .ok_or_else(|| app_error!("Invalid log line, expected a 'msg_length' field"))
        .and_then(parse_as_utf8_lossy)?;

    let segment_id = parts
        .next()
        .ok_or_else(|| app_error!("Invalid log line, expected a 'segment_id' field"))
        .and_then(parse_as_utf8_lossy)
        .map(LogSegmentId::new)?;

    let status = parts
        .next()
        .ok_or_else(|| app_error!("Invalid log line, expected a 'status' field"))
        .and_then(parse_log_segment_status)?;

    let warnings = parts
        .next()
        .ok_or_else(|| app_error!("Invalid log line, expected a 'warnings' field"))
        .and_then(parse_as_utf8_lossy)?;

    let errors = parts
        .next()
        .ok_or_else(|| app_error!("Invalid log line, expected a 'errors' field"))
        .and_then(parse_as_utf8_lossy)?;

    let msg = parts
        .next()
        .ok_or_else(|| app_error!("Invalid log line, expected a 'msg' field"))
        .map(|v| v.to_vec())?;

    Ok(LogLine::Segmented {
        msg_length,
        segment_id,
        status,
        warnings,
        errors,
        msg,
    })
}
