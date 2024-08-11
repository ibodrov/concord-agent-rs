use std::path::Path;

use bollard::{
    container::{self, CreateContainerOptions, LogOutput, StartContainerOptions},
    secret::{ContainerWaitResponse, HostConfig, Mount},
    Docker,
};
use concord_client::model::ProcessId;
use futures_util::StreamExt;

use crate::{app_err, app_error, error::AppError};

impl From<bollard::errors::Error> for AppError {
    fn from(err: bollard::errors::Error) -> Self {
        AppError::new(&format!("{}", err))
    }
}

pub async fn run(process_id: ProcessId, work_dir: &Path) -> Result<(), AppError> {
    // Prepare the runner.json file
    let runner_json = serde_json::to_string_pretty(
        &serde_json::json!({ "agentId": "00000000-0000-0000-0000-000000000000" }),
    )
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
        "JAVA_ARGS=-DlogLevel=INFO -cp /home/concord/.m2/repository/com/walmartlabs/concord/runtime/v2/concord-runner-v2/{0}/concord-runner-v2-{0}-jar-with-dependencies.jar com.walmartlabs.concord.runtime.v2.runner.Main {1}",
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

    // Process the logs line by line, differentiating between stdout and stderr
    while let Some(log) = logs.next().await {
        match log {
            Ok(LogOutput::StdOut { message }) => {
                print!("STDOUT: {}", String::from_utf8_lossy(&message));
            }
            Ok(LogOutput::StdErr { message }) => {
                eprint!("STDERR: {}", String::from_utf8_lossy(&message));
            }
            Err(err) => {
                eprintln!("Error: {}", err);
            }
            _ => {}
        }
    }

    // Wait for the container to finish and get the exit code
    let mut wait_stream = docker.wait_container::<String>(&container_name, None);
    if let Some(result) = wait_stream.next().await {
        match result {
            Ok(ContainerWaitResponse { status_code, .. }) => {
                println!("Container exited with status code: {}", status_code);
                return Ok(());
            }
            Err(e) => {
                eprintln!("Error while waiting for container: {:?}", e);
                return app_err!("Error while waiting for container: {:?}", e);
            }
        }
    }

    app_err!("Container exited unexpectedly")
}
