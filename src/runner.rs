use std::path::Path;

use bollard::{
    container::{self, CreateContainerOptions, LogOutput, StartContainerOptions},
    secret::{ContainerWaitResponse, HostConfig, Mount},
    Docker,
};
use bytes::Bytes;
use concord_client::{
    api_client::ProcessApiClient,
    model::{AgentId, LogSegmentId, LogSegmentUpdateRequest, ProcessId},
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
    let env = format!("RUNNER_ARGS={config_path}");

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
        env: Some(vec![&env]),
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
                let mut segments = Vec::new();
                let mut invalid_segments = Vec::new();
                let pos =
                    segment_header_parser::parse(&message, &mut segments, &mut invalid_segments)?; // TODO handle error here instead of bubbling up
                if pos != message.len() {
                    warn!(
                        "Unparsed data: {:?}",
                        String::from_utf8_lossy(&message[pos..])
                    );
                }

                for segment_header_parser::Segment { header, msg_start } in segments {
                    if header.length > 0 {
                        let msg = message[msg_start..msg_start + header.length].to_vec();
                        process_api
                            .append_to_log_segment(process_id, header.segment_id, Bytes::from(msg))
                            .await
                            .map_err(|e| {
                                app_error!("Error while appending to a log segment: {e}")
                            })?;
                    }

                    process_api
                        .update_log_segment(
                            process_id,
                            header.segment_id,
                            &LogSegmentUpdateRequest {
                                status: Some(header.status),
                                warnings: Some(header.warn_count),
                                errors: Some(header.error_count),
                            },
                        )
                        .await
                        .map_err(|e| {
                            app_error!("Error while updating a log segment status: {e}")
                        })?;
                }

                for pos in invalid_segments {
                    let msg = message[pos.start..pos.end].to_vec();
                    process_api
                        .append_to_log_segment(
                            process_id,
                            LogSegmentId::default(),
                            Bytes::from(msg),
                        )
                        .await
                        .map_err(|e| app_error!("Error while appending to a log segment: {e}"))?;
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

mod segment_header_parser {
    use concord_client::model::{LogSegmentId, LogSegmentStatus};
    use core::str;
    use std::str::FromStr;

    use crate::{app_err, app_error, error::AppError};

    #[derive(Clone, PartialEq)]
    pub struct LogSegmentHeader {
        pub length: usize,
        pub segment_id: LogSegmentId,
        pub status: LogSegmentStatus,
        pub warn_count: u16,
        pub error_count: u16,
    }

    pub struct Segment {
        pub header: LogSegmentHeader,
        pub msg_start: usize,
    }

    pub struct Position {
        pub start: usize,
        pub end: usize,
    }

    impl Position {
        pub fn new(start: usize, end: usize) -> Self {
            Self { start, end }
        }
    }

    enum Field {
        MsgLength,
        SegmentId,
        Status,
        Warnings,
        Errors,
    }

    impl Field {
        fn next(&self) -> Option<Field> {
            match self {
                Field::MsgLength => Some(Field::SegmentId),
                Field::SegmentId => Some(Field::Status),
                Field::Status => Some(Field::Warnings),
                Field::Warnings => Some(Field::Errors),
                Field::Errors => None,
            }
        }

        fn parse_from_utf8<F, E>(value: &[u8]) -> Result<F, AppError>
        where
            F: FromStr<Err = E>,
            E: std::fmt::Display,
        {
            let str = str::from_utf8(value).map_err(|e| app_error!("{}", e))?;
            str.parse().map_err(|e| app_error!("{}", e))
        }

        pub fn parse_status_from_utf8(value: &[u8]) -> Result<LogSegmentStatus, AppError> {
            let i = Self::parse_from_utf8(value)?;
            match i {
                0 => Ok(LogSegmentStatus::Running),
                1 => Ok(LogSegmentStatus::Ok),
                2 => Ok(LogSegmentStatus::Suspended),
                3 => Ok(LogSegmentStatus::Error),
                _ => app_err!("Invalid segment_status value: {i}"),
            }
        }

        fn process(
            &self,
            value: &[u8],
            mut builder: LogSegmentHeader,
        ) -> Result<LogSegmentHeader, AppError> {
            match self {
                Field::MsgLength => {
                    builder.length = Self::parse_from_utf8(value)?;
                }
                Field::SegmentId => {
                    builder.segment_id = LogSegmentId::new(Self::parse_from_utf8(value)?);
                }
                Field::Status => builder.status = Self::parse_status_from_utf8(value)?,
                Field::Warnings => {
                    builder.warn_count = Self::parse_from_utf8(value)?;
                }
                Field::Errors => {
                    builder.error_count = Self::parse_from_utf8(value)?;
                }
            }

            Ok(builder)
        }
    }

    pub fn parse(
        ab: &[u8],
        segments: &mut Vec<Segment>,
        invalid_segments: &mut Vec<Position>,
    ) -> Result<usize, AppError> {
        enum State {
            FindHeader,
            FieldData,
            EndField,
        }

        let mut state = State::FindHeader;
        let mut field = Field::MsgLength;
        let mut current_header = LogSegmentHeader {
            length: 0,
            segment_id: LogSegmentId::default(),
            status: LogSegmentStatus::Running,
            warn_count: 0,
            error_count: 0,
        };

        let mut mark = None;
        let mut pos = 0;
        let mut field_start = 0;
        let mut continue_parse = true;

        while continue_parse {
            match state {
                State::FindHeader => {
                    if pos >= ab.len() {
                        continue_parse = false;
                        continue;
                    }

                    let ch = ab[pos];
                    pos += 1;

                    if ch == b'|' {
                        if let Some(m) = mark {
                            invalid_segments.push(Position::new(m, pos - 1));
                        }
                        mark = Some(pos - 1);
                        field_start = pos;
                        state = State::FieldData;
                    } else if mark.is_none() {
                        mark = Some(pos - 1);
                    }
                }
                State::FieldData => {
                    if pos >= ab.len() {
                        continue_parse = false;
                        continue;
                    }

                    let ch = ab[pos];
                    pos += 1;

                    if ch == b'|' {
                        state = State::EndField;
                        continue;
                    }

                    if !ch.is_ascii_digit() || pos - field_start > 20 {
                        field = Field::MsgLength;
                        state = State::FindHeader;
                        continue;
                    }
                }
                State::EndField => {
                    let field_value = &ab[field_start..pos - 1];
                    if field_value.is_empty() {
                        field = Field::MsgLength;
                        state = State::FindHeader;
                        pos -= 1;
                        continue;
                    }

                    let field_str = &ab[field_start..pos - 1];
                    current_header = field.process(field_str, current_header)?;

                    field = match field.next() {
                        Some(f) => f,
                        None => {
                            let header = current_header.clone();
                            let header_len = header.length;
                            segments.push(Segment {
                                header,
                                msg_start: pos,
                            });
                            let actual_length = std::cmp::min(header_len, ab.len() - pos);
                            pos += actual_length;

                            field = Field::MsgLength;
                            mark = None;
                            state = State::FindHeader;
                            continue;
                        }
                    };

                    field_start = pos;
                    state = State::FieldData;
                }
            }
        }

        if let Some(m) = mark {
            if matches!(state, State::FindHeader) {
                invalid_segments.push(Position::new(m, ab.len()));
                return Ok(ab.len());
            } else {
                return Ok(m);
            }
        }

        Ok(ab.len())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        // Helper functions for tests
        fn msg(ab: &[u8], segment: &Segment) -> String {
            let to = std::cmp::min(ab.len(), segment.msg_start + segment.header.length);
            String::from_utf8_lossy(&ab[segment.msg_start..to]).to_string()
        }

        fn bb(segment_id: i64, msg: &str) -> Vec<u8> {
            let ab = msg.as_bytes().to_vec();
            let header = serialize_header(&LogSegmentHeader {
                segment_id: LogSegmentId::new(segment_id),
                length: ab.len(),
                warn_count: 1,
                error_count: 2,
                status: LogSegmentStatus::Running,
            });

            [header, ab].concat()
        }

        fn serialize_header(header: &LogSegmentHeader) -> Vec<u8> {
            format!(
                "|{}|{}|{}|{}|{}|",
                header.length,
                header.segment_id,
                header.status as usize,
                header.warn_count,
                header.error_count
            )
            .as_bytes()
            .to_vec()
        }

        #[test]
        fn test1() {
            let log = "hello";
            let ab = bb(1, log);

            let mut segments = Vec::new();
            let mut invalid_segments = Vec::new();
            let result = parse(&ab, &mut segments, &mut invalid_segments).unwrap();

            assert_eq!(ab.len(), result);
            assert_eq!(1, segments.len());
            assert_eq!(log, msg(&ab, &segments[0]));
            assert_eq!(0, invalid_segments.len());
        }

        #[test]
        fn test1_1() {
            let ab = [bb(1, "hello-1"), bb(2, "hello-21")].concat();

            let mut segments = Vec::new();
            let mut invalid_segments = Vec::new();
            let result = parse(&ab, &mut segments, &mut invalid_segments).unwrap();

            assert_eq!(ab.len(), result);
            assert_eq!(2, segments.len());
            assert_eq!("hello-1", msg(&ab, &segments[0]));
            assert_eq!("hello-21", msg(&ab, &segments[1]));
            assert_eq!(0, invalid_segments.len());
        }

        #[test]
        fn test2() {
            let log = "hello";
            let ab = log.as_bytes().to_vec();

            let mut segments = Vec::new();
            let mut invalid_segments = Vec::new();
            let result = parse(&ab, &mut segments, &mut invalid_segments).unwrap();

            assert_eq!(ab.len(), result);
            assert_eq!(0, segments.len());
            assert_eq!(1, invalid_segments.len());
            let i = &invalid_segments[0];
            assert_eq!(log, String::from_utf8_lossy(&ab[i.start..i.end]));
        }

        #[test]
        fn test3() {
            let log = "hello";
            let mut ab = vec![b'1', b'2', b'3'];
            ab.extend(bb(2, log));

            let mut segments = Vec::new();
            let mut invalid_segments = Vec::new();
            let result = parse(&ab, &mut segments, &mut invalid_segments).unwrap();

            assert_eq!(ab.len(), result);
            assert_eq!(1, segments.len());
            assert_eq!("hello", msg(&ab, &segments[0]));
            assert_eq!(1, invalid_segments.len());
            let i = &invalid_segments[0];
            assert_eq!("123", String::from_utf8_lossy(&ab[i.start..i.end]));
        }

        #[test]
        fn test4() {
            let log = "hello";
            let mut ab = vec![b'1', b'2', b'3'];
            ab.extend(bb(2, log));

            let mut segments = Vec::new();
            let mut invalid_segments = Vec::new();
            let result = parse(&ab, &mut segments, &mut invalid_segments).unwrap();

            assert_eq!(ab.len(), result);
            assert_eq!(1, segments.len());
            assert_eq!(1, invalid_segments.len());
            let i = &invalid_segments[0];
            assert_eq!("123", String::from_utf8_lossy(&ab[i.start..i.end]));
        }

        #[test]
        fn test5() {
            let ab = vec![b'|', b'5', b'|', b'2', b'|', b'1'];

            let mut segments = Vec::new();
            let mut invalid_segments = Vec::new();
            let result = parse(&ab, &mut segments, &mut invalid_segments).unwrap();

            assert_eq!(0, result);
            assert_eq!(0, segments.len());
            assert_eq!(0, invalid_segments.len());
        }

        #[test]
        fn test6() {
            let ab = vec![b'a', b'b', b'c', b'|', b'5', b'|', b'2', b'|', b'1'];

            let mut segments = Vec::new();
            let mut invalid_segments = Vec::new();
            let result = parse(&ab, &mut segments, &mut invalid_segments).unwrap();

            assert_eq!(3, result);
            assert_eq!(0, segments.len());
            assert_eq!(1, invalid_segments.len());
            let i = &invalid_segments[0];
            assert_eq!("abc", String::from_utf8_lossy(&ab[i.start..i.end]));
        }

        #[test]
        fn test7() {
            let log = "hello";
            let full = bb(1, log);
            let ab = full[0..full.len() - 3].to_vec();

            let mut segments = Vec::new();
            let mut invalid_segments = Vec::new();
            let result = parse(&ab, &mut segments, &mut invalid_segments).unwrap();

            assert_eq!(ab.len(), result);
            assert_eq!(1, segments.len());
            assert_eq!("he", msg(&ab, &segments[0]));
            assert_eq!(0, invalid_segments.len());
        }

        #[test]
        fn test_parse_segment_end_marker() {
            let log = "|0|552|1|0|0|";
            let ab = log.as_bytes().to_vec();

            let mut segments = Vec::new();
            let mut invalid_segments = Vec::new();

            let result = parse(&ab, &mut segments, &mut invalid_segments).unwrap();

            assert_eq!(ab.len(), result);
            assert_eq!(1, segments.len());
            let s = &segments[0];
            assert_eq!(0, s.header.length);
            assert_eq!(LogSegmentStatus::Ok, s.header.status);
        }
    }
}
