use concord_client::model::AgentId;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct ProcessInfo {
    #[serde(rename = "sessionToken")]
    session_token: String,
}

#[derive(Debug, Serialize)]
struct ProcessConfiguration {
    #[serde(rename = "processInfo")]
    process_info: ProcessInfo,
}

#[derive(Debug, Serialize)]
struct RunnerConfiguration {
    #[serde(rename = "agentId")]
    agent_id: AgentId,
}
