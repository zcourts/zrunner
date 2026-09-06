use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

pub const JOB_SCHEMA: &str = "zrunner.job.v1";
pub const CONTROL_SCHEMA: &str = "zrunner.control.v1";
pub const EVENT_SCHEMA: &str = "zrunner.event.v1";
pub const OUTPUT_SCHEMA: &str = "zrunner.output.v1";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum JobProfile {
    Rust,
    DockerBuild,
    Generic,
    IoHeavy,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum CompileSlots {
    Auto(String),
    Exact(u16),
}

impl Default for CompileSlots {
    fn default() -> Self {
        Self::Auto("auto".to_owned())
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResourceRequest {
    #[serde(default)]
    pub compile_slots: CompileSlots,
    #[serde(default)]
    pub memory_mib: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Job {
    pub schema: String,
    pub id: Ulid,
    pub runner: String,
    pub group: String,
    pub cwd: String,
    pub argv: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub priority: i32,
    pub profile: JobProfile,
    #[serde(default)]
    pub resources: ResourceRequest,
    #[serde(default)]
    pub locks: Vec<String>,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
    #[serde(default)]
    pub retry_on_runner_restart: u16,
    #[serde(default = "default_output_ttl")]
    pub output_ttl_seconds: u64,
}

impl Job {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema != JOB_SCHEMA {
            return Err("unsupported job schema");
        }
        if self.runner.is_empty() || self.group.is_empty() || self.cwd.is_empty() {
            return Err("runner, group, and cwd are required");
        }
        if self.argv.is_empty() || self.argv[0].is_empty() {
            return Err("argv must contain a program");
        }
        if self.timeout_seconds == 0 || self.output_ttl_seconds == 0 {
            return Err("timeouts must be positive");
        }
        if matches!(&self.resources.compile_slots, CompileSlots::Auto(value) if value != "auto") {
            return Err("compile_slots string must be auto");
        }
        if self.locks.iter().any(String::is_empty) {
            return Err("locks must not be empty");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ControlAction {
    Cancel,
    SetPriority { priority: i32 },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Control {
    pub schema: String,
    pub job: Ulid,
    #[serde(flatten)]
    pub action: ControlAction,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Accepted,
    Queued,
    Started,
    Interrupted,
    Completed,
    Failed,
    Cancelled,
    Rejected,
}

impl JobState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Rejected
        )
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct JobEvent {
    pub schema: String,
    pub id: Ulid,
    pub job: Ulid,
    pub runner: String,
    pub state: JobState,
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_sequence: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputEncoding {
    Utf8,
    Base64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct JobOutput {
    pub schema: String,
    pub job: Ulid,
    pub sequence: u64,
    pub stream: OutputStream,
    pub encoding: OutputEncoding,
    pub data: String,
    pub timestamp: DateTime<Utc>,
}

const fn default_timeout() -> u64 {
    3600
}

const fn default_output_ttl() -> u64 {
    86400
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_command() {
        let job = Job {
            schema: JOB_SCHEMA.to_owned(),
            id: Ulid::new(),
            runner: "debian1".to_owned(),
            group: "job-one".to_owned(),
            cwd: "/tmp".to_owned(),
            argv: Vec::new(),
            env: BTreeMap::new(),
            priority: 0,
            profile: JobProfile::Rust,
            resources: ResourceRequest::default(),
            locks: Vec::new(),
            timeout_seconds: 1,
            retry_on_runner_restart: 0,
            output_ttl_seconds: 1,
        };
        assert_eq!(job.validate(), Err("argv must contain a program"));
    }
}
