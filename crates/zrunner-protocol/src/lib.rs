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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RustCodegenBackend {
    Cranelift,
    Llvm,
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
pub struct ExclusivityRequest {
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Job {
    pub schema: String,
    pub id: Ulid,
    pub runner: String,
    pub group: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub cwd: String,
    pub argv: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub priority: i32,
    pub profile: JobProfile,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rust_codegen_backend: Option<RustCodegenBackend>,
    #[serde(default)]
    pub resources: ResourceRequest,
    #[serde(default)]
    pub locks: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclusive: Option<ExclusivityRequest>,
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
        if self
            .project
            .as_ref()
            .is_some_and(|project| project.trim().is_empty())
        {
            return Err("project must not be empty");
        }
        if self.group != format!("job-{}", self.id.to_string().to_ascii_lowercase()) {
            return Err("group must be job-<lowercase job ULID>");
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
        if self.rust_codegen_backend.is_some() && !matches!(self.profile, JobProfile::Rust) {
            return Err("rust_codegen_backend requires the rust profile");
        }
        if self
            .exclusive
            .as_ref()
            .is_some_and(|request| request.reason.trim().is_empty())
        {
            return Err("exclusive jobs require a reason");
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
        let id = Ulid::new();
        let job = Job {
            schema: JOB_SCHEMA.to_owned(),
            id,
            runner: "debian1".to_owned(),
            group: format!("job-{}", id.to_string().to_ascii_lowercase()),
            project: None,
            cwd: "/tmp".to_owned(),
            argv: Vec::new(),
            env: BTreeMap::new(),
            priority: 0,
            profile: JobProfile::Rust,
            rust_codegen_backend: None,
            resources: ResourceRequest::default(),
            locks: Vec::new(),
            exclusive: None,
            timeout_seconds: 1,
            retry_on_runner_restart: 0,
            output_ttl_seconds: 1,
        };
        assert_eq!(job.validate(), Err("argv must contain a program"));
    }

    #[test]
    fn rejects_a_group_that_does_not_match_the_job_id() {
        let mut job = Job {
            schema: JOB_SCHEMA.to_owned(),
            id: Ulid::new(),
            runner: "debian1".to_owned(),
            group: "job-wrong".to_owned(),
            project: None,
            cwd: "/tmp".to_owned(),
            argv: vec!["true".to_owned()],
            env: BTreeMap::new(),
            priority: 0,
            profile: JobProfile::Generic,
            rust_codegen_backend: None,
            resources: ResourceRequest::default(),
            locks: Vec::new(),
            exclusive: None,
            timeout_seconds: 1,
            retry_on_runner_restart: 0,
            output_ttl_seconds: 1,
        };
        let expected = format!("job-{}", job.id.to_string().to_ascii_lowercase());
        assert_eq!(
            job.validate(),
            Err("group must be job-<lowercase job ULID>")
        );
        job.group = expected;
        assert_eq!(job.validate(), Ok(()));
    }

    #[test]
    fn rust_codegen_override_requires_the_rust_profile() {
        let id = Ulid::new();
        let job = Job {
            schema: JOB_SCHEMA.to_owned(),
            id,
            runner: "debian1".to_owned(),
            group: format!("job-{}", id.to_string().to_ascii_lowercase()),
            project: None,
            cwd: "/tmp".to_owned(),
            argv: vec!["true".to_owned()],
            env: BTreeMap::new(),
            priority: 0,
            profile: JobProfile::Generic,
            rust_codegen_backend: Some(RustCodegenBackend::Llvm),
            resources: ResourceRequest::default(),
            locks: Vec::new(),
            exclusive: None,
            timeout_seconds: 1,
            retry_on_runner_restart: 0,
            output_ttl_seconds: 1,
        };
        assert_eq!(
            job.validate(),
            Err("rust_codegen_backend requires the rust profile")
        );
    }
}
