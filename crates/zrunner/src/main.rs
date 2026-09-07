use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Value, json};
use ulid::Ulid;
use zrunner_core::{Allocation, HostCapacity, Limits, Scheduler};
use zrunner_protocol::{
    CONTROL_SCHEMA, Control, ControlAction, EVENT_SCHEMA, JOB_SCHEMA, Job, JobEvent, JobOutput,
    JobProfile, JobState, OUTPUT_SCHEMA, OutputEncoding, OutputStream,
};

const DEFAULT_ROOT: &str = "/home/zcourts/projects/projects/.ai/message-board";
const FLUSH_BYTES: usize = 256 * 1024;
const FLUSH_AFTER: Duration = Duration::from_secs(2);
const TERMINATION_GRACE: Duration = Duration::from_secs(10);

#[derive(Debug, Deserialize)]
struct Config {
    #[serde(default = "default_zboard", alias = "aiboard")]
    zboard: PathBuf,
    #[serde(default = "default_root")]
    board_root: PathBuf,
    #[serde(default = "default_runner")]
    runner: String,
    #[serde(default)]
    docker_builder: Option<String>,
    #[serde(default)]
    limits: ConfigLimits,
}

#[derive(Debug, Default, Deserialize)]
struct ConfigLimits {
    max_running_jobs: Option<usize>,
    max_compile_slots: Option<u16>,
    host_memory_reserve_mib: Option<u64>,
    max_job_memory_mib: Option<u64>,
    cpu_pressure_limit: Option<f64>,
    memory_pressure_limit: Option<f64>,
    io_pressure_limit: Option<f64>,
}

impl Config {
    fn limits(&self) -> Limits {
        let defaults = Limits::default();
        Limits {
            max_running_jobs: self
                .limits
                .max_running_jobs
                .unwrap_or(defaults.max_running_jobs),
            max_compile_slots: self
                .limits
                .max_compile_slots
                .unwrap_or(defaults.max_compile_slots),
            host_memory_reserve_mib: self
                .limits
                .host_memory_reserve_mib
                .unwrap_or(defaults.host_memory_reserve_mib),
            max_job_memory_mib: self
                .limits
                .max_job_memory_mib
                .unwrap_or(defaults.max_job_memory_mib),
            cpu_pressure_limit: self
                .limits
                .cpu_pressure_limit
                .unwrap_or(defaults.cpu_pressure_limit),
            memory_pressure_limit: self
                .limits
                .memory_pressure_limit
                .unwrap_or(defaults.memory_pressure_limit),
            io_pressure_limit: self
                .limits
                .io_pressure_limit
                .unwrap_or(defaults.io_pressure_limit),
        }
    }
}

fn default_zboard() -> PathBuf {
    PathBuf::from("zboard")
}

fn default_root() -> PathBuf {
    PathBuf::from(DEFAULT_ROOT)
}

fn default_runner() -> String {
    fs::read_to_string("/etc/hostname")
        .unwrap_or_else(|_| "debian1".to_owned())
        .trim()
        .to_owned()
}

enum DaemonEvent {
    Board(Value),
    Output(Ulid, OutputStream, Vec<u8>),
    StreamClosed(Ulid),
}

struct OutputBuffer {
    bytes: Vec<u8>,
    last_flush: Instant,
}

impl Default for OutputBuffer {
    fn default() -> Self {
        Self {
            bytes: Vec::new(),
            last_flush: Instant::now(),
        }
    }
}

struct RunningJob {
    job: Job,
    child: Child,
    started: Instant,
    sequence: u64,
    stdout: OutputBuffer,
    stderr: OutputBuffer,
    streams_open: usize,
    termination: Option<Termination>,
}

struct Termination {
    sent_at: Instant,
    state: JobState,
}

fn main() {
    if let Err(error) = run_main() {
        eprintln!("zrunner: {error:#}");
        std::process::exit(2);
    }
}

fn run_main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let command = args.next().unwrap_or_else(|| "help".to_owned());
    if matches!(command.as_str(), "help" | "--help" | "-h") {
        println!("zrunner daemon [--config <path>]\nzrunner example-job");
        return Ok(());
    }
    if command == "example-job" {
        let id = Ulid::new();
        let job = Job {
            schema: JOB_SCHEMA.to_owned(),
            id,
            runner: default_runner(),
            group: format!("job-{}", id.to_string().to_ascii_lowercase()),
            cwd: std::env::current_dir()?.to_string_lossy().into_owned(),
            argv: vec![
                "cargo".to_owned(),
                "check".to_owned(),
                "--locked".to_owned(),
            ],
            env: Default::default(),
            priority: 0,
            profile: JobProfile::Rust,
            resources: Default::default(),
            locks: vec![format!("cargo-target:{}", default_runner())],
            timeout_seconds: 3600,
            retry_on_runner_restart: 1,
            output_ttl_seconds: 86400,
        };
        println!("{}", serde_json::to_string_pretty(&job)?);
        return Ok(());
    }
    if command != "daemon" {
        bail!("unknown command '{command}'");
    }
    let mut config_path = None;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(
                    args.next().context("--config requires a path")?,
                ));
            }
            unknown => bail!("unknown option '{unknown}'"),
        }
    }
    let config = load_config(config_path.as_deref())?;
    Daemon::start(config)?.run()
}

fn load_config(path: Option<&Path>) -> Result<Config> {
    let path = path
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("ZRUNNER_CONFIG").map(PathBuf::from));
    match path {
        Some(path) => serde_json::from_reader(BufReader::new(
            fs::File::open(&path).with_context(|| format!("open {}", path.display()))?,
        ))
        .with_context(|| format!("decode {}", path.display())),
        None => Ok(serde_json::from_value(json!({}))?),
    }
}

struct Daemon {
    config: Config,
    limits: Limits,
    board: Child,
    board_input: ChildStdin,
    events: Receiver<DaemonEvent>,
    event_tx: Sender<DaemonEvent>,
    scheduler: Scheduler,
    running: HashMap<Ulid, RunningJob>,
    terminal: HashSet<Ulid>,
    history_replayed: bool,
    pending_live_protocols: Vec<Value>,
    _lock: fs::File,
}

impl Daemon {
    fn start(config: Config) -> Result<Self> {
        let lock_path = std::env::temp_dir().join(format!("zrunner-{}.lock", config.runner));
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(&lock_path)?;
        let result = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            bail!("another zrunner daemon owns {}", lock_path.display());
        }

        let mut board = Command::new(&config.zboard)
            .arg("run")
            .arg("--root")
            .arg(&config.board_root)
            .args(["--project", "zrunner", "--session"])
            .arg(&config.runner)
            .arg("--path")
            .arg(env!("CARGO_MANIFEST_DIR"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .context("start Zboard transport")?;
        let board_input = board.stdin.take().context("open Zboard stdin")?;
        let board_output = board.stdout.take().context("open Zboard stdout")?;
        let (event_tx, events) = mpsc::channel();
        let board_tx = event_tx.clone();
        thread::spawn(move || {
            for line in BufReader::new(board_output).lines() {
                match line {
                    Ok(line) => match serde_json::from_str(&line) {
                        Ok(value) => {
                            if board_tx.send(DaemonEvent::Board(value)).is_err() {
                                break;
                            }
                        }
                        Err(error) => eprintln!("zrunner: invalid Zboard output: {error}"),
                    },
                    Err(error) => {
                        eprintln!("zrunner: read Zboard output: {error}");
                        break;
                    }
                }
            }
        });
        let limits = config.limits();
        Ok(Self {
            config,
            limits,
            board,
            board_input,
            events,
            event_tx,
            scheduler: Scheduler::default(),
            running: HashMap::new(),
            terminal: HashSet::new(),
            history_replayed: false,
            pending_live_protocols: Vec::new(),
            _lock: lock,
        })
    }

    fn run(mut self) -> Result<()> {
        self.board_command(json!({"op":"group.create","name":"zrunner"}))?;
        self.board_command(json!({"op":"group.join","name":"zrunner"}))?;
        self.board_command(json!({"op":"history","limit":1000}))?;
        let mut last_admission = Instant::now() - Duration::from_secs(10);
        loop {
            match self.events.recv_timeout(Duration::from_millis(250)) {
                Ok(event) => self.handle_event(event)?,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => bail!("event channel closed"),
            }
            while let Ok(event) = self.events.try_recv() {
                self.handle_event(event)?;
            }
            self.flush_due()?;
            self.poll_children()?;
            if self.scheduler.queued_len() > 0 && last_admission.elapsed() >= Duration::from_secs(5)
            {
                let host = HostCapacity::sample_linux()?;
                if let Some((job, allocation)) = self.scheduler.next(&self.limits, &host)? {
                    self.start_job(job, allocation)?;
                }
                last_admission = Instant::now();
            }
            if self.board.try_wait()?.is_some() {
                bail!("Zboard transport exited");
            }
        }
    }

    fn handle_event(&mut self, event: DaemonEvent) -> Result<()> {
        match event {
            DaemonEvent::Board(value) => self.handle_board(value),
            DaemonEvent::Output(id, stream, bytes) => {
                let running = self
                    .running
                    .get_mut(&id)
                    .context("output for unknown job")?;
                let buffer = match stream {
                    OutputStream::Stdout => &mut running.stdout,
                    OutputStream::Stderr => &mut running.stderr,
                };
                buffer.bytes.extend(bytes);
                if buffer.bytes.len() >= FLUSH_BYTES {
                    self.flush(id, stream)?;
                }
                Ok(())
            }
            DaemonEvent::StreamClosed(id) => {
                if let Some(running) = self.running.get_mut(&id) {
                    running.streams_open = running.streams_open.saturating_sub(1);
                }
                Ok(())
            }
        }
    }

    fn handle_board(&mut self, value: Value) -> Result<()> {
        match value.get("type").and_then(Value::as_str) {
            Some("message") => {
                if let Some(message) = value.get("message")
                    && let Some(protocol) = protocol_value(message)
                {
                    if self.history_replayed {
                        self.handle_protocol_value(protocol, true)?;
                    } else {
                        self.pending_live_protocols.push(protocol);
                    }
                }
            }
            Some("history") => {
                if !self.history_replayed {
                    let messages = value
                        .get("messages")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or_default();
                    for protocol in sorted_history_protocols(messages) {
                        self.handle_protocol_value(protocol, false)?;
                    }
                    self.history_replayed = true;
                    for protocol in std::mem::take(&mut self.pending_live_protocols) {
                        self.handle_protocol_value(protocol, true)?;
                    }
                }
            }
            Some("error") => eprintln!("zrunner: Zboard error: {value}"),
            _ => {}
        }
        Ok(())
    }

    fn handle_protocol_value(&mut self, value: Value, announce: bool) -> Result<()> {
        match value.get("schema").and_then(Value::as_str) {
            Some(JOB_SCHEMA) => {
                let mut job: Job = match serde_json::from_value(value) {
                    Ok(job) => job,
                    Err(error) => {
                        eprintln!("zrunner: reject malformed job: {error}");
                        return Ok(());
                    }
                };
                if job.runner == self.config.runner && !self.terminal.contains(&job.id) {
                    if matches!(job.profile, JobProfile::DockerBuild) {
                        let Some(builder) = self.config.docker_builder.as_deref() else {
                            self.publish_state(
                                &job,
                                JobState::Rejected,
                                Some("docker-build jobs are disabled on this runner"),
                                None,
                            )?;
                            self.terminal.insert(job.id);
                            return Ok(());
                        };
                        if let Err(error) = validate_docker_build(&job) {
                            self.publish_state(
                                &job,
                                JobState::Rejected,
                                Some(&format!("invalid docker-build job: {error:#}")),
                                None,
                            )?;
                            self.terminal.insert(job.id);
                            return Ok(());
                        }
                        job.locks.push(format!("buildkit:{builder}"));
                    }
                    let inserted = match self.scheduler.enqueue(job.clone()) {
                        Ok(inserted) => inserted,
                        Err(error) => {
                            self.publish_state(
                                &job,
                                JobState::Rejected,
                                Some(&format!("invalid job: {error:#}")),
                                None,
                            )?;
                            self.terminal.insert(job.id);
                            return Ok(());
                        }
                    };
                    if inserted {
                        self.board_command(json!({"op":"group.create","name":job.group}))?;
                        self.board_command(json!({"op":"group.join","name":job.group}))?;
                        if announce {
                            self.publish_state(&job, JobState::Accepted, None, None)?;
                            self.publish_state(&job, JobState::Queued, None, None)?;
                        }
                    }
                }
            }
            Some(CONTROL_SCHEMA) => {
                let control: Control = match serde_json::from_value(value) {
                    Ok(control) => control,
                    Err(error) => {
                        eprintln!("zrunner: ignore malformed control: {error}");
                        return Ok(());
                    }
                };
                match control.action {
                    ControlAction::Cancel => {
                        if let Some(job) = self.scheduler.cancel_queued(control.job) {
                            self.terminal.insert(job.id);
                            self.publish_state(
                                &job,
                                JobState::Cancelled,
                                Some("cancelled while queued"),
                                None,
                            )?;
                        } else if let Some(running) = self.running.get(&control.job) {
                            signal_group(running.child.id(), libc::SIGTERM)?;
                            let running = self.running.get_mut(&control.job).expect("running job");
                            running.termination = Some(Termination {
                                sent_at: Instant::now(),
                                state: JobState::Cancelled,
                            });
                        }
                    }
                    ControlAction::SetPriority { priority } => {
                        if !self.scheduler.set_priority(control.job, priority) {
                            eprintln!(
                                "zrunner: ignore priority change for unknown, running, or terminal job {}",
                                control.job
                            );
                        }
                    }
                }
            }
            Some(EVENT_SCHEMA) => {
                let event: JobEvent = match serde_json::from_value(value) {
                    Ok(event) => event,
                    Err(error) => {
                        eprintln!("zrunner: ignore malformed lifecycle event: {error}");
                        return Ok(());
                    }
                };
                if event.runner == self.config.runner && event.state.is_terminal() {
                    self.terminal.insert(event.job);
                    self.scheduler.cancel_queued(event.job);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn start_job(&mut self, job: Job, allocation: Allocation) -> Result<()> {
        validate_parallelism(&job, allocation.compile_slots)?;
        let mut command = Command::new(&job.argv[0]);
        command
            .args(&job.argv[1..])
            .current_dir(&job.cwd)
            .envs(&job.env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        if matches!(job.profile, JobProfile::Rust) {
            command.env("CARGO_BUILD_JOBS", allocation.compile_slots.to_string());
        } else if matches!(job.profile, JobProfile::DockerBuild) {
            command.env(
                "BUILDX_BUILDER",
                self.config
                    .docker_builder
                    .as_deref()
                    .context("docker builder is not configured")?,
            );
        }
        let mut child = command
            .spawn()
            .with_context(|| format!("start job {}", job.id))?;
        let stdout = child.stdout.take().context("capture stdout")?;
        let stderr = child.stderr.take().context("capture stderr")?;
        spawn_stream(job.id, OutputStream::Stdout, stdout, self.event_tx.clone());
        spawn_stream(job.id, OutputStream::Stderr, stderr, self.event_tx.clone());
        self.publish_state(
            &job,
            JobState::Started,
            Some(&format!(
                "compile_slots={} memory_mib={}",
                allocation.compile_slots, allocation.memory_mib
            )),
            None,
        )?;
        self.running.insert(
            job.id,
            RunningJob {
                job,
                child,
                started: Instant::now(),
                sequence: 0,
                stdout: OutputBuffer::default(),
                stderr: OutputBuffer::default(),
                streams_open: 2,
                termination: None,
            },
        );
        Ok(())
    }

    fn poll_children(&mut self) -> Result<()> {
        let ids: Vec<_> = self.running.keys().copied().collect();
        for id in ids {
            let timed_out = self.running.get(&id).is_some_and(|running| {
                running.started.elapsed().as_secs() >= running.job.timeout_seconds
            });
            if timed_out {
                let running = self.running.get_mut(&id).expect("running job");
                if running.termination.is_none() {
                    signal_group(running.child.id(), libc::SIGTERM)?;
                    running.termination = Some(Termination {
                        sent_at: Instant::now(),
                        state: JobState::Failed,
                    });
                }
            }
            let force_kill = self.running.get(&id).is_some_and(|running| {
                running
                    .termination
                    .as_ref()
                    .is_some_and(|termination| termination.sent_at.elapsed() >= TERMINATION_GRACE)
            });
            if force_kill {
                let running = self.running.get(&id).expect("running job");
                signal_group(running.child.id(), libc::SIGKILL)?;
            }
            let status = self
                .running
                .get_mut(&id)
                .expect("running job")
                .child
                .try_wait()?;
            if let Some(status) = status {
                self.flush(id, OutputStream::Stdout)?;
                self.flush(id, OutputStream::Stderr)?;
                let running = self.running.remove(&id).expect("running job");
                self.scheduler.finish(id);
                self.terminal.insert(id);
                let state = running.termination.as_ref().map_or_else(
                    || {
                        if status.success() {
                            JobState::Completed
                        } else {
                            JobState::Failed
                        }
                    },
                    |termination| termination.state,
                );
                let detail = format!("elapsed_ms={}", running.started.elapsed().as_millis());
                self.publish_state(&running.job, state, Some(&detail), status.code())?;
            }
        }
        Ok(())
    }

    fn flush_due(&mut self) -> Result<()> {
        let ids: Vec<_> = self.running.keys().copied().collect();
        for id in ids {
            for stream in [OutputStream::Stdout, OutputStream::Stderr] {
                let due = {
                    let running = self.running.get(&id).expect("running job");
                    let buffer = match stream {
                        OutputStream::Stdout => &running.stdout,
                        OutputStream::Stderr => &running.stderr,
                    };
                    !buffer.bytes.is_empty() && buffer.last_flush.elapsed() >= FLUSH_AFTER
                };
                if due {
                    self.flush(id, stream)?;
                }
            }
        }
        Ok(())
    }

    fn flush(&mut self, id: Ulid, stream: OutputStream) -> Result<()> {
        let running = self.running.get_mut(&id).context("flush unknown job")?;
        let buffer = match stream {
            OutputStream::Stdout => &mut running.stdout,
            OutputStream::Stderr => &mut running.stderr,
        };
        if buffer.bytes.is_empty() {
            return Ok(());
        }
        let bytes = std::mem::take(&mut buffer.bytes);
        buffer.last_flush = Instant::now();
        running.sequence += 1;
        let (encoding, data) = match String::from_utf8(bytes) {
            Ok(text) => (OutputEncoding::Utf8, text),
            Err(error) => (
                OutputEncoding::Base64,
                base64::engine::general_purpose::STANDARD.encode(error.into_bytes()),
            ),
        };
        let output = JobOutput {
            schema: OUTPUT_SCHEMA.to_owned(),
            job: id,
            sequence: running.sequence,
            stream,
            encoding,
            data,
            timestamp: Utc::now(),
        };
        let group = running.job.group.clone();
        let ttl = running.job.output_ttl_seconds;
        let summary = format!("job {id} {stream:?} output #{}", running.sequence);
        self.board_command(json!({
            "op":"send",
            "group":group,
            "message":summary,
            "meta":output,
            "ttl_seconds":ttl
        }))
    }

    fn publish_state(
        &mut self,
        job: &Job,
        state: JobState,
        detail: Option<&str>,
        exit_code: Option<i32>,
    ) -> Result<()> {
        let event = JobEvent {
            schema: EVENT_SCHEMA.to_owned(),
            id: Ulid::new(),
            job: job.id,
            runner: self.config.runner.clone(),
            state,
            timestamp: Utc::now(),
            detail: detail.map(str::to_owned),
            exit_code,
            last_sequence: self.running.get(&job.id).map(|running| running.sequence),
        };
        let summary = format!("job {} {state:?}", job.id).to_ascii_lowercase();
        self.board_command(json!({
            "op":"send",
            "group":"zrunner",
            "message":summary,
            "meta":event
        }))
    }

    fn board_command(&mut self, value: Value) -> Result<()> {
        serde_json::to_writer(&mut self.board_input, &value)?;
        self.board_input.write_all(b"\n")?;
        self.board_input.flush().context("flush Zboard command")
    }
}

fn protocol_value(message: &Value) -> Option<Value> {
    if let Some(meta) = message.get("meta").filter(|value| !value.is_null()) {
        return Some(meta.clone());
    }
    message
        .get("message")
        .and_then(Value::as_str)
        .and_then(|body| serde_json::from_str(body).ok())
}

fn history_replay_order(value: &Value) -> u8 {
    match value.get("schema").and_then(Value::as_str) {
        Some(EVENT_SCHEMA) => 0,
        Some(JOB_SCHEMA) => 1,
        Some(CONTROL_SCHEMA) => 2,
        _ => 3,
    }
}

fn sorted_history_protocols(history: &[Value]) -> Vec<Value> {
    let mut protocols: Vec<_> = history.iter().filter_map(protocol_value).collect();
    protocols.sort_by_key(history_replay_order);
    protocols
}

fn spawn_stream(
    id: Ulid,
    stream: OutputStream,
    mut reader: impl Read + Send + 'static,
    sender: Sender<DaemonEvent>,
) {
    thread::spawn(move || {
        let mut bytes = vec![0; 64 * 1024];
        loop {
            match reader.read(&mut bytes) {
                Ok(0) => break,
                Ok(count) => {
                    if sender
                        .send(DaemonEvent::Output(id, stream, bytes[..count].to_vec()))
                        .is_err()
                    {
                        return;
                    }
                }
                Err(error) => {
                    eprintln!("zrunner: read {stream:?} for {id}: {error}");
                    break;
                }
            }
        }
        let _ = sender.send(DaemonEvent::StreamClosed(id));
    });
}

fn validate_parallelism(job: &Job, allocation: u16) -> Result<()> {
    if !matches!(job.profile, JobProfile::Rust) {
        return Ok(());
    }
    for (index, argument) in job.argv.iter().enumerate() {
        let requested = if argument == "-j" || argument == "--jobs" {
            job.argv
                .get(index + 1)
                .and_then(|value| value.parse::<u16>().ok())
        } else {
            argument
                .strip_prefix("-j")
                .and_then(|value| value.parse::<u16>().ok())
        };
        if requested.is_some_and(|requested| requested > allocation) {
            bail!("job requests more Cargo jobs than its allocation of {allocation}");
        }
    }
    Ok(())
}

fn validate_docker_build(job: &Job) -> Result<()> {
    if job.argv.first().map(String::as_str) != Some("docker")
        || job.argv.get(1).map(String::as_str) != Some("buildx")
        || job.argv.get(2).map(String::as_str) != Some("build")
    {
        bail!("argv must begin with docker buildx build");
    }
    if job
        .argv
        .iter()
        .any(|argument| argument == "--builder" || argument.starts_with("--builder="))
    {
        bail!("the runner selects the bounded BuildKit builder");
    }
    Ok(())
}

fn signal_group(pid: u32, signal: i32) -> Result<()> {
    let result = unsafe { libc::kill(-(pid as i32), signal) };
    if result == 0 {
        Ok(())
    } else {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(())
        } else {
            Err(anyhow!(error)).context("signal job process group")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use zrunner_protocol::ResourceRequest;

    #[test]
    fn structured_meta_is_the_protocol_payload() {
        let message = json!({
            "message":"queue job",
            "meta":{"schema":"zrunner.job.v1","id":"01M1WKTEST"}
        });
        assert_eq!(
            protocol_value(&message),
            Some(json!({"schema":"zrunner.job.v1","id":"01M1WKTEST"}))
        );
    }

    #[test]
    fn legacy_json_string_remains_readable() {
        let message = json!({
            "message":"{\"schema\":\"zrunner.control.v1\",\"action\":\"cancel\"}"
        });
        assert_eq!(
            protocol_value(&message),
            Some(json!({"schema":"zrunner.control.v1","action":"cancel"}))
        );
    }

    #[test]
    fn history_replays_terminal_events_before_job_submissions() {
        assert_eq!(history_replay_order(&json!({"schema":EVENT_SCHEMA})), 0);
        assert_eq!(history_replay_order(&json!({"schema":JOB_SCHEMA})), 1);
        assert_eq!(history_replay_order(&json!({"schema":CONTROL_SCHEMA})), 2);
    }

    #[test]
    fn live_protocols_wait_behind_complete_history_replay() {
        let history = vec![json!({
            "message":"old durable job",
            "meta":{"schema":JOB_SCHEMA,"id":"old-job"}
        })];
        let pending = [json!({"schema":JOB_SCHEMA,"id":"new-job"})];
        let mut protocols = sorted_history_protocols(&history);
        protocols.extend(pending);

        assert_eq!(protocols[0]["id"], "old-job");
        assert_eq!(protocols[1]["id"], "new-job");
    }

    #[test]
    fn docker_build_requires_the_runner_selected_buildx_builder() {
        let id = Ulid::new();
        let mut job = Job {
            schema: JOB_SCHEMA.to_owned(),
            id,
            runner: "debian1".to_owned(),
            group: format!("job-{}", id.to_string().to_ascii_lowercase()),
            cwd: "/tmp".to_owned(),
            argv: vec![
                "docker".to_owned(),
                "buildx".to_owned(),
                "build".to_owned(),
                ".".to_owned(),
            ],
            env: BTreeMap::new(),
            priority: 0,
            profile: JobProfile::DockerBuild,
            resources: ResourceRequest::default(),
            locks: Vec::new(),
            timeout_seconds: 60,
            retry_on_runner_restart: 0,
            output_ttl_seconds: 60,
        };
        assert!(validate_docker_build(&job).is_ok());
        job.argv.push("--builder=unbounded".to_owned());
        assert!(validate_docker_build(&job).is_err());
    }
}
