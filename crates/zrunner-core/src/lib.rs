use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use ulid::Ulid;
use zrunner_protocol::{CompileSlots, Job};

#[derive(Clone, Debug)]
pub struct Limits {
    pub max_running_jobs: usize,
    pub max_compile_slots: u16,
    pub host_memory_reserve_mib: u64,
    pub max_job_memory_mib: u64,
    pub cpu_pressure_limit: f64,
    pub memory_pressure_limit: f64,
    pub io_pressure_limit: f64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_running_jobs: 2,
            max_compile_slots: 6,
            host_memory_reserve_mib: 4096,
            max_job_memory_mib: 12288,
            cpu_pressure_limit: 25.0,
            memory_pressure_limit: 2.0,
            io_pressure_limit: 8.0,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct HostCapacity {
    pub available_memory_mib: u64,
    pub cpu_pressure_some_avg10: f64,
    pub memory_pressure_full_avg10: f64,
    pub io_pressure_full_avg10: f64,
}

impl HostCapacity {
    pub fn sample_linux() -> Result<Self> {
        Ok(Self {
            available_memory_mib: mem_available_mib()?,
            cpu_pressure_some_avg10: pressure_avg10("/proc/pressure/cpu", "some")?,
            memory_pressure_full_avg10: pressure_avg10("/proc/pressure/memory", "full")?,
            io_pressure_full_avg10: pressure_avg10("/proc/pressure/io", "full")?,
        })
    }
}

fn mem_available_mib() -> Result<u64> {
    let source = fs::read_to_string("/proc/meminfo").context("read /proc/meminfo")?;
    let kib = source
        .lines()
        .find_map(|line| line.strip_prefix("MemAvailable:"))
        .and_then(|value| value.split_whitespace().next())
        .context("MemAvailable is absent from /proc/meminfo")?
        .parse::<u64>()
        .context("parse MemAvailable")?;
    Ok(kib / 1024)
}

fn pressure_avg10(path: &str, class: &str) -> Result<f64> {
    let source = fs::read_to_string(path).with_context(|| format!("read {path}"))?;
    let line = source
        .lines()
        .find(|line| line.starts_with(class))
        .with_context(|| format!("missing {class} pressure in {path}"))?;
    line.split_whitespace()
        .find_map(|field| field.strip_prefix("avg10="))
        .context("missing avg10 pressure")?
        .parse()
        .context("parse avg10 pressure")
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueueKey {
    priority: i32,
    id: Ulid,
}

impl Ord for QueueKey {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for QueueKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug)]
pub struct Allocation {
    pub compile_slots: u16,
    pub memory_mib: u64,
}

#[derive(Default)]
pub struct Scheduler {
    jobs: HashMap<Ulid, Job>,
    queue: BTreeSet<QueueKey>,
    running: HashMap<Ulid, Allocation>,
    locks: HashSet<String>,
}

impl Scheduler {
    pub fn enqueue(&mut self, job: Job) -> Result<bool> {
        job.validate().map_err(anyhow::Error::msg)?;
        if self.jobs.contains_key(&job.id) {
            return Ok(false);
        }
        self.queue.insert(QueueKey {
            priority: job.priority,
            id: job.id,
        });
        self.jobs.insert(job.id, job);
        Ok(true)
    }

    pub fn set_priority(&mut self, id: Ulid, priority: i32) -> Result<()> {
        let job = self.jobs.get_mut(&id).context("unknown job")?;
        if self.running.contains_key(&id) {
            bail!("cannot reprioritize a running job");
        }
        self.queue.remove(&QueueKey {
            priority: job.priority,
            id,
        });
        job.priority = priority;
        self.queue.insert(QueueKey { priority, id });
        Ok(())
    }

    pub fn cancel_queued(&mut self, id: Ulid) -> Option<Job> {
        let job = self.jobs.get(&id)?;
        if self.running.contains_key(&id) {
            return None;
        }
        self.queue.remove(&QueueKey {
            priority: job.priority,
            id,
        });
        self.jobs.remove(&id)
    }

    pub fn next(
        &mut self,
        limits: &Limits,
        host: &HostCapacity,
    ) -> Result<Option<(Job, Allocation)>> {
        if self.running.len() >= limits.max_running_jobs || self.queue.is_empty() {
            return Ok(None);
        }
        let key = self.queue.first().cloned().expect("queue is not empty");
        let job = self.jobs.get(&key.id).expect("queue points to job");
        let requested_memory = job.resources.memory_mib.max(512);
        if requested_memory > limits.max_job_memory_mib {
            bail!("job {} requests more memory than the host limit", job.id);
        }
        let used_slots: u16 = self.running.values().map(|item| item.compile_slots).sum();
        let free_slots = limits.max_compile_slots.saturating_sub(used_slots);
        if free_slots == 0 {
            return Ok(None);
        }
        let requested_slots = match &job.resources.compile_slots {
            CompileSlots::Exact(value) => (*value).max(1),
            CompileSlots::Auto(_) => free_slots.clamp(1, 4),
        };
        if requested_slots > free_slots
            || host.available_memory_mib
                < requested_memory.saturating_add(limits.host_memory_reserve_mib)
            || host.cpu_pressure_some_avg10 > limits.cpu_pressure_limit
            || host.memory_pressure_full_avg10 > limits.memory_pressure_limit
            || host.io_pressure_full_avg10 > limits.io_pressure_limit
            || job.locks.iter().any(|lock| self.locks.contains(lock))
        {
            return Ok(None);
        }
        self.queue.remove(&key);
        for lock in &job.locks {
            self.locks.insert(lock.clone());
        }
        let allocation = Allocation {
            compile_slots: requested_slots,
            memory_mib: requested_memory,
        };
        self.running.insert(job.id, allocation.clone());
        Ok(Some((job.clone(), allocation)))
    }

    pub fn finish(&mut self, id: Ulid) -> Option<Job> {
        self.running.remove(&id)?;
        let job = self.jobs.remove(&id)?;
        for lock in &job.locks {
            self.locks.remove(lock);
        }
        Some(job)
    }

    pub fn queued_len(&self) -> usize {
        self.queue.len()
    }
}

pub fn sample_capacity(window: Duration) -> Result<HostCapacity> {
    let deadline = Instant::now() + window;
    let mut samples = Vec::new();
    while Instant::now() < deadline {
        samples.push(HostCapacity::sample_linux()?);
        std::thread::sleep(
            Duration::from_millis(250).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
    let count = samples.len().max(1) as f64;
    Ok(HostCapacity {
        available_memory_mib: samples
            .iter()
            .map(|sample| sample.available_memory_mib)
            .min()
            .unwrap_or_default(),
        cpu_pressure_some_avg10: samples
            .iter()
            .map(|sample| sample.cpu_pressure_some_avg10)
            .sum::<f64>()
            / count,
        memory_pressure_full_avg10: samples
            .iter()
            .map(|sample| sample.memory_pressure_full_avg10)
            .sum::<f64>()
            / count,
        io_pressure_full_avg10: samples
            .iter()
            .map(|sample| sample.io_pressure_full_avg10)
            .sum::<f64>()
            / count,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use zrunner_protocol::{JOB_SCHEMA, JobProfile, ResourceRequest};

    use super::*;

    fn job(priority: i32) -> Job {
        Job {
            schema: JOB_SCHEMA.to_owned(),
            id: Ulid::new(),
            runner: "debian1".to_owned(),
            group: "job-test".to_owned(),
            cwd: "/tmp".to_owned(),
            argv: vec!["true".to_owned()],
            env: BTreeMap::new(),
            priority,
            profile: JobProfile::Generic,
            resources: ResourceRequest {
                compile_slots: CompileSlots::Exact(1),
                memory_mib: 512,
            },
            locks: Vec::new(),
            timeout_seconds: 10,
            retry_on_runner_restart: 0,
            output_ttl_seconds: 10,
        }
    }

    #[test]
    fn higher_priority_runs_first_then_fifo() {
        let mut scheduler = Scheduler::default();
        let low = job(0);
        let high = job(50);
        scheduler.enqueue(low.clone()).unwrap();
        scheduler.enqueue(high.clone()).unwrap();
        let host = HostCapacity {
            available_memory_mib: 16_000,
            ..HostCapacity::default()
        };
        let (selected, _) = scheduler.next(&Limits::default(), &host).unwrap().unwrap();
        assert_eq!(selected.id, high.id);
    }
}
