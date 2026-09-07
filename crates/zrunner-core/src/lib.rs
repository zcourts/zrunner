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
    jobs: HashMap<Ulid, ScheduledJob>,
    queue: BTreeSet<QueueKey>,
    running: HashMap<Ulid, Allocation>,
    locks: HashSet<String>,
}

#[derive(Clone)]
struct ScheduledJob {
    job: Job,
    project: String,
}

impl Scheduler {
    pub fn enqueue(&mut self, job: Job, project: String) -> Result<bool> {
        job.validate().map_err(anyhow::Error::msg)?;
        if project.is_empty() {
            bail!("job project is required");
        }
        if self.jobs.contains_key(&job.id) {
            return Ok(false);
        }
        self.queue.insert(QueueKey {
            priority: job.priority,
            id: job.id,
        });
        self.jobs.insert(job.id, ScheduledJob { job, project });
        Ok(true)
    }

    pub fn set_priority(&mut self, id: Ulid, priority: i32) -> bool {
        let Some(scheduled) = self.jobs.get_mut(&id) else {
            return false;
        };
        if self.running.contains_key(&id) {
            return false;
        }
        self.queue.remove(&QueueKey {
            priority: scheduled.job.priority,
            id,
        });
        scheduled.job.priority = priority;
        self.queue.insert(QueueKey { priority, id });
        true
    }

    pub fn cancel_queued(&mut self, id: Ulid) -> Option<Job> {
        let scheduled = self.jobs.get(&id)?;
        if self.running.contains_key(&id) {
            return None;
        }
        self.queue.remove(&QueueKey {
            priority: scheduled.job.priority,
            id,
        });
        self.jobs.remove(&id).map(|scheduled| scheduled.job)
    }

    pub fn next(
        &mut self,
        limits: &Limits,
        host: &HostCapacity,
    ) -> Result<Option<(Job, Allocation)>> {
        if self.running.len() >= limits.max_running_jobs || self.queue.is_empty() {
            return Ok(None);
        }
        if self.running.keys().any(|id| {
            self.jobs
                .get(id)
                .is_some_and(|scheduled| scheduled.job.exclusive.is_some())
        }) {
            return Ok(None);
        }
        let used_slots: u16 = self.running.values().map(|item| item.compile_slots).sum();
        let free_slots = limits.max_compile_slots.saturating_sub(used_slots);
        if free_slots == 0 {
            return Ok(None);
        }
        let running_projects: HashSet<&str> = self
            .running
            .keys()
            .filter_map(|id| {
                self.jobs
                    .get(id)
                    .map(|scheduled| scheduled.project.as_str())
            })
            .collect();
        let unrepresented_projects: HashSet<&str> = self
            .queue
            .iter()
            .filter_map(|key| self.jobs.get(&key.id))
            .map(|scheduled| scheduled.project.as_str())
            .filter(|project| !running_projects.contains(project))
            .collect();
        let mut selected = None;
        for key in &self.queue {
            let scheduled = self.jobs.get(&key.id).expect("queue points to job");
            if !unrepresented_projects.is_empty()
                && !unrepresented_projects.contains(scheduled.project.as_str())
            {
                continue;
            }
            let job = &scheduled.job;
            let requested_memory = job.resources.memory_mib.max(512);
            if requested_memory > limits.max_job_memory_mib {
                bail!("job {} requests more memory than the host limit", job.id);
            }
            let requested_slots = match &job.resources.compile_slots {
                CompileSlots::Exact(value) => (*value).max(1),
                CompileSlots::Auto(_) => free_slots.clamp(1, 4),
            };
            if job.exclusive.is_some() && !self.running.is_empty() {
                // An explicitly authorized exclusive job is a drain barrier.
                // Lower-priority work must not keep it waiting indefinitely.
                return Ok(None);
            }
            if requested_slots <= free_slots
                && host.available_memory_mib
                    >= requested_memory.saturating_add(limits.host_memory_reserve_mib)
                && host.cpu_pressure_some_avg10 <= limits.cpu_pressure_limit
                && host.memory_pressure_full_avg10 <= limits.memory_pressure_limit
                && host.io_pressure_full_avg10 <= limits.io_pressure_limit
                && !job.locks.iter().any(|lock| self.locks.contains(lock))
            {
                selected = Some((key.clone(), requested_slots, requested_memory));
                break;
            }
        }
        let Some((key, requested_slots, requested_memory)) = selected else {
            return Ok(None);
        };
        let job = &self.jobs.get(&key.id).expect("queue points to job").job;
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
        let scheduled = self.jobs.remove(&id)?;
        for lock in &scheduled.job.locks {
            self.locks.remove(lock);
        }
        Some(scheduled.job)
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
        let id = Ulid::new();
        Job {
            schema: JOB_SCHEMA.to_owned(),
            id,
            runner: "debian1".to_owned(),
            group: format!("job-{}", id.to_string().to_ascii_lowercase()),
            project: None,
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
            exclusive: None,
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
        scheduler.enqueue(low.clone(), "alpha".to_owned()).unwrap();
        scheduler.enqueue(high.clone(), "alpha".to_owned()).unwrap();
        let host = HostCapacity {
            available_memory_mib: 16_000,
            ..HostCapacity::default()
        };
        let (selected, _) = scheduler.next(&Limits::default(), &host).unwrap().unwrap();
        assert_eq!(selected.id, high.id);
    }

    #[test]
    fn reprioritizing_running_or_unknown_job_is_a_nonfatal_noop() {
        let mut scheduler = Scheduler::default();
        let running = job(0);
        scheduler
            .enqueue(running.clone(), "alpha".to_owned())
            .unwrap();
        let host = HostCapacity {
            available_memory_mib: 16_000,
            ..HostCapacity::default()
        };
        scheduler.next(&Limits::default(), &host).unwrap().unwrap();

        assert!(!scheduler.set_priority(running.id, 100));
        assert!(!scheduler.set_priority(Ulid::new(), 100));
    }

    #[test]
    fn ordinary_docker_build_can_share_host_capacity() {
        let mut scheduler = Scheduler::default();
        let running = job(50);
        let mut docker = job(100);
        docker.profile = JobProfile::DockerBuild;
        scheduler
            .enqueue(running.clone(), "alpha".to_owned())
            .unwrap();
        let host = HostCapacity {
            available_memory_mib: 16_000,
            ..HostCapacity::default()
        };
        scheduler.next(&Limits::default(), &host).unwrap().unwrap();
        scheduler.enqueue(docker, "beta".to_owned()).unwrap();
        assert!(scheduler.next(&Limits::default(), &host).unwrap().is_some());
    }

    #[test]
    fn explicit_exclusive_job_drains_then_blocks_the_host() {
        let mut scheduler = Scheduler::default();
        let mut docker = job(100);
        docker.profile = JobProfile::DockerBuild;
        docker.exclusive = Some(zrunner_protocol::ExclusivityRequest {
            reason: "qualify bounded builder".to_owned(),
        });
        let ordinary = job(110);
        scheduler
            .enqueue(ordinary.clone(), "alpha".to_owned())
            .unwrap();
        let host = HostCapacity {
            available_memory_mib: 16_000,
            ..HostCapacity::default()
        };
        scheduler.next(&Limits::default(), &host).unwrap().unwrap();
        scheduler
            .enqueue(docker.clone(), "beta".to_owned())
            .unwrap();
        assert!(scheduler.next(&Limits::default(), &host).unwrap().is_none());
        scheduler.finish(ordinary.id).unwrap();
        let (selected, _) = scheduler.next(&Limits::default(), &host).unwrap().unwrap();
        assert_eq!(selected.id, docker.id);
        scheduler.enqueue(job(50), "alpha".to_owned()).unwrap();
        assert!(scheduler.next(&Limits::default(), &host).unwrap().is_none());
    }

    #[test]
    fn blocked_ordinary_head_does_not_strand_capacity() {
        let mut scheduler = Scheduler::default();
        let mut lock_owner = job(110);
        lock_owner.locks.push("shared".to_owned());
        let mut blocked = job(100);
        blocked.locks.push("shared".to_owned());
        let backfill = job(50);
        scheduler
            .enqueue(lock_owner.clone(), "alpha".to_owned())
            .unwrap();
        scheduler.enqueue(blocked, "alpha".to_owned()).unwrap();
        scheduler
            .enqueue(backfill.clone(), "beta".to_owned())
            .unwrap();
        let host = HostCapacity {
            available_memory_mib: 16_000,
            ..HostCapacity::default()
        };
        scheduler.next(&Limits::default(), &host).unwrap().unwrap();
        let (selected, _) = scheduler.next(&Limits::default(), &host).unwrap().unwrap();
        assert_eq!(selected.id, backfill.id);
    }

    #[test]
    fn represents_waiting_projects_before_admitting_a_duplicate() {
        let mut scheduler = Scheduler::default();
        let limits = Limits {
            max_running_jobs: 4,
            max_compile_slots: 4,
            ..Limits::default()
        };
        let host = HostCapacity {
            available_memory_mib: 16_000,
            ..HostCapacity::default()
        };
        let fission_first = job(100);
        let fission_second = job(90);
        let worka = job(80);
        let keldra = job(70);
        let infra = job(60);
        scheduler
            .enqueue(fission_first.clone(), "fission".to_owned())
            .unwrap();
        scheduler
            .enqueue(fission_second.clone(), "fission".to_owned())
            .unwrap();
        scheduler
            .enqueue(worka.clone(), "worka".to_owned())
            .unwrap();
        scheduler
            .enqueue(keldra.clone(), "keldra".to_owned())
            .unwrap();
        scheduler
            .enqueue(infra.clone(), "infra".to_owned())
            .unwrap();

        let mut selected = Vec::new();
        for _ in 0..4 {
            selected.push(scheduler.next(&limits, &host).unwrap().unwrap().0.id);
        }

        assert_eq!(
            selected,
            vec![fission_first.id, worka.id, keldra.id, infra.id]
        );
        assert_eq!(scheduler.queued_len(), 1);
    }

    #[test]
    fn one_project_can_fill_spare_capacity_when_nobody_else_waits() {
        let mut scheduler = Scheduler::default();
        let limits = Limits {
            max_running_jobs: 4,
            max_compile_slots: 4,
            ..Limits::default()
        };
        let host = HostCapacity {
            available_memory_mib: 16_000,
            ..HostCapacity::default()
        };
        for priority in [40, 30, 20, 10] {
            scheduler
                .enqueue(job(priority), "fission".to_owned())
                .unwrap();
        }

        for _ in 0..4 {
            assert!(scheduler.next(&limits, &host).unwrap().is_some());
        }
        assert_eq!(scheduler.queued_len(), 0);
    }
}
