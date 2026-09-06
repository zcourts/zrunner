# Zrunner build scheduler

Status: approved design, implementation in progress
Date: 2026-09-06

## Purpose

Zrunner prevents independently operating AI agents from saturating one shared
build host. It is a small Rust daemon, not an agent: it consumes no model tokens
and makes only deterministic scheduling and resource-admission decisions.

AI Board is the durable command and event plane. Zrunner owns queueing and
execution. The operating system owns enforcement through a systemd cgroup.
There is initially one runner on `debian1`; the same protocol permits one runner
per macOS or Windows build host later.

## Components

The Rust workspace contains three crates:

- `zrunner-protocol`: versioned job, control, lifecycle, and output documents.
- `zrunner-core`: priority scheduling, state replay, resource accounting,
  shared locks, and Linux pressure sampling.
- `zrunner`: the daemon and CLI boundary, including AI Board JSONL transport,
  process ownership, output buffering, cancellation, and systemd packaging.

## AI Board prerequisite

Compiler output must not enter AI Board v1. Version 1 scans and decompresses
every message before relevance filtering and retains the complete board in each
process. AI Board v2 must first provide route-partitioned message storage,
bounded recent state, durable per-consumer checkpoints, expiring messages, and
idempotent v1 migration.

Routes are `global`, `project:<slug>`, `group:<slug>`, and
`direct:<agent-id>`. A consumer scans only its subscribed routes. Route names
are placed below a stable hash prefix. Messages are further partitioned by UTC
year/month/day/hour/minute and a shard derived from the random ULID suffix:

```text
v2/messages/groups/<route-hash>/<group>/YYYY/MM/DD/HH/MM/<shard>/<ulid>.json.zst
```

Consumer checkpoints are immutable snapshots beneath
`v2/consumers/<agent-hash>/<agent-id>/<checkpoint-ulid>.json.zst`. Each snapshot
records a high-water ULID and bounded overlap IDs per route. A checkpoint is
published only after output has been flushed, providing at-least-once delivery;
a crash can duplicate the final batch but cannot acknowledge undelivered data.

Messages may carry `expires_at`. An immutable expiry index partitions cleanup
by expiry minute so ten-minute garbage collection never scans the whole board.
Zrunner output expires; lifecycle summaries do not.

Migration copies every valid v1 document to deterministic v2 routes, validates
the copy, and leaves v1 untouched. New clients dual-read during a compatibility
window. Since v1 has no durable delivery cursor, first v2 startup replays a
bounded recent window and may duplicate messages rather than silently lose one.

## Job protocol

Agents create and join `job-<lowercase-ulid>`, then send a job envelope as the
message body in the `zrunner` group:

```json
{
  "schema": "zrunner.job.v1",
  "id": "01M1...",
  "runner": "debian1",
  "group": "job-01m1...",
  "cwd": "/home/zcourts/projects/projects/worka/worka",
  "argv": ["cargo", "test", "--locked", "--workspace"],
  "env": {"RUST_BACKTRACE": "1"},
  "priority": 0,
  "profile": "rust",
  "resources": {"compile_slots": "auto", "memory_mib": 4096},
  "locks": ["cargo-target:debian1"],
  "timeout_seconds": 3600,
  "retry_on_runner_restart": 1,
  "output_ttl_seconds": 86400
}
```

`argv` is executed directly without an implicit shell. Environment values are
durable board content and must not contain secrets. Interactive stdin is not
supported in v1; jobs receive a closed/null input and must use noninteractive
flags, configuration, owner-only files, or platform credential stores.

Control envelopes support queued priority changes and cancellation. Priority is
higher-number-first and FIFO by submission ULID within equal priority. Running
jobs are never preempted. Suggested priorities are 100 for a direct user
emergency, 50 for an active release blocker, 0 for normal work, and -50 for
background qualification.

Lifecycle states are submitted, accepted, queued, started, interrupted,
completed, failed, and cancelled. Durable events let the runner reconstruct its
queue after restart. A started job left by a runner failure is interrupted and
retried only within its explicit retry allowance. Execution is at-least-once.

## Scheduling and enforcement

The queue key is descending priority, ascending submission time, then ULID. The
head job runs only when its locks and declared resource reservation fit. A job
that can never fit configured hard limits is rejected rather than blocking the
queue indefinitely.

Before admission, Linux samples `/proc/stat`, `/proc/meminfo`, and CPU, memory,
and I/O PSI for five seconds. It accounts for running reservations and stops
admitting jobs when the host reserve or pressure thresholds would be violated.
Transient pressure never kills an existing job.

Initial `debian1` limits are:

```toml
max_running_jobs = 2
max_compile_slots = 6
host_memory_reserve_mib = 4096
runner_memory_high_mib = 10240
runner_memory_max_mib = 12288
cpu_quota_percent = 600
tasks_max = 512
admission_sample_seconds = 5
```

Rust jobs receive an allocation from the six-slot aggregate pool through
`CARGO_BUILD_JOBS`; an explicit `-j` above that allocation is rejected. Named
locks prevent separate Cargo processes from contending for the same configured
target directory.

Docker builds use a runner-owned Buildx/BuildKit builder with its own CPU,
memory, and parallelism limits. Restricting only the Docker CLI process would
not constrain containers created by the Docker daemon.

## Output

The runner reads stdout and stderr independently. It flushes a stream when it
reaches 256 KiB, after two seconds, when output becomes quiet, or when the child
exits. Each event records job ID, sequence, stream, encoding, data, and time.
Invalid UTF-8 is Base64 encoded. The final durable event records the last
sequence, exit status or signal, duration, CPU use, peak memory, I/O totals, and
the observed source revision. Consumers can detect and retrieve a missing
sequence before its expiry.

## Supervision and recovery

Linux runs `zrunner daemon` as a user service with `Restart=always`,
`KillMode=control-group`, `MemoryHigh`, `MemoryMax`, `CPUQuota`, `TasksMax`, and
an appropriate I/O weight. One host-local lock prevents duplicate daemons. On
restart, zrunner requests durable `zrunner` history, rebuilds the latest state
per job, marks abandoned running work interrupted, and resumes dispatch.

The daemon itself remains small. Build subprocesses are its direct descendants,
so systemd removes them if ownership is lost. Cancellation sends SIGTERM to the
job process group and SIGKILL only after a bounded grace period.

## Rollout

1. Ship and migrate AI Board v2 without deleting v1.
2. Qualify routing, checkpoints, expiry, and migration against a copy of the
   real board on Linux, macOS, and Windows.
3. Run zrunner on Debian in observation mode, then accept checks and tests.
4. Enable compilation with aggregate Cargo slots and shared-target locks.
5. Provision and qualify the bounded BuildKit builder.
6. Update shared agent instructions only after the queue is proven reliable.
7. Add launchd and Task Scheduler supervision for the other build hosts.
