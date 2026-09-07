# Zrunner build scheduler

Status: initial Linux implementation
Date: 2026-09-06

## Purpose

Zrunner prevents independently operating AI agents from saturating one shared
build host. It is a small Rust daemon, not an agent: it consumes no model tokens
and makes only deterministic scheduling and resource-admission decisions.

Zboard is the durable command and event plane. Zrunner owns queueing and
execution. The operating system owns enforcement through a systemd cgroup.
There is initially one runner on `debian1`; the same protocol permits one runner
per macOS or Windows build host later.

## Components

The Rust workspace contains three crates:

- `zrunner-protocol`: versioned job, control, lifecycle, and output documents.
- `zrunner-core`: priority scheduling, state replay, resource accounting,
  shared locks, and Linux pressure sampling.
- `zrunner`: the daemon and CLI boundary, including Zboard JSONL transport,
  process ownership, output buffering, cancellation, and systemd packaging.

## Zboard prerequisite

Compiler output must not enter Zboard v1. Version 1 scans and decompresses
every message before relevance filtering and retains the complete board in each
process. Zboard v2 must first provide route-partitioned message storage,
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
the copy, and leaves v1 untouched. The coordinated cutover stops v1 writers,
runs the idempotent migration, and then starts v2 clients. Since v1 has no
durable delivery cursor, first v2 startup establishes its checkpoint without
replaying the complete historical board; history remains explicitly available.

## Job protocol

Agents create and join `job-<lowercase-ulid>`, then send a concise human message
with the job envelope in the Zboard message's structured `meta` field in the
`zrunner` group:

```json
{
  "op": "send",
  "group": "zrunner",
  "message": "Queue Worka workspace tests",
  "meta": {
    "schema": "zrunner.job.v1",
    "id": "01M1...",
    "runner": "debian1",
    "group": "job-01m1...",
    "project": "worka",
    "cwd": "/home/zcourts/projects/projects/worka/worka",
    "argv": ["cargo", "test", "--locked", "--workspace"],
    "env": {
      "CARGO_TARGET_DIR": "/home/zcourts/projects/projects/build/debian1/worka",
      "RUST_BACKTRACE": "1"
    },
    "priority": 0,
    "profile": "rust",
    "resources": {"compile_slots": "auto", "memory_mib": 4096},
    "locks": ["cargo-target:debian1:worka"],
    "timeout_seconds": 3600,
    "retry_on_runner_restart": 1,
    "output_ttl_seconds": 86400
  }
}
```

For Rust jobs, admission verifies that `CARGO_TARGET_DIR` is exactly the
project directory beneath `build/<runner>/` and that the matching
`cargo-target:<runner>:<project>` lock is present. The platform-level target and
cross-project Cargo lock are rejected so one project cannot recreate global
target contention. The target project is the final directory of `cwd`; queue
fairness remains bound separately to the submitting agent's project.

The runner temporarily accepts the original JSON-string `message` form so
already queued jobs survive the live upgrade. New producers use `meta`.

`argv` is executed directly without an implicit shell. Environment values are
durable board content and must not contain secrets. Interactive stdin is not
supported in v1; jobs receive a closed/null input and must use noninteractive
flags, configuration, owner-only files, or platform credential stores.

Control envelopes support queued priority changes and cancellation. Priority is
higher-number-first and FIFO by submission ULID within equal priority. Running
jobs are never preempted. Suggested priorities are 100 for a direct user
emergency, 50 for an active release blocker, 0 for normal work, and -50 for
background qualification.

Lifecycle states are accepted, queued, started, completed, failed, cancelled,
and rejected. Durable events are written to the `zrunner` group so the runner
can reconstruct queued and terminal work after restart. Execution is
at-least-once: nonterminal jobs are replayed after runner restart and therefore
must be safe to repeat. Bounded retry accounting is a later capability despite
the reserved `retry_on_runner_restart` field.

## Scheduling and enforcement

The queue key is descending priority, ascending submission time, then ULID. The
scheduler first represents every waiting project across available running slots.
While a queued project has no running job, a project that is already represented
cannot take another slot. Once all waiting projects are represented, remaining
capacity may run additional jobs from those projects. Priority and FIFO order the
eligible jobs within each admission pass. The runner validates an explicit job
`project` against the Zboard submitter identity; legacy jobs derive it from that
identity.

The scheduler selects the highest-priority ordinary job whose locks and declared
resource reservation fit, so a temporarily blocked ordinary head does not
strand capacity. A job that can never fit configured hard limits is rejected
rather than blocking the queue indefinitely.

Host-wide exclusivity is exceptional rather than profile-implied. A job must
include a non-empty `exclusive.reason`, its Zboard submitter project and profile
must appear in the runner's exclusivity policy, and the accepted request remains
in durable job history. An authorized exclusive job acts as a drain barrier at
its priority and blocks all admission while it runs. An unauthorized request is
rejected. Ordinary Docker builds never receive host exclusivity implicitly.

Before admission, Linux reads `/proc/meminfo` plus the kernel's ten-second CPU,
memory, and I/O PSI averages. It accounts for running reservations and stops
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
admission_interval_seconds = 5

[exclusive]
allowed_projects = ["infra"]
allowed_profiles = ["docker-build"]
```

Rust jobs receive an allocation from the six-slot aggregate pool through
`CARGO_BUILD_JOBS`; an explicit `-j` above that allocation is rejected. Named
locks prevent separate Cargo processes from contending for the same configured
target directory.

Docker-build jobs use the runner-selected `docker-container` Buildx builder.
That BuildKit container has its own CPU, memory, swap, and parallelism bounds;
restricting only the Docker CLI process would not constrain containers created
by the Docker daemon. A runner-owned named lock serializes jobs using that
builder while unrelated jobs may run when resource admission permits. The
runner accepts only a direct `docker buildx build` argument array, rejects
caller-selected `--builder` options, and injects the configured builder through
`BUILDX_BUILDER`.

## Output

The runner reads stdout and stderr independently. It flushes a stream when it
reaches 256 KiB, after two seconds, when output becomes quiet, or when the child
exits. Each event records job ID, sequence, stream, encoding, data, and time.
Invalid UTF-8 is Base64 encoded. The final durable event records the last
sequence, exit status, and duration. Consumers can detect a missing output
sequence before its expiry.

## Supervision and recovery

Linux runs `zrunner daemon` as a user service with `Restart=always`,
`KillMode=control-group`, `MemoryHigh`, `MemoryMax`, `CPUQuota`, `TasksMax`, and
an appropriate I/O weight. One host-local lock prevents duplicate daemons. On
restart, zrunner requests history specifically from the durable `zrunner`
command/lifecycle group, rebuilds queued and
terminal state, and resumes dispatch. Live protocol messages received during
bootstrap remain buffered until that history has been applied, so a new arrival
cannot jump ahead of older durable work. Replay restores queue and group state
without emitting duplicate `accepted` or `queued` events. A previously started nonterminal job
is replayed, which is why submitted commands must tolerate at-least-once
execution.

The daemon itself remains small. Build subprocesses remain in its service
cgroup, so systemd removes them if ownership is lost. Cancellation discovers
and signals every process group already present below the supervised command,
waits a bounded grace period, and then sends SIGKILL if needed. A wrapper shell
exiting does not produce a terminal event while one of its tracked descendant
groups is still alive.

## Rollout

1. Ship and migrate Zboard v2 without deleting v1.
2. Qualify routing, checkpoints, expiry, and migration against a copy of the
   real board on Linux, macOS, and Windows.
3. Run zrunner on Debian in observation mode, then accept checks and tests.
4. Enable compilation with aggregate Cargo slots and shared-target locks.
5. Provision and qualify the bounded BuildKit builder.
6. Update shared agent instructions only after the queue is proven reliable.
7. Add launchd and Task Scheduler supervision for the other build hosts.
