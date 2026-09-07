# zrunner

**Keep every agent productive without letting their builds overwhelm the host.**

Zrunner is a lightweight build scheduler for shared development machines. Your
agents submit the same Cargo, test, packaging, or Docker commands they would run
locally; zrunner decides when the machine can safely run them, coordinates
access to shared build caches, and streams the results back through
[Zboard](https://github.com/zcourts/zboard).

It gives a team of independent agents one orderly build lane instead of a pile
of competing compilers, linkers, and BuildKit workers.

## Why zrunner?

- **Protect the whole machine.** Admission checks combine declared job needs
  with available memory and Linux CPU, memory, and I/O pressure. New work waits
  when the host is strained; running work is left intact.
- **Use available capacity well.** Run multiple jobs when resources permit,
  share a bounded pool of Cargo compile slots, and keep a configurable host
  memory reserve for editors, agents, and essential services.
- **Make urgent priorities useful.** Higher-priority queued work moves first,
  while equal-priority work remains FIFO. Priorities can be adjusted after
  submission without pre-empting a build already in progress.
- **End shared-cache fights.** Named locks prevent jobs from concurrently using
  the same Cargo target, release directory, device, or other exclusive
  resource.
- **Bound Docker builds too.** A runner-owned Buildx/BuildKit worker has explicit
  CPU, memory, swap, and internal parallelism limits. Its named lock serializes
  access to that builder without blocking unrelated jobs.
- **Reserve exclusivity for exceptional work.** Host-wide exclusivity is an
  explicit, reason-bearing request restricted by runner policy to approved
  submitter projects and job profiles. Routine builds cannot drain the host.
- **See output where coordination happens.** Stdout and stderr are independently
  buffered and published to a job-specific Zboard group, with ordered sequence
  numbers and Base64 preservation for non-UTF-8 output.
- **Recover without losing the queue.** Durable lifecycle events rebuild queued
  and terminal state after a daemon restart. Job execution is at-least-once, so
  retry-safe commands can resume rather than disappear.
- **Keep the scheduler cheap.** Zrunner is a deterministic Rust daemon, not an
  AI agent. It consumes no model tokens and needs no database or central web
  service beyond the shared-filesystem Zboard transport.

## What a job looks like

Agents send a normal command as structured Zboard metadata—no shell wrapper or
custom build language required:

```json
{
  "op": "send",
  "group": "zrunner",
  "message": "Test the Worka workspace",
  "meta": {
    "schema": "zrunner.job.v1",
    "id": "01M1...",
    "runner": "debian1",
    "group": "job-01m1...",
    "cwd": "/workspace/worka",
    "argv": ["cargo", "test", "--locked", "--workspace"],
    "env": {"RUST_BACKTRACE": "1"},
    "priority": 0,
    "profile": "rust",
    "resources": {"compile_slots": "auto", "memory_mib": 4096},
    "locks": ["cargo-target:debian1"],
    "timeout_seconds": 3600,
    "output_ttl_seconds": 86400
  }
}
```

An approved exceptional job adds a reason-bearing request such as
`"exclusive":{"reason":"approved qualification requiring an idle host"}`;
the runner still rejects it unless the message's submitter project and job
profile are allowlisted locally.

Zrunner publishes a durable lifecycle from `accepted` and `queued` through
`started` to `completed`, `failed`, `cancelled`, or `rejected`. Output events
arrive in the job group while it runs, so the submitting agent can follow the
build just as it would follow a local process.

Queued jobs may be cancelled or reprioritized. Running jobs are never displaced
merely because a newer high-priority request arrives.

## Designed for agent fleets

Zrunner separates coordination from execution:

1. An agent publishes a versioned job envelope to Zboard.
2. The host-local runner restores its durable queue and orders ready work by
   priority, submission time, and ULID.
3. Resource admission and named locks select the highest-priority job that can
   start, allowing unrelated work to use capacity behind a temporarily blocked
   ordinary job.
4. The command runs directly as a supervised child process with closed stdin.
5. Buffered output and a final status return to the job's Zboard group.

That makes the queue visible to agents on Linux, macOS, and Windows even when
the first runner is hosted on Linux. It also keeps credentials out of durable
messages: jobs refer to owner-only files or platform credential stores instead
of embedding secrets in environment metadata.

## Current release

The initial implementation runs as a Linux `systemd --user` service and supports
Rust, generic noninteractive, and bounded Docker Buildx jobs. Interactive stdin
is intentionally not part of the v1 job protocol.

For the complete protocol, configuration model, supervision contract, and
rollout plan, read the [design document](docs/design.md).
