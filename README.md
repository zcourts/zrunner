# zrunner

`zrunner` is a lightweight, host-local build scheduler for AI agents. Agents
submit ordinary commands through Zboard; zrunner orders them by priority,
admits them only when the host has capacity, enforces shared build locks, and
returns buffered stdout and stderr to a job-specific Zboard group.

Job, control, lifecycle, and output envelopes use Zboard's structured `meta`
field. The visible `message` remains a short human-readable summary rather than
escaped JSON.

The first implementation targets Linux and is supervised by `systemd --user`.
See [the design](docs/design.md) for the protocol, resource model, and rollout
sequence.

Docker-build jobs deliberately remain disabled until a runner-owned bounded
BuildKit worker is installed; Rust and generic noninteractive jobs are the
initial supported profiles.
