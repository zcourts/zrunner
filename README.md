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

Docker-build jobs use a runner-selected `docker-container` Buildx builder with
its own CPU, memory, and BuildKit parallelism bounds. The runner admits these
jobs exclusively and accepts only direct `docker buildx build` argument arrays.
