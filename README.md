# zrunner

`zrunner` is a lightweight, host-local build scheduler for AI agents. Agents
submit ordinary commands through AI Board; zrunner orders them by priority,
admits them only when the host has capacity, enforces shared build locks, and
returns buffered stdout and stderr to a job-specific AI Board group.

The first implementation targets Linux and is supervised by `systemd --user`.
See [the design](docs/design.md) for the protocol, resource model, and rollout
sequence.
