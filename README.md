# ferment

A Python library of tools for running a software stack on an ephemeral VM
whose clock you control.

Some systems are best tested as a whole: the services, their databases, the
scheduler, all at once, against realistic data, on a date of your choosing.
Done carefully, a run of such a stack behaves like a pure function of its
inputs (the data, the version of each service, the date), and you can ask
questions like "did the output change when only this one service's version
changed?"

ferment provides the pieces that make that practical:

- **VM providers** (`ferment.vm`): create, list, and delete VMs with a
  delete-me date, and say how to ssh into them. `lima` (local VMs) works
  today; `gcp` is next; `aws` is a placeholder that raises
  `NotImplementedError`.
- **ssh** (`ferment.ssh`): everything after VM creation is plain OpenSSH:
  running commands, pushing source, and tunnels (forward *and* reverse port
  forwards), so a local VM and a cloud VM are driven by the same code.
- **lifetime** (`ferment.lifetime`): deadlines recorded by the provider and
  armed as a timer inside the guest, so a forgotten VM goes away on its own.
- **faked time** (`ferment.faketime`): libfaketime in *offset* mode, so every
  process in the tree agrees on the time no matter when it started.
- **fakeable Go** (`nix/fakeable-go.nix`, the flake's `overlays.default`):
  libfaketime can't reach Go programs, so this patches Go's `time.Now()` to
  honor the same `FAKETIME` offset. `fermentFakeable pkgs.dolt` gives a Dolt
  whose `NOW()` and commit dates follow the fake clock.
- **databases** (`ferment.databases`): MariaDB, or Dolt for checkpoints,
  history, diffs between checkpoints, and hard restores.
- **process-compose supervision** (`ferment.procompose`): one parent for the
  whole workload, so the whole workload shares one clock.
- **time control and checkpoints** (`ferment.server`): a FastAPI app factory
  whose `/time` endpoint restarts the workload under a new date while the
  server itself stays on real time. Checkpoints span every versioned store
  (one external checkpoint, one internal commit each), are taken
  automatically on clock changes, and restore data and clock together. A
  SQLite file on the VM records them, plus who is attached and who did what.
- **sources and builds** (`ferment.sources`, `ferment.server`): the client
  resolves a branch to a commit, mirrors the repo, and sends the VM a git
  bundle of only the commits it lacks; the VM builds in a real git checkout
  on real time, with `SOURCE_DATE_EPOCH` set to the commit's time, and keeps
  successful builds per (source, commit, command). When the client applying
  can't reach a repo either, it queues a source request and any attached
  client that can (`ferment.fulfill`) fetches and sends it.
- **clock checks** (`ferment.clockcheck`): probes for the JVM, Python, a SQL
  database, and coreutils, and a checker that tells you whether they agree.
- **config** (`ferment.config`): `~/.config/ferment/config.toml`, initialized
  on first run.

ferment is a library; a concrete project supplies the workload (which
processes, which data) and its own CLIs. See
[skills-ebb-ferment](https://github.corp.clover.com/clover/skills-ebb-ferment)
for one.

## Why offset mode

libfaketime can start a process at a fixed date (`FAKETIME="@2026-10-03
00:00:00"`), but that clock starts when *each process* starts: a service that
boots 40 seconds after its database runs 40 seconds behind it. ferment always
uses an offset from real time (`FAKETIME="-604800"`), computed once per date
change and given to every process, including ones spawned much later.

libfaketime only affects programs that read the clock through libc. The JVM,
Python, MariaDB, and coreutils do; Go binaries (process-compose, Grafana,
Loki, Prometheus, Dolt) don't. ferment uses that split on purpose: the
supervisor and observability stay on real time, the workload is faked. A Go
program that belongs in the workload (Dolt) is rebuilt with ferment's patched
Go toolchain, which reads the same `FAKETIME` offset.

## Development

```bash
uv sync
uv run pytest
```
