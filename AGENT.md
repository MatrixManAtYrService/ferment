# AI agent instructions

ferment is a public, generic library. Its first consumer is
skills-ebb-ferment (a work project), which supplies a concrete workload and
CLIs. Keep anything specific to one employer, network, or product out of
this repo: no corp hostnames, package mirrors, service names, or credentials.

## Layout

| Module | What |
|---|---|
| `ferment/vm/` | provider contract (`base.py`), `lima.py`, placeholders for gcp/aws |
| `ferment/ssh.py` | run (optionally holding forwards), push_dir (tar over ssh), tunnels (`Forward`, -L and -R) |
| `ferment/relay.py` | make a client-reachable `host:port` appear on a NixOS VM under its own name (`-R` + /etc/hosts + socat) |
| `ferment/lifetime.py` | durations, in-guest poweroff timer |
| `ferment/server/clock.py` | `GuestClock`: the VM's clock is the workload's; offset to real time |
| `ferment/archive.py` | `DoltArchive`: checkpoints of the live MySQL in Dolt (MySQL runs, Dolt remembers) |
| `ferment/procompose.py` | `Process`, `render`, `WorkloadSupervisor` |
| `ferment/agent/` | the workload agent (`app.py`: clock, process-compose, logs, offline DB, probes) and `AgentClient` (`client.py`), which the server drives it through; see `docs/split-vms.md` |
| `ferment/server/` | `TimeController` (clock, checkpoints, restore), `create_app` (routes, event log), `store.py` (SQLite: sessions, events, checkpoints) |
| `ferment/board.py` | `Board`: what attached people see (facts, services, milestones with log-line progress, feed); one model for the terminal, the web page, and status |
| `ferment/server/board.py` | `board_router`: `GET /board` snapshot, `GET /board/stream` server-sent events |
| `ferment/boardview.py` | `BoardView` (Rich: board pinned at the bottom, feed scrolling above; plain lines when not a terminal; the `waiting` queue, ↑↓/enter in a terminal: enter runs what the client's own `resolve(key)` says, never a server-sent command), `Follower` (keeps it current; falls back to /broadcasts on servers without a board) |
| `ferment/fakeboard.py` | a board replaying a scripted run, for building renderers without a VM (`python -m ferment.fakeboard`) |
| `ferment/clockcheck.py` | clock probes + agreement check |
| `ferment/sources.py` | client side of shipping code: resolve refs, mirror, incremental git bundles |
| `ferment/fulfill.py` | `SourceFulfiller`: an attached client fetching code the VM asked for (source requests) |
| `ferment/server/sources.py`, `builds.py` | VM side: bare repos from bundles, worktrees, builds (real time, SOURCE_DATE_EPOCH = commit time, cached per input; each a script in its own systemd unit that outlives the server, finished via an exit-status file) |
| `ferment/volume.py` | checkpoints as btrfs snapshots of MySQL's data dir: `Volume` (workload agent: loop-mounted image, `live` + `snapshots/<id>`), `VolumeSnapshots` (the server's Versioned store, through the agent) |
| `ferment/databases.py` | MariaDB / Dolt processes; Dolt checkpoints, log, diff, restore |
| `ferment/config.py` | `~/.config/ferment/config.toml` |
| `ferment/progress.py` | `Task`: a client's long work on the board with done/total (server: `Tasks` in `server/board.py`, lapses when its client stops reporting) |
| `ferment/needs.py` | capabilities (`relay:`, `fetch:`, `data:`) and `Waiting`: a client waiting for one says so on the board |
| `ferment/data.py`, `ferment/server/data.py` | data for the workload: fragments (gzip'd SQL by sha256, keyed by source/database/table/selector/version) in the `data` cache, datasets, data requests an attached client fulfills (`DataFulfiller` + a per-source handler), loading a dataset into MySQL |
| `ferment/bootstrap.py` | prepare a machine to host fermenters, once, over ssh: idempotent steps (ok/changed/manual/failed); `macos` = a Mac running Lima from a pinned nixpkgs |

## Dev environment (nix)

`.envrc` + `flake.nix` (uv2nix, editable install via `$REPO_ROOT`) give a
devShell with python, uv, ruff, and lima; direnv activates it. Keep nix out of
README.md: users only need uv.

## Rules

- Providers stop at "here's an ssh target". Anything above that goes through
  `ferment.ssh` so it works on every provider.
- Time is the workload VM's system clock. Don't add per-process time shims;
  providers must keep that guest's clock from being synced back (see
  vm/lima.py), and nothing that needs real time (builds, TLS, observability)
  runs there: it goes on the control VM (docs/split-vms.md).
- Services run on MySQL. Checkpoints are volume snapshots; Dolt, when used,
  only stores copies (for diffs).
- No TLS between real-time and faked processes: certs minted under the fake
  clock look not-yet-valid or expired to everyone else.
- Store schema changes are new entries appended to `store.MIGRATIONS`; never
  edit an existing one (fermenters in the field are at older versions).
- A provider that isn't built raises `NotImplementedError`, not a half-working
  implementation.
