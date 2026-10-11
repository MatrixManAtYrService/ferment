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
| `ferment/faketime.py` | `FakeClock`: libfaketime offset-mode env |
| `ferment/procompose.py` | `Process`, `render`, `WorkloadSupervisor` |
| `ferment/server/` | `TimeController` (clock, checkpoints, restore), `create_app` (routes, event log), `store.py` (SQLite: sessions, events, checkpoints) |
| `ferment/clockcheck.py` | clock probes + agreement check |
| `ferment/databases.py` | MariaDB / Dolt processes; Dolt checkpoints, log, diff, restore |
| `ferment/config.py` | `~/.config/ferment/config.toml` |
| `nix/fakeable-go.nix` | overlay: `fermentGo`, `fermentFakeable` (Go that obeys FAKETIME) |

## Dev environment (nix)

`.envrc` + `flake.nix` (uv2nix, editable install via `$REPO_ROOT`) give a
devShell with python, uv, ruff, and lima; direnv activates it. Keep nix out of
README.md: users only need uv.

## Rules

- Providers stop at "here's an ssh target". Anything above that goes through
  `ferment.ssh` so it works on every provider.
- Faked processes get one shared offset (`FakeClock.env`), never libfaketime's
  start-at mode.
- A Go program inside the faked tree must be built with `fermentFakeable`;
  otherwise it silently runs on real time (stock Dolt's `NOW()` does).
- No TLS between real-time and faked processes: certs minted under the fake
  clock look not-yet-valid or expired to everyone else.
- Store schema changes are new entries appended to `store.MIGRATIONS`; never
  edit an existing one (fermenters in the field are at older versions).
- A provider that isn't built raises `NotImplementedError`, not a half-working
  implementation.
