# AI agent instructions

ferment is a public, generic library. Its first consumer is
skills-ebb-ferment (a work project), which supplies a concrete workload and
CLIs. Keep anything specific to one employer, network, or product out of
this repo: no corp hostnames, package mirrors, service names, or credentials.

## Layout

| Module | What |
|---|---|
| `ferment/vm/` | provider contract (`base.py`), `lima.py`, placeholders for gcp/aws |
| `ferment/ssh.py` | run, push_dir (tar over ssh), tunnels (`Forward`, -L and -R) |
| `ferment/lifetime.py` | durations, in-guest poweroff timer |
| `ferment/faketime.py` | `FakeClock`: libfaketime offset-mode env |
| `ferment/procompose.py` | `Process`, `render`, `WorkloadSupervisor` |
| `ferment/server/` | `TimeController`, `create_app` (health/status/time) |
| `ferment/clockcheck.py` | clock probes + agreement check |
| `ferment/config.py` | `~/.config/ferment/config.toml` |

## Dev environment (nix)

`.envrc` + `flake.nix` (uv2nix, editable install via `$REPO_ROOT`) give a
devShell with python, uv, ruff, and lima; direnv activates it. Keep nix out of
README.md: users only need uv.

## Rules

- Providers stop at "here's an ssh target". Anything above that goes through
  `ferment.ssh` so it works on every provider.
- Faked processes get one shared offset (`FakeClock.env`), never libfaketime's
  start-at mode.
- A provider that isn't built raises `NotImplementedError`, not a half-working
  implementation.
