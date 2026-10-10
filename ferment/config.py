"""Client-side config: ~/.config/ferment/config.toml.

Initialized from the caller's defaults on first run. Holds the VM backend
(which provider, and its options) and which fermenter is currently attached,
so short-lived commands (e.g. an attached-session CLI) know where to send
requests without flags.

    [backend]
    provider = "lima"

    [backend.lima]
    cpus = 4

    [attached]
    name = "myfermenter"
    server_url = "http://localhost:43004"
"""

from __future__ import annotations

import os
import sys
import tomllib
from pathlib import Path

import tomli_w


def config_path() -> Path:
    """FERMENT_CONFIG wins (tests, multiple identities); else XDG."""
    override = os.environ.get("FERMENT_CONFIG")
    if override:
        return Path(override)
    base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return base / "ferment" / "config.toml"


def state_dir() -> Path:
    """Client-side long-lived state (session files, snapshots, ...)."""
    base = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
    return base / "ferment"


def _merge(defaults: dict, found: dict) -> dict:
    """Defaults fill in keys the file lacks; the file wins everywhere else."""
    out = dict(defaults)
    for key, value in found.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _merge(out[key], value)
        else:
            out[key] = value
    return out


def load(defaults: dict) -> dict:
    """Read the config, writing `defaults` to disk first if there is none."""
    path = config_path()
    if not path.exists():
        save(defaults)
        print(f"ferment: initialized {path}", file=sys.stderr)
        return dict(defaults)
    with path.open("rb") as f:
        return _merge(defaults, tomllib.load(f))


def save(cfg: dict) -> None:
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_bytes(tomli_w.dumps(cfg).encode())
    tmp.replace(path)
