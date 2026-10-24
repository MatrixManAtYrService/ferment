"""Prepare a machine to host fermenters: once, over ssh, from the client.

A bootstrap is a list of steps. Each looks before it changes anything and
reports `ok` (already so), `changed` (made so), `manual` (a person has to do
it; the detail says what) or `failed`. Run it again until every step is ok:
when one fails in a way the code doesn't handle, fix the step rather than
typing commands on the host.

Kinds of host:
- `macos`: an Apple Silicon Mac that runs Lima for this client
  (`[backend.lima] host`). Lima comes from a pinned nixpkgs, built on the
  host into a fixed path, so limactl doesn't depend on a login shell's PATH.

Other kinds (a cloud project, say) come later; `steps()` raises
NotImplementedError for them.
"""

from __future__ import annotations

import re
import shlex
import subprocess
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

from ferment.vm.lima import CACHE_ROOT

# Same nixpkgs as ferment's flake.lock (and so the same Lima as its devShell);
# a test keeps them in step.
NIXPKGS_REV = "7c8764b7c7b09b34f632464276218ef9090eaa11"
LIMA_FLAKE = f"github:NixOS/nixpkgs/{NIXPKGS_REV}#lima"
NIX = "/nix/var/nix/profiles/default/bin/nix"
# On the host: a nix out-link (a GC root), so limactl has a stable path.
LIMA_LINK = ".local/share/ferment/lima"

# pmset settings an always-on VM host needs: never sleep, come back after a
# power cut. Lima VMs stop when the host sleeps.
POWER = {"sleep": "0", "disksleep": "0", "autorestart": "1"}

# What the host keeps for itself when sizing VMs.
RESERVE_MEMORY_GIB = 2
CONTROL = {"cpus": 4, "memory_gib": 4}


@dataclass
class Outcome:
    step: str
    status: str  # ok | changed | manual | failed
    detail: str = ""


@dataclass
class Host:
    """An ssh destination, and how to reach it.

    `run` is non-interactive (BatchMode); `run_tty` gives the command a
    terminal, for sudo's password prompt, and is None when nobody is there to
    answer one.
    """

    dest: str
    run: Callable[[str, str, str | None], subprocess.CompletedProcess] | None = None
    run_tty: Callable[[str], int] | None = None
    facts: dict = field(default_factory=dict)

    def sh(self, script: str, input: str | None = None) -> subprocess.CompletedProcess:
        return (self.run or ssh_capture)(self.dest, script, input)


def ssh_capture(dest: str, script: str, input: str | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", dest, "--", script],
        input=input,
        text=True,
        capture_output=True,
    )


def ssh_tty(dest: str, script: str) -> int:
    return subprocess.run(["ssh", "-tt", dest, "--", script]).returncode


def _err(p: subprocess.CompletedProcess) -> str:
    return (p.stderr or p.stdout or f"exit {p.returncode}").strip().splitlines()[-1]


# -- macos --------------------------------------------------------------------


def step_ssh(h: Host) -> Outcome:
    p = h.sh("true")
    if p.returncode != 0:
        return Outcome(
            "ssh",
            "manual",
            f"can't ssh to {h.dest} without a password ({_err(p)}): turn on Remote Login there, "
            f"then `ssh-copy-id {h.dest}`",
        )
    return Outcome("ssh", "ok", h.dest)


def step_macos(h: Host) -> Outcome:
    p = h.sh(
        "uname -s; uname -m; sw_vers -productVersion; sysctl -n hw.memsize hw.ncpu; "
        'echo "$HOME"; df -k "$HOME" | tail -1'
    )
    lines = p.stdout.split("\n")
    if p.returncode != 0 or len(lines) < 7:
        return Outcome("macos", "failed", _err(p))
    system, arch, version, mem, ncpu, home, df = lines[:7]
    if system != "Darwin" or arch != "arm64":
        return Outcome("macos", "failed", f"{h.dest} is {system}/{arch}; this kind wants an Apple Silicon Mac")
    h.facts.update(
        macos=version,
        memory_gib=int(mem) // 2**30,
        cpus=int(ncpu),
        home=home,
        free_disk_gib=int(df.split()[3]) // 2**20,
    )
    return Outcome(
        "macos",
        "ok",
        f"macOS {version}, {h.facts['cpus']} cpus, {h.facts['memory_gib']} GiB, {h.facts['free_disk_gib']} GiB free",
    )


def step_nix(h: Host) -> Outcome:
    p = h.sh(f"{NIX} --version")
    if p.returncode != 0:
        return Outcome("nix", "manual", f"install Nix on {h.dest} (the same installer as this machine's)")
    return Outcome("nix", "ok", p.stdout.strip().splitlines()[0])


def step_lima(h: Host) -> Outcome:
    link = f"$HOME/{LIMA_LINK}"
    before = h.sh(f"readlink {link}").stdout.strip()
    p = h.sh(
        f"mkdir -p $(dirname {link}) && "
        f"{NIX} --extra-experimental-features 'nix-command flakes' build --out-link {link} "
        f"--print-out-paths {LIMA_FLAKE}"
    )
    if p.returncode != 0:
        return Outcome("lima", "failed", f"building {LIMA_FLAKE}: {_err(p)}")
    limactl = f"{h.facts['home']}/{LIMA_LINK}/bin/limactl"
    v = h.sh(f"{shlex.quote(limactl)} --version")
    if v.returncode != 0:
        return Outcome("lima", "failed", f"{limactl} --version: {_err(v)}")
    h.facts["limactl"] = limactl
    status = "ok" if before == p.stdout.strip() else "changed"
    return Outcome("lima", status, f"{v.stdout.strip()} at {limactl}")


def step_caches(h: Host) -> Outcome:
    root = CACHE_ROOT.replace("~", "$HOME", 1)
    p = h.sh(f'if [ -d "{root}" ]; then echo ok; else mkdir -p "{root}" && echo changed; fi')
    if p.returncode != 0:
        return Outcome("caches", "failed", _err(p))
    return Outcome("caches", p.stdout.strip(), CACHE_ROOT)


def parse_power(pmset_custom: str) -> dict[str, str]:
    """`pmset -g custom` → the AC Power settings (a desktop Mac has only those)."""
    out: dict[str, str] = {}
    section = None
    for line in pmset_custom.splitlines():
        if line and not line.startswith(" "):
            section = line.strip().rstrip(":")
            continue
        if section == "AC Power" and (m := re.match(r"\s+(\S+)\s+(\S+)", line)):
            out[m.group(1)] = m.group(2)
    return out


def power_command() -> str:
    return "sudo pmset -a " + " ".join(f"{k} {v}" for k, v in POWER.items())


def step_power(h: Host) -> Outcome:
    def wrong() -> dict[str, str]:
        have = parse_power(h.sh("pmset -g custom").stdout)
        return {k: have.get(k, "?") for k, v in POWER.items() if have.get(k) != v}

    off = wrong()
    if not off:
        return Outcome("power", "ok", ", ".join(f"{k} {v}" for k, v in POWER.items()))
    if h.run_tty is None:
        return Outcome("power", "manual", f"on {h.dest}: `{power_command()}` (now {off}), or re-run in a terminal")
    h.run_tty(power_command())
    if still := wrong():
        return Outcome("power", "failed", f"still {still} after `{power_command()}`")
    return Outcome("power", "changed", ", ".join(f"{k} {v}" for k, v in POWER.items()))


def trust_step(pubkey: Path) -> Callable[[Host], Outcome]:
    """Let another client (e.g. a machine that attaches) log in to the host:
    Lima over ssh, and the jump to its VMs."""

    def step(h: Host) -> Outcome:
        name = f"trust {pubkey.name}"
        try:
            key = pubkey.read_text().strip()
        except OSError as e:
            return Outcome(name, "failed", str(e))
        parts = key.split()
        if len(parts) < 2 or not parts[0].startswith(("ssh-", "ecdsa-", "sk-")):
            return Outcome(name, "failed", f"{pubkey} isn't an ssh public key")
        script = (
            "mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && "
            "chmod 600 ~/.ssh/authorized_keys && key=$(cat) && "
            f"if grep -qF {shlex.quote(parts[1])} ~/.ssh/authorized_keys; then echo ok; "
            'else printf "%s\\n" "$key" >> ~/.ssh/authorized_keys && echo changed; fi'
        )
        p = h.sh(script, key)
        if p.returncode != 0:
            return Outcome(name, "failed", _err(p))
        return Outcome(name, p.stdout.strip(), " ".join(parts[2:]) or parts[0])

    return step


def lima_backend(h: Host) -> dict:
    """`[backend.lima]` for a client using this host: most of its memory and
    cpus for the workload VM, the rest for the control VM and the host."""
    mem, cpus, disk = h.facts["memory_gib"], h.facts["cpus"], h.facts["free_disk_gib"]
    return {
        "host": h.dest,
        "limactl": h.facts["limactl"],
        "cpus": max(2, cpus - CONTROL["cpus"]),
        "memory_gib": max(4, mem - CONTROL["memory_gib"] - RESERVE_MEMORY_GIB),
        # Sparse: a VM takes what it writes, so these are ceilings. Leave
        # room for the caches and dumps beside them.
        "disk_gib": max(50, min(150, disk // 2)),
        "control_cpus": CONTROL["cpus"],
        "control_memory_gib": CONTROL["memory_gib"],
        "control_disk_gib": max(50, min(100, disk // 3)),
    }


def steps(kind: str, trust: list[Path] = ()) -> list[Callable[[Host], Outcome]]:
    if kind == "macos":
        return [step_ssh, step_macos, step_nix, step_lima, step_caches, step_power, *map(trust_step, trust)]
    raise NotImplementedError(f"bootstrapping a {kind!r} host isn't built yet")


# Later steps assume these (no ssh, no nix, no Lima): stop when one isn't done.
PREREQUISITES = {"ssh", "macos", "nix", "lima"}


def run(h: Host, plan: list[Callable[[Host], Outcome]]) -> Iterator[Outcome]:
    for step in plan:
        out = step(h)
        yield out
        if out.step in PREREQUISITES and out.status not in ("ok", "changed"):
            return
