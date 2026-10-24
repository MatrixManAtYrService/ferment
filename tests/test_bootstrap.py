import json
import subprocess
from pathlib import Path

import pytest

from ferment import bootstrap
from ferment.bootstrap import Host

PMSET = """\
Battery Power:
 sleep                1
 disksleep            10
AC Power:
 Sleep On Power Button 1
 sleep                0
 autorestart          1
 disksleep            10
"""


class FakeMac:
    """Answers bootstrap's scripts the way a fresh Mac would."""

    def __init__(self, pmset=PMSET):
        self.pmset = pmset
        self.linked = ""
        self.authorized = ""
        self.tty: list[str] = []

    def run(self, dest, script, input=None):
        def done(out="", code=0):
            return subprocess.CompletedProcess(script, code, out, "")

        if script.startswith("uname"):
            return done("Darwin\narm64\n15.6.1\n17179869184\n10\n/Users/matt\n/dev/disk3s1 1 2 216006656 5% /\n")
        if script.startswith("readlink"):
            return done(self.linked)
        if " build --out-link " in script:
            self.linked = "/nix/store/x-lima-2.1.3\n"
            return done(self.linked)
        if script == "pmset -g custom":
            return done(self.pmset)
        if "authorized_keys" in script:
            body = input.split()[1]
            if body in self.authorized:
                return done("ok\n")
            self.authorized += input + "\n"
            return done("changed\n")
        if "mkdir -p" in script and "cache" in script:
            return done("ok\n")
        return done("limactl version 2.1.3\n" if "--version" in script else "")

    def run_tty(self, script):
        self.tty.append(script)
        self.pmset = PMSET.replace("disksleep            10", "disksleep            0")
        return 0


def _run(mac, *, tty=True, trust=()):
    h = Host("matt@mini", run=mac.run, run_tty=mac.run_tty if tty else None)
    return h, {o.step: o for o in bootstrap.run(h, bootstrap.steps("macos", list(trust)))}


def test_macos_bootstrap_converges_then_is_all_ok(tmp_path):
    key = tmp_path / "book.pub"
    key.write_text("ssh-ed25519 AAAAbook matt@book\n")
    mac = FakeMac()
    h, first = _run(mac, trust=[key])
    assert first["lima"].status == "changed" and first["power"].status == "changed"
    assert first[f"trust {key.name}"].status == "changed"
    assert mac.tty == ["sudo pmset -a sleep 0 disksleep 0 autorestart 1"]
    assert h.facts["limactl"] == "/Users/matt/.local/share/ferment/lima/bin/limactl"  # absolute: no ~ to expand

    _, second = _run(mac, trust=[key])
    assert {o.status for o in second.values()} == {"ok"}
    assert mac.authorized.count("AAAAbook") == 1


def test_without_a_terminal_power_is_manual_but_the_rest_still_runs(tmp_path):
    key = tmp_path / "book.pub"
    key.write_text("ssh-ed25519 AAAAbook matt@book\n")
    _, out = _run(FakeMac(), tty=False, trust=[key])
    assert out["power"].status == "manual" and "sudo pmset -a" in out["power"].detail
    assert out[f"trust {key.name}"].status == "changed"


def test_no_ssh_stops_before_anything_else():
    def refuse(dest, script, input=None):
        return subprocess.CompletedProcess(script, 255, "", "Permission denied (publickey).")

    out = list(bootstrap.run(Host("matt@mini", run=refuse), bootstrap.steps("macos")))
    assert [(o.step, o.status) for o in out] == [("ssh", "manual")]


def test_power_reads_the_ac_settings():
    assert bootstrap.parse_power(PMSET) == {"Sleep": "On", "sleep": "0", "autorestart": "1", "disksleep": "10"}


def test_lima_backend_leaves_the_host_room():
    h = Host("matt@mini", facts={"memory_gib": 16, "cpus": 10, "free_disk_gib": 206, "limactl": "/l"})
    b = bootstrap.lima_backend(h)
    assert b["memory_gib"] + b["control_memory_gib"] == 16 - bootstrap.RESERVE_MEMORY_GIB
    assert (b["cpus"], b["disk_gib"], b["control_disk_gib"]) == (6, 103, 68)
    assert b["host"] == "matt@mini" and b["limactl"] == "/l"


def test_unbuilt_kinds_say_so():
    with pytest.raises(NotImplementedError):
        bootstrap.steps("gcp")


def test_lima_comes_from_the_nixpkgs_the_flake_pins():
    lock = json.loads((Path(__file__).parent.parent / "flake.lock").read_text())
    assert lock["nodes"]["nixpkgs"]["locked"]["rev"] == bootstrap.NIXPKGS_REV
