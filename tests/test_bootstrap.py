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
    assert b["memory_gib"] + b["control_memory_gib"] == 16 - bootstrap.Sizing().reserve_memory_gib
    assert (b["cpus"], b["disk_gib"], b["control_disk_gib"]) == (6, 88, 50)
    assert b["host"] == "matt@mini" and b["limactl"] == "/l"
    assert (b["vm_type"], b["image"]) == ("vz", "")  # replaces a Linux host's, if one was configured


def test_sizing_gives_a_big_host_to_its_vms():
    h = Host("f@box", facts={"memory_gib": 125, "cpus": 32, "free_disk_gib": 900, "limactl": "/l"})
    sizing = bootstrap.Sizing(
        reserve_cpus=2, reserve_memory_gib=4, reserve_disk_gib=100, control_cpus=8, control_memory_gib=16
    )
    b = bootstrap.lima_backend(h, sizing)
    assert (b["cpus"], b["memory_gib"]) == (22, 105)
    assert (b["control_cpus"], b["control_memory_gib"]) == (8, 16)
    assert (b["disk_gib"], b["control_disk_gib"]) == (534, 266)
    pinned = bootstrap.lima_backend(h, bootstrap.Sizing(reserve_disk_gib=100, control_disk_gib=150))
    assert (pinned["disk_gib"], pinned["control_disk_gib"]) == (650, 150)


class FakeDebian:
    """Answers bootstrap's scripts the way a fresh Debian box would; run_tty
    does what the sudo commands would."""

    def __init__(self):
        self.kvm = False
        self.masked = False
        self.links: dict[str, str] = {}
        self.egress_files = ""
        self.egress_unit = "disabled\ninactive\n"
        self.tty: list[str] = []

    def run(self, dest, script, input=None):
        def done(out="", code=0):
            return subprocess.CompletedProcess(script, code, out, "")

        if script.startswith("uname"):
            return done(
                "Linux\nx86_64\nDebian GNU/Linux 13 (trixie)\n134217728000\n32\n/home/f\nf\n1001\n"
                "/dev/nvme0n1p2 1 2 943718400 5% /\n"
            )
        if script.startswith("test -r /dev/kvm"):
            return done(code=0 if self.kvm else 1)
        if script == "test -e /dev/kvm":
            return done()
        if script.startswith("readlink"):
            return done(self.links.get(script.split()[1], ""))
        if " build --out-link " in script:
            link = script.split("--out-link ")[1].split()[0]
            self.links[link] = "/nix/store/x-lima-2.1.3\n" if "lima" in link else "/nix/store/x-nixos.qcow2\n"
            return done(self.links[link])
        if script.startswith("systemctl is-enabled sleep.target"):
            return done("masked\n" * 4 if self.masked else "static\n" * 4)
        if script.startswith(f"cat {bootstrap.EGRESS_FILE}"):
            return done(self.egress_files + self.egress_unit)
        if script == bootstrap.EGRESS_PROBE:
            return done(code=124 if self.egress_unit == "enabled\nactive\n" else 0)
        if "mkdir -p" in script and "cache" in script:
            return done("ok\n")
        return done("limactl version 2.1.3\n" if "--version" in script else "")

    def run_tty(self, script):
        self.tty.append(script)
        if "usermod -aG kvm" in script:
            self.kvm = True
        if "systemctl mask" in script:
            self.masked = True
        if bootstrap.EGRESS_UNIT in script:
            self.egress_files = bootstrap.egress_rules(1001) + bootstrap.EGRESS_UNIT_TEXT
            self.egress_unit = "enabled\nactive\n"
        return 0


def _run_debian(box, *, tty=True):
    h = Host("f@box", run=box.run, run_tty=box.run_tty if tty else None)
    return h, {o.step: o for o in bootstrap.run(h, bootstrap.steps("debian"))}


def test_debian_bootstrap_converges_then_is_all_ok():
    box = FakeDebian()
    h, first = _run_debian(box)
    assert {s: o.status for s, o in first.items() if o.status != "ok"} == {
        "kvm": "changed",
        "lima": "changed",
        "images": "changed",
        "sleep": "changed",
        "egress": "changed",
    }
    assert (h.facts["uid"], h.facts["vm_type"]) == (1001, "qemu")
    assert h.facts["image"] == "/home/f/.local/share/ferment/images/nixos-lima-v0.0.5-x86_64.qcow2"
    b = bootstrap.lima_backend(h)
    assert (b["vm_type"], b["image"]) == ("qemu", h.facts["image"])

    box.tty.clear()
    _, second = _run_debian(box)
    assert {o.status for o in second.values()} == {"ok"}
    assert box.tty == []


def test_egress_without_a_terminal_is_manual():
    _, out = _run_debian(FakeDebian(), tty=False)
    assert out["egress"].status == "manual" and out["kvm"].status == "manual"


def test_egress_that_still_gets_out_fails():
    box = FakeDebian()
    _run_debian(box)
    box.run_tty = lambda script: 0
    real = box.run

    def still_open(dest, script, input=None):
        if script == bootstrap.EGRESS_PROBE:
            return subprocess.CompletedProcess(script, 0, "", "")
        return real(dest, script, input)

    box.run = still_open
    _, out = _run_debian(box)
    assert out["egress"].status == "failed" and "1.1.1.1" in out["egress"].detail


def test_egress_rules_bind_only_the_vms_account():
    rules = bootstrap.egress_rules(1001)
    body = [line.strip() for line in rules.splitlines() if line.strip().startswith("meta skuid")]
    assert body and all(line.startswith("meta skuid 1001 ") for line in body)
    assert body[-1] == "meta skuid 1001 counter drop"
    assert 'oifname "lo" accept' in body[0] and "established,related accept" in body[1]


def test_images_are_fetched_by_digest():
    from ferment.vm.lima import IMAGES

    expr = bootstrap.image_expr(IMAGES[1])
    assert 'outputHashAlgo = "sha512"' in expr and IMAGES[1]["digest"].split(":")[1] in expr
    assert 'builder = "builtin:fetchurl"' in expr


def test_unbuilt_kinds_say_so():
    with pytest.raises(NotImplementedError):
        bootstrap.steps("gcp")


def test_lima_comes_from_the_nixpkgs_the_flake_pins():
    lock = json.loads((Path(__file__).parent.parent / "flake.lock").read_text())
    assert lock["nodes"]["nixpkgs"]["locked"]["rev"] == bootstrap.NIXPKGS_REV
