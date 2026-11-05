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
- `debian`: a Linux machine (Debian, KVM) that does the same with qemu. Its
  VMs get no egress, as a cloud fermenter on a locked-down subnet gets none:
  the account the client logs in as (which runs Lima, qemu and Lima's
  user-mode network) may answer connections and use the host's loopback,
  but not open connections out (`egress_rules`). So the VM image is fetched
  up front, by the nix daemon, which isn't that account.

How much of the host the VMs get is `Sizing`.

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

from ferment.vm.lima import CACHE_ROOT, IMAGES

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

# On the host: QEMU from the same nixpkgs (the build Lima's limactl wraps),
# and a wrapper Lima runs instead (QEMU_SYSTEM_<ARCH>): bin/qemu-system-<arch>,
# with share/ beside it, where Lima looks for QEMU's firmware.
QEMU_FLAKE = f"github:NixOS/nixpkgs/{NIXPKGS_REV}#qemu"
QEMU_LINK = ".local/share/ferment/qemu"
QEMU_WRAP = ".local/share/ferment/qemu-wrap"

# On the host: nix out-links (GC roots) to the VM images, for Lima to boot
# from a local file when its own downloads have no egress.
IMAGE_LINK = ".local/share/ferment/images"


@dataclass
class Sizing:
    """How much of the host a fermenter's VMs get: the control VM what's
    asked for it, the workload VM the rest of what the host doesn't reserve.
    Disks are sparse (a VM takes what it writes), so they're ceilings; the
    disk reserve is room for the host itself, the caches beside the VMs
    (VMSpec.caches) and the images."""

    reserve_cpus: int = 0
    reserve_memory_gib: int = 2
    reserve_disk_gib: int | None = None  # None: a third of the free disk
    control_cpus: int = 4
    control_memory_gib: int = 4
    control_disk_gib: int | None = None  # None: a third of the VMs' disk


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


# -- debian -------------------------------------------------------------------


def step_debian(h: Host) -> Outcome:
    p = h.sh(
        'uname -s; uname -m; (. /etc/os-release && echo "$PRETTY_NAME"); '
        "awk '/^MemTotal:/ {print $2 * 1024}' /proc/meminfo; nproc; "
        'echo "$HOME"; id -un; id -u; df -k "$HOME" | tail -1'
    )
    lines = p.stdout.split("\n")
    if p.returncode != 0 or len(lines) < 9:
        return Outcome("debian", "failed", _err(p))
    system, arch, release, mem, ncpu, home, user, uid, df = lines[:9]
    if system != "Linux" or arch not in ("x86_64", "aarch64"):
        return Outcome("debian", "failed", f"{h.dest} is {system}/{arch}; this kind wants x86_64 or aarch64 Linux")
    h.facts.update(
        release=release,
        arch=arch,
        memory_gib=int(mem) // 2**30,
        cpus=int(ncpu),
        home=home,
        user=user,
        uid=int(uid),
        free_disk_gib=int(df.split()[3]) // 2**20,
        vm_type="qemu",
    )
    return Outcome(
        "debian",
        "ok",
        f"{release} {arch}, {h.facts['cpus']} cpus, {h.facts['memory_gib']} GiB, "
        f"{h.facts['free_disk_gib']} GiB free; VMs run as {user}",
    )


def step_kvm(h: Host) -> Outcome:
    if h.sh("test -r /dev/kvm && test -w /dev/kvm").returncode == 0:
        return Outcome("kvm", "ok", "/dev/kvm")
    if h.sh("test -e /dev/kvm").returncode != 0:
        return Outcome("kvm", "failed", "no /dev/kvm: turn on virtualization (VT-x / AMD-V) in the firmware")
    command = f"sudo usermod -aG kvm {shlex.quote(h.facts['user'])}"
    if h.run_tty is None:
        return Outcome("kvm", "manual", f"on {h.dest}: `{command}`, or re-run in a terminal")
    h.run_tty(command)
    # Each step is a new ssh login, so it has the new group, unless ssh
    # multiplexes it onto an old connection.
    if h.sh("test -r /dev/kvm && test -w /dev/kvm").returncode != 0:
        return Outcome("kvm", "manual", f"ran `{command}`; re-run once a new login to {h.dest} picks up the group")
    return Outcome("kvm", "changed", f"{h.facts['user']} in group kvm")


def image_expr(image: dict) -> str:
    """A nix expression for one of lima.IMAGES, fetched by the nix daemon
    and checked against the image's digest."""
    algo, _, digest = image["digest"].partition(":")
    name = image["location"].rsplit("/", 1)[-1]
    return (
        f'derivation {{ name = "{name}"; builder = "builtin:fetchurl"; system = "builtin"; '
        f'url = "{image["location"]}"; outputHashMode = "flat"; '
        f'outputHashAlgo = "{algo}"; outputHash = "{digest}"; }}'
    )


def step_images(h: Host) -> Outcome:
    image = next((i for i in IMAGES if i["arch"] == h.facts["arch"]), None)
    if image is None:
        return Outcome("images", "failed", f"no VM image for {h.facts['arch']}")
    name = image["location"].rsplit("/", 1)[-1]
    link = f"$HOME/{IMAGE_LINK}/{name}"
    before = h.sh(f"readlink {link}").stdout.strip()
    p = h.sh(
        f"mkdir -p $HOME/{IMAGE_LINK} && "
        f"{NIX} --extra-experimental-features nix-command build --out-link {link} --print-out-paths "
        f"--expr {shlex.quote(image_expr(image))}"
    )
    if p.returncode != 0:
        return Outcome("images", "failed", f"fetching {image['location']}: {_err(p)}")
    h.facts["image"] = f"{h.facts['home']}/{IMAGE_LINK}/{name}"
    return Outcome("images", "ok" if before == p.stdout.strip() else "changed", h.facts["image"])


def qemu_wrapper(real: str) -> str:
    """A qemu-system-<arch> that runs `real` with Lima's user-v2 netdev
    swapped: `-netdev socket,id=X,fd=N` aborts QEMU (built with io_uring)
    under sustained traffic, `net_fill_rstate: Assertion 'size == 0' failed`
    (lima-vm/lima#5722); `-netdev stream` on the same fd doesn't use that code."""
    return f"""\
#!/bin/sh
# Written by ferment's bootstrap: QEMU with Lima's user-v2 netdev as
# `stream`, not `socket`, which aborts under sustained traffic
# (https://github.com/lima-vm/lima/issues/5722).
for a do
  shift
  case $a in
    socket,id=*,fd=*) rest=${{a#socket,}}; set -- "$@" "stream,${{rest%%,fd=*}},addr.type=fd,addr.str=${{a##*,fd=}}" ;;
    *) set -- "$@" "$a" ;;
  esac
done
exec {shlex.quote(real)} "$@"
"""


def step_qemu(h: Host) -> Outcome:
    name = f"qemu-system-{h.facts['arch']}"
    home = h.facts["home"]
    link, wrap = f"{home}/{QEMU_LINK}", f"{home}/{QEMU_WRAP}"
    wrapper, script = f"{wrap}/bin/{name}", qemu_wrapper(f"{link}/bin/{name}")
    before = h.sh(f"readlink {link}").stdout.strip()
    p = h.sh(
        f"{NIX} --extra-experimental-features 'nix-command flakes' build --out-link {link} "
        f"--print-out-paths {QEMU_FLAKE}"
    )
    if p.returncode != 0:
        return Outcome("qemu", "failed", f"building {QEMU_FLAKE}: {_err(p)}")
    look = f"cat {wrapper} 2>/dev/null; readlink {wrap}/share"
    status = "ok" if before == p.stdout.strip() else "changed"
    if h.sh(look).stdout != script + f"{link}/share\n":
        w = h.sh(
            f"mkdir -p {wrap}/bin && cat > {wrapper} && chmod 755 {wrapper} && ln -sfn {link}/share {wrap}/share",
            script,
        )
        if w.returncode != 0:
            return Outcome("qemu", "failed", f"writing {wrapper}: {_err(w)}")
        status = "changed"
    v = h.sh(f"{wrapper} --version")
    if v.returncode != 0:
        return Outcome("qemu", "failed", f"{wrapper} --version: {_err(v)}")
    h.facts["qemu"] = wrapper
    return Outcome("qemu", status, f"{v.stdout.splitlines()[0]}, user-v2 as a stream netdev: {wrapper}")


SLEEP_TARGETS = ("sleep.target", "suspend.target", "hibernate.target", "hybrid-sleep.target")


def step_sleep(h: Host) -> Outcome:
    """A desktop install suspends when idle, and the VMs stop with it."""
    check = "systemctl is-enabled " + " ".join(SLEEP_TARGETS)
    if set(h.sh(check).stdout.split()) == {"masked"}:
        return Outcome("sleep", "ok", "suspend and hibernate masked")
    command = "sudo systemctl mask " + " ".join(SLEEP_TARGETS)
    if h.run_tty is None:
        return Outcome("sleep", "manual", f"on {h.dest}: `{command}`, or re-run in a terminal")
    h.run_tty(command)
    if set(h.sh(check).stdout.split()) != {"masked"}:
        return Outcome("sleep", "failed", f"still not masked after `{command}`")
    return Outcome("sleep", "changed", "suspend and hibernate masked")


EGRESS_TABLE = "ferment_egress"
EGRESS_FILE = "/etc/ferment/egress.nft"
EGRESS_UNIT = "ferment-egress.service"
# Somewhere on the internet, by address (so DNS doesn't decide the answer).
EGRESS_PROBE = "timeout 5 bash -c 'exec 3<>/dev/tcp/1.1.1.1/443' 2>/dev/null"


def egress_rules(uid: int) -> str:
    """nftables rules: `uid` (the account that runs the VMs) opens no
    connections off the host. What a cloud VM on a locked-down subnet still
    has from its metadata server, DNS and NTP, it keeps."""
    return f"""\
# Written by ferment's bootstrap: fermenter VMs get no egress. Lima, qemu and
# Lima's user-mode network run as uid {uid}, so their traffic leaves the host as
# that uid's. It may answer ssh, use loopback (Lima's port forwards), resolve
# names and keep time, and open nothing else. Dropped attempts are logged:
#   journalctl -k -g ferment-egress
table inet {EGRESS_TABLE}
delete table inet {EGRESS_TABLE}
table inet {EGRESS_TABLE} {{
  chain output {{
    type filter hook output priority filter; policy accept;
    meta skuid {uid} oifname "lo" accept
    meta skuid {uid} ct state established,related accept
    meta skuid {uid} meta l4proto {{ tcp, udp }} th dport 53 accept
    meta skuid {uid} udp dport 123 accept
    meta skuid {uid} limit rate 10/second burst 20 packets log prefix "ferment-egress: "
    meta skuid {uid} counter drop
  }}
}}
"""


EGRESS_UNIT_TEXT = f"""\
[Unit]
Description=ferment: no egress for the account that runs fermenter VMs
# nftables.service flushes the whole ruleset when it (re)starts
After=nftables.service
PartOf=nftables.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/usr/sbin/nft -f {EGRESS_FILE}
ExecStop=/usr/sbin/nft delete table inet {EGRESS_TABLE}

[Install]
WantedBy=multi-user.target
"""


def egress_install_script(uid: int) -> str:
    return "\n".join(
        [
            "set -e",
            "[ -x /usr/sbin/nft ] || sudo apt-get install -y nftables",
            "sudo install -d /etc/ferment",
            f"printf '%s' {shlex.quote(egress_rules(uid))} | sudo tee {EGRESS_FILE} >/dev/null",
            f"printf '%s' {shlex.quote(EGRESS_UNIT_TEXT)} | sudo tee /etc/systemd/system/{EGRESS_UNIT} >/dev/null",
            "sudo systemctl daemon-reload",
            f"sudo systemctl enable {EGRESS_UNIT}",
            f"sudo systemctl restart {EGRESS_UNIT}",
        ]
    )


def step_egress(h: Host) -> Outcome:
    uid = h.facts["uid"]
    want = egress_rules(uid) + EGRESS_UNIT_TEXT + "enabled\nactive\n"
    look = (
        f"cat {EGRESS_FILE} /etc/systemd/system/{EGRESS_UNIT} 2>/dev/null; "
        f"systemctl is-enabled {EGRESS_UNIT}; systemctl is-active {EGRESS_UNIT}"
    )
    undo = f"undo: `sudo systemctl disable --now {EGRESS_UNIT}`"
    status = "ok"
    if h.sh(look).stdout != want:
        if h.run_tty is None:
            return Outcome("egress", "manual", f"re-run in a terminal to block {h.facts['user']}'s egress (sudo)")
        h.run_tty(egress_install_script(uid))
        if h.sh(look).stdout != want:
            return Outcome("egress", "failed", f"{EGRESS_UNIT} isn't enabled and running {EGRESS_FILE}")
        status = "changed"
    if h.sh(EGRESS_PROBE).returncode == 0:
        return Outcome("egress", "failed", f"rules in place, but {h.facts['user']} still reached 1.1.1.1:443")
    return Outcome("egress", status, f"{h.facts['user']} (uid {uid}) opens no connections off the host; {undo}")


# -- both ---------------------------------------------------------------------


def lima_backend(h: Host, sizing: Sizing | None = None) -> dict:
    """`[backend.lima]` for a client using this host: the control VM what
    `sizing` asks for it, the workload VM the rest of what the host doesn't
    reserve."""
    sizing = sizing or Sizing()
    mem, cpus, disk = h.facts["memory_gib"], h.facts["cpus"], h.facts["free_disk_gib"]
    vms_disk = disk - (sizing.reserve_disk_gib if sizing.reserve_disk_gib is not None else disk // 3)
    control_disk = sizing.control_disk_gib or max(50, vms_disk // 3)
    return {
        "host": h.dest,
        "limactl": h.facts["limactl"],
        # Always both, so configuring a different kind of host replaces them.
        "vm_type": h.facts.get("vm_type", "vz"),
        "image": h.facts.get("image", ""),
        "qemu": h.facts.get("qemu", ""),
        "cpus": max(2, cpus - sizing.reserve_cpus - sizing.control_cpus),
        "memory_gib": max(4, mem - sizing.reserve_memory_gib - sizing.control_memory_gib),
        "disk_gib": max(50, vms_disk - control_disk),
        "control_cpus": sizing.control_cpus,
        "control_memory_gib": sizing.control_memory_gib,
        "control_disk_gib": control_disk,
    }


def steps(kind: str, trust: list[Path] = ()) -> list[Callable[[Host], Outcome]]:
    trusted = [trust_step(k) for k in trust]
    if kind == "macos":
        return [step_ssh, step_macos, step_nix, step_lima, step_caches, step_power, *trusted]
    if kind == "debian":
        return [
            step_ssh,
            step_debian,
            step_kvm,
            step_nix,
            step_lima,
            step_qemu,
            step_images,
            step_caches,
            step_sleep,
            step_egress,
            *trusted,
        ]
    raise NotImplementedError(f"bootstrapping a {kind!r} host isn't built yet")


# Later steps assume these (no ssh, no host facts, no nix, no Lima): stop when one isn't done.
PREREQUISITES = {"ssh", "macos", "debian", "nix", "lima"}


def run(h: Host, plan: list[Callable[[Host], Outcome]]) -> Iterator[Outcome]:
    for step in plan:
        out = step(h)
        yield out
        if out.step in PREREQUISITES and out.status not in ("ok", "changed"):
            return
