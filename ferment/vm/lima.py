"""VMs via Lima (macOS vz, or qemu on Linux), booting a NixOS image.

Free and fast to iterate on, and close enough to a cloud VM for everything
above the provider: the VM is reached only over ssh (Lima's own port
forwarding and home-directory mounts are switched off so nothing works here
that would not work in the cloud). The exception is caches (VMSpec.caches):
directories under `cache_root` on the Lima host, mounted writable at
CACHE_MOUNT/<name>, kept when the VM goes.

The Lima host can be this machine or another one (`host`: an ssh
destination). Remotely, every limactl call runs over ssh, and clients reach
the VM's sshd by jumping through the host (ProxyJump), the same shape as
reaching a cloud VM through a bastion or IAP. Either way the client logs in to
the VM with its own key (`authorized_keys` on the spec, or `authorize_key`
later), never with Lima's.

A split fermenter (docs/split-vms.md) is two instances, NAME (control, on
real time) and NAME-workload (the fake clock), both on Lima's user-v2
network, where each reaches the other as lima-<instance>.internal. Clients
reach the workload VM through the control VM, and the control VM reaches it
as `fermenter-workload` with a key of its own. Commands take the fermenter's
name; the workload instance comes along (extend, share, rm).

Lima has no labels or instance metadata, so ferment keeps its own record in
the instance directory: <lima instance dir>/ferment.json. Only instances that
carry one are listed as ferment's. The record is also the source of truth for
guest state a reboot can lose (transient systemd units, files NixOS resets at
boot): the trusted keys and the delete-me date. Every `ssh_target` compares
the guest's boot id with the recorded one and re-applies them on a change,
through Lima's own key, which no reboot locks out.
"""

from __future__ import annotations

import datetime as dt
import json
import shlex
import subprocess
import sys

from ferment.identity import client_key
from ferment.lifetime import deadline_script, disarm_script
from ferment.vm.base import CACHE_MOUNT, WORKLOAD_SSH_HOST, Provider, ProviderError, SshTarget, VMInfo, VMSpec, utcnow

# Same image pixiebox runs on (nixos-lima v0.0.5): NixOS, nix preinstalled.
IMAGES = [
    {
        "location": "https://github.com/nixos-lima/nixos-lima/releases/download/v0.0.5/nixos-lima-v0.0.5-aarch64.qcow2",
        "arch": "aarch64",
        "digest": "sha512:e1daeb0dcec65c624253603ab5ec06f0831b0940cd95a88903f9bfd0ee4009b2c45806b868674c7e8cb12941e50799e85d710fc0e9ad659059108cebbc4d19c1",  # noqa: E501
    },
    {
        "location": "https://github.com/nixos-lima/nixos-lima/releases/download/v0.0.5/nixos-lima-v0.0.5-x86_64.qcow2",
        "arch": "x86_64",
        "digest": "sha512:51fbe74c569736f1141f1c6efeaa21a0901dff0bec5bc1e863c04c7765e150c3bebd82b7f50905fb7a0a9a9b050852c250ffbdcacd17b0dc15aeb86d47587436",  # noqa: E501
    },
]

RECORD = "ferment.json"
# On the Lima host (Lima expands ~): rebuildable data, so share/, not state/.
CACHE_ROOT = "~/.local/share/ferment/cache"

# vz carries ssh over vsock and the guest never learns that a client went away:
# sessions linger, keeping their -R ports bound. Have sshd probe and drop them.
# Each vsock connection runs a fresh `sshd -i`, so this applies to the next one.
# /etc/ssh/sshd_config is a nix-store symlink on NixOS: this replaces it with a
# real file, which lasts until the next activation (i.e. a reboot), and
# _sync_guest re-applies it after one.
SSHD_KEEPALIVE = """\
if ! grep -q '^ClientAliveInterval' /etc/ssh/sshd_config; then
  tmp=$(mktemp)
  { printf 'ClientAliveInterval 5\\nClientAliveCountMax 3\\n'; cat /etc/ssh/sshd_config; } > "$tmp"
  sudo install -m 644 "$tmp" /etc/ssh/sshd_config && rm -f "$tmp"
fi
"""


# The workload runs at the guest's own clock (ferment.server.clock), so
# nothing may sync it back to real time: NixOS's timesyncd, and Lima's guest
# agent, which sets the time on the host's behalf every few seconds. The agent
# keeps running (it does other jobs) without the capability to set the clock;
# its sync attempts then fail harmlessly. Both are runtime settings, re-applied
# after a reboot by _sync_guest.
GUEST_CLOCK_FREE = """\
sudo systemctl stop systemd-timesyncd 2>/dev/null || true
d=/run/systemd/system/lima-guestagent.service.d
if [ ! -f $d/ferment-clock.conf ]; then
  sudo mkdir -p $d
  printf '[Service]\\nCapabilityBoundingSet=~CAP_SYS_TIME\\n' | sudo tee $d/ferment-clock.conf >/dev/null
  sudo systemctl daemon-reload
  sudo systemctl restart lima-guestagent
fi
"""


WORKLOAD_SUFFIX = "-workload"
CONTROL_KEY = "~/.ssh/ferment_workload"


def control_ssh_script(workload: str) -> str:
    """On the control VM: a key of its own, and `ssh fermenter-workload`
    reaching the workload VM with it. Prints the public key."""
    entry = (
        f"Host {WORKLOAD_SSH_HOST}\n  HostName lima-{workload}.internal\n  IdentityFile {CONTROL_KEY}\n"
        "  IdentitiesOnly yes\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  LogLevel ERROR\n"
    )
    return f"""\
mkdir -p ~/.ssh && chmod 700 ~/.ssh
[ -f {CONTROL_KEY} ] || ssh-keygen -q -t ed25519 -N '' -C ferment-control -f {CONTROL_KEY}
touch ~/.ssh/config && chmod 600 ~/.ssh/config
grep -q '^Host {WORKLOAD_SSH_HOST}$' ~/.ssh/config || printf {shlex.quote(entry)} >> ~/.ssh/config
cat {CONTROL_KEY}.pub
"""


def trust_control_script(control: str) -> str:
    """On the workload VM: let the control VM through NixOS's firewall, on any
    port (the agent, the database). A runtime rule, re-applied after a reboot
    with the rest of the guest state."""
    return f"""\
ip=$(getent hosts lima-{control}.internal | awk '{{print $1; exit}}')
if [ -n "$ip" ] && ! sudo iptables -C nixos-fw -s "$ip" -j nixos-fw-accept 2>/dev/null; then
  sudo iptables -I nixos-fw -s "$ip" -j nixos-fw-accept
fi
"""


def lima_config(spec: VMSpec, *, private_network: bool = False, cache_root: str = CACHE_ROOT) -> dict:
    """`private_network`: on Lima's user-v2 network, where instances reach
    each other (lima-<name>.internal) and still reach the internet.
    `cache_root`: where spec.caches live on the Lima host."""
    cfg = {
        "vmType": "vz",
        "images": IMAGES,
        "cpus": spec.cpus,
        "memory": f"{spec.memory_gib}GiB",
        "disk": f"{spec.disk_gib}GiB",
        # A cloud VM can't see the laptop's disk; neither can this one, except
        # for the caches it asked for.
        "mounts": [
            {"location": f"{cache_root}/{name}", "mountPoint": f"{CACHE_MOUNT}/{name}", "writable": True}
            for name in spec.caches
        ],
        "portForwards": [
            # NixOS + Lima DHCP interception workaround
            # (https://github.com/nixos-lima/nixos-lima/issues/50)
            {"proto": "udp", "guestPort": 68, "guestIP": "0.0.0.0", "ignore": True},
            # No automatic forwarding: ports reach the client only through
            # ferment's ssh tunnel, as they would from a cloud VM.
            {"guestIP": "0.0.0.0", "guestIPMustBeZero": False, "guestPortRange": [1, 65535], "ignore": True},
            {"guestIP": "127.0.0.1", "guestPortRange": [1, 65535], "ignore": True},
        ],
        "containerd": {"system": False, "user": False},
    }
    if private_network:
        cfg["networks"] = [{"lima": "user-v2"}]
    return cfg


class LimaHost:
    """Where limactl runs: here (host=None) or over ssh on `host`.

    Commands are shell scripts; remotely they're interpreted by the login
    shell of the host (often zsh on macOS), so keep them POSIX.
    """

    def __init__(self, host: str | None, limactl: str = "limactl"):
        self.host = host or None
        self.limactl = limactl

    def run(self, script: str, *, input: str | None = None, capture: bool = True) -> str:
        argv = ["ssh", "-o", "BatchMode=yes", self.host, "--", script] if self.host else ["bash", "-c", script]
        try:
            result = subprocess.run(argv, input=input, text=True, capture_output=capture, check=True)
        except FileNotFoundError as e:
            raise ProviderError(f"{argv[0]} not found: {e}")
        except subprocess.CalledProcessError as e:
            where = f" on {self.host}" if self.host else ""
            detail = (e.stderr or "").strip() if capture else f"exit {e.returncode}"
            raise ProviderError(f"{script.split()[0]}{where} failed: {detail}")
        return result.stdout if capture else ""

    def limactl_cmd(self, *args: str) -> str:
        return " ".join(shlex.quote(a) for a in (self.limactl, *args))


class LimaProvider(Provider):
    name = "lima"

    def __init__(
        self,
        host: str | None = None,
        limactl: str = "limactl",
        cpus: int | None = None,
        memory_gib: int | None = None,
        disk_gib: int | None = None,
        control_cpus: int | None = None,
        control_memory_gib: int | None = None,
        control_disk_gib: int | None = None,
        cache_root: str = CACHE_ROOT,
    ):
        """cpus, memory_gib, disk_gib: the VM that runs the workload (the only
        VM, or a split fermenter's workload VM). control_*: a split
        fermenter's control VM (the server, builds, the archive)."""
        self.lima = LimaHost(host, limactl)
        self.cache_root = cache_root
        # Names whose guest rebooted since last seen (noticed by ssh_target):
        # callers may need to restart whatever they ran there.
        self.rebooted: set[str] = set()
        self.defaults = {k: v for k, v in {"cpus": cpus, "memory_gib": memory_gib, "disk_gib": disk_gib}.items() if v}
        self.control_defaults = {
            k: v
            for k, v in {"cpus": control_cpus, "memory_gib": control_memory_gib, "disk_gib": control_disk_gib}.items()
            if v
        }

    # -- host-side state ----------------------------------------------------

    def _instances(self) -> dict[str, dict]:
        """name -> limactl's instance info, plus `record` (ferment.json, or None)."""
        out = self.lima.run(
            self.lima.limactl_cmd("list", "--format", "json")
            + "; echo ---records---; "
            + 'find "${LIMA_HOME:-$HOME/.lima}" -mindepth 2 -maxdepth 2 -name '
            + RECORD
            + ' | while read -r f; do printf "%s\t" "$(basename "$(dirname "$f")")"; tr -d "\n" < "$f"; echo; done'
        )
        listing, _, records = out.partition("---records---")
        instances = {}
        for line in listing.splitlines():
            if line.strip():
                inst = json.loads(line)
                instances[inst["name"]] = {**inst, "record": None}
        for line in records.splitlines():
            if "\t" in line:
                name, raw = line.split("\t", 1)
                if name in instances:
                    instances[name]["record"] = json.loads(raw)
        return instances

    def _instance(self, name: str, instances: dict[str, dict] | None = None) -> dict:
        """A fermenter's (control or only) instance, by the fermenter's name."""
        inst = (instances if instances is not None else self._instances()).get(name)
        if inst is None or inst["record"] is None:
            where = f" on {self.lima.host}" if self.lima.host else ""
            raise ProviderError(f"no lima VM named {name!r} managed by ferment{where}")
        if inst["record"].get("role") == "workload":
            control = inst["record"].get("control")
            raise ProviderError(f"{name!r} is the workload VM of fermenter {control!r}; use {control!r}")
        return inst

    def _pair(self, name: str) -> tuple[dict, dict | None]:
        """A fermenter's instance and, if it's split, its workload instance."""
        instances = self._instances()
        inst = self._instance(name, instances)
        workload = inst["record"].get("workload")
        if workload is None:
            return inst, None
        winst = instances.get(workload)
        if winst is None or winst["record"] is None:
            raise ProviderError(f"fermenter {name!r} has lost its workload VM {workload!r}")
        return inst, winst

    def _write_record(self, inst: dict, record: dict) -> None:
        path = shlex.quote(f"{inst['dir']}/{RECORD}")
        self.lima.run(f"cat > {path}", input=json.dumps(record, indent=2) + "\n")

    def _guest(self, inst: dict, script: str) -> str:
        """Run `script` in the guest with Lima's own key, which only the Lima host holds."""
        cfg = shlex.quote(inst["sshConfigFile"])
        return self.lima.run(f"ssh -F {cfg} lima-{shlex.quote(inst['name'])} -- {shlex.quote(script)}")

    def _apply_guest_state(self, inst: dict) -> str:
        """Put recorded guest state in place (idempotent); returns the boot id."""
        record = inst["record"]
        script = SSHD_KEEPALIVE + _keys_script(record.get("authorized_keys", []))
        if record.get("role") == "control":  # keeps real time, and reaches its workload VM
            script += control_ssh_script(record["workload"])
        else:  # runs the workload, at its clock
            script += GUEST_CLOCK_FREE
            if record.get("role") == "workload":
                script += trust_control_script(record["control"])
        if record.get("delete_after"):
            script += deadline_script(dt.datetime.fromisoformat(record["delete_after"])) + "\n"
        return self._guest(inst, script + "cat /proc/sys/kernel/random/boot_id").strip().splitlines()[-1]

    def _sync_guest(self, inst: dict) -> None:
        """Re-apply recorded guest state if the guest booted since we last looked."""
        record = inst["record"]
        boot_id = self._guest(inst, "cat /proc/sys/kernel/random/boot_id").strip()
        if record.get("boot_id") == boot_id:
            return
        if "authorized_keys" not in record:
            # Made before keys were recorded: adopt whatever ferment had added.
            lima_key = self.lima.run('cat "${LIMA_HOME:-$HOME/.lima}/_config/user.pub"').strip()
            present = self._guest(inst, "cat ~/.ssh/authorized_keys 2>/dev/null || true").splitlines()
            record["authorized_keys"] = [k.strip() for k in present if k.startswith("ssh-") and k.strip() != lima_key]
        rebooted = "boot_id" in record
        record["boot_id"] = self._apply_guest_state(inst)
        self._write_record(inst, record)
        if rebooted:
            print(
                f"ferment: {inst['name']} rebooted since last seen; restored ssh keys, "
                "sshd keepalive, the delete-me timer, and its free clock",
                file=sys.stderr,
            )
            self.rebooted.add(inst["name"])

    def _info(self, inst: dict) -> VMInfo:
        record = inst["record"] or {}
        deadline = record.get("delete_after")
        return VMInfo(
            name=inst["name"],
            provider=self.name,
            status=inst.get("status", "unknown"),
            delete_after=dt.datetime.fromisoformat(deadline) if deadline else None,
            labels=record.get("labels", {}),
        )

    # -- Provider -----------------------------------------------------------

    def _start(self, spec: VMSpec, *, private_network: bool) -> dict:
        import yaml

        if spec.caches:  # Lima won't mount a directory that doesn't exist
            root = self.cache_root.replace("~", '"$HOME"', 1) if self.cache_root.startswith("~") else self.cache_root
            self.lima.run("mkdir -p " + " ".join(f"{root}/{shlex.quote(c)}" for c in spec.caches))
        cfg = yaml.safe_dump(
            lima_config(spec, private_network=private_network, cache_root=self.cache_root), sort_keys=False
        )
        # limactl reads the template from stdin with "-"; its progress streams
        # to the terminal (first boot downloads the image, which takes a while).
        self.lima.run(
            self.lima.limactl_cmd("start", "--tty=false", f"--name={spec.name}", "-"),
            input=cfg,
            capture=False,
        )
        return self._instances()[spec.name]

    def _finish(self, inst: dict, record: dict) -> None:
        inst["record"] = record
        record["boot_id"] = self._apply_guest_state(inst)
        self._write_record(inst, record)

    def create(self, spec: VMSpec) -> VMInfo:
        for key, value in self.defaults.items():
            setattr(spec.workload or spec, key, value)
        if spec.workload:
            for key, value in self.control_defaults.items():
                setattr(spec, key, value)
        workload = f"{spec.name}{WORKLOAD_SUFFIX}" if spec.workload else None
        existing = self._instances()
        for name in filter(None, (spec.name, workload)):
            if name in existing:
                raise ProviderError(f"a lima instance named {name!r} already exists")
        record = {
            "delete_after": (utcnow() + spec.ttl).isoformat() if spec.ttl is not None else None,
            "labels": spec.labels,
            "authorized_keys": [_check_key(k) for k in spec.authorized_keys],
        }
        inst = self._start(spec, private_network=workload is not None)
        if workload is None:
            self._finish(inst, record)
            return self._info(inst)

        w = spec.workload
        winst = self._start(
            VMSpec(name=workload, ttl=spec.ttl, cpus=w.cpus, memory_gib=w.memory_gib, disk_gib=w.disk_gib),
            private_network=True,
        )
        self._finish(inst, {**record, "role": "control", "workload": workload})
        control_key = self._guest(inst, control_ssh_script(workload)).strip().splitlines()[-1]
        self._finish(
            winst,
            {
                **record,
                "authorized_keys": [*record["authorized_keys"], _check_key(control_key)],
                "role": "workload",
                "control": spec.name,
            },
        )
        return self._info(inst)

    def list(self) -> list[VMInfo]:
        return [
            self._info(inst)
            for inst in self._instances().values()
            if inst["record"] is not None and inst["record"].get("role") != "workload"
        ]

    def delete(self, name: str) -> None:
        # Only fermenters ferment made, and a split one's workload VM with it.
        instances = self._instances()
        inst = self._instance(name, instances)
        names = [name]
        workload = inst["record"].get("workload")
        if workload in instances:
            names.append(workload)
        self.lima.run(self.lima.limactl_cmd("delete", "--force", *names))

    def set_deadline(self, name: str, delete_after: dt.datetime | None) -> None:
        for inst in filter(None, self._pair(name)):
            inst["record"]["delete_after"] = delete_after.isoformat() if delete_after else None
            self._write_record(inst, inst["record"])
            self._guest(inst, deadline_script(delete_after) if delete_after else disarm_script())

    def authorize_key(self, name: str, public_key: str) -> None:
        """Let `public_key` log in to the fermenter's VMs (recorded, so it survives reboots)."""
        key = _check_key(public_key)
        for inst in filter(None, self._pair(name)):
            self._sync_guest(inst)  # adopt pre-existing keys before adding to the record
            keys = inst["record"].setdefault("authorized_keys", [])
            if key not in keys:
                keys.append(key)
            self._guest(inst, _keys_script([key]))
            self._write_record(inst, inst["record"])

    def _running(self, inst: dict) -> str:
        """Check it's up, re-apply guest state after a reboot; returns its ssh user."""
        if inst.get("status") != "Running":
            raise ProviderError(f"lima VM {inst['name']!r} is {inst.get('status')}, not Running")
        self._sync_guest(inst)
        cfg = shlex.quote(inst["sshConfigFile"])
        return self.lima.run(f"awk '$1 == \"User\" {{print $2; exit}}' {cfg}").strip()

    def ssh_target(self, name: str) -> SshTarget:
        inst, winst = self._pair(name)
        user = self._running(inst)
        if winst is not None:
            # Whatever reaches control may next go through it to the workload
            # (deploy's copy, the agent): after a reboot, that needs control's
            # key and firewall rule back on the workload VM too.
            self._running(winst)
        options = self._client_options()
        if self.lima.host:
            options["ProxyJump"] = self.lima.host
        return SshTarget(host="127.0.0.1", port=inst["sshLocalPort"], user=user, options=options)

    def workload_host(self, name: str) -> str | None:
        workload = self._instance(name)["record"].get("workload")
        return f"lima-{workload}.internal" if workload else None

    def workload_target(self, name: str) -> SshTarget | None:
        _, winst = self._pair(name)
        if winst is None:
            return None
        user = self._running(winst)
        # Through the control VM: its sshd forwards to the workload VM's
        # address on their private network (ssh -W). A ProxyCommand rather
        # than ProxyJump, so the hop gets the same options (key, host keys).
        hop = self.ssh_target(name).argv()
        hop[-1:-1] = ["-W", "%h:%p"]
        options = {**self._client_options(), "ProxyCommand": " ".join(shlex.quote(a) for a in hop)}
        return SshTarget(host=f"lima-{winst['name']}.internal", user=user, options=options)

    def _client_options(self) -> dict[str, str]:
        key, _ = client_key()
        return {
            "IdentityFile": str(key),
            "IdentitiesOnly": "yes",
            # Ephemeral VM on the host's loopback: its host key is new every
            # time and the port gets reused. Lima itself does the same.
            "StrictHostKeyChecking": "no",
            "UserKnownHostsFile": "/dev/null",
            "LogLevel": "ERROR",
        }


def _check_key(public_key: str) -> str:
    key = public_key.strip()
    if not key.startswith("ssh-"):
        raise ProviderError("that doesn't look like an ssh public key")
    return key


def _keys_script(keys: list[str]) -> str:
    """Shell that makes sure each key is in ~/.ssh/authorized_keys (idempotent)."""
    f = "~/.ssh/authorized_keys"
    lines = [f"mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch {f} && chmod 600 {f}"]
    lines += [f"grep -qxF {shlex.quote(k)} {f} || echo {shlex.quote(k)} >> {f}" for k in keys]
    return "\n".join(lines) + "\n"
