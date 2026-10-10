"""VMs via Lima (macOS vz, or qemu on Linux), booting a NixOS image.

Free and fast to iterate on, and close enough to a cloud VM for everything
above the provider: the VM is reached only over ssh (Lima's own port
forwarding and home-directory mounts are switched off so nothing works here
that would not work in the cloud).

The Lima host can be this machine or another one (`host`: an ssh
destination). Remotely, every limactl call runs over ssh, and clients reach
the VM's sshd by jumping through the host (ProxyJump), the same shape as
reaching a cloud VM through a bastion or IAP. Either way the client logs in to
the VM with its own key (`authorized_keys` on the spec, or `authorize_key`
later), never with Lima's.

Lima has no labels or instance metadata, so ferment keeps its own record in
the instance directory: <lima instance dir>/ferment.json. Only instances that
carry one are listed as ferment's.
"""

from __future__ import annotations

import datetime as dt
import json
import shlex
import subprocess

from ferment.identity import client_key
from ferment.vm.base import Provider, ProviderError, SshTarget, VMInfo, VMSpec, utcnow

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

# vz carries ssh over vsock and the guest never learns that a client went away:
# sessions linger, keeping their -R ports bound. Have sshd probe and drop them.
# Each vsock connection runs a fresh `sshd -i`, so this applies to the next one.
SSHD_KEEPALIVE = """\
if ! grep -q '^ClientAliveInterval' /etc/ssh/sshd_config; then
  tmp=$(mktemp)
  { printf 'ClientAliveInterval 5\\nClientAliveCountMax 3\\n'; cat /etc/ssh/sshd_config; } > "$tmp"
  sudo install -m 644 "$tmp" /etc/ssh/sshd_config && rm -f "$tmp"
fi
"""


def lima_config(spec: VMSpec) -> dict:
    return {
        "vmType": "vz",
        "images": IMAGES,
        "cpus": spec.cpus,
        "memory": f"{spec.memory_gib}GiB",
        "disk": f"{spec.disk_gib}GiB",
        # A cloud VM can't see the laptop's disk; neither can this one.
        "mounts": [],
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
    ):
        self.lima = LimaHost(host, limactl)
        self.defaults = {k: v for k, v in {"cpus": cpus, "memory_gib": memory_gib, "disk_gib": disk_gib}.items() if v}

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

    def _instance(self, name: str) -> dict:
        inst = self._instances().get(name)
        if inst is None or inst["record"] is None:
            where = f" on {self.lima.host}" if self.lima.host else ""
            raise ProviderError(f"no lima VM named {name!r} managed by ferment{where}")
        return inst

    def _write_record(self, inst: dict, record: dict) -> None:
        path = shlex.quote(f"{inst['dir']}/{RECORD}")
        self.lima.run(f"cat > {path}", input=json.dumps(record, indent=2) + "\n")

    def _guest(self, inst: dict, script: str) -> None:
        """Run `script` in the guest with Lima's own key, which only the Lima host holds."""
        cfg = shlex.quote(inst["sshConfigFile"])
        self.lima.run(f"ssh -F {cfg} lima-{shlex.quote(inst['name'])} -- {shlex.quote(script)}")

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

    def create(self, spec: VMSpec) -> VMInfo:
        for key, value in self.defaults.items():
            setattr(spec, key, value)
        if spec.name in self._instances():
            raise ProviderError(f"a lima instance named {spec.name!r} already exists")

        import yaml

        cfg = yaml.safe_dump(lima_config(spec), sort_keys=False)
        # limactl reads the template from stdin with "-"; its progress streams
        # to the terminal (first boot downloads the image, which takes a while).
        self.lima.run(
            self.lima.limactl_cmd("start", "--tty=false", f"--name={spec.name}", "-"),
            input=cfg,
            capture=False,
        )
        inst = self._instances()[spec.name]
        record = {"delete_after": (utcnow() + spec.ttl).isoformat(), "labels": spec.labels}
        self._write_record(inst, record)
        inst["record"] = record
        self._guest(inst, SSHD_KEEPALIVE)
        for key in spec.authorized_keys:
            self.authorize_key(spec.name, key)
        return self._info(inst)

    def list(self) -> list[VMInfo]:
        return [self._info(inst) for inst in self._instances().values() if inst["record"] is not None]

    def delete(self, name: str) -> None:
        self._instance(name)  # refuse to delete instances ferment didn't make
        self.lima.run(self.lima.limactl_cmd("delete", "--force", name))

    def set_deadline(self, name: str, delete_after: dt.datetime) -> None:
        inst = self._instance(name)
        record = {**inst["record"], "delete_after": delete_after.isoformat()}
        self._write_record(inst, record)

    def authorize_key(self, name: str, public_key: str) -> None:
        """Let `public_key` log in to the VM."""
        inst = self._instance(name)
        key = public_key.strip()
        if not key.startswith("ssh-"):
            raise ProviderError("that doesn't look like an ssh public key")
        q, keys = shlex.quote(key), "~/.ssh/authorized_keys"
        self._guest(
            inst,
            f"mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch {keys} && "
            f"(grep -qxF {q} {keys} || echo {q} >> {keys}) && chmod 600 {keys}",
        )

    def ssh_target(self, name: str) -> SshTarget:
        inst = self._instance(name)
        if inst.get("status") != "Running":
            raise ProviderError(f"lima VM {name!r} is {inst.get('status')}, not Running")
        cfg = shlex.quote(inst["sshConfigFile"])
        user = self.lima.run(f"awk '$1 == \"User\" {{print $2; exit}}' {cfg}").strip()
        key, _ = client_key()
        options = {
            "IdentityFile": str(key),
            "IdentitiesOnly": "yes",
            # Ephemeral VM on the host's loopback: its host key is new every
            # time and the port gets reused. Lima itself does the same.
            "StrictHostKeyChecking": "no",
            "UserKnownHostsFile": "/dev/null",
            "LogLevel": "ERROR",
        }
        if self.lima.host:
            options["ProxyJump"] = self.lima.host
        return SshTarget(host="127.0.0.1", port=inst["sshLocalPort"], user=user, options=options)
