"""Local VMs via Lima (macOS vz, or qemu on Linux), booting a NixOS image.

Free and fast to iterate on, and close enough to a cloud VM for everything
above the provider: the VM is reached only over ssh (Lima's own port
forwarding and home-directory mounts are switched off so nothing works here
that would not work in the cloud).

Lima has no labels or instance metadata, so ferment keeps its own record in
the instance directory: <lima instance dir>/ferment.json. Only instances that
carry one are listed as ferment's.
"""

from __future__ import annotations

import datetime as dt
import json
import subprocess
from pathlib import Path

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


class LimaProvider(Provider):
    name = "lima"

    def __init__(self, cpus: int | None = None, memory_gib: int | None = None, disk_gib: int | None = None):
        self.defaults = {k: v for k, v in {"cpus": cpus, "memory_gib": memory_gib, "disk_gib": disk_gib}.items() if v}

    # -- limactl ------------------------------------------------------------

    def _limactl(self, *args: str, capture: bool = True) -> str:
        cmd = ["limactl", *args]
        try:
            result = subprocess.run(cmd, check=True, text=True, capture_output=capture)
        except FileNotFoundError:
            raise ProviderError("limactl not found; install Lima (https://lima-vm.io)")
        except subprocess.CalledProcessError as e:
            detail = (e.stderr or "").strip()
            raise ProviderError(f"{' '.join(cmd)} failed: {detail}")
        return result.stdout if capture else ""

    def _instances(self) -> list[dict]:
        out = self._limactl("list", "--format", "json")
        return [json.loads(line) for line in out.splitlines() if line.strip()]

    @staticmethod
    def _record(inst: dict) -> dict | None:
        path = Path(inst.get("dir", "")) / RECORD
        if not path.exists():
            return None
        return json.loads(path.read_text())

    def _instance(self, name: str) -> dict:
        for inst in self._instances():
            if inst.get("name") == name and self._record(inst) is not None:
                return inst
        raise ProviderError(f"no lima VM named {name!r} managed by ferment")

    # -- Provider -----------------------------------------------------------

    def create(self, spec: VMSpec) -> VMInfo:
        for key, value in self.defaults.items():
            setattr(spec, key, value)
        if any(inst.get("name") == spec.name for inst in self._instances()):
            raise ProviderError(f"a lima instance named {spec.name!r} already exists")

        import yaml

        cfg = yaml.safe_dump(lima_config(spec), sort_keys=False)
        # limactl reads the template from stdin with "-"; output streams to the
        # terminal (first boot downloads the image, which takes a while).
        try:
            subprocess.run(
                ["limactl", "start", "--tty=false", f"--name={spec.name}", "-"],
                input=cfg,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise ProviderError(f"limactl start {spec.name} failed (exit {e.returncode})")

        delete_after = utcnow() + spec.ttl
        inst = next(i for i in self._instances() if i.get("name") == spec.name)
        self._write_record(inst, {"delete_after": delete_after.isoformat(), "labels": spec.labels})
        return self._info(inst)

    def _write_record(self, inst: dict, record: dict) -> None:
        (Path(inst["dir"]) / RECORD).write_text(json.dumps(record, indent=2) + "\n")

    def _info(self, inst: dict) -> VMInfo:
        record = self._record(inst) or {}
        deadline = record.get("delete_after")
        return VMInfo(
            name=inst["name"],
            provider=self.name,
            status=inst.get("status", "unknown"),
            delete_after=dt.datetime.fromisoformat(deadline) if deadline else None,
            labels=record.get("labels", {}),
        )

    def list(self) -> list[VMInfo]:
        return [self._info(inst) for inst in self._instances() if self._record(inst) is not None]

    def delete(self, name: str) -> None:
        self._instance(name)  # refuse to delete instances ferment didn't make
        self._limactl("delete", "--force", name)

    def set_deadline(self, name: str, delete_after: dt.datetime) -> None:
        inst = self._instance(name)
        record = self._record(inst) or {}
        record["delete_after"] = delete_after.isoformat()
        self._write_record(inst, record)

    def ssh_target(self, name: str) -> SshTarget:
        inst = self._instance(name)
        if inst.get("status") != "Running":
            raise ProviderError(f"lima VM {name!r} is {inst.get('status')}, not Running")
        # Lima writes a complete ssh config (key, user, port) per instance.
        return SshTarget(host=f"lima-{name}", config_file=inst["sshConfigFile"])
