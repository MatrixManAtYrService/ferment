"""The provider contract: what ferment needs from anything that hosts a VM.

A provider only creates, lists, and destroys VMs, records their deadline, and
says how to ssh in. Everything above that (pushing code, tunnels, running the
stack) speaks plain OpenSSH to the target a provider returns, so it is the
same code for a local VM and a cloud one.
"""

from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


# On a split fermenter's control VM, `ssh fermenter-workload` reaches the
# workload VM (the provider sets that up), so whatever runs there (deploy
# scripts, the server) needn't know how the provider names or addresses it.
WORKLOAD_SSH_HOST = "fermenter-workload"

# Where a VM finds the caches it asked for (VMSpec.caches): CACHE_MOUNT/<name>.
CACHE_MOUNT = "/mnt/ferment-cache"


@dataclass
class WorkloadSpec:
    """The second VM of a split fermenter (docs/split-vms.md): it runs the
    workload at the fake date, next to a control VM that stays on real time."""

    cpus: int = 4
    memory_gib: int = 8
    disk_gib: int = 50


@dataclass
class VMSpec:
    name: str
    ttl: dt.timedelta | None  # None: no delete-me date (ferment.lifetime.NEVER)
    cpus: int = 4
    memory_gib: int = 8
    disk_gib: int = 50
    labels: dict[str, str] = field(default_factory=dict)
    # Public keys allowed to ssh in as the VM's user (at least the creator's).
    authorized_keys: list[str] = field(default_factory=list)
    # Given: this VM is the control VM, and the provider also makes the
    # workload VM, on a private network with it. None: one VM does both.
    workload: WorkloadSpec | None = None
    # Named caches that outlive the VM and are shared with later ones (e.g. a
    # package manager's downloads), writable at CACHE_MOUNT/<name>: the one
    # thing a fermenter keeps from the machine that hosts it. Each provider
    # supplies them its own way (Lima: a directory on the Lima host). Only
    # this VM gets them, not a split fermenter's workload VM.
    caches: list[str] = field(default_factory=list)


@dataclass
class VMInfo:
    name: str
    provider: str
    status: str  # provider's own word for it: Running, Stopped, ...
    delete_after: dt.datetime | None
    labels: dict[str, str] = field(default_factory=dict)

    @property
    def expired(self) -> bool:
        return self.delete_after is not None and self.delete_after <= utcnow()

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "provider": self.provider,
            "status": self.status,
            "delete_after": self.delete_after.isoformat() if self.delete_after else None,
            "expired": self.expired,
            "labels": self.labels,
        }


@dataclass
class SshTarget:
    """Enough to build an `ssh` command line for a VM.

    `options` are `-o` style settings (ProxyCommand, IdentityFile, ...) so a
    provider can express IAP tunnels, jump hosts, or a lima ssh config
    without the caller knowing which.
    """

    host: str
    user: str | None = None
    port: int | None = None
    options: dict[str, str] = field(default_factory=dict)
    config_file: str | None = None

    def argv(self) -> list[str]:
        args = ["ssh"]
        if self.config_file:
            args += ["-F", self.config_file]
        if self.port:
            args += ["-p", str(self.port)]
        for key, value in self.options.items():
            args += ["-o", f"{key}={value}"]
        args.append(f"{self.user}@{self.host}" if self.user else self.host)
        return args


class ProviderError(RuntimeError):
    pass


class Provider(ABC):
    name: str

    @abstractmethod
    def create(self, spec: VMSpec) -> VMInfo:
        """Create and boot a VM; return once it accepts ssh."""

    @abstractmethod
    def list(self) -> list[VMInfo]:
        """VMs this provider manages for ferment (not every VM it can see)."""

    def get(self, name: str) -> VMInfo:
        for vm in self.list():
            if vm.name == name:
                return vm
        raise ProviderError(f"no {self.name} VM named {name!r}")

    @abstractmethod
    def delete(self, name: str) -> None: ...

    @abstractmethod
    def set_deadline(self, name: str, delete_after: dt.datetime | None) -> None:
        """Move the delete-me date (extend, or shorten); None removes it."""

    @abstractmethod
    def ssh_target(self, name: str) -> SshTarget:
        """How this client reaches the VM, logging in with its own key
        (ferment.identity.client_key)."""

    @abstractmethod
    def authorize_key(self, name: str, public_key: str) -> None:
        """Let another public key log in (sharing a VM with a teammate)."""

    def workload_host(self, name: str) -> str | None:
        """Where the control VM reaches the workload VM (a name or address on
        their private network); None if the fermenter is one VM."""
        return None

    def workload_target(self, name: str) -> SshTarget | None:
        """How this client reaches the workload VM (through the control VM);
        None if the fermenter is one VM."""
        return None
