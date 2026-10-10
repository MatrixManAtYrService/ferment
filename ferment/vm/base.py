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


@dataclass
class VMSpec:
    name: str
    ttl: dt.timedelta
    cpus: int = 4
    memory_gib: int = 8
    disk_gib: int = 50
    labels: dict[str, str] = field(default_factory=dict)


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
    def set_deadline(self, name: str, delete_after: dt.datetime) -> None:
        """Move the delete-me date (extend, or shorten)."""

    @abstractmethod
    def ssh_target(self, name: str) -> SshTarget: ...
