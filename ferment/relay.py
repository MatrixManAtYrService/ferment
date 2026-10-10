"""Make a host that only the client can reach appear on the VM under its own name.

    VM:   name -> 127.77.0.N                                   (/etc/hosts)
          socat 127.77.0.N:port -> 127.0.0.1:vm_port           (root, so any port)
    ssh:  -R 127.0.0.1:vm_port -> name:port                    (client connects)

TCP passthrough: TLS runs end to end and the name matches the certificate, so
URLs that work on the client work unchanged on the VM (package mirrors behind
a VPN, say). Traffic flows only while some client session holds the forwards
(`forwards()`, passed to ssh.run); otherwise connections are refused.

NixOS guests only.
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass

from ferment import ssh
from ferment.ssh import Forward
from ferment.vm.base import SshTarget

UNIT_PREFIX = "ferment-relay"
HOSTS_TAG = "# ferment-relay"
# NixOS maps the hostname to 127.0.0.2; stay clear of it.
LOOPBACK_NET = "127.77.0"
FIRST_VM_PORT = 18440


@dataclass(frozen=True)
class Relay:
    host: str
    port: int
    address: str  # VM loopback address `host` resolves to
    vm_port: int  # VM loopback port the -R forward binds

    def forward(self) -> Forward:
        return Forward(0, self.vm_port, reverse=True, dest=f"{self.host}:{self.port}", label=self.host)


def relays(specs: list[str]) -> list[Relay]:
    """["host:port", ...] -> Relays with VM addresses and ports assigned in order."""
    out = []
    for i, spec in enumerate(specs):
        host, sep, port = spec.rpartition(":")
        if not sep or not host or not port.isdigit():
            raise ValueError(f"relay {spec!r}: expected host:port")
        out.append(Relay(host, int(port), f"{LOOPBACK_NET}.{i + 1}", FIRST_VM_PORT + i))
    return out


def forwards(relays: list[Relay]) -> list[Forward]:
    return [r.forward() for r in relays]


def install_script(relays: list[Relay], *, socat: str = "nixpkgs#socat", inputs_from: str | None = None) -> str:
    """Shell script (run as the VM's ssh user) that replaces any previous relays with `relays`."""
    nix = "nix --extra-experimental-features 'nix-command flakes'"
    lines = [
        "set -eu",
        f"sudo systemctl stop '{UNIT_PREFIX}-*' 2>/dev/null || true",
        f"sudo systemctl reset-failed '{UNIT_PREFIX}-*' 2>/dev/null || true",
        f"if [ ! -L /etc/hosts ]; then sudo sed -i '/ {HOSTS_TAG}$/d' /etc/hosts; fi",
    ]
    if not relays:
        return "\n".join(lines) + "\n"
    build = f"{nix} build --no-link --print-out-paths"
    if inputs_from:
        build += f" --inputs-from {shlex.quote(inputs_from)}"
    lines += [
        # A session whose client vanished keeps its -R port bound, swallowing relayed
        # traffic (lima's ssh-over-vsock never reports the disconnect). Have sshd drop them.
        "if ! grep -q '^ClientAliveInterval' /etc/ssh/sshd_config; then",
        "  tmp=$(mktemp)",
        "  { printf 'ClientAliveInterval 5\\nClientAliveCountMax 3\\n'; cat /etc/ssh/sshd_config; } > \"$tmp\"",
        '  sudo install -m 644 "$tmp" /etc/ssh/sshd_config && rm -f "$tmp"',
        "fi",
        # A store symlink until now; a real file lasts until the next NixOS activation.
        'if [ -L /etc/hosts ]; then sudo cp --remove-destination "$(readlink -f /etc/hosts)" /etc/hosts; fi',
        "printf '%s\\n' "
        + " ".join(shlex.quote(f"{r.address} {r.host} {HOSTS_TAG}") for r in relays)
        + " | sudo tee -a /etc/hosts >/dev/null",
        "sudo systemctl try-restart nscd.service 2>/dev/null || true",
        f"socat=$({build} {shlex.quote(socat)})/bin/socat",
    ]
    for i, r in enumerate(relays):
        lines.append(
            f'sudo systemd-run --quiet --unit={UNIT_PREFIX}-{i + 1} --property=Restart=always "$socat" '
            f"TCP-LISTEN:{r.port},bind={r.address},reuseaddr,fork TCP:127.0.0.1:{r.vm_port}"
        )
    return "\n".join(lines) + "\n"


def install(
    target: SshTarget, relays: list[Relay], *, socat: str = "nixpkgs#socat", inputs_from: str | None = None
) -> None:
    """Point each relay's host at the VM end of its forward. Idempotent; [] removes them all."""
    ssh.run(target, install_script(relays, socat=socat, inputs_from=inputs_from))
