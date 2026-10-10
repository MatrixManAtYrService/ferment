"""This client's ssh identity for logging in to VMs.

A dedicated keypair, made on first use under the client state dir, rather
than whatever is in ~/.ssh: it's what a VM is told to trust at create time,
and what a teammate hands over to be let in (`authorize_key`).
"""

from __future__ import annotations

import socket
import subprocess
from pathlib import Path

from ferment.config import state_dir


def client_key() -> tuple[Path, str]:
    """(private key path, public key line), creating the pair if needed."""
    key = state_dir() / "ssh" / "id_ed25519"
    if not key.exists():
        key.parent.mkdir(parents=True, exist_ok=True)
        key.parent.chmod(0o700)
        comment = f"ferment@{socket.gethostname()}"
        subprocess.run(
            ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", comment, "-f", str(key)],
            check=True,
        )
    return key, key.with_suffix(".pub").read_text().strip()
