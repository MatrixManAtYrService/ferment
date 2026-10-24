"""Delete-me dates.

Two layers, because the point is that a forgotten VM stops costing money even
if nobody runs another ferment command:

- The provider records the deadline (cloud labels/native termination, or
  lima's ferment.json) so `ls` can show it and a reaper can act on it.
- The guest arms its own timer: at the deadline it powers itself off. That
  needs no client, no credentials, and no cron job anywhere else.

A lifetime of `never` means no date at all: for VMs on hardware you own,
where a forgotten one costs nothing but its share of the host.
"""

from __future__ import annotations

import datetime as dt
import re

from ferment import ssh
from ferment.vm.base import SshTarget, utcnow

GUARD_UNIT = "ferment-deadline"

_DURATION = re.compile(r"^(\d+)([mhd])$")
NEVER = "never"


def parse_duration(text: str) -> dt.timedelta:
    """'90m', '72h', '3d' -> timedelta."""
    m = _DURATION.match(text.strip())
    if not m:
        raise ValueError(f"bad duration {text!r}: use e.g. 90m, 72h, 3d")
    n, unit = int(m.group(1)), m.group(2)
    return {"m": dt.timedelta(minutes=n), "h": dt.timedelta(hours=n), "d": dt.timedelta(days=n)}[unit]


def parse_lifetime(text: str) -> dt.timedelta | None:
    """A duration (parse_duration), or 'never' -> None: no delete-me date."""
    return None if text.strip() == NEVER else parse_duration(text)


def disarm_script() -> str:
    """Shell that stops the in-guest poweroff timer, if one is armed."""
    return (
        f"sudo systemctl stop {GUARD_UNIT}.timer {GUARD_UNIT}.service 2>/dev/null; "
        f"sudo systemctl reset-failed {GUARD_UNIT}.timer {GUARD_UNIT}.service 2>/dev/null; true"
    )


def deadline_script(delete_after: dt.datetime) -> str:
    """Shell (run as a sudo-capable guest user) that (re)arms the in-guest
    poweroff timer for `delete_after`.

    A transient systemd timer relative to now (the guest clock is the real
    one; only workload processes are time-faked). Transient units don't
    survive a reboot, so providers re-arm it when they notice one. A deadline
    already past powers off within a minute.
    """
    seconds = max(60, int((delete_after - utcnow()).total_seconds()))
    return (
        f"{disarm_script()}; "
        f"sudo systemd-run --quiet --unit={GUARD_UNIT} --on-active={seconds}s "
        f"--description='ferment delete-me deadline {delete_after.isoformat()}' systemctl poweroff"
    )


def arm_guest_deadline(target: SshTarget, delete_after: dt.datetime) -> None:
    """(Re)arm the in-guest poweroff timer over ssh."""
    ssh.run(target, deadline_script(delete_after))
