"""Hearing what others sharing a fermenter are doing.

Attached clients follow the fermenter's broadcast log entries (see
ferment.server.logs) and print everyone else's, so two people don't set the
clock, restore, or build the same thing at once without knowing.
"""

from __future__ import annotations

import sys
import threading

import httpx

from ferment.client import whoami


class BroadcastWatcher(threading.Thread):
    POLL = 3.0

    def __init__(self, server_url: str, me: str | None = None):
        super().__init__(daemon=True)
        self.url = server_url
        self.me = me or whoami()
        self.stop = threading.Event()
        self.offset: int | None = None

    def poll(self) -> list[dict]:
        """Others' broadcasts since the last poll (the first poll starts at now)."""
        r = httpx.get(f"{self.url}/broadcasts", params={"after": -1 if self.offset is None else self.offset}, timeout=5)
        body = r.json()
        self.offset = body["offset"]
        return [e for e in body["entries"] if e.get("who") != self.me]

    def run(self) -> None:
        while not self.stop.wait(self.POLL):
            try:
                entries = self.poll()
            except (httpx.HTTPError, ValueError, KeyError):
                continue  # tunnel not up yet, server restarting, ...
            for e in entries:
                print(f"[{e.get('real_time', '')[11:19]}] {e['message']}", file=sys.stderr, flush=True)
