"""Needs: work waiting for something only an attached client can provide.

A fermenter can't reach some things itself (a package mirror behind a VPN, a
private git host), and the clients that can come and go: a laptop attaches
for an hour, closes its lid, comes back tomorrow. So work that needs one of
them waits for it instead of failing, and says what it's waiting for.

A capability names what's needed, in the same terms attached sessions
advertise what they serve:

    relay:<host>:<port>   the VM reaches host:port through a client (ferment.relay)
    fetch:<git host>      a client fetches code from that host for the VM (ferment.fulfill)

A client that's waiting registers a need with the server (`Waiting`) and
keeps it fresh while it waits, so the board can say "fmnt apply needs
someone attached who relays nexus.example.com:443 (12m)". A need nobody
refreshes lapses, like a session nobody heartbeats. Source requests are
needs too (fetch:<their repo's host>) without being registered: the board
reads them from the queue.
"""

from __future__ import annotations

import sys

import httpx

from ferment.client import WHO_HEADER, whoami


def relay(spec: str) -> str:
    """'host:port' -> its capability."""
    return f"relay:{spec}"


def fetch(host: str) -> str:
    return f"fetch:{host}"


def offered(session: dict) -> set[str]:
    """What an attached session (store.sessions()) serves."""
    return {relay(r) for r in session.get("relays", [])} | {fetch(h) for h in session.get("fetches", [])}


def describe(capability: str) -> str:
    kind, _, what = capability.partition(":")
    if kind == "relay":
        return f"someone attached who relays {what}"
    if kind == "fetch":
        return f"someone attached who fetches from {what}"
    return f"someone attached who provides {capability}"


class Waiting:
    """`with Waiting(url, capability, purpose) as w:` while a client waits;
    call `w.beat()` every so often. Leaving the block closes the need: met
    if it ended normally, given up if by an exception (ctrl-c).

    Advisory: the board learns what's waiting, nothing depends on it, so a
    server that predates needs (404) or a blip is ignored.
    """

    def __init__(self, server_url: str, capability: str, purpose: str):
        self.url = server_url
        self.capability = capability
        self.purpose = purpose
        self.id: int | None = None
        self.headers = {WHO_HEADER: whoami()}

    def _call(self, method: str, path: str, **kw) -> httpx.Response | None:
        try:
            return httpx.request(method, f"{self.url}{path}", headers=self.headers, timeout=10, **kw)
        except httpx.HTTPError:
            return None

    def __enter__(self) -> Waiting:
        r = self._call("POST", "/needs", json={"capability": self.capability, "purpose": self.purpose})
        if r is not None and r.status_code == 200:
            self.id = r.json()["id"]
        return self

    def beat(self) -> None:
        if self.id is None:  # the server wasn't reachable at first
            self.__enter__()
            return
        r = self._call("POST", f"/needs/{self.id}/heartbeat")
        if r is not None and r.status_code == 404:  # lapsed (e.g. this machine slept): register anew
            self.id = None
            self.__enter__()

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.id is not None:
            self._call("DELETE", f"/needs/{self.id}", params={"met": exc_type is None})
        if exc_type is KeyboardInterrupt:
            print(f"gave up waiting for {describe(self.capability)}", file=sys.stderr)
