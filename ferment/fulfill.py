"""An attached client fetching code on the VM's behalf.

The VM can't reach some repos (corp GitHub behind a VPN), but a client
attached to it can. Anyone may queue a source request on the fermenter
(POST /source-requests); a client running a SourceFulfiller for that repo's
host claims it, resolves and fetches with its own git access, and uploads a
bundle of just what the VM lacks. Credentials never leave the client.
"""

from __future__ import annotations

import sys
import threading
from collections.abc import Callable

import httpx

from ferment import sources
from ferment.client import WHO_HEADER, whoami


class SourceFulfiller(threading.Thread):
    POLL = 3.0

    def __init__(self, server_url: str, hosts: list[str], available: Callable[[str], bool] | None = None):
        """`available(capability)`: whether this client can serve it right now
        (e.g. its route to the git host is up); None: always."""
        super().__init__(daemon=True)
        self.url = server_url
        self.hosts = set(hosts)
        self.available = available or (lambda _cap: True)
        self.headers = {WHO_HEADER: whoami()}
        self.stop = threading.Event()

    def _say(self, msg: str) -> None:
        print(f"fetching for the fermenter: {msg}", file=sys.stderr)

    def run(self) -> None:
        while not self.stop.wait(self.POLL):
            try:
                pending = httpx.get(f"{self.url}/source-requests", timeout=5).json()
            except (httpx.HTTPError, ValueError):
                continue  # tunnel not up yet, or a blip
            for req in pending:
                host = sources.repo_host(req["repo"])
                if host in self.hosts and self.available(f"fetch:{host}") and self._claim(req["id"]):
                    self.fulfill(req)

    def _claim(self, request_id: int) -> bool:
        try:
            r = httpx.post(f"{self.url}/source-requests/{request_id}/claim", headers=self.headers, timeout=5)
        except httpx.HTTPError:
            return False
        return r.status_code == 200  # 409: someone else got it

    def fulfill(self, req: dict) -> None:
        name, repo, ref = req["name"], req["repo"], req["ref"]
        try:
            commit = sources.resolve(repo, ref)
            mirror = sources.fetch(repo, commit)
            have = httpx.get(f"{self.url}/sources/{name}", timeout=30).json()["commits"]
            if sources.missing(mirror, commit, have):
                data = sources.bundle(mirror, commit, have)
                self._say(f"{name} {ref} = {commit[:12]}, sending {len(data) / 1e6:.1f} MB")
                httpx.post(
                    f"{self.url}/sources/{name}", content=data, headers=self.headers, timeout=600
                ).raise_for_status()
            body = {"commit": commit}
        except Exception as e:  # report it to the requester rather than lose it here
            self._say(f"{name} {ref} failed: {e}")
            body = {"error": str(e)}
        try:
            httpx.post(
                f"{self.url}/source-requests/{req['id']}/finish", json=body, headers=self.headers, timeout=30
            ).raise_for_status()
        except httpx.HTTPError as e:
            self._say(f"couldn't report request {req['id']}: {e}")
