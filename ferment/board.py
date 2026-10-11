"""What a fermenter shows the people attached to it.

One model, several renderers: `attach` pins it in a terminal, the fermenter's
web page shows it in a browser, a status command prints it once. The server
keeps the board and renderers only draw it, so they can't disagree.

Sections:

- facts: short values that must stay in view (fermenter, clock, who's attached)
- services: things you can open or connect to, and whether they're up
- milestones: the known steps of a run. A run always starts from scratch, so
  its steps are known ahead of time, and so is roughly how much log each one
  writes before it's done: progress is lines seen over lines expected, and a
  finished run's line counts (`actuals`) are the next run's estimates.
- feed: what happened, oldest first (broadcasts, milestone transitions)

Every change bumps `version`; `wait` blocks until it moves, which is what the
event stream (ferment.server.board) is built on.
"""

from __future__ import annotations

import datetime as dt
import threading
from collections import deque
from typing import Literal

from pydantic import BaseModel, computed_field

ServiceState = Literal["unknown", "down", "starting", "up"]
MilestoneState = Literal["pending", "running", "done", "failed"]


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


class Fact(BaseModel):
    key: str
    label: str
    value: str


class Service(BaseModel):
    name: str
    label: str
    port: int | None = None  # on the VM; a client maps it to its own local port
    scheme: str = "http"  # how to reach it: http, mysql, ...
    path: str = "/"
    state: ServiceState = "unknown"
    detail: str | None = None


class Milestone(BaseModel):
    id: str
    label: str
    group: str = ""  # renderers roll up a group once all of it is done
    source: str | None = None  # whose log lines count toward it (a process name)
    expected: int | None = None  # lines it usually takes; None: progress unknown
    state: MilestoneState = "pending"
    lines: int = 0
    started: str | None = None
    finished: str | None = None
    detail: str | None = None

    @computed_field
    @property
    def progress(self) -> float | None:
        """0..1. Never 1 until it's actually done: estimates are only estimates."""
        if self.state == "done":
            return 1.0
        if not self.expected:
            return None
        return min(self.lines / self.expected, 0.99)


class FeedEntry(BaseModel):
    seq: int
    real_time: str
    event: str
    message: str
    who: str | None = None
    fake_time: str | None = None


class Snapshot(BaseModel):
    version: int
    fake_offset: float | None  # seconds from real time, so renderers can tick the fake clock
    facts: list[Fact]
    services: list[Service]
    milestones: list[Milestone]
    feed: list[FeedEntry]  # only entries after the `after` asked for
    feed_seq: int  # the last entry's seq; ask for entries after it next time


class Board:
    def __init__(self, feed_limit: int = 500):
        self._changed = threading.Condition()
        self.version = 0
        self.fake_offset: float | None = None
        self.facts: dict[str, Fact] = {}
        self.services: dict[str, Service] = {}
        self.milestones: dict[str, Milestone] = {}
        self.feed: deque[FeedEntry] = deque(maxlen=feed_limit)
        self.feed_seq = 0

    def _bump(self) -> None:
        # caller holds self._changed
        self.version += 1
        self._changed.notify_all()

    def wait(self, version: int, timeout: float) -> int:
        """Block until the board is past `version` (or `timeout`); the current version."""
        with self._changed:
            self._changed.wait_for(lambda: self.version != version, timeout)
            return self.version

    def snapshot(self, after: int | None = None) -> Snapshot:
        """The whole board; the feed only past seq `after` (None: all that's kept)."""
        with self._changed:
            return Snapshot(
                version=self.version,
                fake_offset=self.fake_offset,
                facts=[f.model_copy() for f in self.facts.values()],
                services=[s.model_copy() for s in self.services.values()],
                milestones=[m.model_copy() for m in self.milestones.values()],
                feed=[e for e in self.feed if after is None or e.seq > after],
                feed_seq=self.feed_seq,
            )

    def reset(self) -> None:
        """Start a new run: everything but the feed goes. Watchers keep their
        streams and see the board empty out."""
        with self._changed:
            self.fake_offset = None
            self.facts.clear()
            self.services.clear()
            self.milestones.clear()
            self._bump()

    # -- facts, clock, services -------------------------------------------------

    def set_fact(self, key: str, value: str, label: str | None = None) -> None:
        with self._changed:
            old = self.facts.get(key)
            self.facts[key] = Fact(key=key, label=label or (old.label if old else key), value=value)
            self._bump()

    def drop_fact(self, key: str) -> None:
        with self._changed:
            if self.facts.pop(key, None) is not None:
                self._bump()

    def set_clock(self, offset: float | None) -> None:
        with self._changed:
            self.fake_offset = offset
            self._bump()

    def set_service(self, name: str, **fields) -> Service:
        """Add a service or change some of its fields."""
        with self._changed:
            old = self.services.get(name)
            base = old.model_dump() if old else {"name": name, "label": name}
            self.services[name] = svc = Service(**{**base, **fields})
            self._bump()
            return svc

    # -- milestones ---------------------------------------------------------------

    def plan(self, milestones: list[Milestone], estimates: dict[str, int] | None = None) -> None:
        """Add steps to the run (after any already planned); `estimates` (a
        previous run's `actuals`) fill in `expected` where it's missing."""
        estimates = estimates or {}
        with self._changed:
            for m in milestones:
                if m.expected is None and m.id in estimates:
                    m = m.model_copy(update={"expected": estimates[m.id]})
                self.milestones[m.id] = m
            self._bump()

    def start(self, mid: str, detail: str | None = None) -> None:
        with self._changed:
            m = self.milestones[mid]
            m.state, m.started, m.detail = "running", _now(), detail
            self._bump()

    def count(self, source: str, n: int = 1) -> None:
        """`n` more log lines from `source`: progress for its running milestones."""
        with self._changed:
            hit = False
            for m in self.milestones.values():
                if m.state == "running" and m.source == source:
                    m.lines += n
                    hit = True
            if hit:
                self._bump()

    def finish(self, mid: str, ok: bool = True, detail: str | None = None) -> None:
        with self._changed:
            m = self.milestones[mid]
            m.state, m.finished, m.detail = ("done" if ok else "failed"), _now(), detail
            self._bump()

    def actuals(self) -> dict[str, int]:
        """Lines each finished milestone took: estimates for the next run."""
        with self._changed:
            return {m.id: m.lines for m in self.milestones.values() if m.state == "done" and m.source}

    # -- feed -----------------------------------------------------------------------

    def post(self, event: str, message: str, who: str | None = None, fake_time: str | None = None) -> FeedEntry:
        with self._changed:
            self.feed_seq += 1
            entry = FeedEntry(
                seq=self.feed_seq, real_time=_now(), event=event, message=message, who=who, fake_time=fake_time
            )
            self.feed.append(entry)
            self._bump()
            return entry
