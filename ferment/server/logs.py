"""The fermenter server's structured log: JSON lines (structlog).

One line per event in <state>/logs/fermenter.jsonl, the file a log shipper
(Alloy -> Loki) tails. Events meant for people carry `broadcast=True` and a
human `message`, plus `who` (whose action it was): attached clients poll for
them and print other people's, so everyone sharing a fermenter sees what the
others are doing.

Until the log server exists, the fermenter serves broadcasts from this file
(GET /broadcasts, by byte offset); afterwards the same fields are a LogQL
query: {job="fermenter"} | json | broadcast="true".
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

import structlog

log = structlog.get_logger("fermenter")

_lock = threading.Lock()


class _JsonLinesFile:
    """structlog logger factory target: appends each rendered line to a file."""

    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)

    def msg(self, message: str) -> None:
        with _lock, self.path.open("a") as f:
            f.write(message + "\n")
        print(message, flush=True)  # and to stdout -> journald

    info = warning = error = debug = msg


def configure(path: Path) -> None:
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True, key="real_time"),
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=lambda *_: _JsonLinesFile(path),
        cache_logger_on_first_use=False,
    )


def broadcast(event: str, who: str, message: str, **fields) -> None:
    """Log an event every attached person should hear about."""
    log.info(event, broadcast=True, who=who, message=message, **fields)


def broadcasts(path: Path, after: int) -> tuple[list[dict], int]:
    """Broadcast entries written after byte offset `after` (-1: none, just
    tell me where the end is), and the offset to ask from next time."""
    if not path.exists():
        return [], 0
    size = path.stat().st_size
    if after < 0 or after > size:  # first call, or the file was replaced
        return [], size
    with path.open("rb") as f:
        f.seek(after)
        data = f.read()
    end = after + data.rfind(b"\n") + 1  # whole lines only
    entries = []
    for line in data[: end - after].splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if entry.get("broadcast"):
            entries.append(entry)
    return entries, end
