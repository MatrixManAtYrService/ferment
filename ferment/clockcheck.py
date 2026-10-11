"""Do the workload's runtimes agree about what time it is?

Each probe process repeatedly overwrites <probe dir>/<name>.txt with the time
*it* observes (one ISO-8601 line). The reader compares that against the
file's mtime, stamped by the kernel: `observed - mtime` is how far that
runtime's idea of now is from the system clock's.

The workload runs at the VM's own clock, so every probe should agree with it
(offset ~0): a canary that no runtime has its own idea of the time (a
container with its own clock, a cached timezone, a stale JVM).

Probes:
  python  python -m ferment.clockcheck probe <dir> <name>
  java    java ClockProbe.java <dir> <name>      (ferment/probes/ClockProbe.java)
  mariadb shell_probe_command(...)               (asks the server for NOW(6))
"""

from __future__ import annotations

import datetime as dt
import os
import sys
import time
from pathlib import Path

JAVA_PROBE = Path(__file__).parent / "probes" / "ClockProbe.java"


def write_atomic(path: Path, text: str) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(text)
    tmp.replace(path)


def python_probe(probe_dir: Path, name: str, interval: float = 1.0) -> None:
    probe_dir.mkdir(parents=True, exist_ok=True)
    while True:
        write_atomic(probe_dir / f"{name}.txt", dt.datetime.now(dt.UTC).isoformat() + "\n")
        time.sleep(interval)


def shell_probe_command(probe_dir: str, name: str, query_cmd: str, interval: int = 1) -> str:
    """A bash loop that writes whatever `query_cmd` prints (one timestamp)."""
    out = f"{probe_dir}/{name}.txt"
    return (
        f"mkdir -p {probe_dir}; while true; do "
        f"v=$({query_cmd}) && printf '%s\\n' \"$v\" > {out}.tmp && mv {out}.tmp {out}; "
        f"sleep {interval}; done"
    )


def _parse(text: str) -> dt.datetime:
    text = text.strip().replace(" ", "T")
    when = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    # mariadb's NOW() has no zone; the workload runs with TZ=UTC.
    return when if when.tzinfo else when.replace(tzinfo=dt.UTC)


def read_probes(probe_dir: Path, stale_after: float = 5.0, since: dt.datetime | None = None) -> list[dict]:
    """A probe is live if it wrote within `stale_after` seconds (they write
    every second) and after `since`, the real time the current workload was
    launched; anything older came from a predecessor, or a dead probe."""
    results = []
    now = time.time()
    cutoff = since.timestamp() if since else 0.0
    for path in sorted(probe_dir.glob("*.txt")):
        name = path.stem
        try:
            mtime = path.stat().st_mtime
            observed = _parse(path.read_text())
        except (OSError, ValueError) as e:
            results.append({"probe": name, "error": str(e)})
            continue
        real = dt.datetime.fromtimestamp(mtime, dt.UTC)
        results.append(
            {
                "probe": name,
                "observed": observed.isoformat(),
                "written_at_real": real.isoformat(),
                "offset_seconds": round((observed - real).total_seconds(), 3),
                "stale": (now - mtime) > stale_after or mtime < cutoff,
            }
        )
    return results


def agreement(results: list[dict], expected_offset: int | None, tolerance: float = 2.0) -> dict:
    """Summarize: do live probes share one offset, and is it the requested one?"""
    live = [r for r in results if "offset_seconds" in r and not r["stale"]]
    if not live:
        return {"agree": False, "reason": "no live probes"}
    offsets = [r["offset_seconds"] for r in live]
    spread = max(offsets) - min(offsets)
    agree = spread <= tolerance
    if expected_offset is not None:
        agree = agree and all(abs(o - expected_offset) <= tolerance for o in offsets)
    return {
        "agree": agree,
        "spread_seconds": round(spread, 3),
        "expected_offset_seconds": expected_offset,
        "live_probes": len(live),
        "stale_or_broken": [r["probe"] for r in results if r not in live],
    }


def main() -> None:
    if len(sys.argv) == 4 and sys.argv[1] == "probe":
        python_probe(Path(sys.argv[2]), sys.argv[3], float(os.environ.get("PROBE_INTERVAL", "1")))
    else:
        print("usage: python -m ferment.clockcheck probe <dir> <name>", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
