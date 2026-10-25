"""What changed between two points in a MySQL binlog, table by table.

A checkpoint records where the binlog was (ferment.volume: `volume:binlog`,
`file:position`). With row-based logging (MySQL 8.4's default) every insert,
update and delete between two such points is in the binlog with its rows,
updates with their before and after images, so a run can be read back as
"what did it write": which tables, how many rows, which columns an update
actually changed, and what to. That's how a run's effect is learned (and,
for a run of our own, undone) without diffing tables after the fact.

`mysqlbinlog -v --base64-output=DECODE-ROWS` prints rows as

    ### UPDATE `db`.`t`
    ### WHERE
    ###   @1=7
    ###   @2='OPEN'
    ### SET
    ###   @1=7
    ###   @2='CLOSED'

with columns by position; `summarize` names them from `columns` (the
table's columns in order, from information_schema).
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field

_STMT = re.compile(r"^### (INSERT INTO|UPDATE|DELETE FROM) `([^`]+)`\.`([^`]+)`")
_COL = re.compile(r"^###\s+@(\d+)=(.*)$")
SAMPLES = 3


@dataclass
class TableChanges:
    database: str
    table: str
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    changed_columns: dict[str, int] = field(default_factory=dict)  # column -> updates that changed it
    samples: list[dict] = field(default_factory=list)  # a few updates: {column: [before, after]} (changed only)

    def as_dict(self) -> dict:
        return {
            "database": self.database,
            "table": self.table,
            "inserts": self.inserts,
            "updates": self.updates,
            "deletes": self.deletes,
            "changed_columns": dict(sorted(self.changed_columns.items(), key=lambda kv: -kv[1])),
            "samples": self.samples,
        }


def events(lines: Iterable[str]) -> Iterator[tuple[str, str, str, dict[int, str], dict[int, str]]]:
    """(kind, database, table, before, after) per row: before is WHERE (update,
    delete), after is SET (insert, update); columns by 1-based position."""
    kind = db = table = None
    before: dict[int, str] = {}
    after: dict[int, str] = {}
    section = None

    def done():
        if kind is not None:
            return (kind, db, table, before, after)
        return None

    for line in lines:
        line = line.rstrip("\n")
        m = _STMT.match(line)
        if m:
            ev = done()
            if ev:
                yield ev
            kind = {"INSERT INTO": "insert", "UPDATE": "update", "DELETE FROM": "delete"}[m.group(1)]
            db, table = m.group(2), m.group(3)
            before, after = {}, {}
            section = "after" if kind == "insert" else "before"
            continue
        if kind is None:
            continue
        if line == "### WHERE":
            section = "before"
        elif line == "### SET":
            section = "after"
        elif (c := _COL.match(line)) is not None:
            (before if section == "before" else after)[int(c.group(1))] = c.group(2)
        elif not line.startswith("###"):
            ev = done()
            if ev:
                yield ev
            kind = None
    ev = done()
    if ev:
        yield ev


def summarize(lines: Iterable[str], columns: Callable[[str, str], list[str]]) -> list[dict]:
    """Per table, most-changed first. `columns(db, table)`: its column names
    in order (cached here: called once per table)."""
    tables: dict[tuple[str, str], TableChanges] = {}
    names: dict[tuple[str, str], list[str]] = {}
    for kind, db, table, before, after in events(lines):
        key = (db, table)
        tc = tables.get(key)
        if tc is None:
            tc = tables[key] = TableChanges(db, table)
            names[key] = columns(db, table)
        if kind == "insert":
            tc.inserts += 1
        elif kind == "delete":
            tc.deletes += 1
        else:
            tc.updates += 1
            cols = names[key]
            changed = {}
            for pos, new in after.items():
                old = before.get(pos)
                if old != new:
                    name = cols[pos - 1] if 0 < pos <= len(cols) else f"@{pos}"
                    tc.changed_columns[name] = tc.changed_columns.get(name, 0) + 1
                    changed[name] = [old, new]
            if changed and len(tc.samples) < SAMPLES:
                tc.samples.append(changed)
    out = sorted(tables.values(), key=lambda t: -(t.inserts + t.updates + t.deletes))
    return [t.as_dict() for t in out]


def span(names: list[str], start: str, stop: str | None) -> list[tuple[str, int | None, int | None]]:
    """The binlog files (in index order) from `start` to `stop` (`file:position`;
    None: to the end), each with the positions to read it between."""
    s_file, s_pos = start.rsplit(":", 1)
    e_file, e_pos = stop.rsplit(":", 1) if stop else (None, None)
    if s_file not in names:
        raise LookupError(f"binlog {s_file} isn't there (purged?)")
    out = []
    for n in names[names.index(s_file) :]:
        out.append((n, int(s_pos) if n == s_file else None, int(e_pos) if n == e_file else None))
        if n == e_file:
            return out
    if e_file is not None:
        raise LookupError(f"binlog {e_file} isn't after {s_file}")
    return out
