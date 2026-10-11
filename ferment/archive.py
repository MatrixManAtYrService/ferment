"""DoltArchive: MySQL runs, Dolt remembers.

The workload's live database is MySQL (production's engine). Its history is
kept here: one Dolt repo per database, fed by mysqldump at each checkpoint,
so checkpoints get Dolt's commits, tags, table/row diffs, and hard restores
without Dolt having to run the services' SQL.

What goes into Dolt as real tables: tables (schema + data) and triggers.
What Dolt can't hold as-is (views like JobRunr's `GROUP BY ... WITH ROLLUP`,
stored routines) is kept as SQL text in each repo's `ferment_extra` table and
replayed into MySQL on restore.

Dolt runs on the fermenter, so its commits carry the fermenter's (fake) date
and history reads in workload time. When the workload is stopped (clock
changes, restores), MySQL is started briefly on its own data dir to dump or
load it.
"""

from __future__ import annotations

import contextlib
import json
import subprocess
import tempfile
from pathlib import Path

from ferment.databases import MySQL

EXTRA = "ferment_extra"


class ArchiveError(RuntimeError):
    pass


class DoltArchive:
    """A ferment.server.time_control.Versioned store for a live MySQL."""

    def __init__(self, root: Path, live: MySQL, dolt: str = "dolt"):
        self.root = root
        self.live = live
        self.dolt_bin = dolt

    # -- dolt ------------------------------------------------------------------------

    def _dolt(self, db: str, *args: str, input: str | None = None) -> str:
        result = subprocess.run(
            [self.dolt_bin, *args], cwd=self.root / db, input=input, text=True, capture_output=True,
        )  # fmt: skip
        if result.returncode != 0:
            raise ArchiveError(f"dolt {args[0]} ({db}): {(result.stderr or result.stdout).strip()[-2000:]}")
        return result.stdout

    def _repo(self, db: str) -> None:
        repo = self.root / db
        if not (repo / ".dolt").exists():
            repo.mkdir(parents=True, exist_ok=True)
            self._dolt(db, "init", "--name", "fermenter", "--email", "fermenter@localhost")

    def repos(self) -> list[str]:
        return sorted(p.parent.name for p in self.root.glob("*/.dolt"))

    # -- checkpoints -------------------------------------------------------------------

    def _server(self, running: bool):
        return contextlib.nullcontext(self.live.port) if running else self.live.offline_server()

    def snapshot(self, message: str, tags: list[str], *, running: bool) -> dict[str, str]:
        """Copy every live database into its Dolt repo, commit, tag; returns
        {"archive:<db>": commit}."""
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            with self._server(running) as port:
                dbs = self.live.databases(port)
                for db in dbs:
                    self._dump(db, port, work)
            parts = {}
            for db in dbs:
                parts[f"archive:{db}"] = self._commit(db, work, message, tags)
        return parts

    def _dump(self, db: str, port: int, work: Path) -> None:
        with self.live.connect(port, db) as conn, conn.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.views WHERE table_schema = %s", (db,))
            views = [r[0] for r in cur.fetchall()]
            view_sql = []
            for v in views:
                cur.execute(f"SHOW CREATE VIEW `{v}`")
                view_sql.append(f"DROP VIEW IF EXISTS `{v}`;\n{cur.fetchone()[1]};")
        (work / f"{db}.views.sql").write_text("\n".join(view_sql))
        self.live.dump(db, port, work / f"{db}.routines.sql", routines=True)
        # Tables only: views are kept as text (above), not as Dolt views.
        ignore = [f"--ignore-table={db}.{v}" for v in views]
        self.live.dump(db, port, work / f"{db}.tables.sql", routines=False, extra=ignore)

    def _commit(self, db: str, work: Path, message: str, tags: list[str]) -> str:
        self._repo(db)
        # Start from empty, so tables dropped since the last checkpoint go too.
        existing = json.loads(self._dolt(db, "sql", "-r", "json", "-q", "SHOW FULL TABLES") or "{}")
        drops = []
        for row in existing.get("rows", []):
            name = next(v for k, v in row.items() if k.startswith("Tables_in_"))
            drops.append(f"DROP {'VIEW' if row['Table_type'] == 'VIEW' else 'TABLE'} IF EXISTS `{name}`;")
        if drops:
            self._dolt(db, "sql", input="\n".join(drops))
        self._dolt(db, "sql", input=(work / f"{db}.tables.sql").read_text())
        extra = [(work / f"{db}.{kind}.sql").read_text() for kind in ("views", "routines")]
        self._dolt(db, "sql", input=(
            f"CREATE TABLE `{EXTRA}` (kind VARCHAR(16) PRIMARY KEY, statements LONGTEXT);\n"
            + "".join(
                f"INSERT INTO `{EXTRA}` VALUES ('{k}', UNHEX('{v.encode().hex()}'));\n"
                for k, v in zip(("views", "routines"), extra, strict=True)
            )
        ))  # fmt: skip
        self._dolt(db, "add", "-A")
        self._dolt(db, "commit", "--allow-empty", "-m", message)
        for tag in tags:
            self._dolt(db, "tag", tag)
        return self._dolt(db, "sql", "-r", "csv", "-q", "SELECT HASHOF('HEAD') AS h").strip().splitlines()[-1]

    def restore(self, parts: dict[str, str]) -> None:
        """Make the live MySQL exactly what it was at these commits (the
        workload is stopped). Databases not in the checkpoint are dropped."""
        archived = {k.split(":", 1)[1]: ref for k, ref in parts.items() if k.startswith("archive:")}
        if not archived:
            raise ArchiveError("this checkpoint has no archived databases (made before MySQL + Dolt?)")
        with tempfile.TemporaryDirectory() as tmp, self.live.offline_server() as port:
            for db in self.live.databases(port):
                if db not in archived:
                    self.live.load(db, port, f"DROP DATABASE `{db}`;", replace=False)
            for db, ref in archived.items():
                # History continues from the restored point; later commits stay
                # reachable through their ckpt-<id> tags.
                self._dolt(db, "reset", "--hard", ref)
                dump = Path(tmp) / f"{db}.sql"
                self._dolt(db, "dump", "-r", "sql", "-f", "-fn", str(dump))
                sql = dump.read_text()
                rows = json.loads(
                    self._dolt(db, "sql", "-r", "json", "-q", f"SELECT kind, HEX(statements) AS s FROM `{EXTRA}`")
                    or "{}"
                )
                extra = {r["kind"]: bytes.fromhex(r["s"] or "").decode() for r in rows.get("rows", [])}
                self.live.load(db, port, sql, replace=True)
                self.live.load(db, port, f"DROP TABLE IF EXISTS `{EXTRA}`;", replace=False)
                for kind in ("views", "routines"):
                    if extra.get(kind, "").strip():
                        self.live.load(db, port, extra[kind], replace=False)

    def diff(self, from_ref: str, to_ref: str = "HEAD") -> dict[str, list[dict]]:
        """Per database: which tables changed between two refs, and how much.
        Refs: commits, or the ckpt-<id> / name tags each checkpoint gets."""
        out = {}
        for db in self.repos():
            q = f"SELECT * FROM DOLT_DIFF_STAT('{from_ref}', '{to_ref}') WHERE table_name != '{EXTRA}'"
            try:
                rows = json.loads(self._dolt(db, "sql", "-r", "json", "-q", q) or "{}").get("rows", [])
            except ArchiveError as e:
                out[db] = [{"error": str(e)}]
                continue
            out[db] = [{k: (str(v) if v is not None else None) for k, v in r.items()} for r in rows]
        return out

    def row_diff(self, db: str, table: str, from_ref: str, to_ref: str = "HEAD") -> list[dict]:
        q = f"SELECT * FROM DOLT_DIFF('{from_ref}', '{to_ref}', '{table}')"
        return json.loads(self._dolt(db, "sql", "-r", "json", "-q", q) or "{}").get("rows", [])
