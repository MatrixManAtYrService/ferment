"""Running a service's schema migrations for Dolt, on real MySQL.

Dolt stores the data, but it doesn't run every migration MySQL does (parser
gaps, stored-procedure quirks). Migrations are where compatibility is worst
(DDL, procedures, grants), so they run on the real thing: a scratch MySQL
(the line production runs), then the result is copied into Dolt with
mysqldump, which is what Dolt is built to import. The service then starts on
Dolt with nothing pending.

    pending?  flyway info against Dolt          (usually no: done, instantly)
    yes:      Dolt --mysqldump--> scratch MySQL --flyway migrate-->
              --mysqldump (no routines)--> Dolt

The scratch MySQL runs under the workload's fake clock, so anything a
migration stamps with NOW() agrees with the rest of the workload.

Tools come from FERMENT_MYSQL_BIN (a MySQL 8 bin dir: mysqld, mysql,
mysqldump) and FERMENT_FLYWAY (the flyway CLI).
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
from pathlib import Path

from ferment.databases import Dolt


class MigrationError(RuntimeError):
    pass


def _run(argv: list[str], *, env: dict | None = None, input: str | None = None, out=None) -> str:
    result = subprocess.run(
        argv, env={**os.environ, **(env or {})}, input=input, text=True,
        stdout=out or subprocess.PIPE, stderr=subprocess.PIPE,
    )  # fmt: skip
    if result.returncode != 0:
        raise MigrationError(f"{Path(argv[0]).name} failed: {(result.stderr or '').strip()[-2000:]}")
    return result.stdout if out is None else ""


class Migrator:
    def __init__(self, work_dir: Path, mysql_bin: str | None = None, flyway: str | None = None):
        self.work = work_dir
        self.mysql_bin = Path(mysql_bin or os.environ.get("FERMENT_MYSQL_BIN", ""))
        self.flyway = flyway or os.environ.get("FERMENT_FLYWAY", "flyway")

    def _flyway(self, command: str, url: str, migrations: Path) -> str:
        return _run([
            self.flyway, f"-url={url}", "-user=root", "-password=", f"-locations=filesystem:{migrations}", command,
        ])  # fmt: skip

    def pending(self, dolt: Dolt, schema: str, migrations: Path) -> int:
        """How many migrations Dolt's copy of `schema` hasn't had (all, if it doesn't exist)."""
        if schema not in dolt.databases():
            return sum(1 for p in migrations.glob("V*.sql"))
        info = self._flyway("info", f"jdbc:mysql://127.0.0.1:{dolt.port}/{schema}", migrations)
        return len(re.findall(r"\|\s*Pending\s*\|", info))

    def migrate(self, dolt: Dolt, schema: str, migrations: Path, clock_env: dict[str, str], log) -> int:
        """Bring Dolt's `schema` up to date; returns how many migrations ran."""
        n = self.pending(dolt, schema, migrations)
        if n == 0:
            return 0
        log(f"{schema}: {n} pending migration(s); running them on a scratch MySQL")
        mysql = lambda name: str(self.mysql_bin / name)  # noqa: E731
        work = self.work / schema
        shutil.rmtree(work, ignore_errors=True)
        work.mkdir(parents=True)
        sock, port = work / "sock", 3307
        cli = [mysql("mysql"), "--no-defaults", f"--socket={sock}", "-uroot"]
        dolt_cli = ["mariadb", "--no-defaults", "--skip-ssl", "-h", "127.0.0.1", "-P", str(dolt.port), "-uroot"]

        _run([mysql("mysqld"), "--no-defaults", "--initialize-insecure", f"--datadir={work / 'data'}"], env=clock_env)
        server = subprocess.Popen(
            [mysql("mysqld"), "--no-defaults", f"--datadir={work / 'data'}", f"--socket={sock}", f"--port={port}",
             "--bind-address=127.0.0.1", "--mysqlx=OFF", f"--pid-file={work / 'pid'}"],
            env={**os.environ, **clock_env}, stdout=(work / "mysqld.log").open("w"), stderr=subprocess.STDOUT,
        )  # fmt: skip
        try:
            deadline = time.monotonic() + 120
            while subprocess.run([*cli, "-e", "SELECT 1"], capture_output=True).returncode != 0:
                if server.poll() is not None or time.monotonic() > deadline:
                    raise MigrationError(f"scratch mysqld didn't start; see {work / 'mysqld.log'}")
                time.sleep(1)
            _run(
                [*cli, "-e", "CREATE USER 'root'@'127.0.0.1'; GRANT ALL ON *.* TO 'root'@'127.0.0.1' WITH GRANT OPTION"]
            )

            dump_in = work / "from-dolt.sql"
            if schema in dolt.databases():
                # Dolt's dump of the current state (schema + data) into MySQL.
                _run([mysql("mysqldump"), "--no-defaults", "-h", "127.0.0.1", "-P", str(dolt.port), "-uroot",
                      "--skip-routines", "--set-gtid-purged=OFF", "--column-statistics=0", "--no-tablespaces",
                      "--databases", schema], out=dump_in.open("w"))  # fmt: skip
                _run(cli, input=dump_in.read_text())
            else:
                _run([*cli, "-e", f"CREATE DATABASE `{schema}`"])

            out = self._flyway("migrate", f"jdbc:mysql://127.0.0.1:{port}/{schema}", migrations)
            applied = re.search(r"Successfully applied (\d+) migration", out)
            log(f"{schema}: flyway: {applied.group(0) if applied else out.strip().splitlines()[-1]}")

            dump_out = work / "to-dolt.sql"
            _run([mysql("mysqldump"), "--no-defaults", f"--socket={sock}", "-uroot", "--skip-routines",
                  "--single-transaction", "--set-gtid-purged=OFF", "--databases", schema],
                 out=dump_out.open("w"))  # fmt: skip
            _run([*dolt_cli, "-e", f"DROP DATABASE IF EXISTS `{schema}`"])
            _run(dolt_cli, input=dump_out.read_text())
        finally:
            server.terminate()
            server.wait(timeout=60)
        left = self.pending(dolt, schema, migrations)
        if left:
            raise MigrationError(f"{schema}: still {left} pending after migrating; see {work}")
        log(f"{schema}: copied back into dolt; up to date")
        return int(applied.group(1)) if applied else n
