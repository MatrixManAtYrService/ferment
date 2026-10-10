import datetime as dt
import os
import time

import pytest

from ferment import config
from ferment.clockcheck import agreement, read_probes
from ferment.faketime import FakeClock
from ferment.lifetime import parse_duration
from ferment.procompose import Process, render
from ferment.ssh import Forward, tunnel_argv
from ferment.vm import get_provider
from ferment.vm.base import SshTarget


def test_config_initializes_then_merges_new_defaults(tmp_path, monkeypatch):
    monkeypatch.setenv("FERMENT_CONFIG", str(tmp_path / "config.toml"))
    first = config.load({"backend": {"provider": "lima"}})
    assert first == {"backend": {"provider": "lima"}}
    assert (tmp_path / "config.toml").exists()

    cfg = config.load({"backend": {"provider": "gcp", "lima": {"cpus": 4}}, "attached": {}})
    # the file's choice wins; keys it lacks come from defaults
    assert cfg["backend"]["provider"] == "lima"
    assert cfg["backend"]["lima"] == {"cpus": 4}


def test_fake_clock_is_an_offset_not_a_start_time():
    now = dt.datetime(2026, 10, 10, 12, 0, tzinfo=dt.UTC)
    past = FakeClock.starting_at(dt.datetime(2026, 10, 3, tzinfo=dt.UTC), now=now)
    assert past.offset_seconds == -(7 * 86400 + 12 * 3600)
    assert past.env("/lib/libfaketime.so.1")["FAKETIME"] == f"-{7 * 86400 + 12 * 3600}"
    future = FakeClock.starting_at(dt.datetime(2026, 10, 11, 12, 0, tzinfo=dt.UTC), now=now)
    assert future.env("x")["FAKETIME"] == "+86400"


def test_only_faked_processes_get_the_clock(tmp_path):
    procs = [Process("db", "mariadbd"), Process("grafana", "grafana", faked=False)]
    cfg = render(procs, {"LD_PRELOAD": "lib"}, tmp_path)
    assert "LD_PRELOAD=lib" in cfg["processes"]["db"]["environment"]
    assert cfg["processes"]["grafana"]["environment"] == []


def test_clock_agreement_uses_real_mtime(tmp_path):
    offset = -86400
    real = time.time()
    for name in ("java", "python"):
        p = tmp_path / f"{name}.txt"
        p.write_text(dt.datetime.fromtimestamp(real + offset, dt.UTC).isoformat())
        os.utime(p, (real, real))
    results = read_probes(tmp_path)
    assert all(abs(r["offset_seconds"] - offset) < 0.01 for r in results)
    assert agreement(results, offset)["agree"]
    assert not agreement(results, 0)["agree"]


def test_tunnel_options_precede_destination():
    target = SshTarget(host="lima-x", config_file="/tmp/ssh.config")
    argv = tunnel_argv(target, [Forward(43004, 8400), Forward(0, 18443, reverse=True, dest="nexus:443")])
    assert argv[-1] == "lima-x"
    assert "127.0.0.1:43004:127.0.0.1:8400" in argv
    assert "127.0.0.1:18443:nexus:443" in argv
    # never ride a shared control master: the tunnel must die with its process
    assert "ControlMaster=no" in argv


def test_durations():
    assert parse_duration("72h") == dt.timedelta(hours=72)
    with pytest.raises(ValueError):
        parse_duration("soon")


def test_unbuilt_providers_say_so():
    for name in ("gcp", "aws"):
        with pytest.raises(NotImplementedError):
            get_provider(name).list()


def test_probe_written_before_relaunch_is_stale(tmp_path):
    # A probe that died across a workload restart must not count as agreeing.
    real = time.time()
    p = tmp_path / "mariadb.txt"
    p.write_text(dt.datetime.fromtimestamp(real, dt.UTC).isoformat())
    os.utime(p, (real - 2, real - 2))  # fresh by age...
    relaunched = dt.datetime.fromtimestamp(real - 1, dt.UTC)  # ...but older than the workload
    (result,) = read_probes(tmp_path, since=relaunched)
    assert result["stale"]
    assert not agreement([result], None)["agree"]


def test_databases_speak_without_tls_and_only_dolt_is_versioned(tmp_path):
    from ferment.databases import make_database

    maria, dolt = make_database("mariadb", tmp_path), make_database("dolt", tmp_path)
    # a cert minted under the fake clock would be rejected by real-time clients
    assert "--skip-ssl" in maria.processes()[0].command
    assert "--skip-ssl" in maria.client and "--skip-ssl" in dolt.client
    assert not maria.versioned and dolt.versioned
    assert f"--data-dir {tmp_path / 'dolt'}" in dolt.processes()[0].command
    with pytest.raises(ValueError):
        make_database("sqlite", tmp_path)


def test_go_patch_hooks_the_one_time_now_call_site():
    # nix/fakeable-go.nix substitutes this exact line; --replace-fail breaks
    # the build (loudly) if a Go upgrade changes it.
    from pathlib import Path

    nix = (Path(__file__).parent.parent / "nix" / "fakeable-go.nix").read_text()
    assert "'sec, nsec, mono := runtimeNow()'" in nix
    assert "--replace-fail" in nix
