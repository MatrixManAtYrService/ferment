import datetime as dt
import json
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


def test_commands_can_carry_forwards():
    from ferment import ssh

    target = SshTarget(host="vm", user="me")
    assert ssh.argv(target) == ["ssh", "me@vm"]
    argv = ssh.argv(target, [Forward(0, 18440, reverse=True, dest="mirror:443")])
    assert argv[-1] == "me@vm" and "127.0.0.1:18440:mirror:443" in argv
    # a relay another session already holds must not abort this one
    assert "ExitOnForwardFailure=yes" not in argv


def test_relays_get_their_own_loopback_address_and_port():
    from ferment import relay

    a, b = relay.relays(["mirror.example:443", "cache.example:8080"])
    assert (a.address, a.vm_port, b.address, b.vm_port) == ("127.77.0.1", 18440, "127.77.0.2", 18441)
    assert relay.forwards([a])[0].ssh_args() == ["-R", "127.0.0.1:18440:mirror.example:443"]
    script = relay.install_script([a, b], inputs_from="path:/vm")
    assert "127.77.0.1 mirror.example # ferment-relay" in script
    assert "--inputs-from path:/vm 'nixpkgs#socat'" in script
    assert "TCP-LISTEN:8080,bind=127.77.0.2,reuseaddr,fork TCP:127.0.0.1:18441" in script
    # dead sessions must not keep holding the relay ports
    assert "ClientAliveInterval" in script
    # [] still clears earlier relays, without touching nix or a pristine /etc/hosts
    empty = relay.install_script([])
    assert "systemctl stop 'ferment-relay-*'" in empty and "socat" not in empty and "cp " not in empty
    with pytest.raises(ValueError):
        relay.relays(["no-port"])


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


class FakeLimaHost:
    """Stands in for limactl (local or over ssh): records scripts, replays output."""

    def __init__(self, host):
        self.host, self.limactl, self.scripts = host, "limactl", []

    def limactl_cmd(self, *args):
        return " ".join(("limactl", *args))

    def run(self, script, *, input=None, capture=True):
        self.scripts.append((script, input))
        if script.startswith("limactl list"):
            listing = [
                {
                    "name": "mine",
                    "status": "Running",
                    "dir": "/h/.lima/mine",
                    "sshLocalPort": 6022,
                    "sshConfigFile": "/h/.lima/mine/ssh.config",
                },
                {"name": "notmine", "status": "Running", "dir": "/h/.lima/notmine"},
            ]
            record = {"delete_after": "2026-10-13T00:00:00+00:00", "labels": {"ferment-owner": "me"}}
            return "\n".join(json.dumps(i) for i in listing) + "\n---records---\nmine\t" + json.dumps(record) + "\n"
        if script.startswith("awk"):
            return "lima\n"
        return ""


def _lima(host, monkeypatch, tmp_path):
    from ferment.vm import lima

    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    p = lima.LimaProvider(host=host)
    p.lima = FakeLimaHost(host)
    return p


def test_lima_lists_only_its_own_instances(monkeypatch, tmp_path):
    p = _lima("mac", monkeypatch, tmp_path)
    (vm,) = p.list()
    assert vm.name == "mine" and vm.labels == {"ferment-owner": "me"}
    with pytest.raises(Exception, match="managed by ferment on mac"):
        p.delete("notmine")


def test_remote_lima_target_jumps_through_the_host_with_ferments_key(monkeypatch, tmp_path):
    remote = _lima("matt@mac", monkeypatch, tmp_path).ssh_target("mine")
    assert (remote.host, remote.port, remote.user) == ("127.0.0.1", 6022, "lima")
    assert remote.options["ProxyJump"] == "matt@mac"
    assert remote.options["IdentityFile"].startswith(str(tmp_path))  # ferment's key, not ~/.ssh
    local = _lima(None, monkeypatch, tmp_path).ssh_target("mine")
    assert "ProxyJump" not in local.options


def test_authorize_key_goes_through_limas_own_ssh_config(monkeypatch, tmp_path):
    p = _lima("mac", monkeypatch, tmp_path)
    p.authorize_key("mine", "ssh-ed25519 AAAA teammate\n")
    script = p.lima.scripts[-1][0]
    assert script.startswith("ssh -F /h/.lima/mine/ssh.config lima-mine")
    assert "ssh-ed25519 AAAA teammate" in script and "grep -qxF" in script  # idempotent
    with pytest.raises(Exception, match="public key"):
        p.authorize_key("mine", "-----BEGIN OPENSSH PRIVATE KEY-----")
