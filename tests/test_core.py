import datetime as dt
import json
import os
import time

import pytest

from ferment import config
from ferment.clockcheck import agreement, read_probes
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


def test_guest_clock_keeps_real_time_and_survives_a_reboot(tmp_path):
    import datetime as dt

    from ferment.server.clock import GuestClock

    real = dt.datetime(2026, 10, 10, 12, 0, tzinfo=dt.UTC)
    system = {"t": real, "boot": "b1"}
    make = lambda: GuestClock(  # noqa: E731
        tmp_path / "time.json",
        set_system_time=lambda when: system.update(t=when),
        system_now=lambda: system["t"],
        current_boot=lambda: system["boot"],
    )
    clock = make()
    assert clock.offset == dt.timedelta(0) and not clock.is_set
    clock.set(dt.datetime(2026, 10, 3, tzinfo=dt.UTC))
    assert system["t"] == dt.datetime(2026, 10, 3, tzinfo=dt.UTC)  # the guest's clock moved
    assert clock.real_now() == real  # and we still know what time it really is
    assert not make().ensure()  # same boot: nothing to do

    system.update(t=real + dt.timedelta(hours=1), boot="b2")  # reboot: back on real time
    clock = make()
    assert clock.ensure()
    assert system["t"] == dt.datetime(2026, 10, 3, 1, tzinfo=dt.UTC)  # date restored, an hour on


def test_clients_reports_correct_a_drifted_guest_clock(tmp_path):
    import datetime as dt

    from ferment.server.clock import GuestClock

    real = {"t": dt.datetime(2026, 10, 10, 12, 0, tzinfo=dt.UTC)}
    system = {"t": real["t"]}

    def tick(seconds, guest_too=True):
        real["t"] += dt.timedelta(seconds=seconds)
        if guest_too:
            system["t"] += dt.timedelta(seconds=seconds)

    clock = GuestClock(
        tmp_path / "time.json", set_system_time=lambda when: system.update(t=when), system_now=lambda: system["t"]
    )
    clock.set(dt.datetime(2026, 10, 3, tzinfo=dt.UTC))
    tick(3600, guest_too=False)  # the host slept an hour: the guest's clock stood still
    assert clock.real_now() == real["t"] - dt.timedelta(hours=1)

    clock.observe(real["t"], "alice")
    assert clock.real_now() == real["t"]  # one report fixes it
    assert clock.checked["error_seconds"] == -3600 and clock.checked["who"] == "alice"
    assert GuestClock(tmp_path / "time.json").offset == clock.offset  # and it's kept

    # latency-sized differences don't move it; an outlier among good reports doesn't either
    offset = clock.offset
    clock.observe(real["t"] - dt.timedelta(milliseconds=80))
    clock.observe(real["t"] + dt.timedelta(minutes=5), "bob-with-a-bad-clock")
    clock.observe(real["t"])
    assert clock.offset == offset

    # setting the clock forgets reports about the old offset
    clock.set(dt.datetime(2026, 10, 4, tzinfo=dt.UTC))
    assert len(clock.samples) == 0 and clock.real_now() == real["t"]


def test_process_output_is_teed_to_its_own_log(tmp_path):
    cfg = render([Process("svc", "exec java -jar x.jar")], tmp_path)
    cmd = cfg["processes"]["svc"]["command"]
    assert cmd.startswith(f"exec > >(tee -a {tmp_path}/svc.log) 2>&1")
    assert cmd.endswith("exec java -jar x.jar")
    assert cfg["shell"]["shell_command"] == "bash"


def test_wait_ready_doesnt_count_a_probe_still_in_its_grace_period(tmp_path):
    from ferment.procompose import WorkloadSupervisor

    sup = WorkloadSupervisor(tmp_path, 1)
    sup.probed = {"svc"}
    states = iter(
        [
            [
                {"name": "svc", "status": "Running", "is_ready": "-"},
                {"name": "db", "status": "Running", "is_ready": "-"},
            ],
            [
                {"name": "svc", "status": "Running", "is_ready": "Ready"},
                {"name": "db", "status": "Running", "is_ready": "-"},
            ],
        ]
    )
    seen = []
    sup.processes = lambda: (seen.append(1), next(states))[1]
    assert sup.wait_ready(timeout=5)
    assert len(seen) == 2  # the "-" during svc's initial delay didn't count as ready


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
    assert "ExitOnForwardFailure=yes" in argv
    # best-effort (relays): a port another session holds is not fatal
    assert "ExitOnForwardFailure=no" in tunnel_argv(
        target, [Forward(0, 18443, reverse=True, dest="n:443")], strict=False
    )


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


def test_mysql_is_the_live_database_reached_over_tcp_by_explicit_path(tmp_path):
    from ferment.databases import make_database

    mysql = make_database("mysql", tmp_path)
    mysql.bin = "/opt/mysql84/bin"
    assert mysql.kind == "mysql"
    assert mysql.client.startswith("/opt/mysql84/bin/mysql --no-defaults -h 127.0.0.1 -P 3306 -uroot")
    (proc,) = mysql.processes()
    assert "--initialize-insecure" in proc.command and "/opt/mysql84/bin/mysqld" in proc.command
    assert "--skip-ssl" in make_database("mariadb", tmp_path).processes()[0].command
    with pytest.raises(ValueError):
        make_database("dolt", tmp_path)  # Dolt keeps history now; it doesn't run the workload


class FakeLimaHost:
    """Stands in for limactl (local or over ssh): records scripts, replays output."""

    def __init__(self, host):
        self.host, self.limactl, self.scripts = host, "limactl", []
        self.boot, self.record = "boot-1", None

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
            record = self.record or {"delete_after": "2026-10-13T00:00:00+00:00", "labels": {"ferment-owner": "me"}}
            return "\n".join(json.dumps(i) for i in listing) + "\n---records---\nmine\t" + json.dumps(record) + "\n"
        if script.startswith("awk"):
            return "lima\n"
        if script.startswith("cat") and "user.pub" in script:
            return "ssh-ed25519 LIMAKEY lima\n"
        if "authorized_keys 2>/dev/null" in script:
            return "ssh-ed25519 LIMAKEY lima\nssh-ed25519 OLD someone\n"
        if "boot_id" in script:
            return self.boot + "\n"
        if script.startswith("cat >"):
            self.record = json.loads(input)
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
    script = next(s for s, _ in reversed(p.lima.scripts) if "teammate" in s)
    assert script.startswith("ssh -F /h/.lima/mine/ssh.config lima-mine")
    assert "ssh-ed25519 AAAA teammate" in script and "grep -qxF" in script  # idempotent
    with pytest.raises(Exception, match="public key"):
        p.authorize_key("mine", "-----BEGIN OPENSSH PRIVATE KEY-----")


def test_lima_create_makes_sshd_drop_vanished_clients(monkeypatch, tmp_path):
    from ferment.vm.base import VMSpec

    p = _lima("mac", monkeypatch, tmp_path)
    new = {"name": "new", "status": "Running", "dir": "/h/.lima/new", "sshConfigFile": "/h/.lima/new/ssh.config"}
    started = lambda: any(s.startswith("limactl start") for s, _ in p.lima.scripts)  # noqa: E731
    monkeypatch.setattr(p, "_instances", lambda: {"new": dict(new)} if started() else {})
    p.create(VMSpec(name="new", ttl=dt.timedelta(hours=1)))
    guest = [s for s, _ in p.lima.scripts if s.startswith("ssh -F /h/.lima/new/ssh.config lima-new")]
    assert any("ClientAliveInterval" in s for s in guest)


def test_lima_fermenters_can_live_forever(monkeypatch, tmp_path):
    from ferment.lifetime import parse_lifetime
    from ferment.vm.base import VMSpec

    p = _lima("mac", monkeypatch, tmp_path)
    new = {"name": "new", "status": "Running", "dir": "/h/.lima/new", "sshConfigFile": "/h/.lima/new/ssh.config"}
    started = lambda: any(s.startswith("limactl start") for s, _ in p.lima.scripts)  # noqa: E731
    monkeypatch.setattr(p, "_instances", lambda: {"new": dict(new)} if started() else {})
    p.create(VMSpec(name="new", ttl=parse_lifetime("never")))
    written = [json.loads(i) for s, i in p.lima.scripts if s.startswith("cat >")]
    assert written and all(r["delete_after"] is None for r in written)
    assert not any("systemd-run" in s for s, _ in p.lima.scripts)  # no poweroff timer

    p = _lima("mac", monkeypatch, tmp_path)
    p.set_deadline("mine", None)  # an existing one, made to live forever
    assert p.lima.record["delete_after"] is None
    last = p.lima.scripts[-1][0]
    assert "systemctl stop ferment-deadline.timer" in last and "systemd-run" not in last
    (vm,) = p.list()
    assert vm.delete_after is None and not vm.expired


def test_lima_restores_keys_and_deadline_after_a_reboot(monkeypatch, tmp_path, capsys):
    p = _lima(None, monkeypatch, tmp_path)
    p.ssh_target("mine")  # first look: adopts existing non-lima keys, records the boot
    assert p.lima.record["boot_id"] == "boot-1"
    assert p.lima.record["authorized_keys"] == ["ssh-ed25519 OLD someone"]
    assert "mine" not in p.rebooted

    p.lima.scripts.clear()
    p.ssh_target("mine")  # same boot: nothing re-applied
    assert not any("ferment-deadline" in s for s, _ in p.lima.scripts)

    p.lima.boot = "boot-2"  # the guest rebooted
    p.ssh_target("mine")
    applied = "\n".join(s for s, _ in p.lima.scripts)
    assert "ssh-ed25519 OLD someone" in applied
    assert "ClientAliveInterval" in applied and "ferment-deadline" in applied
    assert "CAP_SYS_TIME" in applied and "systemd-timesyncd" in applied  # the guest's clock stays ours
    assert p.lima.record["boot_id"] == "boot-2"
    assert "mine" in p.rebooted
    assert "rebooted since last seen" in capsys.readouterr().err


def test_unset_clock_starts_on_real_time(tmp_path):
    from ferment.server.clock import GuestClock
    from ferment.server.time_control import TimeController

    class Stack:
        def processes(self):
            return []

    class Supervisor:
        def __init__(self):
            self.started = None

        def stop(self):
            pass

        def launch(self, processes):
            self.started = processes

        def processes(self):
            return []

    sup = Supervisor()
    tc = TimeController(
        Stack(), sup, tmp_path, clock=GuestClock(tmp_path / "time.json", set_system_time=lambda w: None)
    )
    tc.resume()
    assert sup.started is not None  # a fresh fermenter runs right away, on real time
    assert tc.status()["offset_seconds"] == 0


def test_server_request_sends_raw_bodies_and_who(monkeypatch):
    import httpx

    from ferment import client

    sent = {}

    def fake(method, url, **kw):
        sent.update(method=method, **kw)
        return httpx.Response(200, json={"ok": True})

    monkeypatch.setattr(httpx, "request", fake)
    monkeypatch.setenv("FERMENT_USER", "tester")
    client.server_request("http://f", "/sources/x", content=b"# v2 git bundle\n")
    assert sent["method"] == "POST" and sent["content"] == b"# v2 git bundle\n"
    assert sent["headers"] == {client.WHO_HEADER: "tester"}


def test_relay_session_retries_until_it_holds_the_port(tmp_path, capsys):
    """The VM's relay port goes to whoever binds first; ours must keep trying
    (and say so) rather than silently idle without it."""
    from ferment import ssh

    attempts = tmp_path / "attempts"
    fake_ssh = tmp_path / "ssh"
    fake_ssh.write_text(
        "#!/bin/sh\n"
        f"n=$(cat {attempts} 2>/dev/null || echo 0); echo $((n+1)) > {attempts}\n"
        'if [ "$n" -lt 2 ]; then echo "Error: remote port forwarding failed for listen port 18440" >&2; exit 255; fi\n'
        "sleep 30\n"
    )
    fake_ssh.chmod(0o755)
    sup = ssh._Supervised(SshTarget(host="vm"), [Forward(0, 18440, reverse=True, dest="nexus:443")], "relay nexus")
    sup.argv = [str(fake_ssh)]
    sup.RETRY = 0.1
    sup.start()
    deadline = time.time() + 15
    while sup.state != "up" and time.time() < deadline:
        time.sleep(0.1)
    sup.stop()
    err = capsys.readouterr().err
    assert "port held by another session" in err
    assert err.strip().endswith("relay nexus: up")
    assert attempts.read_text().strip() == "3"


class FakePairHost(FakeLimaHost):
    """limactl with a split fermenter's two instances (fx, fx-workload); keeps a record per instance."""

    def __init__(self, host):
        super().__init__(host)
        self.started: list[str] = []
        self.records: dict[str, dict] = {}

    def instances(self):
        out = {}
        for name in self.started:
            out[name] = {
                "name": name,
                "status": "Running",
                "dir": f"/h/.lima/{name}",
                "sshLocalPort": 6022 + len(out),
                "sshConfigFile": f"/h/.lima/{name}/ssh.config",
                "record": self.records.get(name),
            }
        return out

    def run(self, script, *, input=None, capture=True):
        self.scripts.append((script, input))
        if script.startswith("limactl start"):
            self.started.append(script.split("--name=")[1].split()[0])
            return ""
        if script.startswith("cat >"):
            name = script.split("/h/.lima/")[1].split("/")[0]
            self.records[name] = json.loads(input)
            return ""
        if "boot_id" in script:
            return self.boot + "\n"
        if "ssh-keygen" in script:
            return "ssh-ed25519 CONTROLKEY ferment-control\n"
        if script.startswith("awk"):
            return "lima\n"
        return ""


def _pair(monkeypatch, tmp_path, host=None):
    from ferment.vm.base import VMSpec, WorkloadSpec

    p = _lima(host, monkeypatch, tmp_path)
    p.lima = FakePairHost(host)
    monkeypatch.setattr(p, "_instances", p.lima.instances)
    p.create(
        VMSpec(name="fx", ttl=dt.timedelta(hours=1), authorized_keys=["ssh-ed25519 ME me"], workload=WorkloadSpec())
    )
    return p


def test_split_fermenter_is_two_vms_on_a_private_network(monkeypatch, tmp_path):
    p = _pair(monkeypatch, tmp_path)
    assert p.lima.started == ["fx", "fx-workload"]
    starts = [i for s, i in p.lima.scripts if s.startswith("limactl start")]
    assert all("user-v2" in cfg for cfg in starts)
    control, workload = p.lima.records["fx"], p.lima.records["fx-workload"]
    assert (control["role"], control["workload"]) == ("control", "fx-workload")
    assert (workload["role"], workload["control"]) == ("workload", "fx")
    # The control VM reaches the workload VM with a key of its own; clients with theirs.
    assert workload["authorized_keys"] == ["ssh-ed25519 ME me", "ssh-ed25519 CONTROLKEY ferment-control"]
    to = lambda name: "\n".join(s for s, _ in p.lima.scripts if f" lima-{name} " in s)  # noqa: E731
    assert "CAP_SYS_TIME" in to("fx-workload") and "CAP_SYS_TIME" not in to("fx")  # control keeps real time
    assert "Host fermenter-workload" in to("fx") and "lima-fx-workload.internal" in to("fx")
    assert "iptables" in to("fx-workload") and "lima-fx.internal" in to("fx-workload")  # its firewall lets control in


def test_split_fermenter_is_listed_reached_and_removed_by_one_name(monkeypatch, tmp_path):
    p = _pair(monkeypatch, tmp_path, host="matt@mac")
    assert [vm.name for vm in p.list()] == ["fx"]
    assert p.workload_host("fx") == "lima-fx-workload.internal"
    t = p.workload_target("fx")
    assert (t.host, t.user) == ("lima-fx-workload.internal", "lima")
    hop = t.options["ProxyCommand"]
    assert "-W %h:%p" in hop and "ProxyJump=matt@mac" in hop and "-p 6022" in hop  # through the control VM
    assert t.options["IdentityFile"] == p.ssh_target("fx").options["IdentityFile"]
    with pytest.raises(Exception, match="workload VM of fermenter 'fx'"):
        p.delete("fx-workload")
    p.delete("fx")
    assert p.lima.scripts[-1][0] == "limactl delete --force fx fx-workload"


def test_one_vm_fermenters_have_no_workload_vm(monkeypatch, tmp_path):
    p = _lima(None, monkeypatch, tmp_path)
    assert p.workload_host("mine") is None and p.workload_target("mine") is None


def test_mysql_on_a_private_network_is_reached_from_the_control_vm(tmp_path):
    from ferment.databases import MySQL

    workload = MySQL(tmp_path, bin_dir="/m", bind="0.0.0.0")
    (proc,) = workload.processes()
    grants = lambda m: m.processes()[0].command.split(" > ")[0]  # noqa: E731  (the init file's SQL, shell-quoted)
    assert "--bind-address=0.0.0.0" in proc.command and "%" in grants(workload)
    assert "-h 127.0.0.1" in workload.client  # services on the workload VM: loopback
    control = workload.reached_at("lima-fx-workload.internal")
    assert "-h lima-fx-workload.internal" in control.client and workload.host == "127.0.0.1"
    assert "%" not in grants(MySQL(tmp_path, bin_dir="/m"))  # loopback only


def test_lima_mounts_only_the_caches_a_vm_asks_for():
    import datetime as dt

    from ferment.vm import lima
    from ferment.vm.base import CACHE_MOUNT, VMSpec

    plain = lima.lima_config(VMSpec(name="a", ttl=dt.timedelta(hours=1)))
    assert plain["mounts"] == []
    cached = lima.lima_config(VMSpec(name="a", ttl=dt.timedelta(hours=1), caches=["maven"]), cache_root="/c")
    assert cached["mounts"] == [{"location": "/c/maven", "mountPoint": f"{CACHE_MOUNT}/maven", "writable": True}]
