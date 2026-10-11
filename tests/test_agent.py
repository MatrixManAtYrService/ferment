import contextlib
import datetime as dt

import pytest
from fastapi.testclient import TestClient

from ferment.agent.app import create_agent_app
from ferment.agent.client import AgentClient, AgentError
from ferment.procompose import Process, WorkloadSupervisor
from ferment.server.clock import GuestClock
from ferment.server.time_control import TimeController


class Supervisor(WorkloadSupervisor):
    """process-compose that isn't there: remembers what it was told to run."""

    def __init__(self, tmp_path):
        super().__init__(tmp_path / "workload", 1, log_dir=tmp_path / "logs")
        self.launched: list[Process] | None = None

    def launch(self, processes):
        self.launched = processes
        self.probed = {p.name for p in processes if p.readiness_exec}

    def stop(self, timeout=60.0):
        self.launched = None

    @property
    def running(self):
        return self.launched is not None

    def processes(self):
        procs = self.launched or []
        return [{"name": p.name, "status": "Running", "is_ready": "-", "restarts": 0, "pid": 1} for p in procs]


class Database:
    def __init__(self):
        self.offline_runs = 0
        self.up = False

    @contextlib.contextmanager
    def offline_server(self, port=3399):
        self.offline_runs += 1
        self.up = True
        try:
            yield port
        finally:
            self.up = False


@pytest.fixture
def agent(tmp_path):
    system = {"t": dt.datetime(2026, 10, 11, 12, 0, tzinfo=dt.UTC), "boot": "boot-1"}
    sup, db = Supervisor(tmp_path), Database()
    app = create_agent_app(
        sup,
        database=db,
        set_system_time=lambda when: system.update(t=when),
        system_now=lambda: system["t"],
        current_boot=lambda: system["boot"],
    )
    with TestClient(app) as http:
        yield AgentClient("", http=http, connect_for=0), system, sup, db


def test_guest_clock_through_the_agent_keeps_real_time_and_survives_a_reboot(agent, tmp_path):
    client, system, _, _ = agent
    make = lambda: GuestClock(  # noqa: E731
        tmp_path / "time.json", set_system_time=client.set_clock, system_now=client.now, current_boot=client.boot_id
    )
    clock = make()
    real = system["t"]
    clock.set(dt.datetime(2026, 1, 1, tzinfo=dt.UTC))
    assert system["t"] == dt.datetime(2026, 1, 1, tzinfo=dt.UTC)  # the agent's machine moved
    assert abs((clock.now() - system["t"]).total_seconds()) < 1  # read back through the agent
    assert abs((clock.real_now() - real).total_seconds()) < 1

    system.update(t=real + dt.timedelta(hours=1), boot="boot-2")  # rebooted: back on real time
    assert make().ensure()
    assert abs((system["t"] - dt.datetime(2026, 1, 1, 1, tzinfo=dt.UTC)).total_seconds()) < 1


def test_the_clock_counts_forward_between_readings(agent):
    client, system, _, _ = agent
    first = client.now()
    system["t"] += dt.timedelta(days=3)  # the agent isn't asked again until RESAMPLE passes
    assert (client.now() - first).total_seconds() < 1
    client._sample = (client._sample[0], client._sample[1] - client.RESAMPLE - 1, client._sample[2])
    assert (client.now() - first) >= dt.timedelta(days=3)


def test_processes_arrive_whole_and_readiness_comes_from_the_agent(agent):
    client, _, sup, _ = agent
    procs = [
        Process("db", "mysqld", env={"A": "1"}, readiness_exec="true", readiness_failures=7),
        Process("svc", "java -jar x.jar", depends_on=["db"]),
    ]
    client.launch(procs)
    assert sup.launched == procs
    assert client.running
    # db has a probe and says "-" (still in its grace period): not ready; svc has none.
    assert {p["name"]: client.is_ready(p) for p in client.processes()} == {"db": False, "svc": True}
    client.stop()
    assert not client.running and client.processes() == []


def test_time_controller_drives_the_workload_through_the_agent(agent, tmp_path):
    client, system, sup, _ = agent

    class Stack:
        def processes(self):
            return [Process("probe", "sleep infinity")]

    clock = GuestClock(
        tmp_path / "time.json", set_system_time=client.set_clock, system_now=client.now, current_boot=client.boot_id
    )
    tc = TimeController(Stack(), client, tmp_path, clock=clock)
    tc.resume()
    assert [p.name for p in sup.launched] == ["probe"]
    out = tc.set_time(dt.datetime(2027, 3, 1, tzinfo=dt.UTC))
    assert out["ready"] and out["fake_now"].startswith("2027-03-01")
    assert system["t"] == dt.datetime(2027, 3, 1, tzinfo=dt.UTC)


def test_offline_database_only_while_the_workload_is_stopped(agent):
    client, _, sup, db = agent
    client.launch([Process("db", "mysqld")])
    with pytest.raises(AgentError, match="409"), client.offline_db():
        pass
    client.stop()
    with client.offline_db() as port:
        assert port == 3399 and db.up
    assert not db.up and db.offline_runs == 1


def test_logs_are_read_where_the_workload_runs(agent, tmp_path):
    client, _, _, _ = agent
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "svc.log").write_text("starting\nstarted\n")
    assert client.log("svc") == ("starting\nstarted\n", 17)
    assert client.log("svc", 9) == ("started\n", 17)
    assert client.log("../etc/passwd") == ("", 0)
    assert client.log("nope") == ("", 0)
