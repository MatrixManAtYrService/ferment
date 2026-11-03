import sys

import pytest
from fastapi.testclient import TestClient

from ferment import measure
from ferment.server.app import create_app
from tests.test_server import setup  # noqa: F401 - pytest fixture


def test_a_sampler_keeps_what_each_source_gives_and_skips_one_that_fails():
    got = []

    def broken():
        raise ConnectionError("the agent is restarting")

    s = measure.Sampler(
        {"control": lambda: {"mem_used_mib": 900}, "workload": broken}, lambda vm, x: got.append((vm, x))
    )
    s.tick()
    assert got == [("control", {"mem_used_mib": 900})]


def test_summaries_give_peaks_per_vm_and_nested_number():
    samples = [
        {"vm": "workload", "sample": {"mem_used_mib": m, "processes_mib": {"mysql": m / 2}, "ncpu": 6, "x": None}}
        for m in (1000, 3000, 2000)
    ]
    out = measure.summarize(samples)["workload"]
    assert out["mem_used_mib"]["peak"] == 3000 and out["mem_used_mib"]["mean"] == 2000 and out["mem_used_mib"]["n"] == 3
    assert out["processes_mib.mysql"]["peak"] == 1500 and "x" not in out


def test_process_trees_sum_their_descendants():
    procs = {1: (0, "init", 10), 10: (1, "java", 1024 * 100), 11: (10, "java", 1024 * 50), 12: (1, "mysqld", 1024 * 20)}
    assert measure._tree_rss_mib(procs, {"bookkeeper": 10, "mysql": 12, "gone": 99}) == {
        "bookkeeper": 150.0, "mysql": 20.0, "gone": 0.0,
    }  # fmt: skip


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads /proc")
def test_collect_reads_this_machine():
    cpu = measure.CpuMeter()
    measure.collect(cpu)
    s = measure.collect(cpu, {"root": "/"})
    assert s["mem_total_mib"] > 0 and s["disk_used_gib"]["root"] > 0 and s["processes_mib"]


def test_samples_and_marks_over_http(setup):  # noqa: F811
    tc, _ = setup
    app = create_app(tc, measure={"vm": lambda: {"mem_used_mib": 512, "processes_mib": {"mysql": 100.0}}})
    with TestClient(app) as client:
        mark = client.post("/measure/marks", json={"label": "experiment 9 (replay)"}).json()
        tc.store.add_sample("9999-01-01T00:00:00", "vm", {"mem_used_mib": 4096})  # far future: outside the mark
        sampler_tick = {"mem_used_mib": 512}
        tc.store.add_sample(mark["started"], "vm", sampler_tick)
        ended = client.post(f"/measure/marks/{mark['id']}/end").json()
        assert ended["ended"] >= ended["started"]
        summary = client.get("/measure/summary", params={"mark": mark["id"]}).json()
        assert summary["vms"]["vm"]["mem_used_mib"]["peak"] == 512
        assert client.get("/measure/marks", params={"label": "experiment 9"}).json()[0]["id"] == mark["id"]
        assert len(client.get("/measure", params={"mark": mark["id"]}).json()) == 1
        assert client.get("/measure/summary", params={"mark": 999}).status_code == 404
