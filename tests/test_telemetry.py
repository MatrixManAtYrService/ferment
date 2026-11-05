import datetime as dt
import json

import httpx
from fastapi.testclient import TestClient

from ferment.server.app import create_app
from ferment.server.clock import fake_to_real
from ferment.server.telemetry import LogShipper, fake_stamp, file_reader, render_metrics
from tests.test_server import setup  # noqa: F401 - pytest fixture

UTC = dt.UTC


def test_setting_the_clock_keeps_the_offset_history_and_fake_times_convert_by_it(setup):  # noqa: F811
    tc, _ = setup
    first = tc.store.offsets()
    assert [h["why"] for h in first] == ["initial"]
    tc.set_time(dt.datetime(2026, 10, 3, 12, tzinfo=UTC), wait_ready=False)
    history = tc.store.offsets()
    assert [h["why"] for h in history] == ["initial", "set"]
    set_at = dt.datetime.fromisoformat(history[-1]["real_time"])
    # A fake time after the set is that far after the set's real time.
    assert fake_to_real(dt.datetime(2026, 10, 3, 12, 5, tzinfo=UTC), history, now=set_at + dt.timedelta(hours=1)) == (
        set_at + dt.timedelta(minutes=5)
    )
    # The clock never read a fake time before the date it was set to (and not yet the real one).
    assert fake_to_real(dt.datetime(2026, 10, 3, 11, tzinfo=UTC), history, now=set_at) is None


def test_after_the_clock_goes_back_a_fake_time_read_twice_is_the_latest():
    t0 = dt.datetime(2026, 11, 5, 10, tzinfo=UTC)
    day = dt.datetime(2026, 11, 1, tzinfo=UTC)
    history = [
        {"real_time": t0.isoformat(), "offset_seconds": (day - t0).total_seconds()},
        # an hour on, back to the same fake date
        {"real_time": (t0 + dt.timedelta(hours=1)).isoformat(),
         "offset_seconds": (day - t0 - dt.timedelta(hours=1)).total_seconds()},
    ]  # fmt: skip
    got = fake_to_real(day + dt.timedelta(minutes=10), history, now=t0 + dt.timedelta(hours=2))
    assert got == t0 + dt.timedelta(hours=1, minutes=10)


def test_offsets_in_a_window_start_with_the_one_already_in_force(setup):  # noqa: F811
    tc, _ = setup
    s = tc.store
    s.add_offset("2026-11-05T01:00:00+00:00", 10, "set")
    s.add_offset("2026-11-05T03:00:00+00:00", 20, "set")
    got = s.offsets("2026-11-05T02:00:00+00:00", "2026-11-05T04:00:00+00:00")
    assert [h["offset_seconds"] for h in got] == [10, 20]


def test_metrics_render_the_latest_samples_and_the_clock():
    sample = {
        "cpu_busy_pct": 12.5, "ncpu": 22, "mem_used_mib": 900, "psi_io": 3.5, "psi_cpu": None,
        "disk_used_gib": {"volume": 40.5}, "processes_mib": {"mysql": 6618.9, 'we"ird': 1},
    }  # fmt: skip
    text = render_metrics({"workload": (1000.0, sample)}, -86400.0, dt.datetime(2026, 11, 1, tzinfo=UTC))
    assert 'ferment_vm_cpu_busy_percent{vm="workload"} 12.5' in text
    assert 'ferment_vm_pressure_avg10{vm="workload",resource="io"} 3.5' in text
    assert 'resource="cpu"' not in text  # None isn't a value
    assert 'ferment_process_memory_mib{vm="workload",process="mysql"} 6618.9' in text
    assert 'process="we\\"ird"' in text
    assert "ferment_clock_offset_seconds -86400.0" in text
    assert "# TYPE ferment_vm_disk_used_gib gauge" in text


def test_metrics_and_offsets_over_http(setup):  # noqa: F811
    tc, _ = setup
    app = create_app(tc, measure={"vm": lambda: {"mem_used_mib": 512}})
    with TestClient(app) as client:
        r = client.get("/metrics")
        assert r.status_code == 200 and r.headers["content-type"].startswith("text/plain")
        assert "ferment_clock_offset_seconds" in r.text
        assert client.get("/clock/offsets").json()[0]["why"] == "initial"


def test_line_stamps_become_fake_time():
    assert fake_stamp("2026-11-01T03:10:00.123Z  INFO bookkeeper") == "2026-11-01T03:10:00.123+00:00"
    assert fake_stamp("[2026-11-01 03:10:00,5] task") == "2026-11-01T03:10:00.5+00:00"
    assert fake_stamp("2026-11-01T03:10:00+0100 x") == "2026-11-01T03:10:00+01:00"
    assert fake_stamp("no stamp here") is None


def _loki(pushed: list):
    def handle(request: httpx.Request) -> httpx.Response:
        pushed.extend(json.loads(request.content)["streams"])
        return httpx.Response(204)

    return httpx.Client(transport=httpx.MockTransport(handle))


def test_the_shipper_skips_old_logs_on_its_first_start_then_ships_whole_lines_once(tmp_path):
    log = tmp_path / "bookkeeper.log"
    log.write_text("old line, before shipping started\n")
    pushed: list = []
    fake = dt.datetime(2026, 11, 1, 3, 10, tzinfo=UTC)

    def ship():
        return LogShipper(
            "http://loki",
            lambda: {"process/bookkeeper": ({"process": "bookkeeper"}, file_reader(log))},
            tmp_path / "logship.json",
            fake_now=lambda: fake,
            real_now=lambda: dt.datetime(2026, 11, 5, 12, tzinfo=UTC),
            http=_loki(pushed),
        )

    s = ship()
    assert s.tick() == 0
    with log.open("a") as f:
        f.write("2026-11-01T03:09:59Z first\nsecond\npart")
    assert s.tick() == 2
    lines = [v for st in pushed for v in st["values"]]
    assert [v[1] for v in lines] == ["2026-11-01T03:09:59Z first", "second"]
    assert lines[0][2] == {"fake_time": "2026-11-01T03:09:59+00:00"}
    assert lines[1][2] == {"fake_time": fake.isoformat()}  # no stamp: when it was shipped
    assert int(lines[1][0]) > int(lines[0][0])  # each line later than the last: Loki keeps the order
    with log.open("a") as f:
        f.write("ial\n")
    s2 = ship()  # a restart: carries on from the saved offset
    assert s2.tick() == 1
    assert [v for st in pushed for v in st["values"]][-1][1] == "partial"


def test_a_new_log_after_the_first_start_ships_from_its_beginning(tmp_path):
    pushed: list = []
    (tmp_path / "logship.json").write_text("{}")  # shipping already started once
    log = tmp_path / "event.log"
    log.write_text("hello\n")
    s = LogShipper(
        "http://loki",
        lambda: {"process/event": ({"process": "event"}, file_reader(log))},
        tmp_path / "logship.json",
        fake_now=lambda: dt.datetime(2026, 11, 1, tzinfo=UTC),
        real_now=lambda: dt.datetime(2026, 11, 5, tzinfo=UTC),
        http=_loki(pushed),
    )
    assert s.tick() == 1 and pushed[0]["stream"] == {"process": "event"}


def test_annotations_mark_regions_and_notable_events():
    from ferment.server.telemetry import Annotations

    calls: list = []

    def grafana(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path, json.loads(request.content) if request.content else None))
        if request.method == "GET":
            return httpx.Response(200, json=[{"id": 41}])
        return httpx.Response(200, json={"id": 41})

    a = Annotations("http://g/grafana", http=httpx.Client(transport=httpx.MockTransport(grafana)), background=False)
    a.event("attached", "alice attached", dt.datetime(2026, 11, 5, tzinfo=UTC))  # not worth a line
    a.event("restored", "restored checkpoint 6", dt.datetime(2026, 11, 5, tzinfo=UTC))
    a.mark_started({"id": 3, "label": "experiment 7 (replay)", "started": "2026-11-05T10:00:00+00:00"})
    a.mark_ended({"id": 3, "label": "experiment 7 (replay)", "started": "2026-11-05T10:00:00+00:00",
                  "ended": "2026-11-05T11:00:00+00:00"})  # fmt: skip
    assert [c[0] for c in calls] == ["POST", "POST", "GET", "PATCH"]
    assert calls[0][2]["tags"] == ["fermenter", "restored"]
    assert calls[1][2]["tags"] == ["fermenter", "mark", "mark-3"]
    assert calls[3][1] == "/grafana/api/annotations/41" and calls[3][2] == {"timeEnd": 1793876400000}
