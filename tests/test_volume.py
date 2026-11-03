import os
import shutil
from pathlib import Path

import pytest

from ferment.volume import BINLOG_PART, PART, Volume, VolumeError, VolumeSnapshots


class FakeBtrfs:
    """Plays btrfs (and the loop mount) with plain directories."""

    def __init__(self):
        self.calls: list[list[str]] = []
        self.mounted = False
        self.resize_fails = False

    def __call__(self, argv: list[str]) -> str:
        self.calls.append(argv)
        args = argv[2:] if argv[:2] == ["sudo", "-n"] else argv
        cmd = Path(args[0]).name
        if cmd == "truncate":
            Path(args[-1]).touch()
            os.truncate(args[-1], int(args[2]))  # sparse, as on the VM
        elif cmd == "losetup" and args[1] == "-j":
            return f"/dev/loop0: []: ({args[2]})"
        elif cmd == "btrfs" and args[1:3] == ["filesystem", "resize"] and self.resize_fails:
            raise VolumeError("ERROR: unable to resize: No space left on device")
        elif cmd == "mount":
            self.mounted = True
        elif cmd == "btrfs" and args[1:3] == ["subvolume", "create"]:
            Path(args[3]).mkdir()
        elif cmd == "btrfs" and args[1:3] == ["subvolume", "snapshot"]:
            src, dst = args[-2], args[-1]
            shutil.copytree(src, dst, symlinks=True)
        elif cmd == "btrfs" and args[1:3] == ["subvolume", "delete"]:
            shutil.rmtree(args[3])
        elif cmd == "cp":
            shutil.copytree(args[2].rstrip("."), args[3], dirs_exist_ok=True)
        return ""


GIB = 1 << 30


def _volume(tmp_path, monkeypatch, disk: list[int] | None = None) -> tuple[Volume, FakeBtrfs]:
    """`disk`: the VM disk's [total, free] in GiB (a list: a test changes it as the disk fills)."""
    fake = FakeBtrfs()
    disk = disk if disk is not None else [100, 50]
    dirs = {"mysql84": tmp_path / "mysql84", "airflow3": tmp_path / "airflow3"}
    v = Volume(
        tmp_path,
        dirs,
        btrfs_bin="/nix/btrfs/bin",
        run=fake,
        disk=lambda: (disk[0] * GIB, disk[1] * GIB),
        scratch={"mysql84-tmp": tmp_path / "mysql84-tmp"},
    )
    monkeypatch.setattr(Volume, "mounted", lambda self: fake.mounted)
    return v, fake


def test_an_existing_data_dir_moves_onto_the_volume_once(tmp_path, monkeypatch):
    v, fake = _volume(tmp_path, monkeypatch)
    (tmp_path / "mysql84").mkdir()
    (tmp_path / "mysql84" / "ibdata1").write_text("rows")
    done = v.ensure()
    assert any("moved" in d for d in done)
    assert (tmp_path / "mysql84").is_symlink() and (tmp_path / "mysql84" / "ibdata1").read_text() == "rows"
    assert ["/nix/btrfs/bin/mkfs.btrfs", "-q", str(v.image)] in fake.calls  # by path: sudo resets PATH
    assert v.ensure() == []  # nothing left to do

    fake.mounted = False  # a reboot
    assert v.ensure() == [f"mounted it at {v.mount}"]


def test_moving_refuses_while_the_database_runs(tmp_path, monkeypatch):
    v, _ = _volume(tmp_path, monkeypatch)
    (tmp_path / "mysql84").mkdir()
    with pytest.raises(VolumeError, match="workload is running"):
        v.ensure(workload_running=True)
    assert (tmp_path / "mysql84").is_dir() and not (tmp_path / "mysql84").is_symlink()


def test_snapshots_restore_and_keep_their_own_copy(tmp_path, monkeypatch):
    v, _ = _volume(tmp_path, monkeypatch)
    v.ensure()
    data = tmp_path / "mysql84" / "t.ibd"
    data.write_text("day 1")
    v.snapshot("ckpt-1")
    data.write_text("day 2")
    v.snapshot("ckpt-2")
    v.restore("ckpt-1")
    assert data.read_text() == "day 1"
    v.restore("ckpt-2")
    assert data.read_text() == "day 2"
    assert v.list() == ["ckpt-1", "ckpt-2"]
    with pytest.raises(VolumeError, match="exists"):
        v.snapshot("ckpt-1")
    with pytest.raises(VolumeError, match="bad snapshot id"):
        v.snapshot("../escape")
    v.delete("ckpt-1")
    assert v.list() == ["ckpt-2"]


def test_checkpoints_through_the_agent_are_volume_snapshots():
    class Agent:
        def __init__(self):
            self.calls = []

        def snapshot_volume(self, sid):
            self.calls.append(("snapshot", sid))
            return {"id": sid, "binlog": "binlog.000007:1234"}

        def restore_volume(self, sid):
            self.calls.append(("restore", sid))

    agent = Agent()
    store = VolumeSnapshots(agent)
    parts = store.snapshot("before clock change", ["ckpt-7", "loaded"], running=False)
    assert parts == {PART: "ckpt-7", BINLOG_PART: "binlog.000007:1234"}
    store.restore(parts)
    assert agent.calls == [("snapshot", "ckpt-7"), ("restore", "ckpt-7")]
    with pytest.raises(VolumeError, match="before snapshots"):
        store.restore({"other:billing": "abc"})


def test_the_agent_prepares_the_volume_before_the_workload_and_guards_restores(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from ferment.agent.app import create_agent_app

    class Supervisor:
        running = False

        def launch(self, processes):
            self.running = True

        def stop(self):
            self.running = False

        def processes(self):
            return []

    v, _ = _volume(tmp_path, monkeypatch)
    sup = Supervisor()
    with TestClient(create_agent_app(sup, volume=v)) as client:
        assert client.put("/workload", json={"processes": []}).status_code == 200  # waits for the volume
        assert client.get("/health").json()["volume"] == "ready"
        assert client.post("/volume/snapshots/ckpt-1").json() == {"id": "ckpt-1", "binlog": None}  # fine while running
        assert client.post("/volume/snapshots/ckpt-1/restore").status_code == 409  # not while running
        client.delete("/workload")
        assert client.post("/volume/snapshots/ckpt-1/restore").status_code == 200
        assert client.get("/volume").json()["snapshots"] == ["ckpt-1"]


def test_a_stopped_servers_binlog_position_is_its_newest_binlog_and_length(tmp_path):
    from ferment.databases import MySQL

    db = MySQL(tmp_path)
    assert db.binlog_position() is None
    db.data_dir.mkdir()
    (db.data_dir / "binlog.000006").write_bytes(b"x" * 10)
    (db.data_dir / "binlog.000007").write_bytes(b"x" * 1234)
    (db.data_dir / "binlog.index").write_text("./binlog.000006\n./binlog.000007\n")
    assert db.binlog_position() == "binlog.000007:1234"


def test_binlogs_never_expire_by_the_fake_clock(tmp_path):
    from ferment.databases import MySQL

    args = MySQL(tmp_path)._server_args(3306, tmp_path / "s.sock")
    assert "--binlog-expire-logs-auto-purge=OFF" in args
    assert "--tls-version=" in args  # no TLS: its certificate's dates would meet the fake clock


def test_every_dir_lives_in_the_one_subvolume_so_a_snapshot_cuts_them_together(tmp_path, monkeypatch):
    v, _ = _volume(tmp_path, monkeypatch)
    (tmp_path / "airflow3").mkdir()
    (tmp_path / "airflow3" / "airflow.db").write_text("runs: day 1")
    v.ensure()
    assert (tmp_path / "airflow3").resolve() == (v.live / "airflow3").resolve()
    (tmp_path / "mysql84" / "t.ibd").write_text("rows: day 1")
    v.snapshot("ckpt-1")
    (tmp_path / "airflow3" / "airflow.db").write_text("runs: day 2")
    (tmp_path / "mysql84" / "t.ibd").write_text("rows: day 2")
    v.restore("ckpt-1")
    assert (tmp_path / "airflow3" / "airflow.db").read_text() == "runs: day 1"
    assert (tmp_path / "mysql84" / "t.ibd").read_text() == "rows: day 1"


def test_the_old_one_dir_layout_is_converted_and_its_checkpoints_still_restore(tmp_path, monkeypatch):
    v, fake = _volume(tmp_path, monkeypatch)
    # As the first volumes were: the data dir's contents at live's root, linked from mysql84.
    fake.mounted = True
    v.image.write_bytes(b"")
    v.live.mkdir(parents=True)
    (v.live / "ibdata1").write_text("old rows")
    (tmp_path / "mysql84").symlink_to(v.live)
    v.snapshots.mkdir()
    v.snapshot("ckpt-old")

    done = v.ensure()
    assert "moved the old layout into live/mysql84" in done
    assert (tmp_path / "mysql84" / "ibdata1").read_text() == "old rows" and (tmp_path / "airflow3").is_dir()

    (tmp_path / "mysql84" / "ibdata1").write_text("new rows")
    v.restore("ckpt-old")  # an old-layout snapshot: converted as it comes back
    assert (tmp_path / "mysql84" / "ibdata1").read_text() == "old rows"
    assert (tmp_path / "airflow3").is_dir()  # not dangling, though that checkpoint predates it


def test_a_new_image_leaves_the_reserve_free(tmp_path, monkeypatch):
    v, _ = _volume(tmp_path, monkeypatch, disk=[100, 50])
    v.ensure()
    assert v.image.stat().st_size == 35 * GIB  # 50 free, less the 15 reserve


def test_the_image_shrinks_when_the_disk_fills_around_it_and_grows_back(tmp_path, monkeypatch):
    disk = [100, 50]
    v, fake = _volume(tmp_path, monkeypatch, disk)
    v.ensure()
    disk[1] = 20  # logs and builds took 30 GiB outside the volume
    fake.calls.clear()
    assert v.ensure() == ["resized from 35 to 5 GiB (the disk keeps 15 GiB free)"]
    resize = ["sudo", "-n", "/nix/btrfs/bin/btrfs", "filesystem", "resize", str(5 * GIB), str(v.mount)]
    assert fake.calls.index(resize) < fake.calls.index(["truncate", "-s", str(5 * GIB), str(v.image)])
    assert ["sudo", "-n", "losetup", "-c", "/dev/loop0"] in fake.calls
    disk[1] = 60
    assert v.ensure() == ["resized from 5 to 45 GiB (the disk keeps 15 GiB free)"]
    assert v.image.stat().st_size == 45 * GIB


def test_a_shrink_that_doesnt_fit_is_reported_and_leaves_the_image(tmp_path, monkeypatch):
    disk = [100, 50]
    v, fake = _volume(tmp_path, monkeypatch, disk)
    v.ensure()
    disk[1], fake.resize_fails = 20, True
    (done,) = v.ensure()
    assert done.startswith("not resized to 5 GiB") and "No space left" in done
    assert v.image.stat().st_size == 35 * GIB


def test_scratch_dirs_live_on_the_volume_but_out_of_snapshots(tmp_path, monkeypatch):
    v, _ = _volume(tmp_path, monkeypatch)
    (tmp_path / "mysql84-tmp").mkdir()
    (tmp_path / "mysql84-tmp" / "#sql1.ibd").write_text("leftover")
    assert f"{tmp_path / 'mysql84-tmp'} is on it now (scratch: not snapshotted)" in v.ensure()
    assert (tmp_path / "mysql84-tmp").resolve() == (v.scratch / "mysql84-tmp").resolve()
    assert not (v.scratch / "mysql84-tmp" / "#sql1.ibd").exists()  # temp files don't move
    (tmp_path / "mysql84-tmp" / "#sql2.ibd").write_text("sorting")
    v.snapshot("ckpt-1")
    assert list((v.snapshots / "ckpt-1").iterdir()) and not (v.snapshots / "ckpt-1" / "mysql84-tmp").exists()
    assert v.ensure() == []


def test_a_scratch_dir_in_use_waits_for_the_workload_to_stop(tmp_path, monkeypatch):
    v, _ = _volume(tmp_path, monkeypatch)
    (tmp_path / "mysql84").mkdir()
    v.ensure()  # the data dir is moved; the workload starts
    (tmp_path / "mysql84-tmp").unlink()
    (tmp_path / "mysql84-tmp").mkdir()
    (tmp_path / "mysql84-tmp" / "#sql1.ibd").write_text("sorting")
    assert v.ensure(workload_running=True) == [
        f"{tmp_path / 'mysql84-tmp'} moves onto it when the workload is stopped (in use now)"
    ]
    assert (tmp_path / "mysql84-tmp" / "#sql1.ibd").exists()
