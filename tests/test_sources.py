import subprocess
import time

import pytest

from ferment import sources
from ferment.server.builds import Builder
from ferment.server.sources import SourceStore, SourceStoreError
from ferment.server.store import Store


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


@pytest.fixture
def upstream(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "client-state"))
    repo = tmp_path / "upstream"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "development")
    git(repo, "config", "user.email", "t@t")
    git(repo, "config", "user.name", "t")
    (repo / "pom.txt").write_text("v1\n")
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "one")
    return repo


def commit(repo, content):
    (repo / "pom.txt").write_text(content)
    git(repo, "commit", "-qam", content)
    return git(repo, "rev-parse", "HEAD")


def wait(builder_store, build_id, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        b = builder_store.build(build_id)
        if b["status"] in ("succeeded", "failed"):
            return b
        time.sleep(0.05)
    raise AssertionError("build didn't finish")


def test_branch_to_vm_checkout_with_incremental_bundles(upstream, tmp_path):
    url = str(upstream)
    c1 = sources.resolve(url, "development")
    assert c1 == git(upstream, "rev-parse", "HEAD")
    with pytest.raises(sources.SourceError, match="no branch or tag"):
        sources.resolve(url, "nope")

    vm = SourceStore(tmp_path / "vm")
    assert vm.commits("svc") == []
    first = sources.bundle(sources.fetch(url, c1), c1, have=vm.commits("svc"))
    assert vm.receive("svc", first) == [c1]

    c2 = commit(upstream, "v2\n")
    second = sources.bundle(sources.fetch(url, c2), c2, have=vm.commits("svc"))
    assert len(second) < len(first) * 2  # only the new commit's objects, not all history again
    vm.receive("svc", second)
    assert set(vm.commits("svc")) == {c1, c2}

    # c1 arrived as history of c2's bundle too; an older commit needs nothing sent
    assert not sources.missing(sources.fetch(url, c1), c1, [c2])
    assert vm.has("svc", c1)

    tree = vm.worktree("svc", c2)
    assert (tree / "pom.txt").read_text() == "v2\n"
    assert (tree / ".git").exists()  # builds that read git metadata keep working
    with pytest.raises(SourceStoreError, match="upload a bundle first"):
        vm.worktree("svc", "0" * 40)


def test_builds_succeed_reuse_and_fail_loudly(upstream, tmp_path):
    url = str(upstream)
    c1 = sources.resolve(url, "development")
    vm = SourceStore(tmp_path / "vm")
    vm.receive("svc", sources.bundle(sources.fetch(url, c1), c1))
    store = Store(tmp_path / "vm" / "f.db")
    builder = Builder(store, vm, tmp_path / "vm" / "logs")

    ok = builder.request("svc", c1, "mkdir -p target && cp pom.txt target/svc-1.jar", "target/svc-*.jar", "me")
    done = wait(store, ok["id"])
    assert done["status"] == "succeeded" and done["artifact"].endswith("target/svc-1.jar")
    assert (
        builder.request("svc", c1, "mkdir -p target && cp pom.txt target/svc-1.jar", "target/svc-*.jar", "me")["id"]
        == ok["id"]
    )  # cached by (source, commit, command)

    stamp = wait(store, builder.request("svc", c1, "echo epoch=$SOURCE_DATE_EPOCH", None, "me")["id"])
    assert f"epoch={git(upstream, 'log', '-1', '--format=%ct', c1)}" in builder.log(stamp["id"])[0]

    bad = wait(store, builder.request("svc", c1, "echo boom; exit 3", None, "me")["id"])
    assert bad["status"] == "failed" and bad["error"] == "exit 3"
    assert "boom" in builder.log(bad["id"])[0]

    glob = wait(store, builder.request("svc", c1, "true", "target/*.war", "me")["id"])
    assert glob["status"] == "failed" and "matched 0 files" in glob["error"]


def test_source_requests_are_claimed_once_and_expire(tmp_path, monkeypatch):
    import datetime as dt

    from ferment.server import store as store_mod

    s = Store(tmp_path / "f.db")
    r = s.request_source("svc", "git@corp:o/svc.git", "development", "me")
    assert s.request_source("svc", "git@corp:o/svc.git", "development", "you")["id"] == r["id"]  # deduped
    assert s.claim_source_request(r["id"], "vpn-box")
    assert not s.claim_source_request(r["id"], "other-box")  # exactly one winner
    monkeypatch.setattr(store_mod, "CLAIM_TIMEOUT", dt.timedelta(seconds=-1))  # claimant vanished
    assert s.claim_source_request(r["id"], "other-box")


def test_attached_client_fetches_for_the_vm(upstream, tmp_path, monkeypatch):
    """apply on a machine that can't reach the repo queues a request; a
    client that can claims it, fetches, uploads; the commit lands on the VM."""
    import httpx
    from fastapi.testclient import TestClient

    from ferment.fulfill import SourceFulfiller
    from ferment.server.app import create_app
    from ferment.server.time_control import TimeController

    class Stack:
        def processes(self):
            return []

    class Sup:
        running = False

        def stop(self):
            pass

        def start(self, c):
            pass

        def wait_ready(self, timeout=1):
            return True

        def processes(self):
            return []

    vm_state = tmp_path / "vm"
    with TestClient(create_app(TimeController(Stack(), Sup(), "/x", vm_state))) as client:
        base = "http://fermenter"
        for verb in ("get", "post"):
            monkeypatch.setattr(
                httpx,
                verb,
                lambda url, _v=verb, **kw: getattr(client, _v)(
                    url.removeprefix(base), **{k: v for k, v in kw.items() if k != "timeout"}
                ),
            )
        monkeypatch.setattr(sources, "repo_host", lambda repo: "corp.example")  # pretend it's remote

        req = client.post("/source-requests", json={"name": "svc", "repo": str(upstream), "ref": "development"}).json()
        assert req["status"] == "pending"
        assert client.post(f"/source-requests/{req['id']}/finish", json={"commit": "0" * 40}).status_code == 422

        fulfiller = SourceFulfiller(base, ["corp.example"])
        (pending,) = httpx.get(f"{base}/source-requests").json()
        assert fulfiller._claim(pending["id"])
        fulfiller.fulfill(pending)

        done = client.get(f"/source-requests/{req['id']}").json()
        assert done["status"] == "done"
        assert done["commit_sha"] == git(upstream, "rev-parse", "HEAD")
        assert done["commit_sha"] in client.get("/sources/svc").json()["commits"]

        bad = client.post("/source-requests", json={"name": "svc", "repo": str(upstream), "ref": "nope"}).json()
        assert fulfiller._claim(bad["id"])
        fulfiller.fulfill(bad)
        failed = client.get(f"/source-requests/{bad['id']}").json()
        assert failed["status"] == "failed" and "no branch or tag" in failed["error"]
