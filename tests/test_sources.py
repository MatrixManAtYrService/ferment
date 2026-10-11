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
