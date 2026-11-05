import http.server
import socket
import socketserver
import threading

import pytest

from ferment import outbound


@pytest.fixture
def echo():
    """A TCP server on loopback that sends back what it gets."""

    class Echo(socketserver.BaseRequestHandler):
        def handle(self):
            while data := self.request.recv(4096):
                self.request.sendall(data)

    with socketserver.ThreadingTCPServer(("127.0.0.1", 0), Echo) as srv:
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        yield srv.server_address[1]
        srv.shutdown()


@pytest.fixture
def web():
    class Hello(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            body = f"path={self.path} host={self.headers['Host']}".encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    with http.server.ThreadingHTTPServer(("127.0.0.1", 0), Hello) as srv:
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        yield srv.server_address[1]
        srv.shutdown()


def _ask(port: int, request: bytes) -> tuple[bytes, socket.socket]:
    """Send `request` to the proxy; returns its response head and the socket."""
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.sendall(request)
    head = b""
    while b"\r\n\r\n" not in head:
        chunk = s.recv(1)
        if not chunk:
            break
        head += chunk
    return head, s


def test_allowlist_entries():
    allow = ["cache.nixos.org", "github.com:22", "*.githubusercontent.com"]
    assert outbound.allowed(allow, "cache.nixos.org", 443)
    assert outbound.allowed(allow, "Cache.NixOS.org.", 80)
    assert not outbound.allowed(allow, "cache.nixos.org", 8080)
    assert outbound.allowed(allow, "github.com", 22) and not outbound.allowed(allow, "github.com", 443)
    assert outbound.allowed(allow, "objects.githubusercontent.com", 443)
    assert not outbound.allowed(allow, "githubusercontent.com", 443)
    assert not outbound.allowed(allow, "evil.example", 443)


def test_connect_carries_bytes_both_ways(echo):
    events = []
    with outbound.Proxy([f"127.0.0.1:{echo}"], on_event=lambda *e: events.append(e)) as proxy:
        for _ in range(2):
            head, s = _ask(proxy.port, f"CONNECT 127.0.0.1:{echo} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
            with s:
                assert head.startswith(b"HTTP/1.1 200")
                s.sendall(b"hello")
                assert s.recv(5) == b"hello"
        assert proxy.seen["allowed"] == {f"127.0.0.1:{echo}": 2}
    assert events == [("allowed", f"127.0.0.1:{echo}", True), ("allowed", f"127.0.0.1:{echo}", False)]


def test_hosts_off_the_allowlist_are_refused_and_counted(echo):
    with outbound.Proxy(["cache.nixos.org"]) as proxy:
        head, s = _ask(proxy.port, f"CONNECT 127.0.0.1:{echo} HTTP/1.1\r\n\r\n".encode())
        with s:
            assert head.startswith(b"HTTP/1.1 403")
            assert b"allowlist" in s.recv(4096)
        assert proxy.seen["denied"] == {f"127.0.0.1:{echo}": 1}


def test_plain_http_is_sent_on_in_origin_form(web):
    with outbound.Proxy([f"127.0.0.1:{web}"]) as proxy:
        request = (
            f"GET http://127.0.0.1:{web}/nix-cache-info?x=1 HTTP/1.1\r\nHost: 127.0.0.1:{web}\r\n"
            "Proxy-Connection: keep-alive\r\n\r\n"
        )
        head, s = _ask(proxy.port, request.encode())
        with s:
            assert head.startswith(b"HTTP/1.0 200") or head.startswith(b"HTTP/1.1 200")
            assert s.recv(4096) == f"path=/nix-cache-info?x=1 host=127.0.0.1:{web}".encode()


def test_unreachable_hosts_are_a_bad_gateway():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        closed = s.getsockname()[1]  # bound, never listening: refused
    with outbound.Proxy([f"127.0.0.1:{closed}"]) as proxy:
        head, s = _ask(proxy.port, f"CONNECT 127.0.0.1:{closed} HTTP/1.1\r\n\r\n".encode())
        s.close()
        assert head.startswith(b"HTTP/1.1 502")
        assert proxy.seen["failed"] == {f"127.0.0.1:{closed}": 1}


def test_the_vm_gets_both_spellings_and_skips_its_own_hosts():
    e = outbound.env(["nexus.corp.example", "localhost"])
    assert e["https_proxy"] == e["HTTPS_PROXY"] == f"http://127.0.0.1:{outbound.VM_PORT}"
    skip = e["no_proxy"].split(",")
    assert skip.count("localhost") == 1 and ".internal" in skip and skip[-1] == "nexus.corp.example"
    script = outbound.install_script(["nexus.corp.example"])
    assert outbound.ENV_FILE in script and "systemctl set-environment" in script
    assert "try-restart nix-daemon" in script
    assert "unset-environment" in outbound.remove_script()


def test_the_forward_reaches_the_proxy_from_the_vms_port():
    with outbound.Proxy([]) as proxy:
        fwd = proxy.forward()
        assert fwd.ssh_args() == ["-R", f"127.0.0.1:{outbound.VM_PORT}:127.0.0.1:{proxy.port}"]
