"""The VM's outbound connections, carried by an attached client.

A fermenter whose VMs can't reach the internet themselves (a cloud subnet
with no egress) still needs packages while it's set up: nix substitutes and
flake inputs, PyPI. `relay` makes one known host:port appear on the VM; this
carries any host, as the HTTP proxy every one of those tools already knows:

    VM:     https_proxy=http://127.0.0.1:VM_PORT   (ENV_FILE, and systemd's manager
                                                   environment: nix-daemon, builds)
    ssh:    -R 127.0.0.1:VM_PORT -> 127.0.0.1:<the client's Proxy>
    client: Proxy: CONNECT host:port (TLS end to end) or a plain-HTTP request,
            only to hosts on the client's allowlist; the rest refused (403)

The allowlist belongs to the client carrying the traffic: it decides where
the VM may go, and sees each host it went to and each it was refused
(`on_event`), which is how a new fermenter's list gets written. Like a
relay's, the VM port is fixed: the first session to hold it carries
everyone's traffic, and with nobody holding it connections are refused.

Hosts that must not be proxied (the VM's own, the workload VM, relays, which
have their own way through) go in no_proxy.
"""

from __future__ import annotations

import contextlib
import fnmatch
import shlex
import socket
import socketserver
import threading
from collections.abc import Callable
from urllib.parse import urlsplit

from ferment.ssh import Forward

# On the VM's loopback, just below relay.FIRST_VM_PORT.
VM_PORT = 18439
ENV_FILE = "/etc/ferment/outbound.env"
# Reached directly, never through the proxy: loopback (relays bind there too),
# the provider's private names (Lima's lima-*.internal, GCE's
# metadata.google.internal) and the metadata server.
NO_PROXY = ["localhost", "127.0.0.1", "::1", ".internal", "169.254.169.254"]
CONNECT_TIMEOUT = 15
# Shell that puts ENV_FILE's settings in the environment, if there are any.
PRELUDE = f"if [ -f {ENV_FILE} ]; then set -a; . {ENV_FILE}; set +a; fi\n"


def allowed(allow: list[str], host: str, port: int) -> bool:
    """`allow`: "host" (ports 80 and 443), "host:port", with `*` wildcards
    ("*.githubusercontent.com"; a wildcard doesn't match the bare domain)."""
    host = host.lower().rstrip(".")
    for entry in allow:
        name, sep, p = entry.rpartition(":")
        if not sep or not p.isdigit():
            name, ports = entry, (80, 443)
        else:
            ports = (int(p),)
        if port in ports and fnmatch.fnmatchcase(host, name.lower()):
            return True
    return False


def _split(target: str, default_port: int) -> tuple[str, int]:
    host, sep, port = target.rpartition(":")
    if not sep or not port.isdigit():
        return target.strip("[]"), default_port
    return host.strip("[]"), int(port)


def _pipe(src: socket.socket, dst: socket.socket) -> None:
    """Copy until src ends (or either side breaks), then say so to dst."""
    with contextlib.suppress(OSError):
        while data := src.recv(65536):
            dst.sendall(data)
    with contextlib.suppress(OSError):
        dst.shutdown(socket.SHUT_WR)


class _Handler(socketserver.StreamRequestHandler):
    rbufsize = 0  # nothing past the headers may stay behind in a buffer
    server: _Server

    def _refuse(self, status: str, reason: str) -> None:
        body = f"ferment outbound proxy: {reason}\n".encode()
        head = f"HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {len(body)}\r\n"
        head += "Connection: close\r\n\r\n"
        self.wfile.write(head.encode() + body)

    def handle(self) -> None:
        line = self.rfile.readline(65536).decode("latin-1")
        headers = []
        while (h := self.rfile.readline(65536)) not in (b"\r\n", b"\n", b""):
            headers.append(h)
        try:
            method, target, version = line.split()
        except ValueError:
            return self._refuse("400 Bad Request", "not an HTTP request")
        if method == "CONNECT":
            host, port = _split(target, 443)
        else:
            url = urlsplit(target)
            if url.scheme != "http" or not url.hostname:
                return self._refuse("400 Bad Request", f"can't proxy {target!r}: CONNECT for https")
            host, port = url.hostname, url.port or 80
        where = f"{host}:{port}"
        if not allowed(self.server.allow, host, port):
            self.server.event("denied", where)
            return self._refuse("403 Forbidden", f"{where} isn't on this client's allowlist")
        try:
            upstream = socket.create_connection((host, port), timeout=CONNECT_TIMEOUT)
        except OSError as e:
            self.server.event("failed", where)
            return self._refuse("502 Bad Gateway", f"{where}: {e}")
        upstream.settimeout(None)
        self.server.event("allowed", where)
        with upstream:
            if method == "CONNECT":
                self.wfile.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            else:
                # Origin-form, one request per connection (the next may be for another host).
                path = (url.path or "/") + (f"?{url.query}" if url.query else "")
                keep = [h for h in headers if not h.lower().startswith((b"proxy-", b"connection:"))]
                upstream.sendall(
                    f"{method} {path} {version}\r\n".encode() + b"".join(keep) + b"Connection: close\r\n\r\n"
                )
            back = threading.Thread(target=_pipe, args=(upstream, self.connection), daemon=True)
            back.start()
            _pipe(self.connection, upstream)
            back.join()


class _Server(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, port: int, allow: list[str], on_event: Callable[[str, str, bool], None] | None):
        self.allow = list(allow)
        self.on_event = on_event
        self.seen: dict[str, dict[str, int]] = {"allowed": {}, "denied": {}, "failed": {}}
        self._lock = threading.Lock()
        super().__init__(("127.0.0.1", port), _Handler)

    def event(self, verdict: str, where: str) -> None:
        with self._lock:
            counts = self.seen[verdict]
            first = where not in counts
            counts[where] = counts.get(where, 0) + 1
        if self.on_event is not None:
            self.on_event(verdict, where, first)


class Proxy:
    """The client's side: an HTTP proxy on this machine's loopback, serving
    until close(). `on_event(verdict, where, first)` for each connection:
    verdict allowed, denied or failed; `first`: this host:port's first time
    with that verdict, so a caller can say each once."""

    def __init__(self, allow: list[str], *, port: int = 0, on_event: Callable[[str, str, bool], None] | None = None):
        self._server = _Server(port, allow, on_event)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True, name="ferment-outbound")
        self._thread.start()

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    @property
    def seen(self) -> dict[str, dict[str, int]]:
        """verdict -> {host:port: connections}."""
        with self._server._lock:
            return {k: dict(v) for k, v in self._server.seen.items()}

    def forward(self) -> Forward:
        return Forward(0, VM_PORT, reverse=True, dest=f"127.0.0.1:{self.port}", label="outbound")

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def __enter__(self) -> Proxy:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def env(no_proxy: list[str] = ()) -> dict[str, str]:
    """What points a VM's tools at the proxy: both spellings, since tools
    differ in which they read."""
    proxy = f"http://127.0.0.1:{VM_PORT}"
    skip = ",".join(dict.fromkeys([*NO_PROXY, *no_proxy]))
    out = {}
    for key, value in {"http_proxy": proxy, "https_proxy": proxy, "no_proxy": skip}.items():
        out[key] = out[key.upper()] = value
    return out


def install_script(no_proxy: list[str] = ()) -> str:
    """Shell (the VM's ssh user, with sudo) that points the VM's outbound
    traffic at VM_PORT: ENV_FILE for shells (PRELUDE), systemd's manager
    environment for every unit started from now on, and a restarted
    nix-daemon (it fetches substitutes) when that changed anything.
    Idempotent; systemd's environment is runtime state, so deploy re-applies
    it after a reboot."""
    settings = env(no_proxy)
    text = "".join(f"{k}={shlex.quote(v)}\n" for k, v in settings.items())
    assignments = " ".join(shlex.quote(f"{k}={v}") for k, v in settings.items())
    return f"""\
set -eu
want={shlex.quote(text)}
if [ "$(cat {ENV_FILE} 2>/dev/null)" != "$(printf '%s' "$want")" ]; then
  sudo install -d /etc/ferment
  printf '%s' "$want" | sudo tee {ENV_FILE} >/dev/null
  changed=1
fi
if ! systemctl show-environment | grep -qxF {shlex.quote(f"https_proxy={settings['https_proxy']}")}; then
  changed=1
fi
sudo systemctl set-environment {assignments}
if [ -n "${{changed:-}}" ]; then sudo systemctl try-restart nix-daemon.service; fi
"""


def remove_script() -> str:
    """Undo install_script (idempotent): the VM reaches out by itself again."""
    names = " ".join(env())
    return f"""\
set -eu
if [ -f {ENV_FILE} ] || systemctl show-environment | grep -q '^https_proxy='; then
  sudo rm -f {ENV_FILE}
  sudo systemctl unset-environment {names}
  sudo systemctl try-restart nix-daemon.service
fi
"""
