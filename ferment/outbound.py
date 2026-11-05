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
import ssl
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from urllib.parse import urlsplit

from ferment.ssh import Forward

# On the VM's loopback, just below relay.FIRST_VM_PORT.
VM_PORT = 18439
ENV_FILE = "/etc/ferment/outbound.env"
# What the VM trusts for HTTPS while it goes through the proxy: the system's
# roots plus every root a carrying client named (outbound_ca: the one its
# network re-signs TLS with, e.g. a corporate inspection proxy's). Added,
# never removed by another client: the port goes to whichever binds it.
CA_DIR = "/etc/ferment/outbound-ca.d"
CA_BUNDLE = "/etc/ferment/outbound-ca.pem"
SYSTEM_CAS = "/etc/ssl/certs/ca-certificates.crt"
# Who reads which: OpenSSL (Python, git, uv's native TLS), nix, curl, requests, git.
CA_VARS = ("SSL_CERT_FILE", "NIX_SSL_CERT_FILE", "CURL_CA_BUNDLE", "REQUESTS_CA_BUNDLE", "GIT_SSL_CAINFO")
# Reached directly, never through the proxy: loopback (relays bind there too),
# the provider's private names (Lima's lima-*.internal, GCE's
# metadata.google.internal) and the metadata server.
NO_PROXY = ["localhost", "127.0.0.1", "::1", ".internal", "169.254.169.254"]
# Per address, and for all of a host's together. Tried IPv4 first: a client
# on a VPN may get IPv6 addresses its routes then drop, each attempt waiting
# out its timeout before the next (a CONNECT to pypi.org took 133s that way).
ADDRESS_TIMEOUT = 4
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


def connect(host: str, port: int) -> socket.socket:
    """A connection to host:port: its IPv4 addresses first, then IPv6, a few
    seconds each, within CONNECT_TIMEOUT for them all."""
    deadline = time.monotonic() + CONNECT_TIMEOUT
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    infos.sort(key=lambda info: info[0] != socket.AF_INET)  # stable: the resolver's order within each
    error: OSError = OSError(f"no addresses for {host}")
    for family, kind, proto, _, address in infos:
        left = deadline - time.monotonic()
        if left <= 0:
            break
        s = socket.socket(family, kind, proto)
        s.settimeout(min(ADDRESS_TIMEOUT, left))
        try:
            s.connect(address)
        except OSError as e:
            s.close()
            error = e
            continue
        s.settimeout(None)
        return s
    raise error


def vm_trust(ca: str = "") -> ssl.SSLContext:
    """TLS verification as the VM will do it: public roots (certifi's, not
    this machine's store, which may hold a corporate root the VM lacks) plus
    `ca` (PEM), the root this client's network re-signs with, if any."""
    try:
        import certifi

        ctx = ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        ctx = ssl.create_default_context()
    if ca.strip():
        ctx.load_verify_locations(cadata=ca)
    return ctx


def _der(cert) -> bytes:  # noqa: ANN001
    """A chain entry as DER: bytes from 3.13's public API, objects from 3.12's private one."""
    return cert if isinstance(cert, bytes) else cert.public_bytes(ssl._ssl.ENCODING_DER)  # type: ignore[attr-defined]


def _handshake(host: str, port: int, ctx: ssl.SSLContext, timeout: float) -> list[bytes]:
    """The certificate chain (DER, leaf first) `ctx` verified for host:port."""
    with connect(host, port) as s:
        s.settimeout(timeout)
        with ctx.wrap_socket(s, server_hostname=host) as tls:
            chain = getattr(tls, "get_verified_chain", None) or tls._sslobj.get_verified_chain  # private before 3.13
            return [_der(c) for c in chain()]


def _unverified_top(host: str, port: int, timeout: float) -> bytes | None:
    """The last certificate host:port sends (DER), trusted or not."""
    ctx = ssl.create_default_context()
    ctx.check_hostname, ctx.verify_mode = False, ssl.CERT_NONE
    with connect(host, port) as s:
        s.settimeout(timeout)
        with ctx.wrap_socket(s, server_hostname=host) as tls:
            chain = getattr(tls, "get_unverified_chain", None) or tls._sslobj.get_unverified_chain
            certs = [_der(c) for c in chain()]
    return certs[-1] if certs else None


def _keychain_candidates(top: bytes) -> list[str]:
    """macOS: roots in the keychains named like the issuer of `top` (Python's
    OpenSSL doesn't read the keychain, where a managed Mac keeps its corporate
    roots). Empty elsewhere, or when `security` finds nothing."""
    if sys.platform != "darwin":
        return []
    pem = ssl.DER_cert_to_PEM_cert(top)
    issuer = subprocess.run(
        ["openssl", "x509", "-noout", "-issuer", "-nameopt", "multiline"], input=pem, capture_output=True, text=True
    ).stdout
    names = [line.split("=", 1)[1].strip() for line in issuer.splitlines() if "commonName" in line]
    found = []
    for name in names:
        out = subprocess.run(["security", "find-certificate", "-a", "-c", name, "-p"], capture_output=True, text=True)
        found += [f"-----BEGIN CERTIFICATE-----{b}" for b in out.stdout.split("-----BEGIN CERTIFICATE-----")[1:]]
    return found


def interception_root(
    host: str, port: int = 443, timeout: float = 10.0, machine: ssl.SSLContext | None = None
) -> str | None:
    """The root this machine's network re-signs TLS with, as PEM, for the VM to
    trust (outbound_ca); None if the VM's trust (public roots) already accepts
    what host:port presents, i.e. nothing intercepts. `machine`: this machine's
    own trust (default: OpenSSL's); on macOS the keychain is tried too.
    Raises OSError when no root it can find makes the VM's trust accept it."""
    try:
        _handshake(host, port, vm_trust(), timeout)
        return None
    except ssl.SSLCertVerificationError:
        pass
    candidates = []
    try:
        chain = _handshake(host, port, machine or ssl.create_default_context(), timeout)
        candidates.append(ssl.DER_cert_to_PEM_cert(chain[-1]))  # the anchor this machine trusts
    except ssl.SSLCertVerificationError:
        if (top := _unverified_top(host, port, timeout)) is not None:
            candidates += _keychain_candidates(top)
    for pem in candidates:
        try:
            _handshake(host, port, vm_trust(pem), timeout)
            return pem
        except ssl.SSLCertVerificationError:
            continue
    raise OSError(
        f"{host}:{port}'s certificate isn't one the VM would trust, and no root this machine has makes it so"
        + ("" if candidates else " (none found: is this machine set up to trust its network's TLS inspection?)")
    )


def unreachable(allow: list[str], timeout: float = 5.0, ca: str = "") -> list[str]:
    """The allowlist's hosts this machine can't reach for the VM, all tried at
    once within `timeout`: DNS (a VPN's resolver may hang on names it won't
    serve), the connection, and on 443 a TLS handshake verified as the VM
    will verify it (vm_trust: a network that intercepts TLS passes only if
    `ca` names its root). Wildcard entries can't be tried, and aren't.
    The VM gets one proxy, whoever's: a client that can't reach some of its
    hosts shouldn't be it."""
    entries = [e for e in allow if "*" not in e]
    reached: set[str] = set()
    trust = vm_trust(ca)

    def attempt(entry: str) -> None:
        host, sep, port = entry.rpartition(":")
        host, port = (host, int(port)) if sep and port.isdigit() else (entry, 443)
        with contextlib.suppress(OSError), connect(host, port) as s:
            if port == 443:
                s.settimeout(timeout)
                with trust.wrap_socket(s, server_hostname=host):
                    pass
            reached.add(entry)

    threads = [threading.Thread(target=attempt, args=(e,), daemon=True) for e in entries]
    for t in threads:
        t.start()
    deadline = time.monotonic() + timeout
    for t in threads:
        t.join(max(0.0, deadline - time.monotonic()))
    return [e for e in entries if e not in reached]


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
            upstream = connect(host, port)
        except OSError as e:
            self.server.event("failed", where)
            return self._refuse("502 Bad Gateway", f"{where}: {e}")
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


# curl (nix's, and others) multiplexing HTTP/2 through a CONNECT proxy now
# and then breaks every request on a connection at once ("Error in the HTTP2
# framing layer"; through tinyproxy too, never direct). HTTP/1.1 doesn't, and
# nix's parallel connections (http-connections) keep it as fast.
NIX_CONFIG = "http2 = false"


def probe_script(allow: list[str]) -> str | None:
    """Shell for the VM: one HTTPS request through VM_PORT to an allowlisted
    host (the first plain name on 443), printing curl's status, when its TLS
    handshake finished, and the certificate's verification result. None if
    the list has no such host."""
    for entry in allow:
        host, sep, port = entry.rpartition(":")
        host = host if sep and port.isdigit() else entry
        if "*" not in host and (not sep or not port.isdigit() or port == "443"):
            proxy = f"http://127.0.0.1:{VM_PORT}"
            fmt = "%{http_code} %{time_appconnect} %{ssl_verify_result}"
            # With the VM's own settings (PRELUDE: the roots it trusts), as its tools go out.
            return PRELUDE + f"curl -s -o /dev/null --max-time 10 -x {proxy} -w {shlex.quote(fmt)} https://{host}/"
    return None


def carried(probe_output: str) -> bool:
    """Did probe_script's request get through? Its TLS handshake finished: a
    forward that's stuck still answers CONNECT, then passes nothing more."""
    try:
        return float(probe_output.split()[1]) > 0
    except (IndexError, ValueError):
        return False


def untrusted(probe_output: str) -> bool:
    """Did probe_script's request reach a server whose certificate the VM
    doesn't trust? The client carrying it is behind TLS interception (a
    corporate proxy re-signing with a root that client trusts and the VM
    doesn't): no forward restart helps."""
    parts = probe_output.split()
    return len(parts) >= 3 and parts[2] not in ("0", "")


def env(no_proxy: list[str] = ()) -> dict[str, str]:
    """What points a VM's tools at the proxy: both spellings, since tools
    differ in which they read; and nix's settings for going through one."""
    proxy = f"http://127.0.0.1:{VM_PORT}"
    skip = ",".join(dict.fromkeys([*NO_PROXY, *no_proxy]))
    out = {}
    for key, value in {"http_proxy": proxy, "https_proxy": proxy, "no_proxy": skip}.items():
        out[key] = out[key.upper()] = value
    out["NIX_CONFIG"] = NIX_CONFIG
    out.update(dict.fromkeys(CA_VARS, CA_BUNDLE))
    return out


def install_script(no_proxy: list[str] = (), ca: str = "") -> str:
    """Shell (the VM's ssh user, with sudo) that points the VM's outbound
    traffic at VM_PORT: ENV_FILE for shells (PRELUDE), systemd's manager
    environment for every unit started from now on, and a restarted
    nix-daemon (it fetches substitutes) when that changed anything. `ca`
    (PEM): a root to trust from now on (CA_DIR), beside the system's and any
    other client's. Idempotent; systemd's environment is runtime state, so
    it's applied again after a reboot (deploy, attach)."""
    settings = env(no_proxy)
    text = "".join(f"{k}={shlex.quote(v)}\n" for k, v in settings.items())
    assignments = " ".join(shlex.quote(f"{k}={v}") for k, v in settings.items())
    pem = ca.strip() + "\n" if ca.strip() else ""
    add_ca = (
        f"ca={shlex.quote(pem)}\n"
        f'name=$(printf "%s" "$ca" | sha256sum | cut -c1-16).pem\n'
        f'if [ ! -f {CA_DIR}/"$name" ]; then\n'
        f"  sudo install -d {CA_DIR}\n"
        f'  printf "%s" "$ca" | sudo tee {CA_DIR}/"$name" >/dev/null\n'
        f"fi\n"
        if pem
        else ""
    )
    return f"""\
set -eu
{add_ca}sudo install -d /etc/ferment
bundle=$(cat {SYSTEM_CAS}; cat {CA_DIR}/*.pem 2>/dev/null || true)
if [ "$(cat {CA_BUNDLE} 2>/dev/null)" != "$bundle" ]; then
  printf '%s\\n' "$bundle" | sudo tee {CA_BUNDLE} >/dev/null
  changed=1
fi
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
  sudo rm -rf {ENV_FILE} {CA_BUNDLE} {CA_DIR}
  sudo systemctl unset-environment {names}
  sudo systemctl try-restart nix-daemon.service
fi
"""
