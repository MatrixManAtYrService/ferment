# Research Prompt: Placing VMs, Hardware and Gateways Across a Few Machines I Own

## Context

I've built the same pattern three or four times now, and I'm about to build it
again across more machines. Before I do, I want to know whether a ready-made
tool or a named concept already covers it, so I adopt that rather than grow a
private one. If nothing fits, I want to know which pieces to borrow, and how to
factor what I have into a common library.

### The pattern, as built so far

1. **Curated-API remote control.** A FastAPI server plus a stateless CLI
   generated from one registry of typed "capabilities". An agent (or I) can do
   only what the API exposes. Each new capability is a commit, so git history
   records how the machine's powers grew, and `--help` lists all of them. There
   are no passthroughs: inputs that reach a subprocess are validated ids, enums
   or shas.
2. **Bootstrap once over ssh, then HTTP only.** A `bootstrap` command is the one
   time the dev machine uses ssh. It pushes the repo to a bare repo on the dev
   machine, creates a read-only deploy key on the target and authorizes it here
   (restricted to `git-upload-pack`), installs an API token, and runs an
   idempotent list of steps from the repo itself (`ok` / `changed` / `manual` /
   `failed`; re-run until all ok). After that, a root "applier" on the target
   fetches any sha on `main` and applies it (`darwin-rebuild switch`), with a
   health-gated rollback. Infra and workload each run as a process-compose
   instance.
3. **atismevu** (a drum appliance): a Mac mini with a USB-MIDI drum kit and a
   USB audio interface (Focusrite Scarlett), under nix-darwin. Processes that
   touch CoreMIDI/CoreAudio must run in the logged-in GUI session, which needs
   auto-login and TCC grants. Everything is on one box, and the hardware is the
   point.
4. **ferment** (a library) and its corp-specific implementation: a
   "fermenter" runs a multi-service stack (Java services, MySQL, Airflow) on
   **VMs whose system clock I set to a fake date**, so a month of batch
   processing can be replayed and repeated with different service versions.
   - It has to be VMs, not containers. Containers share the kernel's wall
     clock, Linux time namespaces don't offset `CLOCK_REALTIME`, and
     libfaketime made every idle JVM burn two cores. So a fermenter is two VMs:
     a **control VM** on real time (API server, builds, history in Dolt,
     observability) and a **workload VM** at the fake date (the services, with
     no outbound TLS).
   - VMs are Lima (Apple Virtualization.framework) on Apple Silicon macOS
     hosts. Lima can run on another machine over ssh, and its VMs are reached
     by ProxyJump.
   - **Attach:** a client runs `attach`, which holds ssh `-L` forwards to the
     fermenter's API and `-R` reverse forwards that relay things only it can
     reach. The client also heartbeats a session and claims queued requests,
     such as "fetch this repo", "relay this mirror", or (planned) "pull this
     prod data". The server knows what each attached session serves.

### The topology I'm moving to (and why it should be configurable)

| Role | Today's machine | Notes |
|---|---|---|
| **Workstation**: editor, coding agent, CLIs, git | Mac desktop | Off the corporate VPN |
| **Gateway**: the only machine on the corporate VPN; serves a package mirror, private git hosting and a read-only prod-data service | Work laptop | **Cannot accept inbound connections** (no Remote Login); can ssh out. Lid closes, VPN drops; it attaches only when needed |
| **Workhorse**: VM host, shared build caches, data dumps, heavy compute | Mac mini (M4, 16 GiB, ~200 GB free), always on | Will be bootstrapped like atismevu |

These roles must be **placeable**. All three could be one laptop, or they could
be spread as above, or later the workhorse could be a cloud VM. Some rules
hold in every placement:
- **Nothing on the gateway is needed while a long run (days) is going.** VPN
  needs are front-loaded.
- **Work that needs a capability waits for it instead of failing.** For
  example, a build step that needs the mirror holds, says "waiting for someone
  on the VPN to attach", and resumes when a session that serves it appears.
- Bulk prod data flows gateway → workhorse directly, never through the
  workstation.
- Day-to-day control is HTTP. ssh is for bootstrap (and, today, tunnels).

### Where it's headed

- **Hardware attached to a machine becomes a placeable resource**: MIDI
  devices, audio interfaces, and eventually robotics (serial and USB
  controllers, cameras, motor drivers). A workload should be able to say "I
  need the drum kit" and run where the kit is, or have the kit reach it.
- Like ferment (generic) versus its corp implementation, I'd like one day to
  extract "bootstrap a machine, then control it over a curated HTTP API, with
  roles placed across machines" into a common library that atismevu and
  ferment both use.

### Fixed decisions (challenge only with a strong reason)

- **VMs, not containers**, wherever clock control matters. Other workloads may
  be plain processes.
- Hosts are **Apple Silicon Macs under nix-darwin** for now. Linux or cloud
  hosts come later, so the answer shouldn't assume macOS forever.
- Nix flakes. Every machine change is a commit applied by sha (I need
  "apply sha X" for bisecting, not just "follow the branch head").
- A small number of machines that I own (2–5), on a home LAN, plus one
  intermittent corporate-VPN laptop. This isn't a fleet.
- Agents run only on the workstation. Other machines expose curated APIs.

---

## Questions

For each area, cover: (1) the established tools, projects or named patterns;
(2) how well each matches my constraints, and **specifically whether it works
with macOS hosts and Apple Silicon** (many don't); (3) adopt, borrow ideas from,
or ignore; (4) a concrete recommendation.

### 1. Is there a wheel?

- Does anything already do "a few heterogeneous personal machines, each
  declaring what it has (VM capacity, attached hardware, network reach), with
  workloads placed by those capabilities and controlled through a typed API"?
  Evaluate at least: **HashiCorp Nomad** (macOS clients, the `qemu` and
  `raw_exec` drivers, device plugins, constraints), **Kubernetes + KubeVirt**
  (and k3s and device plugins / Akri; I'm wary of the weight), **Incus/LXD**
  (VMs plus USB passthrough, clustering), **Proxmox**, **libvirt**, **Clan
  (clan.lol)**, **microvm.nix**, **Balena / Mender / Fleet-style IoT
  managers**, **Tart / Orka / UTM** on macOS, and **ROS 2** (for the robotics
  end). Where does each break against my constraints: macOS hosts, VMs with a
  settable clock, an intermittent gateway that can't be dialed into, and
  apply-by-sha?
- Is "a server queues requests that only some intermittently attached clients
  can fulfil; clients claim them by capability; work blocks until a capable
  client appears" a named pattern? Compare it with self-hosted CI runners and
  labels (GitHub Actions, Buildkite agents), Temporal / Celery task-queue
  routing, Nomad constraints, NATS leaf nodes, and reverse-tunnel agents.
  Which existing library implements it well enough to adopt?

### 2. Bootstrap-then-API

- Prior art for "ssh exactly once, then the machine is controlled only through
  a curated API and a pull-based applier": nixos-anywhere, cloud-init /
  Ignition, Clan's machine setup, comin, Cachix Deploy agents, Salt minions,
  Ansible-pull. Do any support nix-darwin targets? Do any keep "the target
  only applies shas it fetched itself" as a security property?
- Capability-limited remote control: is there a framework that turns typed
  handlers into a REST API plus CLI plus MCP tools, with risk classes and an
  audit log, so I don't maintain my own registry? Compare StackStorm,
  Rundeck, Salt API, Cockpit, and FastAPI-plus-generator setups.

### 3. Connectivity between roles

- Today: ssh `-L` and `-R` forwards, ProxyJump through the VM host, and a
  laptop that dials out. Compare that with WireGuard / Tailscale / Headscale /
  NetBird, frp / rathole / inlets / chisel, and a message bus (NATS). The key
  case is the gateway. It can't accept connections and is often absent, yet
  while present it must expose VPN-only services to VMs on another machine.
  Which option makes "the gateway attached" a first-class, observable state
  without opening the corporate laptop to inbound traffic? (Assume corporate
  policy may forbid installing a mesh VPN on the work laptop. What works with
  only outbound ssh or HTTPS?)
- Moving large data (tens of GB of database dumps) gateway → workhorse:
  resumable, content-addressed transfer over whichever channel is chosen
  (rsync, zstd plus HTTP range requests, restic/kopia-style chunk stores, nix
  copy).

### 4. Hardware as a placeable resource

- On **Apple Silicon macOS hosts**, what can reach a VM? Cover
  Virtualization.framework's USB support (what does and doesn't pass
  through), QEMU/HVF with libusb host passthrough, **USB/IP**, and
  network-native alternatives (RTP-MIDI / Network MIDI 2.0, AVB/Dante/AES67
  for audio, ROS 2 DDS for robots). What does it cost in latency and
  reliability?
- Or should hardware stay with its host and be **exposed as a service** (the
  atismevu approach: a process in the GUI session owns the device and serves it
  over HTTP/WebSocket)? When is each right? How do Kubernetes device plugins,
  Nomad device plugins, or Akri model it, and is that model worth borrowing
  even without the orchestrator?
- macOS specifics: devices that need the GUI session and TCC grants, and
  whether a nix-built service can own them reliably across rebuilds.

### 5. VMs with controlled time

- Confirm or refute: on a shared kernel there's no way to give one process
  tree its own `CLOCK_REALTIME`, so a VM (or a separate kernel such as gVisor
  or a unikernel) is required. Are gVisor, Kata Containers, Firecracker or
  Cloud Hypervisor lighter ways to get a separately settable wall clock? Which
  of them run on Apple Silicon macOS hosts?
- Any prior art for "replay a business calendar on a fake system clock across
  a multi-service stack" (time-travel testing, deterministic simulation
  testing à la Antithesis / FoundationDB, Shadow): ideas worth taking?

### 6. Shaping a common library

- Given the overlap between atismevu (hardware appliance, nix-darwin, applier)
  and ferment (VM provider, clock, attach and relays, request queue, builds),
  what's the right seam for a shared library: a role/placement model, a
  capability registry, a bootstrap/applier kit, an attach/relay transport?
  Name existing libraries that already own each piece, so the shared library
  stays thin.
- How do comparable projects separate generic "platform" code from
  host-specific facts (which machine has the kit, which has the VPN, how much
  RAM)? Cover nixos-facter, Clan inventories, and Nomad node metadata.

### 7. BEAM / OTP for the control plane

I like Erlang, Elixir and Gleam, so weigh this honestly rather than in their
favor. The coordination problem (sessions that come and go, work that waits
for a capable session, supervision) looks like what OTP was built for. The
rest of the system doesn't: the workload is Java, MySQL and Airflow, hardware
access lives in the macOS GUI session (Rust or Python), and ferment is an
existing Python codebase.

- Which parts, if any, would BEAM do clearly better than Python plus systemd
  and process-compose? Examples: process-per-session with monitors instead of
  heartbeat timeouts, `Registry` / `:pg` for "who serves what", a step blocked
  until a capable session registers, supervision trees, and Phoenix LiveView
  for the live board.
- **Distributed Erlang across trust boundaries.** Any connected node can run
  arbitrary code on any other, and nodes expect to dial each other (EPMD,
  dynamic ports). How does that square with a curated-API, no-passthrough
  design and a gateway that can't accept inbound connections? Are there
  workable middle grounds, such as TLS distribution over a tunnel, Partisan,
  hidden nodes, or one node per machine speaking only HTTP between them?
- **Hot code loading in practice.** How often do production Elixir and Erlang
  shops actually use relups and appups, rather than rolling restarts? Does
  hot loading fit, or fight, nix's model of building the whole thing and
  switching to it?
- **Durability over days.** Process state is in memory. What do people pair
  with OTP for long-running, restart-surviving work (Oban, Temporal's Elixir
  SDK, Commanded / event sourcing)? Is that pairing better than a durable
  workflow engine used from Python directly?
- **Nix and macOS.** How well do Elixir and Gleam releases package with nix
  (mix2nix, `beamPackages`, deps_nix), and can a BEAM app own CoreMIDI or
  CoreAudio devices in the macOS GUI session, or is a Rust/Python sidecar
  always needed?
- **Gleam specifically.** How mature are `gleam_otp` and its actor model
  compared with Elixir's OTP, and how well do coding agents (which write much
  of this code) handle each language?

---

## Deliverable

A report that ends with:

- **A verdict:** is there an existing system I should adopt wholesale (and what
  I'd give up), or is this genuinely a build, using which components?
- For each role (workstation, gateway, workhorse, hardware host), what runs
  there in the recommended design, and how the roles find and reach each
  other, both when they share a machine and when they're split.
- The **gateway-absent** story: what keeps running, what waits, and how waiting
  is shown.
- A **BEAM verdict:** rewrite the control plane on it, use it for one piece
  (say which), or borrow only the OTP vocabulary and patterns.
- A short list of **names and concepts to keep in mind**, even if not adopted,
  so my own design uses the vocabulary others will recognize.
- Flag anything that macOS hosts or Apple Silicon make impractical, and what
  the cheapest escape hatch is (for example, a Linux host for the VMs that need
  USB passthrough).
