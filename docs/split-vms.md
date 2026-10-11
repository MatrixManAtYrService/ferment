# Design: a fermenter is two VMs

Status: proposed (2026-10-11). Nothing here is built yet.

## Problem

A fermenter runs its workload at a fake date by setting the VM's system clock
(`ferment.server.clock`). That works well for the workload: the kernel, file
times, MySQL, the JVM and Go all agree, at no cost. libfaketime, the earlier
approach, made idle JVMs burn two cores, so we don't go back to shims.

But everything else on the VM shares that clock, and some of it can't run at
a fake date:

- **TLS.** Builds (Maven, uv, a package mirror reached through the relay) and
  `deploy` (nix fetching from cache.nixos.org) check certificate validity
  against the VM's clock. Set the date outside a cert's window and builds and
  deploys fail. Mirror certs often last about a year, so even a few months of
  offset can do it.
- **Observability.** Prometheus and Loki stamp data with their own clock and
  reject samples that go backwards or too far forwards. `restore` sets the
  clock back by design, so a log and metrics stack sharing the workload's clock
  breaks at the first restore.
- **Real time.** `real_now()` (event logs, checkpoint records, the board) is
  the guest clock minus a recorded offset. Nothing syncs the guest clock (that
  would undo the fake date), so it drifts and stops while the host sleeps.
  `GuestClock.observe` corrects it from attached clients' heartbeats, but with
  nobody attached it goes stale, and every date change builds on the previous
  error.

Neither Linux time namespaces nor containers help. Time namespaces offset only
`CLOCK_MONOTONIC` and `CLOCK_BOOTTIME`, not the wall clock, and containers
share the kernel's clock. Running two different wall clocks takes two kernels.

## Proposal

A fermenter is a pair of VMs:

```
client (ebb-ferment / fmnt / attach)
   │  ssh: -L forwards, -R relays (unchanged)
   ▼
control VM — real time (NTP)                    workload VM — fake time
  ferment server (the API clients use)  ──────►   workload agent (small HTTP API)
  builds (Maven, uv, nix)               private   process-compose: MySQL, services,
  Dolt archive, server SQLite           network     Airflow, ...
  Loki, Prometheus, Grafana                       system clock set by the agent
  relay endpoints, socat port proxies             no outbound TLS, no internet
```

**The control VM** is what "the fermenter" means to clients. It runs the
ferment server with the API it has today: sessions, heartbeats, broadcasts,
source requests, relays, builds, checkpoints, the board. Its clock is real and
NTP-synced, so `real_now()` is just `now()`.

**The workload VM** runs only the workload, plus a small agent that the control
VM drives. It never makes outbound TLS connections and never reaches the
internet. Its software arrives from the control VM (`nix copy`), and anything
the services call out to (an egress proxy, mocks) is plain HTTP to the control
VM or runs locally on the workload VM. That's what makes any date safe.

### Why one server, not two

The CLI could talk to a server on each VM and route each request to the right
one. That would push the split onto every client, and that includes clients
already in the field: the machine that stays attached to relay a VPN-only
mirror runs whatever code it last pulled. With one server on the control VM,
the CLI, attach and the server's HTTP API stay as they are. The workload VM's
agent is an implementation detail behind the control server.

The agent is the same `ferment` package with a different entry point
(`ferment-agent`), so both sides share models and there's one thing to deploy.

## Who does what

| Concern | Today (one VM) | Split |
|---|---|---|
| Fake now | guest clock | workload VM's clock, read through the agent |
| Real now | guest clock − offset, corrected by client heartbeats | control VM's clock (NTP) |
| Setting the date | `sudo date` in the server | agent: `PUT /clock` |
| After a workload VM reboot | `GuestClock.ensure` re-applies the offset | control keeps the offset to its real clock and tells the agent: date = real now + offset |
| process-compose | server's `WorkloadSupervisor` | agent; control sends the rendered config |
| Builds | server, real time, as systemd units | unchanged, on the control VM |
| Artifacts to run | path on the same disk | control pushes them to the agent (content-addressed; kept like builds) |
| Live database | MySQL on loopback | MySQL on the workload VM, reachable from the control VM over the private network |
| Checkpoints | archive starts an offline mysqld locally, dumps into Dolt | agent starts the offline mysqld on the private interface; control dumps into Dolt with `--date` set to the fake time, so `dolt log` still shows fake dates |
| Process logs | `logs/<name>.log` on the VM | written on the workload VM; control pulls them from the agent by offset (as `fmnt process-log` does now) |
| Loki ingestion | n/a | control stamps lines with real receive time and adds `fake_time` as a field. Not Alloy on the workload VM: it would stamp lines with the fake clock, and Loki would reject them. |
| Metrics | n/a | Prometheus on control scrapes the workload VM. Scrape time is Prometheus's clock, so it's real time automatically. |
| Client ports (`attach -L`) | VM loopback | control runs socat port proxies to the workload VM (the same trick as relays), so an older attach's forwards keep working |
| Relays (`-R`) | VM | control VM only; the workload doesn't build, so it needs no mirror |
| Delete-me timer | in-guest `--on-active` timer (monotonic, so clock changes don't move it) | the same timer armed on both VMs; the provider records one deadline for the pair |
| `fermenter ssh` | the VM | control by default; `--workload` jumps through control |

### Provider changes

`Provider.create` makes both VMs and a private network between them. On Lima,
that's the `user-v2` network, which supports VM-to-VM traffic (still to be
verified with vz). On GCP it would be one VPC with no external IP on the
workload instance, and the control instance as its bastion. `ssh_target(name)`
returns the control VM. A new `workload_target(name)` jumps through it. Only
the workload VM gets `GUEST_CLOCK_FREE`; the control VM keeps timesyncd.

Sizing on a 24 GiB Mac: about 4 GiB for control (a Maven build plus
Loki/Prometheus/Grafana) and 8 GiB for the workload. That still fits one
fermenter at a time, as today.

## What gets simpler

- `GuestClock`'s offset bookkeeping, `observe` and the heartbeat time field go
  away. Real time comes from NTP, and fake time is a clock reading.
- Builds, deploys and nix fetches never see a fake date, at any offset.
- The observability stack can be the standard one, with no clock-skew
  workarounds.
- Restore can move the date anywhere, backwards included, without disturbing
  anything that isn't the workload.

## What gets harder

- Two VMs to create, boot, keep alive and delete, and twice the provider
  surface for things that go wrong. On Lima vz, the vsock ssh quirks (the
  ClientAlive settings, authorized_keys rewrites) apply to both.
- Moving artifacts between the VMs. Jars run to hundreds of MB, so push only
  what changed and keep pushed artifacts by hash.
- Checkpoints span the network: control stops the workload, the agent exposes
  the offline database, control dumps it, sets the date and restarts. Any step
  failing has to leave the workload running, as `set_time` does today.
- The agent API becomes a compatibility surface. But both sides deploy
  together from one checkout, so it can change freely as long as it's
  versioned with the control server.

## Alternatives considered

- **One VM, with client time as the real-time reference** (`observe`, now
  implemented). This fixes `real_now()` while a client is attached, but not
  TLS or observability.
- **One VM, builds on the client.** Clients differ in OS and architecture, and
  may be off-VPN. Builds went onto the VM so they'd come out the same no
  matter who triggered them.
- **Time namespaces or containers.** These can't have their own wall clock
  (see Problem).
- **Two full ferment servers with a routing CLI.** See "Why one server, not
  two".

## Steps

Each step leaves a working fermenter.

1. **Agent on one VM.** Move clock setting, process-compose, process logs and
   the offline database behind `ferment-agent`, and have the server call it
   over localhost. No behavior change. This proves the API.
2. **Second VM.** The provider creates the pair and its private network. Deploy
   copies the runtime to the workload VM, the server talks to the agent over
   the private network, socat proxies cover client ports, and artifacts move
   between the VMs.
3. **Archive across the network.** Dolt stays on control, with commit dates
   set explicitly.
4. **Delete the single-VM clock code.** That's `GuestClock`'s offset and
   `observe`, plus the heartbeat time field (kept as optional input, and
   ignored, so older clients still work).
5. **Observability on control**, ingesting from the agent as above.

## Open questions

- Does `user-v2` on Lima vz give stable addresses between VMs across reboots,
  or do we need names via `/etc/hosts`?
- Should the workload VM's clock be resynced to control's (plus the offset)
  after the host sleeps? Fake time losing a few minutes while the laptop slept
  is arguably correct, so the proposal is no for now.
- Where do mocks and egress proxies the services call live: on the workload VM
  (no network hop, but then they run at fake time) or on control (real time,
  plain HTTP across)? Probably decided per service: anything that records or
  forwards traffic to the real world goes on control.
