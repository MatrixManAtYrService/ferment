# Design: a GCP provider

Status (2026-11-05): not started. `get_provider("gcp")` returns
`NotImplementedProvider`. This doc collects what's known so the work can be
picked up later. The project, network and identities it runs in are planned in
ebb-isolated-integration-tests [`docs/PLAN.md`](../../ebb-isolated-integration-tests/docs/PLAN.md),
and that plan already names this provider as the way its runs create and
delete VMs (its §2, "VM lifecycle").

## Problem

A fermenter needs more than a laptop or a corp runner has (MySQL with a
prod-sized buffer pool, the billing services, Airflow). It needs a place to
run on demand, with:

- no internet egress (GCP plan §7: no external IPs, egress denied, ssh only
  through IAP);
- nothing on the VM fetched by the VM itself: everything arrives over the
  attached client's ssh session;
- VMs that are throwaway: labeled, with a delete-me date, and swept.

## Why GCE instances, not Lima on a GCE host

The first idea was to rent a big GCE VM and use it as a Lima host, like
skillet (a local Debian box that rehearses a no-egress host; see
skills-ebb-ferment). That works, but it nests: Lima needs KVM, so the GCE
VM needs nested virtualization (only on some machine types, slower), and the
host itself needs qemu, Lima, nix and a NixOS image installed without egress.

A provider that makes each fermenter VM a GCE instance avoids all of that:

| | Lima on a GCE host | GCE instances (this doc) |
|---|---|---|
| Virtualization | nested (KVM in a VM) | none: the instances are the VMs |
| Host to bootstrap | yes: qemu, Lima, nix, image, with no egress | none |
| Sizing | split one host between fermenters, minus a reserve | each VM gets its own machine type |
| Control ↔ workload network | Lima's user-v2 network | the subnet, with firewall rules |
| Deadline, owner | `ferment.json` in the instance dir | instance labels |
| Caches | 9p mounts of host dirs | a cache disk (see below) |

Everything above the provider stays the same, since it only speaks ssh to the
`SshTarget` a provider returns (`ferment/vm/base.py`): deploy, the server,
relays, outbound, the board, experiments.

## The contract, mapped

| `Provider` method | Lima today | GCP |
|---|---|---|
| `create(spec)` | `limactl create/start` NAME (+ NAME-workload); returns once ssh answers | `instances.insert` for NAME and, for a split fermenter, NAME-workload: no external IP, on the fermenter subnet, the VM service account, per-VM ssh keys in metadata with `block-project-ssh-keys`; wait for ssh through IAP |
| `list()` | instances carrying `ferment.json` | `instances.list` filtered on a ferment label (the `ferment-owner` / `ferment-kind` labels are already set by skills-ebb-ferment) |
| `delete(name)` | `limactl delete` both | delete both instances and their non-cache disks |
| `set_deadline` | `ferment.json` + the in-guest timer (`ferment.lifetime`) | a `delete-after` label (the sweeper reads it) + the same in-guest timer |
| `ssh_target(name)` | ProxyJump through the Lima host | `ProxyCommand=gcloud compute start-iap-tunnel %h 22 --listen-on-stdin …` (or an IAP-aware equivalent the runner can use without gcloud, verify) |
| `authorize_key` | `authorized_keys` via Lima's key | add to the instance's `ssh-keys` metadata |
| `workload_host` / `workload_target` | `lima-NAME-workload.internal`; through the control VM | the workload instance's internal DNS name or IP; through the control VM, unchanged |

Labels hold the deadline as a label value, which allows only lowercase
letters, digits, `-` and `_`: an epoch-seconds string works.

Configuration fits skills-ebb-ferment's multi-host config: a
`[hosts.gcp]` with `provider = "gcp"` and the project, zone, subnet, service
account, machine types and disk sizes (`VMSpec` and `WorkloadSpec`
become machine types plus disk sizes).

## Image and bootstrap

The Lima provider boots a NixOS image (nixos-lima), so nix is there from the
start. For GCE there are two choices:

- **Stock Debian, nix installed over ssh at create.** The GCP plan's decision
  (§2): no images to build, import, scan or maintain. The plan's §9 lists what
  assumes NixOS today: `ferment/relay.py` starts its forwarder from
  `nixpkgs#socat` and edits the NixOS firewall; clock control stops
  timesyncd and Lima's guest agent, where Debian on GCE has chrony and the
  Google guest agent's clock sync. Bootstrap would open the outbound proxy
  (below) first, then run the nix installer through it.
- **A NixOS GCE image built from our flake.** No install step, and the same OS as Lima,
  so nothing NixOS-specific needs a Debian path. But it's an image to keep in
  the project, and the plan's review assumed none. Revisit only if the Debian
  path turns out to be costly.

## Egress: the outbound proxy

The GCP plan (§7) was written when the VM's only way out was relays: fixed
`host:443` endpoints, one per host (Nexus, cache.nixos.org, github.com,
releases.nixos.org). Since then, `ferment/outbound.py` carries any
allowlisted host as an HTTP proxy over the same ssh session (`-R` to the
client's proxy; CONNECT, TLS end to end). The allowlist is the client's, and
each host the VM reached or was refused is reported, which is how a new list
gets written.

skillet showed this works with a fully egress-free fermenter. Its Lima user
is blocked by nftables, and the VMs got nix substitutes, flake inputs,
GitHub, dl.google.com and Python packages through a client on a network that
re-signs TLS (Zscaler: `outbound_ca` adds that root to the VM's CA bundle).
skillet's allowlist is the starting point for GCP's:

```
channels.nixos.org  cache.nixos.org  releases.nixos.org
github.com  api.github.com  codeload.github.com
pypi.org  files.pythonhosted.org  dl.google.com
```

Nexus and corp git stay relays and fetches, as today. The plan's network
section should be updated to say the proxy replaces the per-host relays for
public hosts. Its firewall rules don't change: the VM still opens nothing.

## Caches

`VMSpec.caches` (maven, nix, data, shelf) are host directories on Lima. On
GCP, per the plan (§3):

- a cache disk per control VM, created from the latest cache snapshot,
  mounted at `CACHE_MOUNT`;
- snapshotted back at the end if it changed (a persistent disk is
  read-write on only one VM, so concurrent fermenters each get a copy);
- the shelf (dataset fragments) in a bucket, through Private Google Access.

Without this every fermenter starts cold through the tunnel: about 1.4 GB of
nix and 435 MB of Maven at last count, more for a prod-sized dataset.

## Checkpoints

`ferment/volume.py` keeps the workload's data on a btrfs image file inside
the VM, and checkpoints are its snapshots. That works unchanged on a GCE
instance. Two things can come later:

- the volume on its own data disk, not an image file, so a disk snapshot of
  a loaded MySQL (plan §8) can seed a later run's data disk;
- moving a checkpoint chain between machines (`btrfs send`, as toaster's was
  imported to skillet) into and out of GCE: over ssh as today, or through a
  bucket.

## Steps

1. **Provider skeleton.** `ferment/vm/gcp.py`: create, list, delete,
   set_deadline, ssh_target through IAP, authorize_key, for a single VM.
   Test in a scratch project or the dev project once it exists.
2. **Debian bootstrap.** The proxy before nix, the nix install through it, a
   relay forwarder that doesn't need socat, chrony and the guest agent's
   clock sync off on the workload VM. skills-ebb-ferment's `bootstrap
   --debian` (written for skillet) is the closest existing code.
3. **Split fermenter.** The workload instance, the control VM's key to it,
   firewall rules for ssh and the agent port.
4. **Caches.** The cache disk and its snapshot cycle, then the shelf bucket.
5. **Sweeper.** Deletes instances and disks past `delete-after`, run on a
   schedule (the plan's workflow), with the in-guest timer as a second line.

## Open questions

- **IAP from the runner without gcloud?** `SshTarget.options` can hold any
  ProxyCommand. Whether the runner has gcloud, or needs a small IAP client,
  decides what that command is.
- **Clock on the workload VM.** Setting the date there needs both chrony
  and the Google guest agent's clock sync off. Check that nothing else
  (the metadata server, the hypervisor's kvm-clock) moves it back.
- **Cache disk or bucket for nix?** A disk snapshot is simplest. A binary
  cache in a bucket would be shared across concurrent runs without copies,
  at the cost of signing.
- **Throughput through IAP** for first-time fetches and checkpoint moves
  (plan §7 marks it verify).
- **Sizing.** skills-ebb-ferment's `docs/measuring-a-fermenter.md` and
  skillet's run (MySQL buffer pool about 62 GiB for the replay) suggest the
  plan's starting numbers (workload 8 vCPU / 32 GB) are low for prod-sized
  replays.
