# ferment

A Python library of tools for running a software stack on an ephemeral VM
whose clock you control.

Some systems are best tested as a whole: the services, their databases, the
scheduler, all at once, against realistic data, on a date of your choosing.
Done carefully, a run of such a stack behaves like a pure function of its
inputs (the data, the version of each service, the date), and you can ask
questions like "did the output change when only this one service's version
changed?"

ferment provides the pieces that make that practical:

- **VM providers** (`ferment.vm`): create, list, and delete VMs with a
  delete-me date, and say how to ssh into them. `lima` (local VMs) works
  today; `gcp` is next; `aws` is a placeholder that raises
  `NotImplementedError`.
- **ssh** (`ferment.ssh`): everything after VM creation is plain OpenSSH:
  running commands, pushing source, and tunnels (forward *and* reverse port
  forwards), so a local VM and a cloud VM are driven by the same code.
- **lifetime** (`ferment.lifetime`): deadlines recorded by the provider and
  armed as a timer inside the guest, so a forgotten VM goes away on its own.
- **the guest clock** (`ferment.server.clock`): the workload runs at the
  VM's own system clock, set to whatever date you choose, so every process,
  the kernel, and file timestamps agree by construction. The server
  remembers the offset to real time and puts the date back after a reboot;
  providers keep the guest from syncing its clock (e.g. Lima's guest agent).
- **databases and history** (`ferment.databases`, `ferment.archive`): MySQL
  8.4 runs the workload; each checkpoint is copied into Dolt (one repo per
  database), which gives commits, tags, table/row diffs, and restores. MySQL
  runs, Dolt remembers.
- **process-compose supervision** (`ferment.procompose`): one parent for the
  whole workload, so the whole workload shares one clock.
- **time control and checkpoints** (`ferment.server`): a FastAPI app factory
  whose `/time` endpoint restarts the workload under a new date while the
  server itself stays on real time. Checkpoints span every versioned store
  (one external checkpoint, one internal commit each), are taken
  automatically on clock changes, and restore data and clock together. A
  SQLite file on the VM records them, plus who is attached and who did what.
- **sources and builds** (`ferment.sources`, `ferment.server`): the client
  resolves a branch to a commit, mirrors the repo, and sends the VM a git
  bundle of only the commits it lacks; the VM builds in a real git checkout
  on real time, with `SOURCE_DATE_EPOCH` set to the commit's time, and keeps
  successful builds per (source, commit, command). Each build runs as its own
  systemd unit, so redeploying or restarting the server doesn't kill it: the
  restarted server adopts it and picks up the result. When the client applying
  can't reach a repo either, it queues a source request and any attached
  client that can (`ferment.fulfill`) fetches and sends it.
- **clock checks** (`ferment.clockcheck`): probes for the JVM, Python, a SQL
  database, and coreutils, and a checker that tells you whether they agree.
- **config** (`ferment.config`): `~/.config/ferment/config.toml`, initialized
  on first run.

ferment is a library; a concrete project supplies the workload (which
processes, which data) and its own CLIs. See
[skills-ebb-ferment](https://github.corp.clover.com/clover/skills-ebb-ferment)
for one.

## Why the VM's own clock

The first version faked time per process with libfaketime (LD_PRELOAD). It
can't reach Go programs, needs care to keep processes consistent, and made
every idle JVM spin two CPU cores. A fermenter is a whole VM anyway, so its
system clock is simply set to the workload's date. Everything on the VM
agrees, at no cost. The trade: things on the VM that check certificate dates
(downloading packages) see the fake date too, so build near real time.

## Why MySQL runs and Dolt remembers

Dolt can run a lot of MySQL, but real services find the gaps (WITH ROLLUP,
variables inside stored procedures, some ALTER syntax). So services run on
MySQL, production's engine, and Dolt holds only history: mysqldump in at each
checkpoint, `dolt dump` out on restore. Views and routines Dolt can't hold
are kept as SQL text beside the tables.

## Development

```bash
uv sync
uv run pytest
```
