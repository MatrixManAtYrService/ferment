# Research Prompt: MySQL on btrfs Snapshots as a Checkpoint Mechanism

## Context

I run a multi-service stack (Java services, MySQL 8.4, Airflow) on Linux VMs
whose system clock I set to a fake date, so I can replay a month of batch
processing against a copy of production data and repeat it with different
code versions. The workload checkpoints often: every clock change takes a
checkpoint (about 30 per simulated month), and experiments restore a
checkpoint and branch from it (run day 1 with code A, restore, run day 1
with code B). The database is about 22 GB on disk (two schemas, ~200
tables, ~32 M rows), loaded from production dumps.

Checkpoints used to copy every database into Dolt (mysqldump in, `dolt dump`
out). That was minutes for an empty stack and an hour or more at this size,
so I replaced it with filesystem snapshots. Here is what's built and working
today:

- The VM is NixOS (kernel 6.19) under Apple's Virtualization.framework (via
  Lima) on an Apple Silicon Mac. The VM's root disk is ext4 on a virtio
  disk, backed on the host by a sparse file on APFS.
- MySQL's data dir lives on **btrfs in a sparse image file on that ext4**,
  loop-mounted (`mount -o loop,noatime`). The data dir is a symlink to a
  subvolume `live`.
- A checkpoint is `btrfs subvolume snapshot -r live snapshots/<id>`. It takes
  0.2 s on 22 GB, and I take it with MySQL **running** (on the assumption
  that an atomic snapshot is crash-consistent, which InnoDB recovers from).
- A restore stops MySQL, deletes `live`, snapshots `snapshots/<id>` back to
  a writable `live`, and starts MySQL. The swap is instant; the whole
  restore is about 90 s, almost all of it service restarts.
- Checkpoints form a tree (parent pointers; a restore then more work is a
  branch). Nothing is pruned yet.
- Defaults otherwise: no `nodatacow`, no compression, no quotas/qgroups,
  stock InnoDB settings for MySQL 8.4 (binlog on), with `innodb_flush_method`
  left at its default.

I'd like to know whether others have walked this path, what they learned,
and what I'll regret.

## Questions

For each, say what's established (with sources), what's folklore, and what
you'd recommend for my setup.

### 1. Correctness

- Is a btrfs snapshot of a **running** MySQL 8.4 InnoDB data dir reliably
  crash-consistent? Consider the redo log, the doublewrite buffer, undo
  tablespaces, the binlog, `ibtmp1`, and files MySQL keeps outside the data
  dir. Are there known cases of corrupt or unrecoverable snapshots? Does
  `FLUSH TABLES WITH READ LOCK` or `LOCK INSTANCE FOR BACKUP` add anything?
- Are there InnoDB settings that break snapshot consistency, or make it
  safer (`innodb_flush_method`, `innodb_doublewrite`, `innodb_flush_log_at_trx_commit`, `sync_binlog`)?
  Is turning off the doublewrite buffer safe on a CoW filesystem, as is
  sometimes claimed for ZFS?
- After restoring a snapshot, what does MySQL see: crash recovery each time?
  Binlog and GTID state going backwards? Anything that confuses replication
  (none here) or the services (Flyway, JobRunr)?

### 2. Performance and space

- MySQL/InnoDB on btrfs: what do people report about write amplification and
  fragmentation under CoW, especially for a workload that rewrites the same
  pages repeatedly across many snapshots? Does `chattr +C` (nodatacow) on the
  data dir help, and what does it do to snapshots (a snapshot forces one
  CoW)? `autodefrag`? Compression (zstd) for InnoDB pages?
- The layering: btrfs, in a loop device, on ext4, on a virtio disk, on a
  sparse APFS file. What does each layer cost (double journaling, fsync
  semantics through a loop device, `discard`/TRIM so freed space shrinks
  the sparse image again)? Would a dedicated virtual disk for btrfs be
  meaningfully better?
- How do snapshot count and depth scale (hundreds of snapshots, chains 30+
  deep, branching)? What does deleting a snapshot cost (btrfs cleaner)? How
  do I account for per-snapshot space without qgroups, given that qgroups
  are known to slow btrfs down?
- After a restore the buffer pool is cold. Is there a fast way to warm it,
  e.g. `innodb_buffer_pool_dump_at_shutdown` / `load_at_startup`, given the
  dump file is inside the snapshot?

### 3. Alternatives, at the same layer

Compare btrfs with **ZFS datasets** (recordsize=16k for InnoDB, snapshots,
clones, `zfs send`), **LVM thin snapshots**, **XFS reflinks** (`cp --reflink`
per checkpoint), and **overlayfs** layers, on: speed, space, how far
branching and depth go, maturity for MySQL, and NixOS support. Is any of
them clearly better for "many cheap checkpoints of a ~20–100 GB InnoDB data
dir, with branching"?

### 4. Alternatives, at the database layer

MySQL's Clone plugin, Percona XtraBackup (full and incremental), MySQL Shell
dump/load utilities, transportable tablespaces (`FLUSH TABLES ... FOR
EXPORT`): could any of them give cheap checkpoints or restores, or a
complement to snapshots, such as moving a checkpoint to another machine?

### 5. Prior art

Projects or products doing copy-on-write database snapshots or branching,
and what they learned:
- Database Lab Engine (Postgres.ai; ZFS/LVM thin clones);
- Neon and PlanetScale (branching);
- Redgate Clone / Spawn, Delphix;
- Docker's btrfs and zfs storage drivers used for test databases;
- CI systems that snapshot database fixtures;
- Snapper's retention policies.

Do any support MySQL on infrastructure I control? Is any reusable as a
component (an API, retention, a UI for a snapshot tree), or only as a source
of ideas?

### 6. Diffs between checkpoints

I removed Dolt and plan to diff two checkpoints by starting a MySQL on a
writable copy of each, running `CHECKSUM TABLE` to find the tables that
differ, and then doing a keyed row diff of only those. Compare with Percona's
`pt-table-checksum` and `pt-table-sync`, datafold's `data-diff`
(hash-bisection), `mysqldiff`, and reading the binlog between the two
checkpoints (which tables and rows changed, without scanning). Which is
fastest and most trustworthy at ~200 tables and ~30 M rows, where usually
only a few tables change per simulated day?

## Deliverable

A report ending with:

- **Keep or change.** Is btrfs-in-a-loop-image the right base, or should I
  move to ZFS, a dedicated disk, or another layer? What should I change now
  (mount options, InnoDB settings, `nodatacow`, discard), and what later?
- A short list of **failure modes** to monitor (space, fragmentation, a
  slow cleaner, recovery time), with how to detect each.
- A **retention policy** that fits a checkpoint tree where clock-change
  checkpoints pile up but branch points and named ones matter.
- A recommendation for the **diff mechanism**.
- Flag anything specific to NixOS, Apple's Virtualization.framework, or a
  virtio disk backed by a sparse APFS file that changes the answer.
