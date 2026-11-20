# Topologies: replica detail

This file holds the replica and topology detail that used to sit in section 2.3 of the [README](../README.md). The README keeps the topology table and the short rules, and links here. Section numbers such as (3.3) refer to the README.

The sync runs against one of two PostgreSQL topologies, chosen with `postgres.topology`: `single` (one instance) or `primary_replica` (one main instance plus one or more read replicas). Decoding, Redis mapping, checkpoints and delivery guarantees are identical in both.

## Why the main instance is the only CDC source

- It is the authoritative write source. The slot, its `confirmed_flush_lsn` and the publication all live on one writable node, so there is one clear position to resume from.
- Before PostgreSQL 16 a logical slot cannot be created on a standby at all. From 16 it can, but it needs extra setup (`hot_standby_feedback`, primary `wal_level=logical`) and the slot is not failover-safe before 17. Streaming from a standby is therefore a later opt-in (milestone 7), not v1.

## Replica selection (read-only queries)

In `primary_replica`, the first replica in configured order is used if it is:
1. reachable,
2. in recovery (a server that is *not* a standby is a wrong DSN or a promoted replica, and is never used),
3. streaming from the main instance (`pg_stat_wal_receiver.status = 'streaming'`), and
4. lagging the main instance by at most `replica_max_lag_bytes` (WAL bytes; default 64 MiB).

**How lag is measured.** Lag is the **main instance's current WAL position minus the replica's replay position** (`pg_last_wal_replay_lsn()`). The caller reads `pg_current_wal_lsn()` on the main instance and passes it to the prober, because a replica cannot know it: a replica that lost its upstream only sees its own received position, so it could report lag 0 while it is arbitrarily stale. With the main position passed in, such a replica shows growing lag and is skipped. Not yet in `pgsync/source.go`: the current `Prober` takes only the replica DSN (see 4.2). Changing it is part of milestone 1.

If none qualifies: `read_fallback: primary` (default) reads from the main instance, `read_fallback: none` returns an error (`ErrNoReadSource`). The reasons replicas were skipped are returned so they can be logged. Selection is re-evaluated each time a read connection is needed, so a recovered replica is picked up again.

Rules that the implementation (`pgsync/source.go`) follows:
- **Per-probe timeout.** Each probe runs under its own timeout (5 s), derived from the caller's context, so one slow replica cannot stall selection. A probe that times out is an ordinary skipped replica. Worst case is the timeout times the number of replicas.
- **Cancellation is not a replica problem.** If the caller's context is cancelled or expires, selection returns `ctx.Err()`. It does not skip the replica and does not fall back to the main instance.
- **Errors are redacted.** The text of a probe error is stripped of the DSN and the password before it is stored in the skip reasons.
- **Impossible values are bugs.** A prober that reports a negative lag makes selection fail with an error (not `ErrNoReadSource`); it is not treated as a skippable replica.

## Config validation

At startup, before any connection: `replicas` is only allowed with `primary_replica`, and is an error with `single` (it is never silently ignored). `primary_replica` needs at least one replica. Each replica must be a different instance than the main (`host:port`), use the same database name, and appear only once. `replica_max_lag_bytes` must be above 0.

## Cluster identity (preflight)

A matching database name does not prove that a replica belongs to the main instance. Preflight reads `system_identifier` from each replica (`SELECT system_identifier FROM pg_control_system()`) and from the main instance (`IDENTIFY_SYSTEM`, the same value as for the checkpoint, see 2.4). If they differ, preflight fails with a clear error naming the replica (without its DSN password). A physical replica always has the same identifier as its main. Without this check, a standby of another cluster would pass validation, and the replay comparison in the backfill procedure below would be meaningless.

## What replicas are for

- **Backfill reads (milestone 6).** Copying whole tables loads the database, so it can run on a replica. The slot's exported snapshot only exists on the main instance, so a replica cannot use it. The safe procedure is:
  1. create the slot on the main instance and note its `consistent_point` LSN,
  2. wait until the replica has replayed past that LSN (`pg_last_wal_replay_lsn() >= consistent_point`; the LSNs are only comparable because preflight checked the `system_identifier`),
  3. copy the tables in a `REPEATABLE READ` transaction on the replica,
  4. stream from `consistent_point`.

  Rows read in step 3 may already include changes newer than `consistent_point`. The stream then replays those older changes on top. Writes are state-based and applied in commit order, so Redis still ends at the correct state; a key can briefly show an older value on the way (the same caveat as in 3.3).
- **Reconciliation and verification** (compare Redis against PostgreSQL) and **lag checks**.
- Replicas are **never written to** and never hold the slot.

## Requirements for replicas

Physical streaming replication from the main instance, `hot_standby = on`, the same major version, and a role that can run `pg_is_in_recovery()` and `pg_last_wal_replay_lsn()`. `hot_standby_feedback = on` is recommended for long backfill reads, so replay conflicts do not cancel them.

## Failover and promotion (not automated in v1)

If the main instance is lost and a replica is promoted, the logical slot is not on it (it is only carried over with PostgreSQL 17 failover slots). The operator points `postgres.dsn` at the new main instance, removes it from `replicas`, creates the slot and re-snapshots (milestone 6). The rebuild clears the keys the sync owns and the old checkpoint (3.5), so LSNs of the new main instance are never compared with a position recorded on the old one. If you skip the rebuild, the checkpoint's cluster identity no longer matches and the sync halts (3.3). If `dsn` points at a server that is in recovery, preflight fails with a clear error instead of trying to stream from it.
