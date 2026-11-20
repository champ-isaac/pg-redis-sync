# pg-redis-sync: Summary

A short recap of `README.md`, which is the source of truth. Numbers in brackets, such as (2.3), point to the README section with the details. Replica and topology detail is in [docs/topologies.md](docs/topologies.md). This file adds nothing new.

## 1. What it is and where it stands

`pg-redis-sync` streams row-level changes from PostgreSQL into Redis with logical replication (change data capture, CDC). It is planned as a Go library (`pgsync`) plus a binary (`cmd/pg-redis-sync`).

**Status: early.** Config loading and validation for both topologies, connection-string handling, read-source selection (`pgsync/config.go`, `pgsync/source.go`) and the docker-compose setups exist. Replication, decoding, the Redis sink, preflight, the YAML loader and the binary are design only (5).

## 2. Goals and scope

**Goals (2.1).** Keep Redis in sync with chosen tables, well under 1 second end to end. Never lose a committed change. Preserve order (global commit order in v1). Make replay safe. Run on either topology, chosen by config. Usable as a library with a pluggable Redis side.

**Out of scope in v1 (2.2).** DDL replication, several source databases per process, CDC from a replica, automatic failover, Redis Cluster and several Redis targets, secondary indexes and joins (one row is one key), parallel apply, exactly-once across two systems, and transform scripting.

**Assumptions (1).** PostgreSQL 13+ with `pgoutput`. Redis standalone or Sentinel. Go 1.26. Redis is a derived view: PostgreSQL stays the source of truth, and lost Redis data is rebuilt from a snapshot (milestone 6).

## 3. The two topologies (2.3)

`postgres.topology` is `single` (default) or `primary_replica` (one main plus read replicas). Everything else is identical. The CDC stream, slot and publication always use the main only, because it is the authoritative write source with one position to resume from.

**Replica selection.** The first replica in configured order is used if it is reachable, in recovery, streaming from the main (`pg_stat_wal_receiver.status = 'streaming'`) and within `replica_max_lag_bytes` (default 64 MiB). If none qualifies, `read_fallback: primary` (default) reads from the main and `none` returns `ErrNoReadSource`. `pgsync/source.go` applies a 5 s probe timeout, returns `ctx.Err()` on cancellation, redacts the DSN and password in errors, and rejects negative lag. Config validation, the backfill procedure and replica requirements are in `docs/topologies.md`.

**Failover is manual in v1.** The operator points `postgres.dsn` at the new main, removes it from `replicas`, creates the slot and rebuilds. If the rebuild is skipped, the checkpoint no longer matches the cluster and the sync halts.

## 4. Delivery design (3, 2.3, 2.4)

**Pipeline.** A receiver and decoder feed a bounded channel read by the applier. A slow Redis fills the channel and PostgreSQL buffers WAL, while keepalives continue. The sink writes each batch as one `MULTI/EXEC` together with the checkpoint. Only the last batch of a transaction carries a checkpoint, so acks sit on transaction boundaries. Order: **Redis commit first, made durable, PostgreSQL ack second** (3.2).

**Guarantee (3.3).** At-least-once delivery, idempotent state-based writes (`HSET`, `SET`, `DEL` of full row state), and an atomic checkpoint that suppresses most replays. After a restart a transaction at or below the checkpoint is skipped. Redis readers see eventual consistency.

**Design decisions.** Each is one rule:
- The checkpoint hash `sync:{<slot>}:checkpoint` stores `system_id` and `timeline` (from `IDENTIFY_SYSTEM`), which bind it to one PostgreSQL cluster, and a transaction is skipped as "commit LSN ≤ checkpoint" only if both match the server's, otherwise the sync halts (2.4, 3.3).
- A rebuild sets the snapshot marker to `in_progress`, deletes the keys the sync owns, resets the checkpoint (`lsn`, `system_id`, `timeline`) and only then creates the slot and copies, so it cannot resume from a stale position (3.5).
- Idle advance first writes the WAL end to Redis as the checkpoint and only then acknowledges it to PostgreSQL, so the gap check does not halt a healthy sync after a restart (2.3).
- A batch that carries a checkpoint, and an idle-advance checkpoint write, are followed by `WAITAOF 1 <n> <timeout>`, and `flushedLSN` is set only when the reply shows 1 local fsync and `n` replica fsyncs (`redis.durability_replicas`), otherwise the same batch is retried without an ack (3.2).
- Replica lag is the main's current WAL position, read with `pg_current_wal_lsn()` and passed to the prober, minus the replica's `pg_last_wal_replay_lsn()`, so a replica that lost its upstream shows growing lag instead of 0, which is not yet in `pgsync/source.go` and is part of milestone 1 (2.3, `docs/topologies.md`).
- Preflight compares each replica's `system_identifier` (`pg_control_system()`) with the main's (`IDENTIFY_SYSTEM`) and fails on a mismatch, because a standby of another cluster with the same database name would otherwise pass (2.3, `docs/topologies.md`).
- After a consumer gets the slot it runs `HINCRBY sync:{<slot>}:checkpoint epoch 1`, and every batch uses `WATCH` and continues only if `epoch` is its own and `lsn` is below the batch's first commit LSN, otherwise the instance halts as "fenced" (3.2).
- The sync owns every key under a table's key prefix (the literal template text before the first placeholder), so preflight requires a non-empty, unique, non-overlapping prefix that clears the control keys, and the default `key_prefix` is `sync:<database>:` (2.4).

`WAITAOF` needs Redis 7.2+ with `appendonly yes`, checked in preflight. `redis.durability: none` skips the wait, at the risk of losing acknowledged data.

## 5. Failures and error classes (3.3, 3.4, 4.4)

README keeps one table for failures and errors, and each case appears once. Transient errors are retried with backoff, forever by default. Permanent errors halt and report, because skipping would silently lose data.

- **Crashes (not errors).** A crash before `EXEC`, or in the middle of a split large transaction, replays from the last ack. A crash after `EXEC` is skipped by the checkpoint.
- **Transient.** Replication connection lost or slot already active (standby behaviour). Redis timeout, reset, `LOADING`, `READONLY`, `TRYAGAIN` or `CLUSTERDOWN`: retry the same batch. Redis `OOM`: retry, flagged, needs an operator. A `WAITAOF` shortfall: not acked, retry the same batch.
- **Permanent (halt, no ack).** A command failing inside `EXEC` (for example `WRONGTYPE`), because Redis does not roll back. A checkpoint behind the slot (Redis lost data), with `on_gap: fail`. A `system_id` or `timeline` mismatch. A fenced instance. An unknown relation, undecodable tuple or unsupported type. A key-template, key-prefix or primary-key change.
- **Fixes.** A gap or a cluster mismatch is fixed by a snapshot rebuild, which is milestone 6 and not built yet.
- **Replica problems** never stop the CDC stream. A down or lagging replica is skipped and the main is used, and only `read_fallback: none` errors. A `system_identifier` mismatch, a `dsn` in recovery and negative lag are permanent (`docs/topologies.md`).
- **Shutdown.** `ctx` cancelled or SIGTERM finishes and acks the in-flight batch and returns `nil`.

## 6. Redis layout (2.4)

- **Keys.** One row is one key, from a per-table template over replica-identity columns. Default `sync:pgredissync:public.users:42`, custom `user:{id}`. Values are escaped so keys cannot collide.
- **Values.** `hash` (default, one field per column) or `json` (milestone 6, needs `REPLICA IDENTITY FULL`). A hash omits NULL columns with `HDEL`.
- **Operations.** INSERT and UPDATE write the new row state. A key change is `DEL` of the old key plus a write of the new one. DELETE is `UNLINK`. TRUNCATE follows `on_truncate` (`scan_delete` by default, which is O(keys)).
- **Filters and DDL.** `include_columns`, `exclude_columns`, `rename` and an optional `ttl`. An added column becomes a new field. A changed primary key or incompatible type halts the consumer.
- **Control keys.** `sync:{<slot>}:checkpoint` (`lsn`, `system_id`, `timeline`, `epoch`, `updated_at`) and `sync:{<slot>}:snapshot` (`in_progress` or `done`).

## 7. Initial load and rebuild (3.5, milestone 6)

The order is fixed so no change is lost. Create the slot with `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT`, copy the tables in a `REPEATABLE READ` transaction that imports the snapshot, then stream from `consistent_point`. With `primary_replica` the copy can run on a replica (`docs/topologies.md`). A crash during the copy restarts the copy. The new checkpoint is written in the same `MULTI` that sets the marker to `done`, and until then streaming is refused.

## 8. Implementation (4)

**Packages (4.1).** `cmd/pg-redis-sync`, `pgsync/` (public: `Syncer`, `Run(ctx)`, `Status()`), `change/` (`Change`, `Batch`), `sink/redis/` (`Sink`, `Mapper`, checkpoint store) and `internal/`.

**Interfaces (4.2, signatures only).** README now shows `Change` and `Batch`, `Preflight`, `Prober` with `ReplicaStatus`, `Sink`, `Mapper` and `CheckpointStore`. `Sink.Apply` is atomic and idempotent, and `CheckpointStore.Load` returns a permanent error on a cluster mismatch. `Stream`, `ReadSource` and `Status` are described in a comment or in prose. A pgx `Prober` arrives with milestone 1.

**Configuration (4.3).** YAML for the binary and `pgsync.Config` for library users, with secrets from environment variables. `pgsync.LoadFromEnv` reads `PG_TOPOLOGY`, `PG_DSN`, `PG_REPLICA_DSNS`, `PG_REPLICA_MAX_LAG_BYTES`, `PG_READ_FALLBACK`, `PG_SLOT`, `PG_PUBLICATION`, `REDIS_ADDR`, `REDIS_PASSWORD` and `REDIS_DB`. The `redis` section includes `durability` and `durability_replicas`.

**Principles and observability (4.4).** A batch is the unit of retry, and never ack before Redis commit. Metrics are prefixed `pg_redis_sync_` (lag, retained WAL, latency, batch and queue sizes, retries, `halted`). `/healthz` is unhealthy when halted or when lag does not move.

## 9. Milestones (5)

0 scaffold and preflight (read-source selection done). 1 replication consumer. 2 decoder. 3 Redis sink with atomic checkpoint. 4 reliability, gap detection and idle advance. 5 binary and operations (v0.1.0). 6 rebuild and JSON mode. 7 only if needed: `pgoutput` v2 streaming, parallel apply, Redis Cluster, CDC from a standby, automated failover.

## 10. Known limits

- No DDL replication. Sequences and large objects are not replicated.
- One active consumer per slot, which gives simple active/standby HA. Failover is manual.
- A table with replica identity `NOTHING`, or no primary key with `DEFAULT`, fails preflight.
- A stuck slot keeps WAL on the primary, so `max_slot_wal_keep_size` and lag alerts are needed.
- Exactly-once is impossible. Without the checkpoint, a replay can briefly show an older value.
- `MULTI/EXEC` does not roll back, so partial effects remain until a later replay succeeds.
- A big TRUNCATE is slow.
- If Redis loses acknowledged data (also possible with `redis.durability: none`), the sync halts and the fix is a rebuild, which is not built yet.
- Replica limits (probe time, backfill from a replica) are in `docs/topologies.md`.
- Locally, removing the replica without `down -v` leaves an inactive slot `replica_1` that keeps WAL until it is dropped. `PG_REPLICA_DSNS` is comma-separated, so percent-encode a comma in a password.

## 11. Open questions and prior art (6, 7)

Open: the production topology and what replicas are for, the Redis topology, `hash` versus `json`, whether all tables have primary keys, whether an initial load is needed on day one, and the latency target and write rate. The closest project is `Trendyol/go-pq-cdc-redis` (license not stated). Here the checkpoint is atomic with the data and compared with the slot at start, the topologies are explicit and the sink is a neutral interface.
