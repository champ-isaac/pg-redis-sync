# pg-redis-sync: Summary

This is a short recap of `README.md`, which is the source of truth. It restates the design and its known limits. It adds nothing new. Section numbers in brackets, such as (2.3), point to the README section with the details.

## 1. What it is and where it stands

`pg-redis-sync` streams row-level changes from PostgreSQL into Redis using logical replication (change data capture, CDC). It is planned as a Go library (`pgsync`) plus a ready-to-run binary (`cmd/pg-redis-sync`).

**Status: early.** Per the README, these parts exist:
- configuration loading and validation for both PostgreSQL topologies;
- connection-string handling;
- read-source selection (`pgsync/config.go`, `pgsync/source.go`);
- the local docker-compose setups.

Replication, decoding and the Redis sink are design only. So are the preflight checks, the YAML config loader and the binary (milestone 0 lists preflight as open).

## 2. Goals, non-goals and assumptions

**Goals (2.1)**
- Keep Redis in sync with chosen tables, well under 1 second end to end under normal load.
- Never lose a committed change, through crash, restart, network loss or Redis outage.
- Preserve order: per key, and in v1 global commit order.
- Safe to replay: applying a change twice leaves Redis in the same state.
- Simple operations: one binary, one config file, clear metrics and failure modes.
- Usable as a library, with a pluggable Redis side (custom mapper or sink).
- Run on either PostgreSQL topology, chosen by config, without changing the sync behaviour.

**Non-goals (2.2)**
- DDL replication and automatic schema migration.
- Several source databases in one process.
- Streaming the CDC feed from a replica, and automatic failover or promotion handling.
- Redis Cluster, and fan-out to several Redis targets (v1).
- Secondary indexes, joins and derived keys. One row maps to one key.
- Exactly-once across two systems.
- Parallel apply. v1 applies strictly in commit order.
- Transform scripting. Only include, exclude and rename of columns.

**Assumptions (1)**
- One source database per process, on one of two topologies (see section 3).
- Row-level changes only. DDL is not replicated; additive column changes are tolerated.
- Library plus runnable binary. The binary is a thin wrapper (YAML config, signals, metrics).
- PostgreSQL 13+ (14+ recommended) with the built-in `pgoutput` plugin.
- Redis standalone or Sentinel in v1.
- Go 1.26. Module path `github.com/champ-isaac/pg-redis-sync`.
- Planned dependencies: `pgx/v5` and `pglogrepl` (no custom replication protocol), `go-redis/v9`, `gopkg.in/yaml.v3` (strict, unknown keys are errors), `prometheus/client_golang` in `cmd/` only, `testcontainers-go` for tests, stdlib `log/slog`. The `pgsync` package stays dependency-free until milestone 1.
- Redis is a derived view. PostgreSQL stays the source of truth. If Redis loses data, it is rebuilt from a snapshot (milestone 6).

## 3. The two topologies (2.3)

The topology is set with `postgres.topology`. Everything else (decoding, Redis mapping, checkpoints, delivery guarantees) is identical.

| | `single` (default) | `primary_replica` |
|---|---|---|
| Instances | one | one main (primary) plus one or more read replicas |
| Config | `dsn` | `dsn` (the main instance) and `replicas`, in order of preference |
| CDC stream (slot, publication, `START_REPLICATION`) | the instance | main only |
| Preflight | the instance | main. The main must not be in recovery. Each replica must be reachable, in recovery and on the same database |
| Read-only queries | the instance | the first healthy replica, else the main (`read_fallback`) |

With only `dsn` set, behaviour is the same as before topologies existed.

**Why the main instance is the only CDC source.**
- It is the authoritative write source. The slot, its `confirmed_flush_lsn` and the publication live on one writable node, so there is one position to resume from.
- Before PostgreSQL 16 a logical slot cannot be created on a standby. From 16 it can, but it needs extra setup and is not failover-safe before 17. Streaming from a standby is a later opt-in (milestone 7).

**Replica selection for read-only queries.** In `primary_replica`, the first replica in configured order is used if it is reachable, in recovery (a server that is not a standby is a wrong DSN or a promoted replica and is never used), and at most `replica_max_lag_bytes` behind the main (default 64 MiB). If none qualifies, `read_fallback: primary` (default) reads from the main instance, and `read_fallback: none` returns `ErrNoReadSource`. The reasons for skipped replicas are returned for logging. Selection runs again each time a read connection is needed.

The implementation in `pgsync/source.go` follows these rules:
- **Per-probe timeout.** Each probe gets its own 5-second timeout, derived from the caller's context. A probe that times out is an ordinary skipped replica. Worst case is the timeout times the number of replicas.
- **Cancellation.** If the caller's context is cancelled or expires, selection returns `ctx.Err()`. It neither skips the replica nor falls back to the main instance.
- **Redaction.** Probe error text is stripped of the DSN and password before it is stored in the skip reasons.
- **Negative lag.** A prober that reports negative lag makes selection fail with an error (not `ErrNoReadSource`).

**Config validation** runs at startup, before any connection. `replicas` is an error with `single`. `primary_replica` needs at least one replica. Each replica must be a different `host:port` than the main, use the same database name, and appear once. `replica_max_lag_bytes` must be above 0.

**What replicas are for.** Backfill reads (milestone 6), reconciliation and verification, and lag checks. Replicas are never written to and never hold the slot. A replica needs physical streaming replication from the main, `hot_standby = on`, the same major version, and a role that can run `pg_is_in_recovery()` and `pg_last_wal_replay_lsn()`. `hot_standby_feedback = on` is recommended for long backfill reads.

**Failover is manual in v1.** If the main is lost and a replica is promoted, the logical slot is not on it (only PostgreSQL 17 failover slots carry it over). The operator points `postgres.dsn` at the new main, removes it from `replicas`, creates the slot and re-snapshots. If `dsn` points at a server in recovery, preflight fails with a clear error.

## 4. PostgreSQL side (2.3)

- **Mechanism.** Logical replication over the streaming replication protocol, with `pgoutput`.
- **Server requirements** (checked at startup): `wal_level = logical`; at least one free replication slot and WAL sender; a role with `REPLICATION` and `SELECT` on the synced tables. `max_slot_wal_keep_size` is recommended (for example 10GB) so a dead consumer cannot fill the disk.
- **Publication.** Owned by the user, or created if `create_publication: true`. It publishes insert, update, delete and truncate, with `publish_via_partition_root = true` so partition changes appear under the parent table.
- **Slot.** A persistent named logical slot (default `pg_redis_sync`). PostgreSQL keeps the WAL the slot needs and stores the last confirmed position, `confirmed_flush_lsn`.
- **Streaming.** `START_REPLICATION SLOT <slot> LOGICAL <lsn> (proto_version '1', publication_names '<pub>')`. PostgreSQL sends transactions only after commit and in commit order, so the consumer never sees aborted transactions and never reorders.
- **Resume from the last acknowledged LSN.**
  1. On start the consumer asks for LSN `0/0`, so the server starts at the slot's `confirmed_flush_lsn`.
  2. Standby Status Updates report progress. The LSN is sent only after the Redis write for that transaction is committed.
  3. After a crash PostgreSQL re-sends everything after the last confirmed LSN. Some may already be in Redis, which is harmless (see section 6).
  4. Keepalives are answered quickly, or the server drops the connection (`wal_sender_timeout`, 60 s by default). The receive loop never blocks on Redis.
  5. Idle advance: when everything received is applied and the server's WAL end is ahead (changes on unpublished tables), the WAL end is confirmed, so a quiet table does not hold WAL forever.
- **Replica identity.** `DEFAULT` with a primary key sends the key columns on delete and key-changing update, which is enough. `USING INDEX` is fine. `FULL` sends the whole old row and is needed only for JSON mode with TOAST. `NOTHING`, or no primary key with `DEFAULT`, is a preflight error, because without the key a delete is impossible.

## 5. Redis side (2.4)

- **Keys.** One row is one key, built from a per-table template over primary-key (replica-identity) columns. Default: `sync:public.users:42`. Custom: `user:{id}`, or composite `tenant:{tenant_id}:order:{id}`. Placeholders must be replica-identity columns (checked in preflight). Values are escaped (`:` and `%` percent-encoded) so keys cannot collide.
- **Value types.** `hash` (default, v1): one field per column. `json` (milestone 6): a string holding a JSON object.
- **Change to operation.** All commands of one source transaction go in one `MULTI/EXEC`.

| Change | `hash` | `json` |
|---|---|---|
| INSERT | `HSET` | `SET` |
| UPDATE, key unchanged | `HSET` changed columns; `HDEL` columns that became NULL | `SET` full new row |
| UPDATE, key changed | `DEL` old key + `HSET` new key | `DEL` old key + `SET` new key |
| DELETE | `UNLINK` | `UNLINK` |
| TRUNCATE | per `on_truncate` | same |

- **State-based, not delta-based.** The new row state is always written, never an increment. This makes replay safe.
- **`MULTI/EXEC` is not all-or-nothing.** Redis does not roll back. If `EXEC` succeeds but one command inside fails (for example `WRONGTYPE`), the others still ran. The sink must inspect every command result. Such an error is permanent (see section 8).
- **NULL.** A hash cannot hold NULL, so the field is omitted and `HDEL`-ed. JSON mode writes `null`.
- **TOAST.** Large unchanged columns are omitted in an UPDATE. In `hash` mode those fields are simply not touched. `json` mode needs the full row, so the table needs `REPLICA IDENTITY FULL`, and preflight rejects a `json` table without it.
- **Value encoding.** `hash`: text form, except `bool` as `true`/`false` and `bytea` as base64. `json`: bool, numbers as numbers, `numeric` and `int8` as strings by default, `json/jsonb` embedded, timestamps as RFC 3339 strings, the rest strings. Mapping is by type OID.
- **Filters and TTL.** `include_columns`, `exclude_columns`, `rename`; optional per-table `ttl` applied with `EXPIRE`.
- **TRUNCATE.** `on_truncate`: `scan_delete` (default; `SCAN` the key prefix and `UNLINK`, O(keys)), `ignore` or `fail`.
- **DDL tolerance.** A new `Relation` message refreshes the cache. An added column becomes a new hash field. A changed primary key or an incompatible type change halts the consumer.
- **Checkpoint key.** `sync:{<slot>}:checkpoint`, a hash with `lsn` and `updated_at`, written in the same `MULTI` as the data.

## 6. Architecture and delivery guarantee (3)

**Components (3.1).**

| Component | Responsibility |
|---|---|
| Replication consumer | Owns the replication connection. Creates or verifies the slot, streams, answers keepalives, sends status updates with the latest `flushedLSN`. Own goroutine, never waits on Redis. |
| Change decoder | Turns `pgoutput` messages into typed `Change` values using a relation cache. Handles unchanged-TOAST markers and type OIDs. Marks transaction boundaries. No I/O. |
| Applier | Groups changes into batches (count, bytes or time), calls the sink, retries, then advances `flushedLSN`. The only place that decides when an ack is safe. |
| Redis sink | Maps each `Change` to Redis commands (`Mapper`) and runs them as one `MULTI/EXEC` with the checkpoint write. |
| Checkpoint store | Records the last fully applied commit LSN, atomically with the data in Redis. Read once at startup. |
| Status | Snapshot of pipeline state from `Syncer.Status()`, used by `/healthz`. |
| Source selector | Chooses the server for each connection from the topology. `StreamDSN` is always the main instance; `SelectReadSource` picks a read server. Implemented in `pgsync/source.go`. |

**Concurrency (3.1).** Two goroutines joined by a bounded channel: receiver/decoder to applier. When Redis is slow the channel fills, the decoder stops reading the socket, and PostgreSQL buffers WAL. Keepalives continue, so a slow Redis never causes a replication timeout.

**Batching (3.1).** A batch holds whole transactions where possible (`batch_max_changes`, `batch_max_delay`). A larger transaction is split. Only the last batch of a transaction carries a checkpoint LSN, so the ack always sits on a transaction boundary.

**Per-batch sequence (3.2).** Receive, decode, collect, then `Sink.Apply`: `MULTI`, the mapped commands, the checkpoint `HSET` (only on the last batch of a transaction), `EXEC`. Then `flushedLSN` is set and the receiver sends the status update. **Redis commit first, PostgreSQL ack second.** The reverse order could lose data.

**Delivery guarantee (3.3).** At-least-once delivery with idempotent, state-based writes, plus an atomic checkpoint that suppresses most replays. Exactly-once is impossible because PostgreSQL and Redis share no transaction. At-most-once could lose committed changes. Three layers:
1. **At-least-once:** ack PostgreSQL only after Redis commits. Any crash replays from the last ack.
2. **Idempotent writes:** `HSET`, `SET` and `DEL` of full row state are safe to repeat. No counters or appends.
3. **Atomic checkpoint** (standalone or Sentinel Redis): after a restart the checkpoint is loaded and any transaction with commit LSN at or below it is skipped.

| Failure point | Result |
|---|---|
| Before Redis `EXEC` | Replay from the last ack. |
| After `EXEC`, before PostgreSQL ack | Replay skipped by the checkpoint. |
| Redis unreachable | Retry the same batch with backoff. Keepalives continue. |
| Redis checkpoint behind the slot's `confirmed_flush_lsn` | Redis lost acknowledged data. Gap detected: halt (`on_gap: fail`). Fix is a snapshot rebuild. |
| A command inside `EXEC` fails (`WRONGTYPE`) | Not acked, halt. Partial effects stay and are fixed by the next successful replay once the cause is fixed. |
| Large transaction split over batches, crash in the middle | The whole transaction is replayed. Safe because writes are idempotent. |

Redis readers see **eventual consistency**. A transaction is atomic in Redis only when it fits in one batch. There is no cross-key snapshot isolation.

**Initial load and rebuild (3.5, milestone 6).** Reading all rows and then starting the stream would lose changes made in between, so the order is fixed:
1. Create the slot with the replication-protocol command `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput EXPORT_SNAPSHOT`. It returns `consistent_point` and an exported snapshot. The SQL function `pg_create_logical_replication_slot()` cannot export a snapshot.
2. Copy the tables in a `REPEATABLE READ` transaction that imports the snapshot. With `primary_replica`, copy from a replica instead: create the slot on the main and note `consistent_point`, wait until the replica has replayed past it, then copy on the replica (2.3).
3. Stream from `consistent_point`.

A snapshot row never overwrites a newer change. On the main instance the snapshot equals the state at `consistent_point`. A crash during the copy restarts the copy: the exported snapshot lives only as long as the creating connection, so the slot is dropped and re-created. A Redis marker `sync:{<slot>}:snapshot` (`in_progress` or `done`) blocks streaming while the load is unfinished.

## 7. Implementation (4)

**Package layout (4.1).**
- `cmd/pg-redis-sync`: flags, YAML config, signals, `/healthz`, `/metrics`.
- `pgsync/` (public): `Syncer`, `Run(ctx)`. `config.go` and `source.go` are done.
- `change/` (public): `Change`, `Op`, `Row`, `LSN`. Imports nothing from this repo.
- `sink/redis/` (public): `Sink`, `Mapper` (hash, json), checkpoint store. Imports `change/` only.
- `internal/`: `replication/`, `decode/`, `apply/`, `testutil/`.
- Also `config.example.yaml`, the two compose files, `docker/postgres/pg_hba.conf`, the dev `Dockerfile` (Go 1.26) and `go.mod`.

`pgsync/` is the only package that knows every part. `internal/` is kept for anything the authors want freedom to change.

**Key interfaces (4.2).**
- `change.Change` carries `Op`, `Schema`, `Table`, `Key`, `OldKey` (only when the key changed), `New` (full row; unchanged-TOAST columns absent), `Unchanged`, `Types` (column to type OID), `CommitLSN`, `XID` and `CommitTime`. `change.Batch` has `Changes` and `Checkpoint` (zero unless the batch ends a transaction).
- `internal/replication`: `Stream` (`Next`, `Ack`, `Close`; a fake drives unit tests) and `Preflight` (`Check`).
- `pgsync` (implemented): `Topology`, `Postgres.StreamDSN()`, `Postgres.SelectReadSource(ctx, Prober)`, `Prober.ProbeReplica`, `ReplicaStatus{InRecovery, LagBytes}` and `ReadSource{DSN, Role, Skipped}`. A pgx `Prober` arrives with milestone 1. An implementation must honour its context and never return negative lag.
- `Sink.Apply(ctx, Batch)` must be atomic and idempotent and stores `Checkpoint` with the data. `Mapper.Map(Change)` is a pure function to Redis commands. `CheckpointStore.Load` returns 0 if none; saving is part of `Sink.Apply`.
- `New(cfg, opts...)` with options `WithLogger`, `WithMetrics`, `WithSink`, `WithMapper`; `Run(ctx)`; `Status()` returning `Connected`, `SnapshotRunning`, `LastReceivedLSN`, `LastAppliedLSN`, `PendingChanges`, `LastError` and `Halted`. There is deliberately no `Start`, `Stop` or `Close`: cancelling the context stops gracefully.
- `Metrics` is a small interface. The binary plugs in Prometheus. The library defaults to a no-op.

**Configuration (4.3).** YAML for the binary and a matching `pgsync.Config` for library users. Secrets come from environment variables, never from the file. Sections: `postgres`, `redis`, `sync`, `snapshot`, `retry`, `tables`, `log`, `http`. Everything has a default except `postgres.dsn`, `redis.addr` and `tables`. `primary_replica` also requires `replicas`. Unknown fields are an error.

Today `pgsync.LoadFromEnv` reads these variables:

| Variable | Default |
|---|---|
| `PG_TOPOLOGY` | `single` |
| `PG_DSN` | required |
| `PG_REPLICA_DSNS` (comma-separated; percent-encode a comma in a password) | none; required for `primary_replica`, an error for `single` |
| `PG_REPLICA_MAX_LAG_BYTES` | `67108864` |
| `PG_READ_FALLBACK` | `primary` |
| `PG_SLOT`, `PG_PUBLICATION` | `pg_redis_sync`, `pg_redis_sync_pub` |
| `REDIS_ADDR` (required), `REDIS_PASSWORD`, `REDIS_DB` | |

`PG_DSN` and `REDIS_ADDR` alone still select `single`.

**Local development.** `docker compose up -d --wait` starts the single setup (`postgres` with `wal_level=logical`, `redis`, an `app` container). `docker compose run --rm app go test ./...` runs the unit tests, which need no live databases. For main plus replica, set `COMPOSE_FILE=docker-compose.yaml:docker-compose.replica.yaml`; `app` then runs with `PG_TOPOLOGY=primary_replica`. The replica's healthcheck passes only while its WAL receiver is streaming (`pg_stat_wal_receiver.status = 'streaming'`). Ports on `127.0.0.1`: PostgreSQL 5432, Redis 6379, replica 5433.

**Testing approach (4.5).**
- Unit: decoder against recorded `pgoutput` fixtures; table-driven mapper tests; applier with a fake `Stream` and `Sink`.
- Integration: real PostgreSQL and Redis, against both topologies; in the replica run, stop the replica mid-test and check the sync keeps streaming from the main. Named cases cover the basic operations, key changes, duplicate delivery, a large transaction, slow and down Redis, an initial load during updates and a `WRONGTYPE` halt.
- Fault injection: kill after `EXEC` but before ack, and before `EXEC`; drop the Redis connection mid-batch; restart PostgreSQL. Then assert Redis equals PostgreSQL.
- Soak: sustained writes with periodic restarts.

## 8. Errors, retries and observability (3.4, 4.4)

**Classes.** Transient errors (network, Redis timeout, `LOADING`, `READONLY`, slot busy) are retried with backoff, forever by default. Permanent errors (undecodable message, unknown relation, key-template mismatch, incompatible schema change, a failed command inside `EXEC`) halt and report, because skipping would silently lose data.

| Situation | Class | Behaviour |
|---|---|---|
| Replication connection lost | transient | Reconnect with backoff and resume from the slot position. Unacked work is replayed. |
| Slot already active | transient | Retry with backoff (standby behaviour). |
| Redis timeout, reset, `LOADING`, `READONLY`, `TRYAGAIN`, `CLUSTERDOWN` | transient | Retry the same batch with exponential backoff and jitter. |
| A command inside `EXEC` fails | permanent | Halt without acking. |
| Redis `OOM` | transient, flagged | Retry, log loudly, expose a metric. Needs an operator. |
| Unknown relation, undecodable tuple, unsupported type | permanent | Halt; `Run` returns an error. |
| Key template uses a non-identity column; PK change in `Relation` | permanent | Halt. |
| Redis checkpoint behind slot position | permanent | Halt (`on_gap: fail`). |
| `dsn` points at a server in recovery | permanent | Halt in preflight. |
| A replica is down, lagging or not a standby | transient | Not an error: skip it, use the next, then the main. Only `read_fallback: none` errors. The CDC stream is unaffected. |
| `ctx` cancelled during replica selection | shutdown | `ctx.Err()` returned, no fallback. |
| A prober reports negative lag | permanent (bug) | Error returned, replica not skipped. |
| `ctx` cancelled or SIGTERM | shutdown | Finish and ack the in-flight batch (bounded by a shutdown timeout), close, return `nil`. |

**Principles.** A batch is the unit of retry. Never ack before Redis commit (enforced in the applier). Never skip silently: `skip_and_log` is opt-in. Memory is bounded by channel size and batch limits.

**Observability.** Metrics are prefixed `pg_redis_sync_`: `replication_lag_bytes`, `slot_retained_wal_bytes`, `sync_latency_seconds`, `batch_duration_seconds`, `batch_size`, `queue_size`, `changes_total{op,table}`, `failed_changes_total`, `retries_total{class}`, `last_ack_timestamp` and `halted`. `/healthz` is unhealthy when halted or when lag does not move for too long. Alert on lag, because a stuck slot keeps WAL on the primary.

## 9. Milestones (5)

| # | Content | Done when |
|---|---|---|
| 0 | Scaffold and preflight: `go.mod`, CI, both compose setups, config for both topologies, read-source selection (done), `Preflight` checks (open) | `pg-redis-sync check` reports pass or fail for `wal_level`, slot, publication, replica identity, main not in recovery, replicas are standbys |
| 1 | Replication consumer: connect, slot, `START_REPLICATION`, keepalives, status updates | A change is logged; a restart resumes from the confirmed LSN |
| 2 | Decoder and change model | Fixture tests pass; live changes print as `Change` values |
| 3 | Redis sink, hash mode, atomic checkpoint, ack after commit | End-to-end sync and content-equality test pass |
| 4 | Reliability: retry, reconnect, standby, graceful shutdown, gap detection, idle advance | Fault-injection suite passes |
| 5 | Binary and operations: YAML config, `slog`, Prometheus, `/healthz`, Dockerfile, docs | Runs from a container; v0.1.0 release |
| 6 | Rebuild and JSON mode: initial snapshot for both topologies | Empty Redis rebuilt with no gap or duplicates, also after a kill mid-copy |
| 7 | Only if needed: `pgoutput` v2 streaming, parallel apply, Redis Cluster, dead-letter sink, per-key LSN guard, Redis Streams sink, CDC from a standby, automated failover | Each item justified by a measured need |

## 10. Known limits

All of these are stated in the README.

**Scope**
- No DDL replication. Only additive column changes are tolerated; a changed primary key or incompatible type change halts the consumer (2.4).
- One source database per process (1, 2.2).
- One row maps to one key. No secondary indexes, joins or derived keys. Only include, exclude and rename of columns (2.2).
- v1 applies strictly in commit order, with no parallel apply (2.2).
- Redis Cluster and several Redis targets are not supported in v1 (1, 2.2). The atomic checkpoint needs all keys in one transaction.
- Sequences and large objects are not replicated by logical replication (2.3).

**PostgreSQL side**
- One active consumer per slot. A second instance retries, which gives simple active/standby HA (2.3).
- Logical slots are not carried over on a physical failover before PostgreSQL 17. Failover is manual: re-create the slot and re-snapshot (2.3).
- The CDC stream never comes from a replica in v1. Streaming from a standby is a later opt-in (2.3, milestone 7).
- A table with replica identity `NOTHING`, or no primary key with `DEFAULT`, fails preflight (2.3). A `json` table also needs `REPLICA IDENTITY FULL`, because of TOAST (2.4).
- A stuck slot keeps WAL on the primary, so `max_slot_wal_keep_size` and lag alerts are needed (2.3, 4.4).

**Delivery and Redis side**
- Exactly-once is not possible. Delivery is at-least-once with idempotent writes (2.2, 3.3).
- Redis readers see eventual consistency. A transaction is atomic only when it fits in one batch. No cross-key snapshot isolation (3.3).
- Without the checkpoint, a replay can briefly show an older value for a key. The final state is correct. A per-key LSN guard is a possible later option (3.3).
- `MULTI/EXEC` does not roll back. A failing command such as `WRONGTYPE` halts the sync, and partial effects remain until a later replay succeeds (2.4, 3.3).
- A hash cannot hold NULL, so the field is omitted (2.4).
- Truncating a big table is slow on the Redis side, because `scan_delete` is O(keys) (2.4).
- Redis `OOM` is retried but needs an operator (4.4).
- If Redis loses acknowledged data, the sync halts and the fix is a snapshot rebuild, which is milestone 6 and not built yet (3.3).

**Replicas and initial load**
- Replica selection can take up to the probe timeout times the number of replicas (2.3).
- A backfill copied from a replica may already include changes newer than `consistent_point`, so a key can briefly show an older value (2.3).
- A crash during the initial copy restarts the copy (3.5).
- In the local setup, removing the replica without `down -v` leaves an inactive physical slot `replica_1` on the main instance. It makes the main keep WAL forever until `pg_drop_replication_slot('replica_1')` is run (Local development).
- `PG_REPLICA_DSNS` is split on commas, so a comma inside a password must be percent-encoded (4.3).

**Not built yet**
- Per the README status, replication, decoding, the Redis sink and the YAML loader are design only. Preflight checks are open (Status, 5).

## 11. Open questions (6)

1. Which PostgreSQL topology runs in production, and what are the replicas for: backfill, reconciliation, or CDC from a standby (milestone 7, PostgreSQL 16+)?
2. Which Redis topology: standalone, Sentinel or Cluster? Cluster changes the checkpoint design.
3. `hash` or `json` as the main value type? Currently `hash` is built first.
4. Are all synced tables primary-keyed? Tables without a primary key need `REPLICA IDENTITY FULL` or a unique index.
5. Is an initial load needed on day one? If so, milestone 6 moves earlier.
6. What are the end-to-end latency target and peak write rate? They set batch defaults and decide whether parallel apply is ever needed.

## 12. Prior art and alternatives (7)

The closest existing project is `Trendyol/go-pq-cdc-redis`. Per the README, it uses `pgoutput`, runs an initial load before CDC, supports `string`, `json` and `hash` values and standalone, Sentinel and Cluster Redis, batches through pipelines, and acknowledges only the last LSN after a successful run. Here, the checkpoint is written atomically with the data and compared with the slot at start, the topologies are explicit, and the sink is a neutral interface. Its license is not stated on its GitHub page. Also referenced: `pglogrepl`, `pgx`, and Debezium's PostgreSQL connector for consistent snapshots.

Review suggestions the README did not adopt, and why:
- **Slot as the only checkpoint.** Both are kept: the slot cannot show that Redis lost data.
- **A `Transaction` envelope.** It forces whole transactions into memory. `Change` carries transaction fields and batches can split large transactions.
- **A `Transformer` that returns Redis commands.** `Mapper` lives in `sink/redis`, so the CDC side never sees Redis types.
- **`Pipeline()` for batches.** It is not atomic, and the checkpoint must be atomic with the data.
- **Nine public packages.** Three are public, plus the binary.
- **`Start`, `Stop`, `Close`, `Health`.** Replaced by `Run(ctx)` and `Status()`.
- **Slot creation with the SQL function for the initial load.** It cannot export a snapshot.
- **koanf.** `yaml.v3` plus environment variables is enough.
- **A full `Before` row.** `OldKey` is enough, because no mapping needs the full old row.
