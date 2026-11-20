# pg-redis-sync

Stream row-level changes from PostgreSQL into Redis, using logical replication (CDC).
Ships as a Go library (`pgsync`) and a ready-to-run binary (`cmd/pg-redis-sync`).

> **Status: early.** Implemented so far: configuration loading and validation for both PostgreSQL topologies, connection-string handling, read-source selection (`pgsync/`), and the local docker-compose setups. Replication, decoding and the Redis sink are still design only. This README is the design, architecture and plan. Replica and topology detail lives in [docs/topologies.md](docs/topologies.md).

## Contents

[1 Assumptions](#1-assumptions-to-confirm) · [2 Design](#2-design) · [3 Architecture](#3-system-architecture) · [4 Implementation](#4-implementation) · [5 Milestones](#5-milestones) · [6 Open questions](#6-open-questions) · [7 Prior art](#7-prior-art-and-alternatives-considered)

---

## 1. Assumptions to confirm

| Assumption | Used as | If wrong |
|---|---|---|
| One source database per process, on one of two **topologies** chosen by config: `single` (one PostgreSQL instance) or `primary_replica` (one main instance plus read replicas). See 2.3. | One slot, one publication, one replication connection per process, on the main instance. | Run several processes (one config each). |
| Row-level changes only, no DDL | `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`. We only *tolerate* additive column changes (2.4). | Out of scope. |
| Library plus runnable binary | `pgsync` holds all logic. `cmd/pg-redis-sync` is a thin wrapper. | Library-only: drop `cmd/`. Binary-only: all `internal/`. |

Further assumptions (please confirm or correct):

- **PostgreSQL 13+** (14+ recommended), built-in `pgoutput` plugin (no server extension needed).
- **Redis standalone or Sentinel** in v1. Redis Cluster is deferred (the atomic checkpoint needs all keys in one transaction).
- **Go 1.26.** Planned dependencies: `pgx/v5`, `pglogrepl`, `go-redis/v9`, `yaml.v3`, `prometheus/client_golang` (in `cmd/` only), `testcontainers-go`, stdlib `log/slog`.
- **Module path** `github.com/champ-isaac/pg-redis-sync`. Local dev credentials (not secrets): user `sync`, database `pgredissync`.
- **Redis is a derived view.** Postgres stays the source of truth. Lost Redis data is rebuilt from a snapshot (milestone 6). The CDC stream never comes from a replica in v1 (2.3).

### Local development

```sh
docker compose up -d --wait               # postgres (wal_level=logical), redis, app
docker compose run --rm app go test ./... # unit tests, no live databases needed
docker compose down                       # add -v to delete the volumes

export COMPOSE_FILE=docker-compose.yaml:docker-compose.replica.yaml   # main plus one read replica
docker compose up -d --wait               # + postgres-replica (app runs with PG_TOPOLOGY=primary_replica)
unset COMPOSE_FILE                        # back to the single-instance setup
```

The replica's healthcheck passes only while its WAL receiver is streaming (`pg_stat_wal_receiver.status = 'streaming'`). Ports on `127.0.0.1`: Postgres `5432`, Redis `6379`, replica `5433`. To go back from the replica setup to single, run `docker compose down`, then `select pg_drop_replication_slot('replica_1')` on the main: an inactive slot makes it keep WAL forever (`down -v` avoids this).

---

## 2. Design

### 2.1 Goals

- Keep Redis in sync with chosen tables, well under 1 s end to end under normal load.
- **Never lose a committed change** through crash, restart, network loss or Redis outage.
- **Preserve order** (global commit order in v1) and **safe replay**: a change applied twice leaves Redis in the same state.
- Simple operations (one binary, one config file, clear metrics) and usable as a library with a pluggable Redis side.
- Run against a single instance **or** a main with read replicas, chosen by configuration, with the same sync behaviour.

### 2.2 Non-goals

- DDL replication or automatic schema migration; multiple source databases in one process.
- Streaming the CDC feed from a replica, and automatic failover or promotion handling (2.3).
- Redis Cluster and fan-out to several Redis targets (v1).
- Secondary indexes, joins or derived keys. One row maps to one key.
- Exactly-once across two systems. We get *at-least-once delivery with idempotent writes* (3.3).
- Parallel apply (v1 applies strictly in commit order) and transform scripting (only include/exclude/rename of columns).

### 2.3 PostgreSQL side

**Mechanism:** logical replication over the streaming replication protocol, with the `pgoutput` plugin.

**Server requirements** (checked at startup, see `Preflight` in 4.2): `wal_level = logical`; at least 1 free replication slot and WAL sender; a role with `REPLICATION` and `SELECT` on the synced tables; and, recommended, `max_slot_wal_keep_size` (e.g. 10GB) so a dead consumer cannot fill the disk.

**Publication, slot and streaming.** The publication is owned by the user, or created if `create_publication: true`, with `publish_via_partition_root = true` (partition changes appear under the parent table). The slot is a persistent named logical slot (`pg_redis_sync` by default). Postgres keeps the WAL it needs and stores the last confirmed position (`confirmed_flush_lsn`). `START_REPLICATION SLOT <slot> LOGICAL <lsn> (proto_version '1', publication_names '<pub>')` delivers transactions **only after commit, in commit order**, so the consumer never sees aborted transactions and never reorders.

**Resume from the last acknowledged LSN.** *LSN* = log sequence number, a position in the WAL.

1. On start we ask for `START_REPLICATION ... LOGICAL 0/0`. The server then starts at the slot's `confirmed_flush_lsn`. Postgres, not us, is the primary record of progress.
2. We report progress with *Standby Status Update* messages (flush LSN), **only after the Redis write for that transaction is committed** (3.2).
3. After a crash, Postgres re-sends everything after the last confirmed LSN. Some of it may already be in Redis, which is harmless (3.3).
4. Keepalives must be answered quickly or the server drops us (`wal_sender_timeout`, 60 s by default), so the receive loop never blocks on Redis (3.1).
5. Idle advance: if all received transactions are applied and the server's WAL end is ahead (changes on unpublished tables), we confirm the WAL end, otherwise a quiet table makes the slot hold WAL forever. The order is the same as for data: we **first write that WAL end as the checkpoint to Redis, then acknowledge it to Postgres**. Without the Redis write, the slot would be ahead of the checkpoint after a restart and the gap check (3.3) would halt a healthy sync.

**Replica identity.** `DEFAULT` (primary key) sends the key columns on DELETE and key-changing UPDATE, which is enough. `USING INDEX` is fine. `FULL` sends the whole old row and is needed only for JSON mode with TOAST (2.4). `NOTHING`, or no PK with `DEFAULT`, is a **preflight error**, because without the key we cannot delete.

**Other limits (documented, not solved):** one active consumer per slot (a second instance retries, which gives simple active/standby HA); logical slots are not carried over on a physical failover before PostgreSQL 17, so the operator re-creates the slot and re-snapshots (the rebuild clears the old Redis keys and checkpoint first, 3.5); sequences and large objects are not replicated.

#### Topologies

The topology is chosen with `postgres.topology`. Everything outside the table below (decoding, Redis mapping, checkpoints, delivery guarantees) is identical. With only `dsn` set, nothing changes from before topologies existed.

| | `single` (default) | `primary_replica` |
|---|---|---|
| Instances | one | one **main** plus one or more read replicas |
| Config | `dsn` | `dsn` (the main) and `replicas` (in order of preference) |
| CDC stream (slot, publication, `START_REPLICATION`) | the instance | **main only** |
| Preflight | the instance | main (not in recovery). Each replica: reachable, in recovery, same database, same cluster (`system_identifier`) |
| Read-only queries | the instance | the first healthy replica, else the main (`read_fallback`) |

The main is the only CDC source: it is the authoritative write source with one position to resume from. Replica detail is in [docs/topologies.md](docs/topologies.md). The rules that matter here:

- **Replica selection.** The first replica in configured order is used if it is reachable, in recovery, streaming from the main (`pg_stat_wal_receiver.status = 'streaming'`) and at most `replica_max_lag_bytes` behind (default 64 MiB). If none qualifies, `read_fallback: primary` (default) reads from the main and `none` returns `ErrNoReadSource`. `pgsync/source.go` applies a 5 s probe timeout, returns `ctx.Err()` on cancellation, redacts the DSN and password in errors, and rejects negative lag.
- **How lag is measured.** Lag is the main instance's current WAL position minus the replica's replay position (`pg_last_wal_replay_lsn()`). The caller reads `pg_current_wal_lsn()` on the main and passes it to the prober, so a replica that lost its upstream shows growing lag instead of 0. Not yet in `pgsync/source.go`: the current `Prober` takes only the replica DSN (4.2); changing it is part of milestone 1.
- **Cluster identity (preflight).** Preflight compares each replica's `system_identifier` (`pg_control_system()`) with the main's (`IDENTIFY_SYSTEM`, the same value as for the checkpoint, 2.4) and fails on a mismatch, because a standby of another cluster with the same database name would otherwise pass.
- **Failover is manual in v1.** The operator points `postgres.dsn` at the new main, removes it from `replicas`, creates the slot and re-snapshots (milestone 6). The rebuild clears the keys the sync owns and the old checkpoint (3.5). If it is skipped, the checkpoint's cluster identity no longer matches and the sync halts (3.3).

### 2.4 Redis side

**Keys.** One PostgreSQL row = one Redis key, from a per-table template over primary-key (replica-identity) columns:

```
{key_prefix}{schema}.{table}:{pk}            default  →  sync:pgredissync:public.users:42
user:{id}                                    custom template
tenant:{tenant_id}:order:{id}                composite key
```

Placeholders must be replica-identity columns (preflight). Values are escaped (`:` and `%` percent-encoded) so keys cannot collide.

**Key ownership.** The sync owns every key under a table's **key prefix**: the literal text of its template before the first placeholder (for `user:{id}` that is `user:`). `scan_delete` (TRUNCATE) and the rebuild (3.5) delete by that prefix, so it must be safe to delete. Preflight enforces:
- The prefix is **not empty**. A template that starts with a placeholder (`{id}:x`) is rejected, because it would have to scan every key.
- Prefixes are **unique per table** and **do not overlap**: no prefix may equal or start with another table's, or deleting one table's keys would delete the other's.
- A prefix does **not overlap the control keys** (`sync:{<slot>}:checkpoint` and `sync:{<slot>}:snapshot`).
- The default `key_prefix` is `sync:<database>:`, so two databases that share one Redis (several processes, see 1) do not collide on `public.users`. A custom template needs its own namespace for the same reason.
- Glob characters in a prefix (`*`, `?`, `[`, `]`, `\`) are escaped in the `SCAN MATCH` pattern.

Nothing else may write under an owned prefix. Anything there is deleted by a TRUNCATE or a rebuild.

**Value types** (per table, `type:`): `hash` (default, v1) is a `HASH` with one field per column. `json` (milestone 6) is a `STRING` holding a JSON object.

**Change → operation** (all commands for one source transaction go in one `MULTI/EXEC`):

| Change | `hash` mode | `json` mode |
|---|---|---|
| INSERT | `HSET key col val …` | `SET key <json>` |
| UPDATE, key unchanged | `HSET` changed columns; `HDEL` columns that became NULL | `SET key <json>` (full new row) |
| UPDATE, key changed | `DEL oldKey` + `HSET newKey …` | `DEL oldKey` + `SET newKey` |
| DELETE | `UNLINK key` | `UNLINK key` |
| TRUNCATE | per `on_truncate` | same |

Details that matter:

- **`MULTI/EXEC` is atomic in isolation, not all-or-nothing.** Redis does not roll back. If `EXEC` succeeds but one command inside it fails (for example `WRONGTYPE`), the other commands still ran. The sink must inspect every command result. Such an error is **permanent** (3.3), and the checkpoint write in the same `MULTI` is not proof that the batch is correct.
- **Updates are state-based, not delta-based.** We always write the new row state, which is what makes replays safe.
- **NULL.** A hash cannot hold NULL, so we omit the field and `HDEL` it. JSON mode writes `null`.
- **TOAST.** Large unchanged columns are omitted in an UPDATE. `hash` mode does not touch them. `json` mode needs the full row, so the table needs `REPLICA IDENTITY FULL` (preflight rejects it otherwise).
- **Value encoding** is by type OID: text form in `hash` (`bytea` as base64), typed JSON in `json` (`numeric` and `int8` as strings).
- **Filters and TTL.** `include_columns`, `exclude_columns`, `rename`; optional per-table `ttl` (`EXPIRE`).
- **TRUNCATE.** `on_truncate: scan_delete` (default: `SCAN` the key prefix and `UNLINK`, O(keys), so slow for a big table), `ignore`, or `fail`.
- **DDL tolerance.** A fresh `Relation` message refreshes our cache. An *added column* becomes a new hash field. A changed primary key or incompatible type change **halts** the consumer.
- **Checkpoint key.** `sync:{<slot>}:checkpoint` (a hash with `lsn`, `system_id`, `timeline`, `epoch` and `updated_at`), written inside the same `MULTI` as the data (3.2). `epoch` is the write fence (3.2). `system_id` and `timeline` come from `IDENTIFY_SYSTEM` on the replication connection. They bind the checkpoint to one PostgreSQL cluster: an LSN is only comparable with another LSN from the same cluster and timeline (3.3).

---

## 3. System architecture

### 3.1 Components and data flow

- **Replication consumer:** owns the replication connection (slot, streaming, keepalives, Standby Status Updates with the latest `flushedLSN`). Own goroutine, never waits on Redis.
- **Change decoder:** turns `pgoutput` messages into typed `Change` values (relation cache, TOAST markers, type OIDs, transaction boundaries). No I/O.
- **Applier:** groups changes into batches, calls the sink, retries, then advances `flushedLSN`. The only place that decides when an ack is safe.
- **Redis sink:** maps each `Change` to Redis commands (`Mapper`) and runs them as one `MULTI/EXEC` with the checkpoint write.
- **Checkpoint store:** records the last fully applied commit LSN (or the WAL end after an idle advance), plus the cluster's system identifier and timeline, atomically with the data in Redis. Read once at startup.
- **Source selector:** `StreamDSN` is always the main, `SelectReadSource` picks a read server (2.3). Implemented in `pgsync/source.go`.

**Concurrency.** Two goroutines joined by a bounded channel: receiver/decoder → applier. When Redis is slow the channel fills and Postgres buffers WAL, while keepalives continue, so a slow Redis never causes a replication timeout.

**Batching.** A batch holds whole transactions where possible (`batch_max_changes`, `batch_max_delay`). A larger one is split. Only the **last batch of a transaction** carries a checkpoint LSN, so acks sit on transaction boundaries.

### 3.2 Per-batch sequence

```
Sink.Apply(batch):
  WATCH checkpoint; HMGET epoch, lsn         (fence check)
  MULTI
    <mapped Redis commands>
    HSET checkpoint lsn=<commitEndLSN>       (only on txn-final batch)
  EXEC
  WAITAOF 1 <n> <timeout>                    (only on txn-final batch)
flushedLSN = commitEndLSN  →  receiver sends Standby Status Update
```

Order matters: **Redis commit first, made durable, Postgres ack second.** A crash between the two is harmless (3.3). The reverse order could lose data.

**Durability before ack.** `EXEC` only means that Redis applied the commands in memory. Once we ack, Postgres releases the WAL, so a crash that loses the last second of an `appendfsync everysec` AOF (the compose default), or a Sentinel failover to a replica without the write, would lose acknowledged data and force a rebuild (3.3). So a batch that carries a checkpoint, and an idle-advance checkpoint write, are followed by `WAITAOF 1 <n> <timeout>` on the same connection, with `n` = `redis.durability_replicas`. Only when the reply shows at least 1 local fsync and `n` replica fsyncs do we set `flushedLSN`. A short reply or a timeout is transient: do not ack, retry the same batch. Batches without a checkpoint do not wait. `WAITAOF` needs Redis 7.2+ with `appendonly yes` (checked in preflight). `redis.durability: none` skips the wait, at the risk above.

**Write fence.** One consumer holds the slot, but an old instance can outlive its PostgreSQL connection (still connected to Redis) and write a late batch, which would replay older transactions over newer state and move the checkpoint backwards. So:
- **Epoch.** After a consumer gets the slot and before it loads the checkpoint, it runs `HINCRBY sync:{<slot>}:checkpoint epoch 1` and remembers the result.
- **Check on every batch.** The sink does `WATCH` on the checkpoint key and reads `epoch` and `lsn`. It continues only if `epoch` is its own and `lsn` is below the batch's first commit LSN (the skip rule of 3.3), so the checkpoint never moves backwards. Then `MULTI … EXEC`. A nil reply means the key changed: re-read and check again.
- **A failed check is permanent.** The batch is not applied or acked and the instance halts as "fenced". The new owner carries on. The cost is one extra round trip per batch.

### 3.3 Delivery guarantee

**At-least-once delivery with idempotent, state-based writes, plus an atomic checkpoint that suppresses most replays.** Exactly-once is impossible because Postgres and Redis share no transaction. At-most-once can lose committed changes, which is our first goal.

1. **At-least-once.** We ack Postgres only after Redis commits. Any crash replays from the last ack.
2. **Idempotent writes.** `HSET`, `SET`, `DEL` of full row state are safe to repeat.
3. **Atomic checkpoint (standalone/Sentinel Redis).** The checkpoint is written in the same `MULTI` as the data. After a restart we load it and **skip any transaction with commit LSN ≤ checkpoint**, but only if the stored `system_id` and `timeline` equal the ones the server reports now. On a mismatch we do not compare LSNs at all (see the table below). The checkpoint only moves forward, and only for the current epoch (write fence, 3.2). So the common crash case (applied but not acked) causes no replay at all.

**Failures and error classes.** *Transient* errors are retried with backoff, forever by default. *Permanent* errors halt and report, because skipping would silently lose data.

| Situation | Class | Behaviour |
|---|---|---|
| Crash before `EXEC`; after `EXEC` but before the ack; mid-way through a split large transaction | n/a | Before `EXEC` or mid-transaction: replay from the last ack (idempotent). After `EXEC`: the replay is skipped by the checkpoint. |
| Replication connection lost; slot already active (standby behaviour) | transient | Reconnect or retry with backoff, `START_REPLICATION` from the slot position. Unacked work is replayed. |
| Redis timeout, reset, `LOADING`, `READONLY`, `TRYAGAIN`, `CLUSTERDOWN` | transient | Retry the **same batch** with backoff and jitter. Keepalives continue. |
| Redis `OOM` | transient, flagged | Retry, log loudly. Needs an operator. |
| `EXEC` succeeds, but `WAITAOF` shows the write is not durable | transient | Not acked. Retry the same batch (3.2). |
| A command inside `EXEC` fails (`WRONGTYPE`, wrong arguments) | permanent | Not acked, **halt**. Partial effects stay and are fixed by the next successful replay, once the cause is fixed. |
| Redis checkpoint is **behind** the slot's `confirmed_flush_lsn` | permanent | Redis lost acknowledged data (failover, flush, old backup, or `redis.durability: none`). **Gap detected: halt** (`on_gap: fail`). Fix = snapshot rebuild (milestone 6). |
| Checkpoint's `system_id` or `timeline` differs from the server's (new cluster, restore, `pg_upgrade`, promoted replica) | permanent | LSNs of two clusters cannot be compared, and "skip if ≤ checkpoint" could silently drop new transactions. **Halt** (`on_gap` does not apply). Fix = snapshot rebuild, which clears the old checkpoint (3.5). |
| Epoch in Redis is not ours, or `lsn` is not below the batch's first commit LSN (an old instance still writing) | permanent | Batch not applied. **Halt** this instance as "fenced" (3.2). |
| Unknown relation, undecodable tuple, unsupported type | permanent | Halt. `Run` returns an error. |
| Key template uses a non-identity column; PK change in `Relation`; key prefix empty, duplicated or overlapping (2.4) | permanent | Halt, or a preflight error where it can be caught there. |
| Replica problems: down, lagging or not a standby; a different `system_identifier`; `dsn` in recovery; negative lag; `ctx` cancelled during selection | see note | A down or lagging replica is skipped and the main is used (only `read_fallback: none` errors). A `system_identifier` mismatch, a `dsn` in recovery and negative lag are permanent. Cancellation returns `ctx.Err()`. The CDC stream is never affected ([docs/topologies.md](docs/topologies.md)). |
| `ctx` cancelled / SIGTERM | shutdown | Finish and ack the in-flight batch (bounded by a timeout), close, return `nil`. |

Caveat: if a replay happens without the checkpoint, a key can briefly show an *older* value. The final state is correct. A per-key LSN guard is a possible later option. Redis readers see **eventual consistency**: a transaction is atomic only if it fits in one batch.

### 3.4 Error classes

The failure matrix and the error table are one table, in 3.3.

### 3.5 Initial load and rebuild (milestone 6)

An empty Redis needs the existing rows too. Reading all rows and then starting the stream loses changes made in between, so the order is fixed:

1. Create the slot **with the replication-protocol command** `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput EXPORT_SNAPSHOT`. It returns `consistent_point` and an exported snapshot. The SQL function `pg_create_logical_replication_slot()` cannot export a snapshot.
2. Copy the tables in a `REPEATABLE READ` transaction that imports that snapshot (on a replica: the backfill procedure in [docs/topologies.md](docs/topologies.md)).
3. Stream from `consistent_point`. Changes made during the copy arrive here, in commit order.

Rules:
- **A snapshot row never overwrites a newer change.** On a replica the snapshot may be newer, so a key can briefly show an older value.
- **A crash during the copy restarts it.** The exported snapshot lives only as long as the slot-creating connection, so the slot is dropped and re-created.
- **A rebuild starts clean.** The order is: set the marker to `in_progress`, delete the keys the sync owns (the same per-table key prefixes that `scan_delete` uses, see key ownership in 2.4, via `SCAN` and `UNLINK`), reset the checkpoint (`lsn`, `system_id` and `timeline`; the `epoch` stays, so an old writer stays fenced), then create the slot and copy. A rebuild therefore never resumes from a stale position or keeps keys of deleted rows. The new checkpoint (the `consistent_point`, with the current `system_id` and `timeline`) is written in the same `MULTI` that sets the marker to `done`. A crash anywhere before that leaves `in_progress`, so streaming stays refused and the rebuild runs again.
- **Snapshot marker.** Redis holds `sync:{<slot>}:snapshot` (`in_progress` or `done`).. While it says `in_progress`, streaming is refused, so a half-filled Redis is never followed by a checkpoint that claims it is current.

---

## 4. Implementation

### 4.1 Package layout

- `cmd/pg-redis-sync/` (flags, YAML config, signals, `/healthz`, `/metrics`), `pgsync/` (public: `Syncer`, `Run(ctx)`; `config.go` and `source.go` are done), `change/` (public: `Change`, `Op`, `Row`, `LSN`), `sink/redis/` (public: `Sink`, `Mapper`, checkpoint store).
- `internal/`: `replication/` (connection, preflight, slot, receive loop), `decode/`, `apply/` (batching, retry, ack watermark), `testutil/`. Also `config.example.yaml`, both compose files, `Dockerfile`, `go.mod`.

`change/` imports nothing from this repo. `sink/redis` imports `change/` only. `pgsync/` is the only package that knows every part. `internal/` keeps freedom to change.

### 4.2 Key interfaces (signatures only)

```go
// change/ — the neutral data model between decoder and sink.
type Change struct {
    Op Op; Schema, Table string
    Key, OldKey map[string]any // OldKey only when the key changed
    New         map[string]any // full new row; unchanged-TOAST columns are absent
    Unchanged   []string; Types map[string]uint32 // column → type OID
    CommitLSN LSN; XID uint32; CommitTime time.Time
}

type Batch struct {
    Changes    []Change
    Checkpoint LSN // zero unless this batch ends a transaction; then = commit end LSN
}
```

```go
// internal/replication — a fake Stream (Next, Ack, Close) drives unit tests.
type Preflight interface {
    // wal_level, slot, publication, replica identity, key prefixes (2.4), main not
    // in recovery, each replica a standby with the same system_identifier,
    // Redis 7.2+ with appendonly yes unless redis.durability is none.
    Check(ctx context.Context) error
}
```

```go
// pgsync — topology and source selection (implemented).
func (p Postgres) StreamDSN() (string, error) // the main + replication=database
func (p Postgres) SelectReadSource(ctx context.Context, pr Prober) (ReadSource, error)

// Prober inspects one replica (a pgx implementation arrives with milestone 1). It must
// honour ctx (5 s per call) and never return a negative LagBytes. Known gap: it gets no
// main-instance position, so it cannot compute the true lag (2.3). Milestone 1 adds it.
type Prober interface {
    ProbeReplica(ctx context.Context, dsn string) (ReplicaStatus, error)
}

type ReplicaStatus struct {
    InRecovery bool  // false = not a standby: wrong DSN or promoted; never used
    LagBytes   int64 // main WAL position minus replica replay position
}
```

```go
// sink — Redis is the only implementation in v1.
type Sink interface {
    // Atomic and idempotent. Stores b.Checkpoint with the data when it is != 0.
    Apply(ctx context.Context, b change.Batch) error
    Close() error
}

type Mapper interface { Map(c change.Change) ([]Command, error) } // sink/redis, pure

type CheckpointStore interface {
    // 0 if none. A permanent error if the stored system_id or timeline differs
    // from the server's (3.3). Saving is part of Sink.Apply, so there is no Save.
    Load(ctx context.Context) (change.LSN, error)
}
```

Library entry point: `New(cfg, opts...)` (`WithLogger`, `WithMetrics`, `WithSink`, `WithMapper`), `Run(ctx) error` and `Status()` (`Connected`, `SnapshotRunning`, `LastReceivedLSN`, `LastAppliedLSN`, `PendingChanges`, `LastError`, `Halted`). There is deliberately no `Start`/`Stop`/`Close`: cancelling `ctx` stops gracefully. `ReadSource` is `{DSN, Role, Skipped}`. `Metrics` is a small no-op-by-default interface.

### 4.3 Configuration

YAML for the binary, plus a matching `pgsync.Config` for library users. Secrets come from environment variables (`${VAR}`), never from the file.

```yaml
postgres:
  topology: single               # single | primary_replica
  dsn: ${PG_DSN}                 # the main instance; replication=database is added for you
  replicas: []                   # primary_replica only, in order of preference
  replica_max_lag_bytes: 67108864  # skip a replica that trails the main by more (64 MiB)
  read_fallback: primary         # primary | none, when no replica qualifies for a read
  slot: pg_redis_sync
  publication: pg_redis_sync_pub
redis:
  addr: ${REDIS_ADDR}            # or sentinel: {master_name, addrs}
  checkpoint_key: "sync:{pg_redis_sync}:checkpoint"
  durability: aof                # aof | none. aof = WAITAOF before every ack (3.2). none = ack after EXEC only
  durability_replicas: 0         # replicas that must also fsync before ack; 1 with Sentinel (Redis 7.2+)
sync:
  batch_max_changes: 500
  batch_max_bytes: 4MiB
  batch_max_delay: 50ms
  queue_size: 4096               # decoder → applier channel, the backpressure limit
  on_gap: fail                   # fail | ignore (never ignore in production)
snapshot: {mode: never}          # never | initial (milestone 6)
retry: {initial_backoff: 200ms, max_backoff: 30s, jitter: 0.2, on_permanent_error: halt}
tables:
  - {name: public.users, type: hash, key: "user:{id}", exclude_columns: [password_hash], on_truncate: scan_delete}
    # default key: sync:<database>:public.users:{pk}
```

Everything has a default except `postgres.dsn`, `redis.addr` and `tables` (and `replicas` with `primary_replica`). Config is validated at startup, and unknown fields are an error.

**Environment variables** (read by `pgsync.LoadFromEnv` today): `PG_TOPOLOGY` (`single`), `PG_DSN` (required), `PG_REPLICA_DSNS` (comma-separated, so percent-encode a comma in a password; required for `primary_replica`, an error for `single`), `PG_REPLICA_MAX_LAG_BYTES` (`67108864`), `PG_READ_FALLBACK` (`primary`), `PG_SLOT`, `PG_PUBLICATION`, `REDIS_ADDR` (required), `REDIS_PASSWORD`, `REDIS_DB`. `PG_DSN` and `REDIS_ADDR` alone still select `single`.

### 4.4 Error handling and retries

The failure matrix and error table are one table (3.3). Principles:

- **A batch is the unit of retry.** It is idempotent, so retrying it is always safe.
- **Never ack before Redis commit.** This is enforced in one place (the applier).
- **Never skip silently.** `skip_and_log` is opt-in.
- **Bounded memory.** Channel size and batch limits cap memory.
- **Observable.** Metrics are prefixed `pg_redis_sync_`: `replication_lag_bytes`, `slot_retained_wal_bytes`, `sync_latency_seconds`, `batch_duration_seconds`, `batch_size`, `queue_size`, `changes_total{op,table}`, `failed_changes_total`, `retries_total{class}`, `last_ack_timestamp`, `halted`. `/healthz` is unhealthy when halted or when lag does not move. Alert on lag: a stuck slot keeps WAL on the primary.

### 4.5 Testing approach

- **Unit:** decoder fixtures, mapper tables, applier with a fake `Stream` and `Sink`.
- **Integration:** real Postgres and Redis against **both** topologies (in the replica run, stop the replica mid-test and check the sync keeps streaming). Cases include duplicate delivery, a large transaction, slow and down Redis, an initial load during updates and a `WRONGTYPE` halt.
- **Fault injection:** kill after `EXEC` but before ack, and before `EXEC`; drop the Redis connection mid-batch; restart Postgres. Then assert Redis equals Postgres. **Soak:** sustained writes with periodic restarts.

---

## 5. Milestones

| # | Milestone | Done when |
|---|---|---|
| **0** | Scaffold and preflight: `go.mod`, CI, both compose setups, config for both topologies, read-source selection (**done**), `Preflight` checks (open) | `pg-redis-sync check` reports pass/fail for each preflight check |
| **1** | Replication consumer: slot, `START_REPLICATION`, keepalives, status updates | A change is logged; a restart resumes from the confirmed LSN |
| **2** | Decoder and change model | Fixture tests pass |
| **3** | Redis sink, hash mode, atomic checkpoint, TRUNCATE, ack-after-commit | End-to-end content-equality test passes |
| **4** | Reliability: retry, reconnect, standby, shutdown, gap detection, idle advance | Fault-injection suite passes |
| **5** | Binary and operations: YAML config, `slog`, Prometheus, `/healthz`, Dockerfile | Runs from a container. **v0.1.0** |
| **6** | Rebuild and JSON mode: initial snapshot and backfill for both topologies (3.5) | Empty Redis rebuilt with no gap or duplicates, also after a kill mid-copy |
| **7** | Only if needed: `pgoutput` v2 streaming, parallel apply, Redis Cluster, dead-letter sink, per-key LSN guard, CDC from a standby, automated failover | Each item justified by a measured need |

---

## 6. Open questions

1. Which topology do you run in production, and what are replicas for: backfill, reconciliation, or CDC from a standby (milestone 7)?
2. Which Redis topology: standalone, Sentinel or Cluster? Cluster changes the checkpoint design (3.3).
3. `hash` or `json` as the main value type? Currently `hash` is built first.
4. Are all synced tables primary-keyed? Others need `REPLICA IDENTITY FULL` or a unique index.
5. Do you need an initial load on day one? If yes, milestone 6 moves earlier.
6. What are your latency target and peak write rate? They set batch defaults.

---

## 7. Prior art and alternatives considered

**Prior art.** `github.com/Trendyol/go-pq-cdc-redis` is the closest project (`pgoutput`, an initial load before CDC, all three Redis topologies, pipelined batches, ack after a successful run). Our difference: the checkpoint is written in the same `MULTI` as the data and compared with the slot at start, the topologies are explicit, and the sink is a neutral interface. Its license is not stated, so read its code for ideas only. Also `pglogrepl`, `pgx` and Debezium.

**Suggestions from an outside design review, and what we did.**
- *Slot as the only checkpoint:* **kept both.** The Redis checkpoint removes most replays and detects lost Redis data (3.3).
- *A `Transaction` envelope, a `Transformer`, nine public packages, koanf, a full `Before` row:* **not adopted** (a whole transaction in memory; `Mapper` keeps Redis types out of the CDC side; three public packages instead (4.1); `yaml.v3` plus environment variables; `OldKey` is enough).
- *`Pipeline()` for batches:* **use `MULTI/EXEC`**, because a pipeline is not atomic.
- *SQL slot creation for the initial load:* **not adopted**, because it cannot export a snapshot (3.5).
