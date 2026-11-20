# pg-redis-sync

Stream row-level changes from PostgreSQL into Redis, using logical replication (CDC).
Ships as a Go library (`pgsync`) and a ready-to-run binary (`cmd/pg-redis-sync`).

> **Status: early.** Implemented so far: configuration loading and validation for both PostgreSQL topologies, connection-string handling, read-source selection (`pgsync/`), and the local docker-compose setups. Replication, decoding and the Redis sink are still design only. This README is the design, architecture and plan.

## Contents

1. [Assumptions to confirm](#1-assumptions-to-confirm)
2. [Design](#2-design)
3. [System architecture](#3-system-architecture)
4. [Implementation](#4-implementation)
5. [Milestones](#5-milestones)
6. [Open questions](#6-open-questions)
7. [Prior art and alternatives considered](#7-prior-art-and-alternatives-considered)

---

## 1. Assumptions to confirm

Here is what each assumption means and what changes if it is wrong.

| Assumption | Used as | If wrong |
|---|---|---|
| One source database per process, on one of two **topologies** chosen by config: `single` (one PostgreSQL instance) or `primary_replica` (one main instance plus read replicas). See 2.3. | One slot, one publication, one replication connection per process. The slot always lives on the main instance. | Several databases = run several processes (one config each). Fan-in inside one process is a later, optional step. |
| Row-level changes only, no DDL | Handle `INSERT`, `UPDATE`, `DELETE` (and `TRUNCATE`, see 2.4). Logical decoding never emits DDL anyway. | Schema-change replication would need event triggers or a separate DDL feed. Out of scope. We only *tolerate* additive column changes (see 2.4). |
| Library plus runnable binary | Public package `pgsync` holds all logic. `cmd/pg-redis-sync` is a thin wrapper (YAML config, signals, metrics). | Library-only: drop `cmd/`. Binary-only: make everything `internal/`. |

Further assumptions I made (please confirm or correct):

- **PostgreSQL 13+** (14+ recommended), built-in `pgoutput` plugin. Not `wal2json`/`decoderbufs`, because `pgoutput` needs no server extension and is available on every managed service.
- **Redis standalone or Sentinel** in v1. Redis Cluster is deferred (see 2.3: the atomic checkpoint needs all keys in one transaction).
- **Go 1.26** (as in `go.mod`, the `Dockerfile` and CI). Planned dependencies: `jackc/pgx/v5` + `jackc/pglogrepl` for replication (we do not write our own replication protocol), `redis/go-redis/v9` for Redis, `gopkg.in/yaml.v3` for the binary's config (its `KnownFields(true)` makes unknown keys an error), `prometheus/client_golang` in `cmd/` only, `testcontainers-go` for integration tests, stdlib `log/slog` for logs. The `pgsync` package stays dependency-free until milestone 1. No config framework (such as koanf): YAML plus environment variables is enough.
- **Module path** is `github.com/champ-isaac/pg-redis-sync`, matching the git remote.
- **Local dev credentials** (docker-compose only, not secrets): Postgres user `sync`, database `pgredissync`.
- **Redis is a cache/derived view.** Postgres stays the source of truth. If Redis loses data, we rebuild it from a snapshot (milestone 6).
- **Replicas are read sources, the main instance is the authoritative source for sync.** The CDC stream never comes from a replica in v1 (see 2.3).

### Local development

Single instance (the default):

```sh
docker compose up -d --wait               # postgres (wal_level=logical), redis, app container
docker compose run --rm app go test ./... # unit tests, no live databases needed
docker compose down                       # add -v to also delete the volumes
```

Main plus one read replica. Layer the second file over the first:

```sh
export COMPOSE_FILE=docker-compose.yaml:docker-compose.replica.yaml
docker compose up -d --wait               # + postgres-replica (streaming replica of postgres)
docker compose run --rm app go test ./... # app runs with PG_TOPOLOGY=primary_replica
docker compose down                       # add -v to also delete the volumes
unset COMPOSE_FILE                        # back to the single-instance setup
```

The replica's healthcheck passes only while its WAL receiver is streaming from the main instance (`pg_stat_wal_receiver.status = 'streaming'`), so `up --wait` fails, and the container shows unhealthy, if replication is broken.

In both setups `postgres` is the main instance and holds the slot. The `app` container gets `PG_DSN` and `REDIS_ADDR`; with the replica file it also gets `PG_TOPOLOGY` and `PG_REPLICA_DSNS`. Ports on `127.0.0.1`: Postgres `5432`, Redis `6379`, replica `5433`. Override with `PG_PORT`, `REDIS_PORT`, `PG_REPLICA_PORT` if they are taken.

To go back from the replica setup to single, `docker compose down` (or `rm -sfv postgres-replica`), then drop the leftover physical slot on the main instance: `select pg_drop_replication_slot('replica_1')`. An inactive slot makes the main instance keep WAL forever. `down -v` avoids this by deleting the volumes.

---

## 2. Design

### 2.1 Goals

- Keep Redis in sync with chosen PostgreSQL tables, with low latency (target: well under 1 s end to end under normal load).
- **Never lose a committed change.** Crash, restart, network loss and Redis outage must not drop data.
- **Preserve order** of changes per key (in fact, global commit order in v1).
- **Safe to replay.** A change applied twice leaves Redis in the same state.
- Simple operations: one binary, one config file, clear metrics, clear failure modes.
- Usable as a library, with the Redis side pluggable (custom mapper or custom sink).
- Run against **either** a single PostgreSQL instance **or** a main instance with read replicas, selected by configuration, without changing the sync behaviour.

### 2.2 Non-goals

- DDL replication or automatic schema migration.
- Multiple source databases in one process.
- Streaming the CDC feed from a replica, and automatic failover or promotion handling (see 2.3, "Topologies").
- Redis Cluster, and fan-out to several Redis targets (v1).
- Secondary indexes, joins or derived/aggregated keys. One row maps to one key.
- Exactly-once across two systems. We get *at-least-once delivery with idempotent writes*, which is the practical equivalent (see 3.3).
- Parallel apply (v1 applies strictly in commit order).
- Transform scripting. Only include/exclude/rename of columns.

### 2.3 PostgreSQL side

**Mechanism:** logical replication over the streaming replication protocol, with the `pgoutput` plugin.

**Server requirements** (checked at startup, see `Preflight` in 4.2):

| Setting | Required |
|---|---|
| `wal_level` | `logical` |
| `max_replication_slots`, `max_wal_senders` | at least 1 free of each |
| Role | `REPLICATION` attribute, `SELECT` on synced tables |
| `max_slot_wal_keep_size` | recommended (e.g. 10GB). It stops a dead consumer from filling the disk. |

**Publication.** Owned by the user (default) or created by us if `create_publication: true`:

```sql
CREATE PUBLICATION pg_redis_sync_pub
  FOR TABLE public.users, public.orders
  WITH (publish = 'insert, update, delete, truncate', publish_via_partition_root = true);
```

`publish_via_partition_root = true` makes changes to partitions appear under the parent table, so key mapping uses one table name.

**Replication slot.** A persistent, named logical slot (`pg_redis_sync` by default), created with `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput`. The slot is what makes resume safe: Postgres keeps all WAL the slot still needs, and stores the position we last confirmed (`confirmed_flush_lsn`).

**Streaming.** `START_REPLICATION SLOT <slot> LOGICAL <lsn> (proto_version '1', publication_names '<pub>')`.
Postgres reassembles transactions and sends them **only after commit, in commit order**:
`Begin → (Relation) → Insert/Update/Delete/Truncate … → Commit`.
This means the consumer never sees aborted transactions and never has to reorder.

**Resume from the last acknowledged LSN.** *LSN* = log sequence number, a position in the WAL.

1. On start we ask for `START_REPLICATION ... LOGICAL 0/0`. The server then starts at the slot's `confirmed_flush_lsn`. Postgres, not us, is the primary record of progress.
2. We tell Postgres how far we are done with *Standby Status Update* messages (flush LSN). We send this LSN **only after the Redis write for that transaction is committed** (see 3.2).
3. After a crash, Postgres re-sends everything after the last confirmed LSN. Some of it may already be in Redis. Section 3.3 explains why that is harmless.
4. Keepalives: the server sends `PrimaryKeepalive` and may set `ReplyRequested`. We must answer quickly or the server drops us (`wal_sender_timeout`, 60 s by default). The receive loop therefore never blocks on Redis (see 3.1).
5. Idle advance: if all received transactions are applied and the server's WAL end is ahead (changes on tables we do not publish), we confirm the WAL end. Otherwise the slot holds WAL forever on a quiet table.

**Replica identity** (what Postgres sends about the *old* row on UPDATE/DELETE):

| Table setting | Sent for DELETE / key-changing UPDATE | Our requirement |
|---|---|---|
| `DEFAULT` (has primary key) | primary-key columns only | OK for key-based mapping. The default. |
| `USING INDEX` | columns of that unique index | OK. |
| `FULL` | whole old row | OK. Needed only for JSON mode with TOAST (see 2.4). |
| `NOTHING`, or no PK with `DEFAULT` | nothing | **Preflight error.** Without the key we cannot delete. |

**Other limits (documented, not solved):**

- One active consumer per slot. A second instance gets "slot is active" and retries. This gives simple active/standby HA.
- Logical slots are not carried over on a physical failover before PostgreSQL 17 (failover slots). After a failover the operator re-creates the slot and re-snapshots.
- Sequences and large objects are not replicated by logical replication.

#### Topologies

The sync runs against one of two PostgreSQL topologies, chosen with `postgres.topology`. Everything outside the table below (decoding, Redis mapping, checkpoints, delivery guarantees) is identical.

| | `single` (default) | `primary_replica` |
|---|---|---|
| Instances | one | one **main** (primary) plus one or more read replicas of it |
| Config | `dsn` | `dsn` (the main instance) and `replicas` (list of DSNs, in order of preference) |
| CDC stream (slot, publication, `START_REPLICATION`) | the instance | **main only** |
| Preflight (`wal_level`, slot, publication, replica identity) | the instance | main. Also: the main must **not** be in recovery. Each replica must be reachable, in recovery, and on the same database |
| Read-only queries (backfill reads, reconciliation, lag checks) | the instance | the first healthy replica, else the main (see `read_fallback`) |

The `single` topology is exactly the behaviour from before topologies existed: with only `dsn` set, nothing changes.

**Why the main instance is the only CDC source.**
- It is the authoritative write source. The slot, its `confirmed_flush_lsn` and the publication all live on one writable node, so there is one clear position to resume from.
- Before PostgreSQL 16 a logical slot cannot be created on a standby at all. From 16 it can, but it needs extra setup (`hot_standby_feedback`, primary `wal_level=logical`) and the slot is not failover-safe before 17. Streaming from a standby is therefore a later opt-in (milestone 7), not v1.

**Replica selection (read-only queries).** In `primary_replica`, the first replica in configured order is used if it is:
1. reachable,
2. in recovery (a server that is *not* a standby is a wrong DSN or a promoted replica, and is never used), and
3. lagging the main instance by at most `replica_max_lag_bytes` (WAL bytes; default 64 MiB).

If none qualifies: `read_fallback: primary` (default) reads from the main instance, `read_fallback: none` returns an error (`ErrNoReadSource`). The reasons replicas were skipped are returned so they can be logged. Selection is re-evaluated each time a read connection is needed, so a recovered replica is picked up again.

Rules that the implementation (`pgsync/source.go`) follows:
- **Per-probe timeout.** Each probe runs under its own timeout (5 s), derived from the caller's context, so one slow replica cannot stall selection. A probe that times out is an ordinary skipped replica. Worst case is the timeout times the number of replicas.
- **Cancellation is not a replica problem.** If the caller's context is cancelled or expires, selection returns `ctx.Err()`. It does not skip the replica and does not fall back to the main instance.
- **Errors are redacted.** The text of a probe error is stripped of the DSN and the password before it is stored in the skip reasons.
- **Impossible values are bugs.** A prober that reports a negative lag makes selection fail with an error (not `ErrNoReadSource`); it is not treated as a skippable replica.

**Config validation** (at startup, before any connection): `replicas` is only allowed with `primary_replica`, and is an error with `single` (it is never silently ignored). `primary_replica` needs at least one replica. Each replica must be a different instance than the main (`host:port`), use the same database name, and appear only once. `replica_max_lag_bytes` must be above 0.

**What replicas are for.**
- **Backfill reads (milestone 6).** Copying whole tables loads the database, so it can run on a replica. The slot's exported snapshot only exists on the main instance, so a replica cannot use it. The safe procedure is:
  1. create the slot on the main instance and note its `consistent_point` LSN,
  2. wait until the replica has replayed past that LSN (`pg_last_wal_replay_lsn() >= consistent_point`),
  3. copy the tables in a `REPEATABLE READ` transaction on the replica,
  4. stream from `consistent_point`.

  Rows read in step 3 may already include changes newer than `consistent_point`. The stream then replays those older changes on top. Writes are state-based and applied in commit order, so Redis still ends at the correct state; a key can briefly show an older value on the way (the same caveat as in 3.3).
- **Reconciliation and verification** (compare Redis against PostgreSQL) and **lag checks**.
- Replicas are **never written to** and never hold the slot.

**Requirements for replicas:** physical streaming replication from the main instance, `hot_standby = on`, the same major version, and a role that can run `pg_is_in_recovery()` and `pg_last_wal_replay_lsn()`. `hot_standby_feedback = on` is recommended for long backfill reads, so replay conflicts do not cancel them.

**Failover and promotion (not automated in v1).** If the main instance is lost and a replica is promoted, the logical slot is not on it (it is only carried over with PostgreSQL 17 failover slots). The operator points `postgres.dsn` at the new main instance, removes it from `replicas`, creates the slot and re-snapshots (milestone 6). If `dsn` points at a server that is in recovery, preflight fails with a clear error instead of trying to stream from it.

### 2.4 Redis side

**Keys.** One PostgreSQL row = one Redis key. The key comes from a per-table template built from primary-key (replica-identity) columns:

```
{key_prefix}{schema}.{table}:{pk}            default  →  sync:public.users:42
user:{id}                                    custom template
tenant:{tenant_id}:order:{id}                composite key
```

Rules: placeholders must be replica-identity columns (checked in preflight, because deletes only carry those). Values are escaped (`:` and `%` are percent-encoded) so a key cannot collide with another.

**Value types** (per table, `type:`):

| Type | Redis value | When to use |
|---|---|---|
| `hash` (default, v1) | `HASH`, one field per column | Partial updates, field reads. |
| `json` (milestone 6) | `STRING` holding a JSON object | One `GET`, simple clients. |

**Change → operation** (all commands for one source transaction go in one `MULTI/EXEC`):

| Change | `hash` mode | `json` mode |
|---|---|---|
| INSERT | `HSET key col val …` | `SET key <json>` |
| UPDATE, key unchanged | `HSET` changed/all non-unchanged columns; `HDEL` columns that became NULL | `SET key <json>` (full new row) |
| UPDATE, key changed | `DEL oldKey` + `HSET newKey …`, in the same `MULTI` | `DEL oldKey` + `SET newKey` |
| DELETE | `UNLINK key` (non-blocking delete) | `UNLINK key` |
| TRUNCATE | per `on_truncate` (below) | same |

Details that matter:

- **`MULTI/EXEC` is atomic in isolation, not all-or-nothing.** Redis does not roll back. If `EXEC` succeeds but one command inside it fails (for example `WRONGTYPE`, because another writer put a string where we want a hash), the other commands still ran. The sink must inspect every command result in the reply. Such an error is **permanent** (see 4.4): retrying cannot fix it, and the checkpoint write in the same `MULTI` must not be treated as proof that the batch is correct.
- **Updates are state-based, not delta-based.** We always write the new row state, never "increment" something. This is what makes replays safe.
- **NULL.** A Redis hash cannot hold NULL. We omit the field and `HDEL` it when a column becomes NULL. JSON mode writes `null`.
- **TOAST.** Postgres omits large unchanged columns in an UPDATE (marked "unchanged toast"). In `hash` mode we simply do not touch those fields, so it just works. In `json` mode we need the full row, so the table needs `REPLICA IDENTITY FULL` (the old tuple then supplies the missing values). Preflight rejects a `json` table without it.
- **Value encoding.** `hash`: PostgreSQL text form of the value, except `bool` → `true`/`false` and `bytea` → base64. `json`: `bool` → bool, integer and float → number, `numeric` and `int8` → string by default (no precision loss), `json/jsonb` → embedded, timestamps → RFC 3339 string, everything else → string. Mapping is by type OID.
- **Column filter/rename.** `include_columns`, `exclude_columns` (e.g. drop `password_hash`), `rename`.
- **TTL.** Optional `ttl` per table, applied with `EXPIRE` after each write.
- **TRUNCATE.** `on_truncate: scan_delete` (default: `SCAN` the table's key prefix and `UNLINK`, which is O(keys)), `ignore`, or `fail`. Honest cost: truncating a big table is slow on the Redis side.
- **DDL tolerance.** pgoutput sends a fresh `Relation` message whenever a table's shape changes. We refresh our cache from it. An *added column* then just shows up as a new hash field. A changed primary key or an incompatible type change **halts** the consumer with a clear error instead of guessing.
- **Checkpoint key.** `sync:{<slot>}:checkpoint` (a hash with `lsn` and `updated_at`), written inside the same `MULTI` as the data (see 3.2).

---

## 3. System architecture

### 3.1 Components and data flow

```
         PostgreSQL main (the only CDC source, both topologies)
              ┌──────────────────────────────┐        ┌──────────────────┐
              │ WAL → pgoutput → slot        │ ─ ─ ─▶ │ read replicas    │  primary_replica only:
              └──────────────┬───────────────┘ physical│ (read-only       │  reads for backfill,
                             │ replication protocol    │  queries only)   │  reconciliation, lag
                             │ (1 connection)          └──────────────────┘
 ┌───────────────────────────▼────────────────────────────────────────┐
 │ pg-redis-sync                                                      │
 │                                                                    │
 │  ┌─────────────────────┐   ┌──────────────┐                        │
 │  │ Replication consumer│──▶│ Change       │                        │
 │  │ (receiver goroutine)│   │ decoder      │                        │
 │  │ - slot, START_REPL  │   │ - relation   │                        │
 │  │ - keepalives, acks  │   │   cache      │                        │
 │  └──────────▲──────────┘   │ - tuples →   │                        │
 │             │ flushedLSN   │   Change     │                        │
 │             │ (atomic)     └──────┬───────┘                        │
 │             │                     │ bounded channel (backpressure) │
 │             │              ┌──────▼───────┐       ┌───────────────┐│
 │             └──────────────│ Applier      │──────▶│ Redis sink    ││
 │                 ack after  │ (batches,    │ Batch │ - Mapper      ││
 │                 commit     │  retries)    │       │ - MULTI/EXEC  ││
 │                            └──────┬───────┘       │ - Checkpoint  ││
 │                                   │ Load at start │   store       ││
 │                                   └──────────────▶│               ││
 │                                                   └───────┬───────┘│
 └───────────────────────────────────────────────────────────┼────────┘
                                                             ▼
                                                           Redis
```

| Component | Responsibility |
|---|---|
| **Replication consumer** | Owns the replication connection. Creates/verifies slot, starts streaming, reads raw messages, answers keepalives, sends Standby Status Updates using the latest `flushedLSN`. Runs in its own goroutine and never waits on Redis. |
| **Change decoder** | Turns raw `pgoutput` messages into typed `Change` values. Keeps the relation cache (OID → table, columns, key columns). Handles unchanged-TOAST markers and type OIDs. Marks transaction boundaries. Pure logic, no I/O. |
| **Applier** | Groups changes into batches (by count, bytes or time), calls the sink, retries on error, then advances `flushedLSN`. This is the only place that decides when it is safe to ack. |
| **Redis sink** | Maps each `Change` to Redis commands (`Mapper`), runs them as one `MULTI/EXEC` pipeline together with the checkpoint write. |
| **Checkpoint store** | Records the last fully applied commit LSN. Default implementation lives in Redis (atomic with the data). Read once at startup. |
| **Status** | A snapshot of the pipeline state, returned by `Syncer.Status()` and used by `/healthz` (fields in 4.2): connected or not, last received and last applied LSN, pending changes, snapshot running, last error. Read-only, no I/O. |
| **Source selector** | Decides which PostgreSQL server each connection goes to, from the configured topology (2.3). `StreamDSN` always returns the main instance. `SelectReadSource` returns the first healthy replica (or the main instance) for read-only queries. In `single` it just returns the one instance. Implemented in `pgsync/source.go`. |

**Concurrency model.** Two goroutines joined by a bounded channel: receiver/decoder → applier. When Redis is slow the channel fills, the decoder stops reading from the socket, and Postgres buffers WAL on its side. The receiver keeps sending keepalives the whole time, so a slow Redis never causes a replication timeout.

**Batching.** A batch holds whole transactions where possible (`batch_max_changes`, `batch_max_delay`). A transaction bigger than the limit is split. Only the **last batch of a transaction** carries a checkpoint LSN, so the ack position always sits on a transaction boundary.

### 3.2 Per-batch sequence

```
receive txn ──▶ decode ──▶ Applier.collect ──▶ Sink.Apply(batch):
                                                 MULTI
                                                   <mapped Redis commands>
                                                   HSET checkpoint lsn=<commitEndLSN>   (only on txn-final batch)
                                                 EXEC
                                             ──▶ flushedLSN = commitEndLSN
                                             ──▶ receiver sends Standby Status Update
```

Order matters: **Redis commit first, Postgres ack second.** If we crash between the two, Postgres re-sends and we skip or re-apply harmlessly (3.3). The reverse order could lose data.

### 3.3 Delivery guarantee

**Recommendation: at-least-once delivery with idempotent, state-based writes, plus an atomic checkpoint that suppresses most replays.**

Why not exactly-once? Postgres and Redis share no transaction, so true exactly-once is impossible. Why not at-most-once? It can lose committed changes, which is our first goal.

How the three layers work together:

1. **At-least-once.** We ack Postgres only after Redis commits. Any crash replays from the last ack.
2. **Idempotent writes.** `HSET`, `SET`, `DEL` of full row state are safe to repeat. Replaying a transaction gives the same Redis state as applying it once. No counters, no appends.
3. **Atomic checkpoint (standalone/Sentinel Redis).** The checkpoint is written in the same `MULTI` as the data. After a restart we load it and **skip any transaction with commit LSN ≤ checkpoint**. So the common crash case (applied but not acked) causes no replay at all. Redis then sees each transaction once, in effect.

Failure matrix:

| Crash / failure point | Result |
|---|---|
| Before Redis `EXEC` | Replay from last ack. Applied once. |
| After `EXEC`, before Postgres ack | Replay is skipped by the checkpoint. |
| Redis unreachable | Retry the same batch with backoff. Receiver keeps heartbeating. |
| Redis checkpoint is **behind** the slot's `confirmed_flush_lsn` | Redis lost acknowledged data (failover, flush, restore of an old backup). **Gap detected: halt** (`on_gap: fail`). Fix = snapshot rebuild (milestone 6). |
| `EXEC` succeeds but a command inside it fails (`WRONGTYPE`) | Not acked, **halt** (permanent). Partial effects stay in Redis and are fixed by the next successful replay of the same batch, once the cause (a foreign key type, memory) is fixed. |
| Large transaction split over batches, crash in the middle | The whole transaction is replayed. Safe because writes are idempotent. |

Known caveat of replay: if a crash forces a replay without the checkpoint (e.g. checkpoint store disabled), a key can briefly show an *older* value until the replay catches up. Final state is always correct. A per-key LSN guard (Lua compare-and-set) is a possible later option if this matters.

Consistency seen by Redis readers: **eventually consistent.** Changes from one transaction are atomic in Redis only when they fit in one batch. There is no cross-key snapshot isolation.

### 3.4 Error classes (summary, details in 4.4)

- **Transient** (network, Redis timeout/`LOADING`/`READONLY`, slot busy): retry with backoff, forever by default.
- **Permanent** (undecodable message, unknown relation, config/key-template mismatch, incompatible schema change, a failed command inside `EXEC`): halt and report. Skipping would silently lose data.

### 3.5 Initial load and rebuild (milestone 6)

An empty Redis needs the existing rows, not only future changes. Reading all rows and then starting the stream loses changes made in between, so the order is fixed:

1. Create the slot **with the replication-protocol command** `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput EXPORT_SNAPSHOT` (PostgreSQL 15 and later also accept `(SNAPSHOT 'export')`). This returns the slot's `consistent_point` and an exported snapshot. The SQL function `pg_create_logical_replication_slot()` cannot export a snapshot, so it is not used for this.
2. Copy the tables in a `REPEATABLE READ` transaction that imports that snapshot (on a replica: the procedure in 2.3).
3. Stream from `consistent_point`. Changes made during the copy arrive here, in commit order.

Rules:
- **A snapshot row never overwrites a newer change.** On the main instance the snapshot is exactly the state at `consistent_point`, so replay is clean. On a replica it may be newer; the 2.3 caveat applies.
- **Crash during the copy restarts the copy.** The exported snapshot lives only as long as the connection that created the slot, so it cannot be resumed. On restart the slot is dropped and re-created.
- **Snapshot marker.** Redis holds `sync:{<slot>}:snapshot` (`in_progress` or `done`). While it says `in_progress`, streaming is refused. Otherwise a half-filled Redis would be followed by a checkpoint that claims it is current.
- **Progress** is reported through `Status.SnapshotRunning` and a rows-copied counter.

---

## 4. Implementation

### 4.1 Package layout

```
pg-redis-sync/
├── cmd/
│   └── pg-redis-sync/        main.go: flags, YAML config, signals, /healthz, /metrics
├── pgsync/                   PUBLIC. Syncer, Run(ctx). Wires the parts together.
│   ├── config.go             (done) Config, topologies, env loading, validation, DSN helpers
│   └── source.go             (done) StreamDSN, SelectReadSource, Prober
├── change/                   PUBLIC. Change, Op, Row, LSN types (no deps; shared by sinks)
├── sink/
│   └── redis/                PUBLIC. Sink, Mapper (hash, json), checkpoint store, Lua/pipeline helpers
├── internal/
│   ├── replication/          conn, preflight, slot mgmt, receive loop, standby status, reconnect
│   ├── decode/               pgoutput → change.Change, relation cache, type conversion
│   ├── apply/                batching, retry/backoff, ack watermark
│   └── testutil/             docker-compose / testcontainers helpers, fake stream
├── config.example.yaml
├── docker-compose.yaml       single topology: postgres (wal_level=logical) + redis + app, for dev
├── docker-compose.replica.yaml  override: adds postgres-replica, switches app to primary_replica
├── docker/postgres/pg_hba.conf  lets the replica open replication connections (dev only)
├── Dockerfile                dev image (Go 1.26)
├── go.mod
└── README.md
```

Rule: `change/` imports nothing from this repo. `sink/redis` imports `change/` only. `pgsync/` is the only package that knows every part. Keep `internal/` for anything we want freedom to change.

### 4.2 Key interfaces (signatures only)

```go
// change/ — the neutral data model between decoder and sink.
type LSN uint64

type Op uint8 // Insert, Update, Delete, Truncate

type Change struct {
    Op        Op
    Schema    string
    Table     string
    Key       map[string]any // replica-identity columns (new key)
    OldKey    map[string]any // set only when the key changed
    New       map[string]any // full new row; columns Postgres left out (unchanged TOAST) are absent
    Unchanged []string       // names of columns that were left out as unchanged TOAST
    Types     map[string]uint32 // column → type OID, used for encoding
    CommitLSN LSN            // LSN of the transaction this change belongs to
    XID       uint32         // PostgreSQL transaction id, for logs and tracing
    CommitTime time.Time     // commit time on the main instance; gives end-to-end latency
}

type Batch struct {
    Changes    []Change
    Checkpoint LSN // zero unless this batch ends a transaction; then = commit end LSN
}
```

```go
// internal/replication — what the rest of the code sees of Postgres.
// A fake implementation drives unit tests without a database.
type Stream interface {
    Next(ctx context.Context) (Message, error) // blocks; handles keepalives internally
    Ack(lsn change.LSN)                        // sets flushedLSN (atomic); sent on next status update
    Close() error
}

type Preflight interface {
    // wal_level, slot, publication, replica identity, key templates; the main
    // instance is not in recovery; with primary_replica, each replica is a standby.
    Check(ctx context.Context) error
}
```

```go
// pgsync — topology and source selection (implemented).
type Topology string // TopologySingle (default) | TopologyPrimaryReplica

func (p Postgres) StreamDSN() (string, error) // the main instance + replication=database, in every topology
func (p Postgres) SelectReadSource(ctx context.Context, pr Prober) (ReadSource, error)

// Prober inspects one replica. A pgx implementation arrives with milestone 1;
// tests use a fake, so selection logic needs no database. SelectReadSource gives
// each call its own timeout (5 s) through ctx, so an implementation must honour ctx.
// It must never return a negative LagBytes.
type Prober interface {
    ProbeReplica(ctx context.Context, dsn string) (ReplicaStatus, error)
}

type ReplicaStatus struct {
    InRecovery bool  // false = not a standby: wrong DSN or promoted; never used
    LagBytes   int64 // main WAL position minus replica replay position
}

type ReadSource struct {
    DSN     string
    Role    Role     // RolePrimary | RoleReplica
    Skipped []string // replicas that were rejected and why (passwords redacted)
}
```

```go
// sink — the destination. Redis is the only implementation in v1.
type Sink interface {
    // Apply must be atomic and idempotent for the batch. It also stores b.Checkpoint
    // atomically with the data when b.Checkpoint != 0.
    Apply(ctx context.Context, b change.Batch) error
    Close() error
}

// sink/redis
type Mapper interface {
    Map(c change.Change) ([]Command, error) // pure function: Change → Redis commands
}

type CheckpointStore interface {
    Load(ctx context.Context) (change.LSN, error) // 0 if none
    // Saving is part of Sink.Apply (same MULTI), so there is no separate Save.
}
```

```go
// pgsync — the entry point for library users.
type Syncer struct{ /* … */ }

func New(cfg Config, opts ...Option) (*Syncer, error) // Options: WithLogger, WithMetrics, WithSink, WithMapper
func (s *Syncer) Run(ctx context.Context) error       // blocks until ctx is cancelled or a permanent error
func (s *Syncer) Status() Status                      // safe to call at any time, from any goroutine

type Status struct {
    Connected       bool      // replication connection is up
    SnapshotRunning bool      // initial load in progress (milestone 6)
    LastReceivedLSN change.LSN
    LastAppliedLSN  change.LSN // = last LSN committed to Redis and eligible for ack
    PendingChanges  int        // decoded but not yet applied
    LastError       string     // last transient or permanent error text, redacted
    Halted          bool
}
```

There is deliberately no separate `Start`/`Stop`/`Close`: `Run(ctx)` starts, cancelling `ctx` stops gracefully (4.4), and `Run` returns when everything is closed.

`Metrics` is a small interface (counters, gauges, histograms). The binary plugs in Prometheus. The library defaults to a no-op, so it adds no hard dependency.

### 4.3 Configuration

YAML for the binary, plus a matching `pgsync.Config` struct for library users. Secrets come from environment variables (`${VAR}` expansion), never from the file.

```yaml
postgres:
  topology: single               # single | primary_replica
  dsn: ${PG_DSN}                 # the main (primary) instance: postgres://user:pass@host:5432/db
                                 # replication=database is added for you. Slot and CDC stream use this.
  replicas: []                   # primary_replica only. Read replicas of dsn, in order of preference:
                                 #   - ${PG_REPLICA_1_DSN}
  replica_max_lag_bytes: 67108864  # primary_replica only: skip a replica that trails the main by more (64 MiB)
  read_fallback: primary         # primary | none. When no replica qualifies for a read.
  slot: pg_redis_sync
  publication: pg_redis_sync_pub
  create_slot: true
  create_publication: false      # if true, built from `tables:` below
redis:
  addr: ${REDIS_ADDR}            # or sentinel: {master_name, addrs}
  password: ${REDIS_PASSWORD}
  db: 0
  checkpoint_key: "sync:{pg_redis_sync}:checkpoint"
sync:
  batch_max_changes: 500
  batch_max_bytes: 4MiB
  batch_max_delay: 50ms
  queue_size: 4096               # decoder → applier channel, the backpressure limit
  standby_status_interval: 10s
  on_gap: fail                   # fail | ignore (never ignore in production)
snapshot:
  mode: never                    # never | initial (milestone 6). initial = load existing rows if Redis is empty
retry:
  initial_backoff: 200ms
  max_backoff: 30s
  jitter: 0.2
  max_elapsed: 0                 # 0 = retry forever on transient errors
  on_permanent_error: halt       # halt | skip_and_log (data loss, off by default)
tables:
  - name: public.users
    type: hash
    key: "user:{id}"             # default: sync:public.users:{pk}
    exclude_columns: [password_hash]
    ttl: 0
    on_truncate: scan_delete     # scan_delete | ignore | fail
  - name: public.orders
    type: hash
log:
  level: info
http:
  addr: ":9090"                  # /healthz, /metrics
```

Everything has a default except `postgres.dsn`, `redis.addr` and `tables`. With `topology: primary_replica`, `replicas` is also required. Config is validated at startup. Unknown fields are an error (`yaml.v3` with `KnownFields(true)`).

**Environment variables** (what `pgsync.LoadFromEnv` reads today, before the YAML loader exists):

| Variable | Meaning | Default |
|---|---|---|
| `PG_TOPOLOGY` | `single` or `primary_replica` | `single` |
| `PG_DSN` | main (primary) instance | required |
| `PG_REPLICA_DSNS` | comma-separated replica DSNs (percent-encode a comma inside a password) | none; required for `primary_replica`, an error for `single` |
| `PG_REPLICA_MAX_LAG_BYTES` | largest accepted replica lag | `67108864` |
| `PG_READ_FALLBACK` | `primary` or `none` | `primary` |
| `PG_SLOT`, `PG_PUBLICATION` | slot and publication names | `pg_redis_sync`, `pg_redis_sync_pub` |
| `REDIS_ADDR`, `REDIS_PASSWORD`, `REDIS_DB` | Redis connection | `REDIS_ADDR` required |

Existing single-instance setups need no change: `PG_DSN` and `REDIS_ADDR` alone still select `single`.

### 4.4 Error handling and retries

| Situation | Class | Behaviour |
|---|---|---|
| Replication connection lost | transient | Reconnect with backoff, `START_REPLICATION` again from the slot position. In-flight, unacked work is replayed (safe). |
| Slot already active (another instance) | transient | Retry with backoff. This is the standby behaviour. |
| Redis timeout, connection reset, `LOADING`, `READONLY`, `TRYAGAIN`, `CLUSTERDOWN` | transient | Retry the **same batch** with exponential backoff and jitter. Replication keepalives continue. Reconnect the Redis client if needed. |
| A command inside `EXEC` fails (`WRONGTYPE`, wrong arguments) | permanent | Halt without acking. Retrying cannot help. See 2.4. |
| Redis `OOM` | transient but flagged | Retry, log loudly, expose a metric. Needs an operator. |
| Unknown relation, undecodable tuple, unsupported type | permanent | Halt. Return an error from `Run`. Process exits non-zero. |
| Key template references non-identity column; PK change in `Relation` | permanent | Halt (also caught in preflight where possible). |
| Redis checkpoint behind slot position | permanent | Halt (`on_gap: fail`). |
| `dsn` points at a server in recovery (a replica, or a failed-over main) | permanent | Halt in preflight with a clear error. Never stream from it. |
| A replica is down, lagging or not a standby | transient | Not an error. Skip it and use the next one, then the main instance (`read_fallback`). Only `read_fallback: none` turns this into an error (`ErrNoReadSource`). The CDC stream is unaffected, because it never uses a replica. |
| `ctx` cancelled during replica selection | shutdown | `SelectReadSource` returns `ctx.Err()`. No fallback to the main instance. |
| A prober reports negative lag | permanent (bug) | `SelectReadSource` returns an error. Not treated as a skipped replica. |
| `ctx` cancelled / SIGTERM | shutdown | Stop reading, finish and ack the in-flight batch (bounded by a shutdown timeout), close connections, return `nil`. |

Principles:

- **A batch is the unit of retry.** It is idempotent, so retrying it is always safe.
- **Never ack before Redis commit.** This is enforced in one place (the applier).
- **Never skip silently.** The only "skip" option (`skip_and_log`) is opt-in. A dead-letter sink is a later extension.
- **Bounded memory.** Channel size and batch limits cap memory. Giant transactions are split into batches.
- **Observable.** Metrics, all prefixed `pg_redis_sync_`: `replication_lag_bytes` (server WAL end minus flushed LSN), `slot_retained_wal_bytes` (WAL the main instance keeps for our slot, read from `pg_replication_slots`), `sync_latency_seconds` (commit time on the main instance to Redis commit, from `Change.CommitTime`), `batch_duration_seconds`, `batch_size`, `queue_size` (decoder to applier channel fill), `changes_total{op,table}`, `failed_changes_total`, `retries_total{class}`, `last_ack_timestamp`, `halted`. `/healthz` is unhealthy when halted or when lag does not move for too long. Alert on lag: a stuck slot keeps WAL on the primary.

### 4.5 Testing approach

- **Unit:** decoder against recorded `pgoutput` byte fixtures (insert, update with key change, delete, TOAST, truncate, relation change). Mapper as table-driven tests (Change in, commands out). Applier with a fake `Stream` and fake `Sink` (retry, ack order).
- **Integration:** real Postgres + Redis via docker-compose / testcontainers. Check end-to-end content equality after random workloads. Run the suite against **both** topologies: `docker-compose.yaml` alone (single) and with `docker-compose.replica.yaml` (main plus replica). In the replica run, also stop the replica mid-test and check the sync keeps streaming from the main instance.
- **Behaviour cases to cover** (each one a named integration test): insert, update and delete reach Redis; several updates in a row, the latest committed value wins; a primary-key change deletes the old key; the same change delivered twice leaves the same state; one large transaction; Redis slow, so the queue fills and memory stays bounded; Redis down, so nothing is acked and the batch is retried; an initial load while rows are being updated ends with no lost or stale value; a `WRONGTYPE` key halts the sync without acking.
- **Fault injection:** kill the process after `EXEC` but before ack, and before `EXEC`. Drop the Redis connection mid-batch. Restart Postgres. After each, assert Redis equals Postgres and no data was lost.
- **Soak:** sustained write load with periodic restarts, watching lag and memory.

---

## 5. Milestones

Each milestone ends in something runnable and testable.

| # | Milestone | Done when |
|---|---|---|
| **0** | **Scaffold & preflight.** `go.mod`, CI (vet, test, lint), docker-compose (single and main+replica), config loading/validation for both topologies, read-source selection (**done**), `Preflight` checks (open). | `pg-redis-sync check` reports clear pass/fail for `wal_level`, slot, publication, replica identity, main not in recovery, and each replica being a standby. |
| **1** | **Replication consumer.** Connect, create/verify slot, `START_REPLICATION`, keepalives, status updates. Print raw messages. | Insert in Postgres shows up as a logged message. Restart resumes from the confirmed LSN, with no loss and no endless replay. |
| **2** | **Decoder & change model.** Relation cache, `Change` / `Batch` types, type conversion, TOAST markers, transaction boundaries. | Recorded-fixture unit tests pass. Live changes print as typed `Change` values. |
| **3** | **Redis sink, hash mode.** Mapper, `MULTI/EXEC` batches, atomic checkpoint, key templates, delete/update/key-change, TRUNCATE. Applier with ack-after-commit. | End-to-end sync works. Content equality test passes under random workload. |
| **4** | **Reliability.** Retry/backoff, reconnect, standby behaviour, graceful shutdown, gap detection, idle LSN advance. | Fault-injection suite passes (kill points, Redis/Postgres restarts). |
| **5** | **Binary & operations.** YAML config, `slog`, Prometheus metrics, `/healthz`, Dockerfile, example config, user docs. | Runs from a container with only config + env. Lag and halt state visible in metrics. **v0.1.0 release.** |
| **6** | **Rebuild & JSON mode.** Initial snapshot/backfill. `single`: create slot with `EXPORT_SNAPSHOT`, copy tables in a `REPEATABLE READ` transaction using that snapshot, then stream from the slot's consistent point. `primary_replica`: copy from a replica after it has replayed past the slot's consistent point (2.3). Add `json` value type. Details in 3.5 (protocol-command slot creation, snapshot marker, restart on crash). | Empty Redis can be rebuilt from Postgres with no gap and no duplicates, also when the process is killed mid-copy. Used also to recover from a detected gap. |
| **7** | **Later, only if needed.** `pgoutput` v2 streaming of in-progress large transactions, parallel apply by key hash with a low-watermark ack, Redis Cluster, dead-letter sink, per-key LSN guard, Redis Streams sink, CDC from a standby (PostgreSQL 16+, opt-in `stream_from: replica`), automated failover handling. | Each item is justified by a measured need first. |

---

## 6. Open questions

1. Which PostgreSQL topology do you run in production: a single instance, or a main with replicas? In the replica case, what are the replicas for: backfill reads, reconciliation, or do you want the CDC feed taken from a standby (milestone 7, PostgreSQL 16+)?
2. Which Redis topology do you run (standalone, Sentinel, Cluster)? Cluster changes the checkpoint design (3.3).
3. Do you prefer `hash` or `json` as the main value type? This decides which one is built first (currently `hash`).
4. Are the tables you sync all primary-keyed? Tables with no PK need `REPLICA IDENTITY FULL` or a unique index.
5. Do you need an initial load on day one? If yes, milestone 6 moves earlier.
6. What is your end-to-end latency target and peak write rate? This sets batch defaults and decides whether parallel apply (milestone 7) is ever needed.

---

## 7. Prior art and alternatives considered

**Prior art to study before milestone 1.**
- `github.com/Trendyol/go-pq-cdc-redis` (checked on its GitHub page): the closest existing project. It streams PostgreSQL changes to Redis with `pgoutput`, runs an initial load before CDC, supports `string`, `json` and `hash` values, standalone, Sentinel and Cluster Redis, flushes batches through Redis pipelines by size and timer, and acknowledges only the last LSN after a successful pipeline run (at-least-once). Our difference: the checkpoint is written in the same `MULTI` as the data and compared with the slot at start (gap detection), the topologies are explicit, and the sink is a neutral interface. Its license is not stated on that page, so read its code for ideas only until that is checked.
- `jackc/pglogrepl` and `jackc/pgx`: the replication protocol and driver we build on.
- Debezium's PostgreSQL connector: the reference for consistent snapshots followed by streaming (3.5).

**Alternatives considered, from an outside design review, and what we did.**

| Suggestion | Decision | Why |
|---|---|---|
| Treat the PostgreSQL slot as the only checkpoint; keep a Redis checkpoint only as extra state | **Keep both, with roles.** The slot is what PostgreSQL resumes from. The Redis checkpoint is written atomically with the data, so it removes most replays and detects data lost in Redis (3.3). | A slot position alone cannot tell us that Redis lost data. |
| A `Transaction` envelope holding all events of a transaction | **Not adopted.** `Change` carries `CommitLSN`, `XID` and `CommitTime`, and `Batch.Checkpoint` marks the end of a transaction. | A whole-transaction value forces the full transaction into memory. Batches can split large ones. |
| `Transformer` turning events into Redis commands, `Sink.Apply(commands)` | **Not adopted.** `Mapper` lives in `sink/redis`, so `change/` and the decoder never know Redis. | Keeps the CDC side free of Redis types and allows other sinks. |
| `Pipeline()` for batches | **Use `MULTI/EXEC`** and check every command result (2.4). | A plain pipeline is not atomic, and the checkpoint must be atomic with the data. |
| Public packages `config`, `cdc`, `event`, `snapshot`, `processor`, `redis`, `checkpoint`, `engine`, `metrics` | **Not adopted.** Three public packages (`pgsync`, `change`, `sink/redis`) plus the binary, the rest `internal/` (4.1). | Fewer public promises to keep stable. |
| `Start`, `Stop`, `Close`, `Health` on the engine | **Adopted in part:** `Run(ctx)` and `Status()`. | `Run` with a context is the usual Go shape. `Status()` covers health. |
| Create the slot with `pg_create_logical_replication_slot()` | **Not for the initial load.** Use `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT` (3.5). | The SQL function cannot export a snapshot. |
| koanf for configuration | **Not adopted.** `yaml.v3` plus environment variables. | One small dependency instead of a framework. |
| Status struct, latency and buffer metrics, `Before`/`After` full rows | **Status, latency, queue and slot-retention metrics adopted.** Full `Before` row not added. | `OldKey` is enough for deletes and key changes. A full old row is needed only with `REPLICA IDENTITY FULL`, and no mapping uses it. |
