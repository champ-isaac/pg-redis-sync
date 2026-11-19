# pg-redis-sync

Stream row-level changes from PostgreSQL into Redis, using logical replication (CDC).
Ships as a Go library (`pgsync`) and a ready-to-run binary (`cmd/pg-redis-sync`).

> **Status: design only.** No code exists yet. This README is the design, architecture and plan.

## Contents

1. [Assumptions to confirm](#1-assumptions-to-confirm)
2. [Design](#2-design)
3. [System architecture](#3-system-architecture)
4. [Implementation](#4-implementation)
5. [Milestones](#5-milestones)
6. [Open questions](#6-open-questions)

---

## 1. Assumptions to confirm

You gave three assumptions. All three are workable. I used them as written. Here is what each one means and what changes if it is wrong.

| Assumption | Used as | If wrong |
|---|---|---|
| Single PostgreSQL database | One slot, one publication, one replication connection per process. | Several databases = run several processes (one config each). Fan-in inside one process is a later, optional step. |
| Row-level changes only, no DDL | Handle `INSERT`, `UPDATE`, `DELETE` (and `TRUNCATE`, see 2.4). Logical decoding never emits DDL anyway. | Schema-change replication would need event triggers or a separate DDL feed. Out of scope. We only *tolerate* additive column changes (see 2.4). |
| Library plus runnable binary | Public package `pgsync` holds all logic. `cmd/pg-redis-sync` is a thin wrapper (YAML config, signals, metrics). | Library-only: drop `cmd/`. Binary-only: make everything `internal/`. |

Further assumptions I made (please confirm or correct):

- **PostgreSQL 13+** (14+ recommended), built-in `pgoutput` plugin. Not `wal2json`/`decoderbufs`, because `pgoutput` needs no server extension and is available on every managed service.
- **Redis standalone or Sentinel** in v1. Redis Cluster is deferred (see 2.3: the atomic checkpoint needs all keys in one transaction).
- **Go 1.23+**, `jackc/pgx/v5` + `jackc/pglogrepl` for replication, `redis/go-redis/v9` for Redis, stdlib `log/slog` for logs.
- **Module path** is `github.com/champ-isaac/pg-redis-sync`, matching the git remote.
- **Local dev credentials** (docker-compose only, not secrets): Postgres user `sync`, database `pgredissync`.

### Local development

```sh
docker compose up -d --wait               # postgres (wal_level=logical), redis, app container
docker compose run --rm app go test ./... # unit tests, no live databases needed
docker compose down                       # add -v to also delete the volumes
```

The `app` container gets `PG_DSN` and `REDIS_ADDR`. Postgres and Redis are published on `127.0.0.1:5432` and `127.0.0.1:6379`. Override with `PG_PORT` and `REDIS_PORT` if those ports are taken.
- **Redis is a cache/derived view.** Postgres stays the source of truth. If Redis loses data, we rebuild it from a snapshot (milestone 6).

---

## 2. Design

### 2.1 Goals

- Keep Redis in sync with chosen PostgreSQL tables, with low latency (target: well under 1 s end to end under normal load).
- **Never lose a committed change.** Crash, restart, network loss and Redis outage must not drop data.
- **Preserve order** of changes per key (in fact, global commit order in v1).
- **Safe to replay.** A change applied twice leaves Redis in the same state.
- Simple operations: one binary, one config file, clear metrics, clear failure modes.
- Usable as a library, with the Redis side pluggable (custom mapper or custom sink).

### 2.2 Non-goals

- DDL replication or automatic schema migration.
- Multiple source databases in one process.
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
                         PostgreSQL
              ┌──────────────────────────────┐
              │ WAL → pgoutput → slot        │
              └──────────────┬───────────────┘
                             │ replication protocol (1 connection)
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
| Large transaction split over batches, crash in the middle | The whole transaction is replayed. Safe because writes are idempotent. |

Known caveat of replay: if a crash forces a replay without the checkpoint (e.g. checkpoint store disabled), a key can briefly show an *older* value until the replay catches up. Final state is always correct. A per-key LSN guard (Lua compare-and-set) is a possible later option if this matters.

Consistency seen by Redis readers: **eventually consistent.** Changes from one transaction are atomic in Redis only when they fit in one batch. There is no cross-key snapshot isolation.

### 3.4 Error classes (summary, details in 4.4)

- **Transient** (network, Redis timeout/`LOADING`/`READONLY`, slot busy): retry with backoff, forever by default.
- **Permanent** (undecodable message, unknown relation, config/key-template mismatch, incompatible schema change): halt and report. Skipping would silently lose data.

---

## 4. Implementation

### 4.1 Package layout

```
pg-redis-sync/
├── cmd/
│   └── pg-redis-sync/        main.go: flags, YAML config, signals, /healthz, /metrics
├── pgsync/                   PUBLIC. Syncer, Config, Run(ctx). Wires the parts together.
├── change/                   PUBLIC. Change, Op, Row, LSN types (no deps; shared by sinks)
├── sink/
│   └── redis/                PUBLIC. Sink, Mapper (hash, json), checkpoint store, Lua/pipeline helpers
├── internal/
│   ├── replication/          conn, preflight, slot mgmt, receive loop, standby status, reconnect
│   ├── decode/               pgoutput → change.Change, relation cache, type conversion
│   ├── apply/                batching, retry/backoff, ack watermark
│   └── testutil/             docker-compose / testcontainers helpers, fake stream
├── config.example.yaml
├── docker-compose.yml        postgres (wal_level=logical) + redis, for dev and integration tests
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
    Check(ctx context.Context) error // wal_level, slot, publication, replica identity, key templates
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
```

`Metrics` is a small interface (counters, gauges, histograms). The binary plugs in Prometheus. The library defaults to a no-op, so it adds no hard dependency.

### 4.3 Configuration

YAML for the binary, plus a matching `pgsync.Config` struct for library users. Secrets come from environment variables (`${VAR}` expansion), never from the file.

```yaml
postgres:
  dsn: ${PG_DSN}                 # postgres://user:pass@host:5432/db ; replication=database is added for you
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

Everything has a default except `postgres.dsn`, `redis.addr` and `tables`. Config is validated at startup. Unknown fields are an error.

### 4.4 Error handling and retries

| Situation | Class | Behaviour |
|---|---|---|
| Replication connection lost | transient | Reconnect with backoff, `START_REPLICATION` again from the slot position. In-flight, unacked work is replayed (safe). |
| Slot already active (another instance) | transient | Retry with backoff. This is the standby behaviour. |
| Redis timeout, connection reset, `LOADING`, `READONLY`, `TRYAGAIN`, `CLUSTERDOWN` | transient | Retry the **same batch** with exponential backoff and jitter. Replication keepalives continue. Reconnect the Redis client if needed. |
| Redis `OOM` | transient but flagged | Retry, log loudly, expose a metric. Needs an operator. |
| Unknown relation, undecodable tuple, unsupported type | permanent | Halt. Return an error from `Run`. Process exits non-zero. |
| Key template references non-identity column; PK change in `Relation` | permanent | Halt (also caught in preflight where possible). |
| Redis checkpoint behind slot position | permanent | Halt (`on_gap: fail`). |
| `ctx` cancelled / SIGTERM | shutdown | Stop reading, finish and ack the in-flight batch (bounded by a shutdown timeout), close connections, return `nil`. |

Principles:

- **A batch is the unit of retry.** It is idempotent, so retrying it is always safe.
- **Never ack before Redis commit.** This is enforced in one place (the applier).
- **Never skip silently.** The only "skip" option (`skip_and_log`) is opt-in. A dead-letter sink is a later extension.
- **Bounded memory.** Channel size and batch limits cap memory. Giant transactions are split into batches.
- **Observable.** Metrics: `replication_lag_bytes` (server WAL end minus flushed LSN), `apply_latency_seconds`, `batch_size`, `changes_total{op,table}`, `retries_total{class}`, `last_ack_timestamp`, `halted`. `/healthz` is unhealthy when halted or when lag does not move for too long. Alert on lag: a stuck slot keeps WAL on the primary.

### 4.5 Testing approach

- **Unit:** decoder against recorded `pgoutput` byte fixtures (insert, update with key change, delete, TOAST, truncate, relation change). Mapper as table-driven tests (Change in, commands out). Applier with a fake `Stream` and fake `Sink` (retry, ack order).
- **Integration:** real Postgres + Redis via docker-compose / testcontainers. Check end-to-end content equality after random workloads.
- **Fault injection:** kill the process after `EXEC` but before ack, and before `EXEC`. Drop the Redis connection mid-batch. Restart Postgres. After each, assert Redis equals Postgres and no data was lost.
- **Soak:** sustained write load with periodic restarts, watching lag and memory.

---

## 5. Milestones

Each milestone ends in something runnable and testable.

| # | Milestone | Done when |
|---|---|---|
| **0** | **Scaffold & preflight.** `go.mod`, CI (vet, test, lint), docker-compose, config loading/validation, `Preflight` checks. | `pg-redis-sync check` reports clear pass/fail for `wal_level`, slot, publication, replica identity. |
| **1** | **Replication consumer.** Connect, create/verify slot, `START_REPLICATION`, keepalives, status updates. Print raw messages. | Insert in Postgres shows up as a logged message. Restart resumes from the confirmed LSN, with no loss and no endless replay. |
| **2** | **Decoder & change model.** Relation cache, `Change` / `Batch` types, type conversion, TOAST markers, transaction boundaries. | Recorded-fixture unit tests pass. Live changes print as typed `Change` values. |
| **3** | **Redis sink, hash mode.** Mapper, `MULTI/EXEC` batches, atomic checkpoint, key templates, delete/update/key-change, TRUNCATE. Applier with ack-after-commit. | End-to-end sync works. Content equality test passes under random workload. |
| **4** | **Reliability.** Retry/backoff, reconnect, standby behaviour, graceful shutdown, gap detection, idle LSN advance. | Fault-injection suite passes (kill points, Redis/Postgres restarts). |
| **5** | **Binary & operations.** YAML config, `slog`, Prometheus metrics, `/healthz`, Dockerfile, example config, user docs. | Runs from a container with only config + env. Lag and halt state visible in metrics. **v0.1.0 release.** |
| **6** | **Rebuild & JSON mode.** Initial snapshot/backfill: create slot with `EXPORT_SNAPSHOT`, copy tables in a `REPEATABLE READ` transaction using that snapshot, then stream from the slot's consistent point. Add `json` value type. | Empty Redis can be rebuilt from Postgres with no gap and no duplicates. Used also to recover from a detected gap. |
| **7** | **Later, only if needed.** `pgoutput` v2 streaming of in-progress large transactions, parallel apply by key hash with a low-watermark ack, Redis Cluster, dead-letter sink, per-key LSN guard, Redis Streams sink. | Each item is justified by a measured need first. |

---

## 6. Open questions

1. Is the placeholder module path correct, or what is the real GitHub owner/name?
2. Which Redis topology do you run (standalone, Sentinel, Cluster)? Cluster changes the checkpoint design (3.3).
3. Do you prefer `hash` or `json` as the main value type? This decides which one is built first (currently `hash`).
4. Are the tables you sync all primary-keyed? Tables with no PK need `REPLICA IDENTITY FULL` or a unique index.
5. Do you need an initial load on day one? If yes, milestone 6 moves earlier.
6. What is your end-to-end latency target and peak write rate? This sets batch defaults and decides whether parallel apply (milestone 7) is ever needed.
