# Storage backends: feasibility notes

Written against RusticMQ 0.7.0 (2026-11-17). Nothing here is implemented; any
work starts with a reviewed plan.

## How storage works today

The live state is one in-memory `QueueManager` behind one lock
(`src/sqs/mod.rs`): every message, a delivery index sorted by visibility time,
FIFO groups, the deduplication cache and the receive-attempt cache. Sends,
receives, visibility timeouts and FIFO ordering are all decided in memory.

SQLite (`src/persistence.rs`) is a durable record of changes. A background
thread groups pending changes into one transaction and replies only after it
commits, so an acknowledged change survives a crash. The database is read only
once, at startup (`restore_with_limits`), which loads every message into memory.

## Option 1: Postgres or MySQL in place of SQLite

**Feasible and fairly cheap**, as long as one RusticMQ server writes to one
database.

Why it's cheap:

- `commit()` handles six kinds of change, each one upsert or delete, across
  three tables (`queues`, `messages`, `message_state`). No tricky queries.
- The grouped commits suit a networked database: one round trip covers many
  requests.

The work:

1. Put a small interface in front of the SQLite code, and stop
   `PersistenceError` assuming SQLite.
2. Write the Postgres and MySQL versions, roughly 200 lines each. The tables
   carry over almost unchanged; MySQL uses `ON DUPLICATE KEY UPDATE` instead of
   `ON CONFLICT`.
3. Read the address from config, e.g.
   `messages-storage.uri = "postgresql://…"`. ElasticMQ takes a JDBC `uri`
   there but ships only H2, so this is a RusticMQ-only extension.

Catches:

- **Two servers on one database overwrite each other.** Each trusts its own
  memory. A startup lock (Postgres advisory lock, MySQL `GET_LOCK`) makes a
  second server refuse to start.
- **Memory still holds everything**, so a bigger database doesn't raise the
  message limit.
- **Every commit is a network round trip.** Single-connection sends will be
  slower than local SQLite; `benchmark/compare.py` would show by how much.
- **Build size.** Both have pure-Rust clients, so the static container
  executable still works. Put each behind an optional Cargo feature.
- **Tests need real databases** in Docker, locally and in CI.

Gains: no data volume in the container; backups, replicas and point-in-time
recovery from a managed database; familiar operations for Postgres teams.
No gain in availability, scale-out or message capacity.

### Other backends

| Backend | Feasibility | Notes |
|---|---|---|
| libSQL / Turso | Very easy | SQLite-compatible, nearly the same code, adds remote copies. Cheapest way to get storage off the box. |
| redb / fjall (embedded, pure Rust) | Easy | Could beat SQLite on writes; an optimisation to plan first. |
| Plain append-only log file | Easy | Possibly fastest, but reimplements crash recovery and compaction SQLite already gives. |
| Redis / Valkey | Medium | Durable per write only with `appendfsync always`; another service to run. |
| RocksDB | Medium | C++ dependency complicates the static build. Weak case next to redb. |
| S3 / DynamoDB / etcd / FoundationDB | Not worth it | Too slow per commit, or far more than needed. |

Recommendation: Postgres first, with the startup lock, behind a Cargo feature,
benchmarked against SQLite on disk. MySQL then reuses most of the code. libSQL
is the quick win if the goal is only storage off the container.

## Option 2: the database as the source of truth

RusticMQ as a stateless layer: the database holds the live queues, and several
RusticMQ servers can share it. **Possible but large**, closer to rewriting the
engine than adding a backend. Postgres fits; MySQL is a weaker second; SQLite
only works for several processes on one machine, one writer at a time.

| State today | Database version | Difficulty |
|---|---|---|
| Standard receive: pick visible messages (`selected` in `src/engine.rs`) | `SELECT … WHERE next_delivery <= now ORDER BY … LIMIT n FOR UPDATE SKIP LOCKED`, then `UPDATE` visibility | Easy on Postgres and MySQL 8 |
| FIFO: a group is blocked while its head is in flight | Find groups with a visible head, lock the group so two servers can't both serve it | Hard, contention-prone |
| Message order counter (`order: u64`) | A database sequence | Easy |
| FIFO deduplication (5 min), receive-attempt cache (15 min) | Tables with expiry. Dedup isn't stored in SQLite today, so a restart loses it | Medium |
| Long-poll wakeups (in-process `watch` channel) | Postgres `LISTEN/NOTIFY`; MySQL has none, so polling, trading latency against load | Medium on Postgres, poor on MySQL |
| Delayed and invisible messages | Already a `next_delivery` column; waiters wake on notify or timeout | Easy |
| Retention sweep, DLQ moves, copy-to/move-to | SQL inside the send or receive transaction | Medium |
| Message move tasks (run in memory) | A lease row naming the owning server, with takeover | Medium |
| `ApproximateNumberOfMessages` and other stats | `COUNT` queries or trigger-kept counters | Easy, slow on big queues |
| Clock (`now` from each server) | The database clock, so server clock skew can't break visibility | Easy, touches everything |

Cost:

- `src/engine.rs` (about 1,200 lines) becomes SQL, likely per database. The
  SQS protocol layer in `src/sqs/` mostly stays.
- Most of the 286 tests go through the SQS API and would carry over, run
  against each database in Docker.
- Roughly the size of the original engine port, then two engines to maintain:
  the in-memory one stays the default for speed.
- Every receive becomes a row-locking write over the network. Expect
  throughput well below today's 76k receives/s in memory and 44k durable;
  several times lower is a guess, not a measurement.

Gains: no memory limit on backlog; several servers behind a load balancer, no
single point of failure, rolling deploys; Postgres backups and replicas.

Prior art: [pgmq](https://github.com/pgmq/pgmq) already does SQS-style queues
inside Postgres (send, read with visibility timeout, delete, archive). Que,
River and Oban use the same `SKIP LOCKED` pattern for job queues.

## Option 3: bodies in the database, metadata in memory

If memory is the only concern: keep the in-memory engine but hold only each
message's metadata there (id, visibility time, receive count, group; about
200 bytes), and read bodies from the database on receive. Still one server,
but memory no longer grows with body size (up to 1 MiB each). Maybe a fifth of
Option 2's work, and it works with SQLite, Postgres and MySQL alike.

## Which to pick

- Storage off the container: Option 1 (Postgres, or libSQL).
- Large backlogs on one server: Option 3.
- High availability and scale-out: Option 2, Postgres only at first.
