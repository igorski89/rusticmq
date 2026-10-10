# RusticMQ

A Rust port of [ElasticMQ](https://github.com/softwaremill/elasticmq), based on
upstream commit `b2abbc62954851b5244211fba75c3ac83fb6e0b2`.
The backend is implemented in Rust; the Web UI is excluded.

For prerequisites and release builds on **macOS, Linux and Windows**, see
[BUILDING.md](BUILDING.md), including the minimal Linux container and durable
storage examples.

## Run

```sh
cargo run
```

The SQS endpoint listens on `0.0.0.0:9324`. `GET /health` returns `OK`.
AWS SDK clients can use `http://localhost:9324`, region `elasticmq`, and any local
credentials. Returned queue URLs use account `000000000000`.

Storage uses SQLite in memory by default. Specify a file for durable queues and
messages, including visibility state and receipt handles:

```sh
cargo run -- --database ./rusticmq.sqlite
cargo run -- --config ./elasticmq.conf --database ./rusticmq.sqlite
```

`--database :memory:` (also `:memory`) selects ephemeral storage. H2 database
compatibility is excluded. HOCON (Human-Optimized Config Object Notation) is the
configuration format used by ElasticMQ: it supports readable objects, comments,
includes and duration values. Existing ElasticMQ HOCON files configure startup
queues, durations, routing, tags, server addresses and context paths. For example:

```hocon
queues {
  jobs { defaultVisibilityTimeout = 30 seconds }
}
```

Command-line options override environment variables for the configuration/database
path. The database environment variable overrides HOCON storage settings. Server
environment variables override the corresponding HOCON settings.

| Option | Environment variable | Default |
| --- | --- | --- |
| `--config PATH` | `RUSTICMQ_CONFIG` | No configuration file |
| `--database PATH` | `RUSTICMQ_DATABASE` | `:memory:` |

Additional environment variables configure the server:

| Variable | Default |
| --- | --- |
| `RUSTICMQ_BIND` | `0.0.0.0:9324` |
| `RUSTICMQ_BASE_URL` | `http://localhost:9324` |
| `RUSTICMQ_REGION` | `elasticmq` |
| `RUSTICMQ_ACCOUNT_ID` | `000000000000` |
| `RUSTICMQ_QUEUES_STORAGE` | Disabled (queue definitions file only) |

## Implemented

- Standard and FIFO queue engine, delays, visibility timeouts, receipt handles,
  deduplication, receive attempts, dead-letter routing, copy/move routes and tags.
- SQS JSON and Query/XML: queue lifecycle, attributes and tags; send, receive,
  delete and visibility actions, including batches; queue and dead-letter-source
  listing; permission actions as upstream no-ops.
- Message attributes, MD5 digests, tracing headers, long polling and concurrent
  receives.
- Asynchronous message-move tasks, optional rate limits, cancellation and task
  history.
- SQLite persistence and startup restore, optional queue definitions file, and
  HOCON startup configuration, automatic queue creation templates and relaxed
  limits.
- Queue gauges at `/metrics` in Prometheus text format (a native replacement for
  the upstream JVM's JMX metrics), and wildcard node addresses.

The original Java SDK v1/v2 suites pass all 150 scenarios against RusticMQ, and
the original Node.js AWS SDK suite passes all five tests. Additional Rust, boto3,
Query/XML, process-restart and container checks cover the SQLite replacement and
server behavior. See [PORTING.md](PORTING.md) for the verification matrix.

## Container

```sh
docker build -t rusticmq .
docker run --rm -p 9324:9324 rusticmq
```

This starts an ephemeral instance. `docker compose up --build` uses the supplied
Compose configuration with a named volume and a SQLite file under `/data`.
The image uses a statically linked musl executable in a `scratch` runtime; it
includes CA certificates and license notices, with no shell or shared libraries.
The container runs as UID 10001; bind-mounted database directories must be writable
by that user. Configure `RUSTICMQ_BASE_URL` to the address used by your clients.
The verified multi-architecture image is available in the local registry as
`localhost:5555/igorski89/rusticmq:0.1.0` (also `:latest`). Both Linux AMD64 and
ARM64 pass the complete backend test audit; see [PORTING.md](PORTING.md).
To verify volume persistence across container replacements:

```sh
docker build -t rusticmq:local .
target/sdk-venv/bin/python integration/container.py
```

## Verify

```sh
cargo test
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

For AWS SDK integration tests, start the server on a separate port:

```sh
RUSTICMQ_BIND=127.0.0.1:19324 RUSTICMQ_BASE_URL=http://127.0.0.1:19324 cargo run
```

Then, in another terminal:

```sh
python3 -m venv target/sdk-venv
target/sdk-venv/bin/pip install boto3
RUSTICMQ_ENDPOINT=http://127.0.0.1:19324 target/sdk-venv/bin/python integration/aws_sdk.py
RUSTICMQ_ENDPOINT=http://127.0.0.1:19324 target/sdk-venv/bin/python integration/query.py
```

The integration suite creates isolated queues and deletes them after each test.
For a concurrent standard/FIFO load check that verifies complete delivery:

```sh
RUSTICMQ_ENDPOINT=http://127.0.0.1:19324 target/sdk-venv/bin/python integration/load.py 1000
```

The reported throughput describes that local workload and includes SDK overhead.
To verify SQLite storage across process restarts:

```sh
cargo build
target/sdk-venv/bin/python integration/restart.py
```

To run the original ElasticMQ Java and Node.js SDK suites without editing their
scenarios or assertions:

```sh
brew install openjdk sbt
python3 integration/upstream/run.py ~/Work/elasticmq --nodejs
```

This also requires Node.js/npm and a free port 9321. The runner prepares an
isolated copy of the pinned upstream source, starts its own server, runs the
suites and stops the server. CI runs these suites as well.

## Embed

```rust
use rusticmq::{CreateQueue, QueueManager, ReceiveMessages, SendMessage};

let mut queues = QueueManager::new();
queues.create_queue(CreateQueue::named("example"), 0)?;
queues.send_message("example", SendMessage::body("hello"), 0)?;
let messages = queues.receive_messages("example", ReceiveMessages::count(1), 0)?;
// These operations return Result; use them inside a function returning Result.
```

Engine timestamps are epoch milliseconds. Embedding callers serialize manager
access and supply time. Mutation events can be drained for persistence listeners.

Licensed under Apache 2.0; upstream attribution is preserved in `NOTICE.txt`.
