# ElasticMQ Rust backend port

Reference: ElasticMQ commit `b2abbc62954851b5244211fba75c3ac83fb6e0b2` in
`/Users/igorievsiukov/Work/elasticmq`.

The requested backend is implemented. The Web UI is excluded by instruction.
SQLite replaces the JVM database format; storage defaults to `:memory:` and a
file path enables durable queues and messages. H2 file compatibility is excluded.

## Requirements and verification

| Backend requirement | Implementation | Verification |
| --- | --- | --- |
| Standard/FIFO queues, delays, visibility, receipt handles, statistics, deduplication, receive attempts, tags and queue lifecycle | `src/engine.rs`, `src/model.rs` | 31 engine tests; original Java SDK scenarios; concurrent standard/FIFO delivery workload |
| Dead-letter routing, copy/move routing and bound destination identity | Queue engine and startup dependency ordering | Engine tests for deleted/recreated destinations, late destinations, DLQ copies and receive retries; original SDK DLQ scenarios |
| All 23 upstream SQS actions in JSON and Query/XML | `src/sqs` | 28 Rust HTTP/protocol tests; 150 original Java SDK tests; five original Node.js SDK tests; 14 boto3 checks; three Query/XML workflows exercising all 23 actions |
| Attributes, MD5 digests, binary values, trace headers, validation, batch failures and SDK errors | Protocol conversion and request handlers | Original unchanged Java SDK assertions, including binary attributes, empty custom types and typed JSON exceptions; Rust and Query/XML regression tests |
| Long polling and concurrent consumers | Shared serialized engine with polling outside the mutex | Rust concurrent polling tests; SDK polling scenarios; eight-thread load check |
| Asynchronous message move tasks, rate limits, cancellation and history | Engine tasks and background worker | Engine, Rust HTTP, boto3, original Java SDK and Query/XML tests; persistence failures are retried before further moves |
| Durable SQLite updates, deletes, visibility and startup restore | Transactional SQLite with foreign keys, WAL and full synchronization | 10 persistence tests, including FIFO order/group blocking/deduplication/sequence restore; four real process-restart checks; container replacement check |
| Queue definitions file, startup queues, routing, durations and tags | Atomic definitions writes and HOCON loader | Persistence tests and a process-restart configuration/copy-route test |
| Server defaults, context paths, wildcard URLs, automatic creation, relaxed limits, CORS, health and shutdown | `src/config.rs`, `src/main.rs`, SQS router | Rust HTTP/configuration tests; original SDK health test; process and container lifecycle checks |
| Native metrics, executable/container packaging, embedding and documentation | `/metrics`, Dockerfile, Compose, README, library API | Gauge regression test; non-root Docker build and named-volume check; CI formatting, Clippy, Rust and integration suites |

Queue gauges at `/metrics` replace the upstream JVM's JMX interface. Listing
behavior follows this upstream revision: `MaxResults` and `NextToken` do not
paginate `ListQueues` or `ListDeadLetterSourceQueues`. Permission actions remain
upstream-compatible no-ops.

## Original SDK audit

`integration/upstream/run.py` archives the pinned reference into
`target/upstream-audit`, adds external-server fixtures, starts RusticMQ and runs
the original scenario traits. The original checkout, test scenarios and assertions
are unchanged. Both Java SDK versions pass all 75 scenarios each, for 150 total.
The original Node.js suite uses its existing local-server mode and passes five
tests. The runner supports Homebrew sbt or a standalone launcher JAR, and CI runs
both suites against the pinned source.

The original Java SDK audit initially exposed 32 failures. The implementation
was corrected for JSON exception namespaces and message capitalization, typed
missing-queue errors, queue-name/body/attribute validation messages, FIFO errors
and empty custom attribute suffix normalization. The final original suites pass
with their assertions intact. Signed binary byte arrays are also supported for
compatibility with upstream JSON clients, alongside normal base64 values.

Source review additionally exposed aggregate batch body validation, queue context
validation and legacy redrive queue names. Each has a regression test. The vendored
HOCON parser has a documented fix for trailing indented whitespace in
`vendor/hocon/PATCHES.md`.

## Review fixes

DLQ configuration rejects mismatched FIFO modes and dependency cycles before
mutation. Rejected DLQ delivery no longer aborts receives, matching upstream's
best-effort source removal. Move workers start even if their initial persistence
flush fails; they retry pending events before moving messages. A failed commit
returns an error while keeping the in-memory mutation and pending events for
retry; callers should account for that when retrying requests. Shutdown wakes
long polls, including requests above 20 seconds in relaxed mode.

Derived ordered delivery indexes replace sorting all messages on each receive.
They track insertion, replacement, deletion and visibility updates, and rebuild
from messages after engine deserialization. FIFO group order and head blocking
remain unchanged. Long polls wait for notifications or the next delivery time.

A local release benchmark at depth 50,000 measured 100 standard receives at about
280 ms before and 2.2 ms after; FIFO with eight groups measured about 474 ms before
and 0.3 ms after. Empty probes improved from 284/498 ms to below 0.1 ms. This
isolates receive selection from sending, persistence and network costs; FIFO
receives deliver 80 messages before all eight groups block. The reproducible
command is documented in `BUILDING.md`.

Docker audits rerun the Rust stage with caching disabled for that stage, validate
suite counts, and write execution evidence and a machine-readable summary.
Process/container deletion checks assert visible, invisible and delayed counts
are all zero after restart, so persisted visibility cannot hide a missed delete.

## Current local checks

The review-fix Docker audit passed 247 tests on each of AMD64 and ARM64: 69 Rust,
150 original Java SDK, five original Node.js SDK, 14 boto3, three Query/XML, five
process/container lifecycle scenarios and one named-volume persistence test.
Both summaries confirm Rust tests executed in this run. Logs and machine-readable
summaries are in `target/docker-audit/{amd64,arm64}`. Each architecture also passed
the concurrent 1,000-message standard and FIFO workload.

- 69 Rust tests: 31 engine, 28 HTTP/protocol and 10 persistence.
- 150 original Java SDK scenarios and five original Node.js SDK tests.
- 14 boto3 integration checks and three complete Query/XML workflows.
- Five process-restart checks, including SIGTERM during an active long poll,
  and one non-root named-volume container check spanning three container generations.
- Four audit-evidence regression checks; formatting and Clippy with warnings denied.
- Concurrent standard/FIFO workload: all 1,000 messages per Docker queue and
  10,000 per native queue delivered and deleted without duplicates or omissions
  across eight consumer threads sharing an SDK client.

Throughput from the load script is a local smoke measurement including SDK
overhead; it is not an ElasticMQ comparison. Test results verify the implemented
contracts and tested scenarios, rather than promising every possible input or
operating environment. Run commands are in `README.md` and
`integration/upstream/README.md`.

## Initial Linux Docker publication audit (before review fixes)

The original pre-review audit passed for both `linux/amd64` and `linux/arm64` using
`integration/docker.py`. Rust tests executed in each architecture's musl builder;
the HTTP, SDK, restart and load checks used its final non-root `scratch` image.
ARM64 ran natively in Docker Desktop's Linux VM and AMD64 used its emulation.

| Check | AMD64 | ARM64 |
| --- | --- | --- |
| Rust engine, protocol and persistence tests | 63 passed | 63 passed |
| Original Java SDK v1/v2 scenarios | 150 passed | 150 passed |
| Original Node.js SDK tests | 5 passed | 5 passed |
| boto3 integration tests | 14 passed | 14 passed |
| Query/XML workflows covering all 23 actions | 3 passed | 3 passed |
| Restart scenarios against the Docker runtime | 4 passed | 4 passed |
| Non-root volume persistence across three replacements | 1 passed | 1 passed |
| Concurrent standard/FIFO workload | 1,000 messages each, complete delivery/deletion | 1,000 messages each, complete delivery/deletion |

The initial publication predates the review fixes described above. Both
`localhost:5555/igorski89/rusticmq:latest` and
`localhost:5555/igorski89/rusticmq:0.1.0` were published with the same
multi-architecture manifest digest:

```text
sha256:06d2ad1d59493d76c017aed6c6a3b52c07548d819ae14fbddcc1369c62973a39
```

Registry inspection confirmed both Linux architectures. Each published image's
configuration and filesystem layer digests match its fully tested runtime image;
explicit pulls for both architectures succeeded. Logs and publication metadata
are under `target/docker-audit`. CI repeats the full audit on native AMD64 and
ARM64 runners.
