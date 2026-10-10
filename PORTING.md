# ElasticMQ Rust backend port

Reference: ElasticMQ commit `b2abbc62954851b5244211fba75c3ac83fb6e0b2` in
`/Users/igorievsiukov/Work/elasticmq`.

The requested backend is implemented. The Web UI is excluded by instruction.
SQLite replaces the JVM database format; storage defaults to `:memory:` and a
file path enables durable queues and messages. H2 file compatibility is excluded.

## Requirements and verification

| Backend requirement | Implementation | Verification |
| --- | --- | --- |
| Standard/FIFO queues, delays, visibility, receipt handles, statistics, deduplication, receive attempts, tags and queue lifecycle | `src/engine.rs`, `src/model.rs` | 28 engine tests; original Java SDK scenarios; concurrent standard/FIFO delivery workload |
| Dead-letter routing, copy/move routing and bound destination identity | Queue engine and startup dependency ordering | Engine tests for deleted/recreated destinations, late destinations, DLQ copies and receive retries; original SDK DLQ scenarios |
| All 23 upstream SQS actions in JSON and Query/XML | `src/sqs` | 25 Rust HTTP/protocol tests; 150 original Java SDK tests; five original Node.js SDK tests; 14 boto3 checks; three Query/XML workflows exercising all 23 actions |
| Attributes, MD5 digests, binary values, trace headers, validation, batch failures and SDK errors | Protocol conversion and request handlers | Original unchanged Java SDK assertions, including binary attributes, empty custom types and typed JSON exceptions; Rust and Query/XML regression tests |
| Long polling and concurrent consumers | Shared serialized engine with polling outside the mutex | Rust concurrent polling tests; SDK polling scenarios; eight-client load check |
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

## Final local checks

- 63 Rust tests: 28 engine, 25 HTTP/protocol and 10 persistence.
- 150 original Java SDK scenarios and five original Node.js SDK tests.
- 14 boto3 integration checks and three complete Query/XML workflows.
- Four process-restart checks and one non-root named-volume container check
  spanning three container generations.
- Formatting and Clippy with warnings denied.
- Concurrent standard/FIFO workload: all 1,000 messages per queue delivered and
  deleted without duplicates or omissions across eight consumer clients.

Throughput from the load script is a local smoke measurement including SDK
overhead; it is not an ElasticMQ comparison. Test results verify the implemented
contracts and tested scenarios, rather than promising every possible input or
operating environment. Run commands are in `README.md` and
`integration/upstream/README.md`.

## Linux Docker publication audit

The complete audit passed for both `linux/amd64` and `linux/arm64` using
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

Both `localhost:5555/igorski89/rusticmq:latest` and
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
