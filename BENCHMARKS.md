# RusticMQ and ElasticMQ benchmarks

A comparison of RusticMQ 0.4.0 with ElasticMQ 1.7.1, run on 2026-11-06. It gives
ratios for this machine and setup, not capacity figures. Differences under about
15% should be read as a tie.

## Summary

On durable queues RusticMQ ties ElasticMQ native on concurrent sends, receives twice
as fast as native and as fast as the JVM build, and uses the least CPU, memory and
disk. It is also
the only one that lost no acknowledged message when killed. The ElasticMQ JVM build is
still about twice as fast at concurrent sends, and in memory mode both ElasticMQ
builds lead under concurrent load.

| | RusticMQ | ElasticMQ native | ElasticMQ JVM |
| --- | --- | --- | --- |
| Send, 64 connections, durable (msg/s) | 16,277 | 17,199 | **31,901** |
| Receive and delete, 64 connections, durable (msg/s) | **20,221** | 10,041 | 18,416 |
| Send, 1 connection, durable (msg/s) | 2,665 | 2,386 | 2,873 |
| Send, 64 connections, in memory (msg/s) | 21,675 | 29,966 | **53,753** |
| Receive and delete, 64 connections, in memory (msg/s) | 31,899 | 50,522 | **75,588** |
| Send, 1 connection, in memory (msg/s) | **5,122** | 2,907 | 3,610 |
| FIFO receive and delete, 8 connections, durable (msg/s) | **10,987** | 3,976 | 6,195 |
| Send, 5 MiB bodies, in memory (msg/s) | **108** | 40 | 76 |
| Median send latency at 5,000 msg/s, in memory (ms) | 1.38 | 1.25 | 1.18 |
| Server CPU seconds per 1,000 durable sends, 8 connections | **0.09** | 0.22 | 0.16 |
| Startup to first healthy response, in memory (ms) | **58** | 121 | 1,217 |
| Idle process memory, in memory (MiB) | **0.3** | 14.8 | 151 |
| Peak process memory, 64-connection send, durable (MiB) | **94** | 351 | 1,259 |
| Disk holding about 100,000 messages (MiB) | **90** | 205 | 738 |
| Acknowledged sends lost after `SIGKILL`, three rounds | **0, 0, 0** | 1,942, 2,814, 3,995 | 2,662, 2,262, 3,143 |
| Failed receiving 5 MiB bodies | No | No | **Yes, 5 of 6 rounds** |
| Image size, `linux/arm64` (MB) | **10.9** | 128 | 466 |

Numbers are medians of three rounds. All tables, with the lowest and highest round,
are in [benchmark/results/tables.md](benchmark/results/tables.md), and every
measurement is in [benchmark/results/raw.jsonl](benchmark/results/raw.jsonl).

## Changes since 0.3.0

The 0.3.0 run on 2026-11-04 measured 3,533 durable sends and 8,395 durable receives
per second at 64 connections, and 9,787 sends in memory. Three changes account for
the difference:

- **Group commit.** A writer thread commits the changes of many requests in one SQLite
  transaction. Each request is still answered only after its commit is on disk, so the
  guarantee is unchanged, and the engine lock is no longer held during commits.
- **Less work under the lock.** Sends validate, compute digests and build responses
  outside the engine lock, and the request path allocates about a third less.
- **Optimized for speed.** The image is built with `opt-level = 3` instead of `"z"`,
  which makes it 3 MB larger.

The cost is memory: peak durable memory at 64 connections went from 24 to 94 MiB,
because many more requests are in flight at once.

## Findings

**Concurrency.** RusticMQ's durable sends rise from 2,665 msg/s on one connection to
16,277 at 64, using 1.2 of its 4 cores. ElasticMQ uses 3 to 4. In memory mode RusticMQ
uses 2.5 cores and stays behind both ElasticMQ builds. Ten queues do not help
RusticMQ: one lock covers all queues.

**Durability.** ElasticMQ loses acknowledged messages when the process is killed.
RusticMQ commits to SQLite with `synchronous=FULL` before answering, so its durable
figures carry a stronger guarantee. The ElasticMQ JVM build's durable send lead is
therefore partly a weaker guarantee. The test kills the process only, so it says nothing
about operating system or power failure.

**Large messages.** The JVM server failed to deliver 40 messages of 5 MiB through four
connections in five of six rounds. It answered 503 or stopped accepting connections, in
line with its 1 GiB default heap under the 4 GiB limit seen in the 0.3.0 run. A larger
heap would probably avoid this; that was not tested. RusticMQ is now the fastest at
sending 5 MiB bodies.

**Latency at fixed rates.** All three are close from 200 to 5,000 msg/s, with medians
of 1 to 3 ms. RusticMQ has the narrowest durable tail: p99.9 of 6 to 8 ms against 12 to
21 ms. In memory at 200 msg/s its p99 is the widest, at 8.5 ms against 5 to 6 ms.

**Memory.** RusticMQ starts at 0.3 MiB, and its durable mode peaks at 94 MiB under the
heaviest send. In memory mode it peaks at 299 MiB, because `:memory:` still keeps every
message in an in-memory SQLite database. A JVM's use follows its heap limit: 151 MiB
idle under the 4 GiB limit and 105 MiB under 1 GiB.

**CPU cost.** In durable mode RusticMQ is the cheapest: 0.09 CPU seconds per 1,000
sends and 0.05 per 1,000 receives and deletes, against 0.22 and 0.19 for native and
0.16 and 0.14 for the JVM. In memory mode it is not cheaper: 0.16 and 0.09 against
0.15 and 0.06 for native and 0.11 and 0.05 for the JVM.

## Setup

- **Machine:** Apple M1 Pro, Docker Desktop VM with 10 CPUs and 23.4 GiB. No emulation.
- **Images, all `linux/arm64`:** `rusticmq:0.4.0` (`f6bf623`, musl's allocator),
  `softwaremill/elasticmq-native:1.7.1` and `softwaremill/elasticmq:1.7.1` (Java 11).
- **Limits:** each server on CPUs 0-3 with 4 GiB, the load generator on CPUs 4-7, on
  a private Docker network with no published ports.
- **Configuration:** `sqs-limits = relaxed` and a 64 MiB body limit for all, so that
  5 MiB bodies are accepted. Durable mode is `--data-dir /data` for RusticMQ and the
  documented `messages-storage` H2 file for ElasticMQ, on a named Docker volume.
- **Load generator:** `benchmark/loadgen`, raw unsigned SQS JSON over one keep-alive
  connection per worker. It peaked at 2.6 of 4 cores, so it was not the limit.
- **Rounds:** three, alternating servers, about 82 minutes in all. The other containers
  on the VM kept running. A phase started only when other work was below 15% of the
  VM's CPU, and any phase pushed above 25% was excluded. None of 409 was excluded.

## Scenarios

| Scenario | How |
| --- | --- |
| Warm-up | 15 s of sends, then receives, at 8 connections; reported separately |
| Standard queue | 6 s of sends, then receive and delete all of them, at 1, 8 and 64 connections |
| FIFO | The same at 8 connections with eight message groups |
| Ten queues | The same at 64 connections over ten queues |
| Fixed rate | 10 s at 200, 1,000 and, in memory, 5,000 msg/s; latency counts from each message's scheduled time, so stalls are not hidden |
| Long polls | 1,000 connections waiting on an empty queue for 10 s |
| Backlog | Fill 100,000 messages in batches of ten, record memory and disk, receive 5,000; durable mode then restarts and checks the rest |
| Bodies | 800 of 256 KiB and 40 of 5 MiB, then receive them |
| `SIGKILL` | Durable: 8 connections send, the server is killed at 3 s, restarted and drained; compares acknowledged messages with those that survived |
| 1 GiB limit | In memory: a fresh server, its idle memory, sends and 50,000 messages |

Bodies are 256 bytes unless stated. Receives ask for up to ten messages, followed by
`DeleteMessageBatch`. Every drain checks that each message arrived exactly once: no
run had a duplicate, and the only incomplete drains were the JVM failures above.

## Limits of these results

- One laptop's Docker VM, whose virtual disk may treat `fsync` differently from a
  server's disk. Durable numbers in particular may not carry over.
- ElasticMQ 1.7.1 is 44 commits behind the source RusticMQ was ported from.
- `:memory:` in RusticMQ is still SQLite, while ElasticMQ's memory mode has no database.
- Memory is the cgroup's anonymous memory. A JVM's is mostly heap it has not yet handed
  back, not live data, so memory per message says little about it.
- Disk figures include what earlier phases left behind in each database.
- The phases run in a fixed order on one server process. Large bodies run last because
  their memory stays with the allocator or heap.

## Run it

```sh
python3 benchmark/compare.py run
python3 benchmark/compare.py report
```

This needs Docker with `linux/arm64` and the images above, and Python 3. It takes
about 90 minutes, and `--quick` shortens the phases to check the setup. An interrupted
run resumes at the first unfinished round. To start again, delete
`benchmark/results/raw.jsonl`.
