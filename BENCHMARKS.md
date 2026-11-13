# RusticMQ and ElasticMQ benchmarks

A comparison of RusticMQ 0.7.0 with ElasticMQ 1.7.1, run on 2026-11-13. It gives
ratios for this machine and setup, not capacity figures. Differences under about
15% should be read as a tie.

## Summary

In memory mode RusticMQ now ties the ElasticMQ JVM build on concurrent receives and
uses far less memory. It sends 3.6 times as fast as the JVM build on one connection
and receives FIFO messages 6.8 times as fast. The JVM build still sends about 1.5
times as fast at 64 connections. On durable queues RusticMQ ties the JVM build on concurrent sends and
receives 3.0 times as fast. It is the only one that lost no acknowledged message
when killed.

| | RusticMQ | ElasticMQ native | ElasticMQ JVM |
| --- | --- | --- | --- |
| Send, 64 connections, in memory (msg/s) | 37,037 | 30,878 | **54,515** |
| Receive and delete, 64 connections, in memory (msg/s) | **75,854** | 58,197 | **73,449** |
| Send, 1 connection, in memory (msg/s) | **13,412** | 3,072 | 3,705 |
| FIFO receive and delete, 8 connections, in memory (msg/s) | **57,569** | 5,033 | 8,500 |
| Send, 64 connections, durable (msg/s) | **33,339** | 17,932 | **34,135** |
| Receive and delete, 64 connections, durable (msg/s) | **56,154** | 10,334 | 18,821 |
| FIFO receive and delete, 8 connections, durable (msg/s) | **21,769** | 4,408 | 6,535 |
| Send, 5 MiB bodies, in memory (msg/s) | **173** | 39 | 71 |
| Receive, 256 KiB bodies, durable (msg/s) | **2,133** | 903 | 867 |
| Median send latency at 5,000 msg/s, in memory (ms) | 1.32 | 1.17 | 1.17 |
| p99 send latency at 5,000 msg/s, in memory (ms) | **2.79** | 11.5 | **2.58** |
| Server CPU seconds per 1,000 sends, 8 connections, in memory | **0.07** | 0.15 | 0.11 |
| Server CPU seconds per 1,000 durable sends, 8 connections | **0.09** | 0.22 | 0.16 |
| Startup to first healthy response, in memory (ms) | **55** | 92 | 1,084 |
| Idle process memory, in memory (MiB) | **0.17** | 14.8 | 152 |
| Memory with 1,000 waiting long polls, in memory (MiB) | **3.9** | 676 | 1,242 |
| Memory holding 100,000 messages, in memory (MiB) | **107** | 899 | 1,243 |
| Disk holding about 100,000 messages (MiB) | **163** | **167** | 794 |
| Acknowledged sends lost after `SIGKILL`, three rounds | **0, 0, 0** | 1,320, 2,606, 2,195 | 2,581, 2,288, 2,925 |
| Failed receiving 5 MiB bodies | No | No | **Yes, 4 of 6 rounds** |
| Image size, `linux/arm64` (MB) | **11.0** | 128 | 466 |

Numbers are medians of three rounds. All tables, with the lowest and highest round,
are in [benchmark/results/tables.md](benchmark/results/tables.md), and every
measurement is in [benchmark/results/raw.jsonl](benchmark/results/raw.jsonl).

## Changes since 0.5.0

The 0.5.0 run on 2026-11-08 is kept in
[benchmark/results/raw-0.5.0.jsonl](benchmark/results/raw-0.5.0.jsonl), and the 0.4.0
run in [benchmark/results/raw-0.4.0.jsonl](benchmark/results/raw-0.4.0.jsonl).

| In memory, RusticMQ | 0.5.0 | 0.7.0 |
| --- | --- | --- |
| Send, 1 connection (msg/s) | 6,151 | 13,412 |
| Send, 64 connections (msg/s) | 25,765 | 37,037 |
| Receive and delete, 8 connections (msg/s) | 25,756 | 75,899 |
| Receive and delete, 64 connections (msg/s) | 46,301 | 75,854 |
| FIFO receive and delete, 8 connections (msg/s) | 18,998 | 57,569 |
| Fill 100,000 messages in batches of ten (msg/s) | 45,271 | 83,464 |
| Send 5 MiB bodies (msg/s) | 123 | 173 |
| Peak memory, 64-connection send (MiB) | 401 | 233 |
| Memory with 1,000 long polls (MiB) | 242 | 3.9 |
| Memory holding 100,000 messages (MiB) | 345 | 107 |

- **No SQLite in memory mode** (0.7.0). Without `--data-dir`, messages live in the
  engine alone. Before, `:memory:` kept a second copy of every message in an
  in-memory SQLite database, and each request waited for the writer thread to commit
  it. That copy explains the memory, and the commit wait explains most of the speed.
  `--data-dir :memory:` keeps the old behaviour.
- **Yield before large responses** (0.7.0). A receive whose bodies add up to 64 KiB
  or more lets other tasks run first. Without it, the tasks it woke waited until the
  large response was built. Receives of 256 KiB and 5 MiB bodies are about 1.7 times
  as fast as without it in a separate comparison. Smaller responses do not yield, and
  their speed is unchanged.

Durable figures rose 16 to 39%, but ElasticMQ's rose by up to 17% in this run as well,
so only part of that is RusticMQ's. 0.7.0 does not change the durable write path;
0.5.1 updated SQLite to 3.51.3.
Durable 256 KiB receives went from 1,834 to 2,133 msg/s and 5 MiB receives from 78 to
101.

Peak durable memory at 64 connections rose from 181 to 211 MiB, and disk at the
backlog step from 130 to 163 MiB. As before, the faster earlier phases store more
messages, and SQLite keeps the space they used rather than shrinking the file.

Earlier releases: 0.3.0 measured 3,533 durable sends and 8,395 durable receives per
second at 64 connections. 0.4.0 added group commit: a writer thread commits the
changes of many requests in one SQLite transaction and answers each request only
after its commit is on disk. 0.4.1 switched to time-ordered message IDs, which append
to the queue index instead of touching a random page. 0.5.0 stored receive state
apart from the message, so receives no longer rewrite the body, and took the engine
lock once per receive. 0.5.0 reached 28,662 durable sends and 45,674 durable receives.

## Findings

**Concurrency.** In memory mode the JVM build sends about 1.5 times as fast as
RusticMQ at 64 connections, and they tie on receives. At 8 connections they tie on
sends and RusticMQ receives 1.2 times as fast. Ten queues do not help RusticMQ: one lock covers all queues. Its
durable sends rise from 3,711 msg/s on one connection to 33,339 at 64, using 2.5 of
its 4 cores. ElasticMQ uses 3 to 4.

**FIFO.** RusticMQ receives FIFO messages 6.8 times as fast as the JVM build in memory
and 3.3 times as fast on durable queues. The JVM build sends FIFO messages about 1.3 times as fast in both modes.

**Durability.** ElasticMQ loses acknowledged messages when the process is killed.
RusticMQ commits to SQLite with `synchronous=FULL` before answering, so its durable
figures carry a stronger guarantee. The test kills the process only, so it says
nothing about operating system or power failure.

**Large messages.** The JVM server failed to deliver 40 messages of 5 MiB through four
connections in four of six rounds. It answered 503 or stopped accepting connections, in
line with its 1 GiB default heap under the 4 GiB limit. A larger heap would probably
avoid this; that was not tested. RusticMQ is the fastest at sending large bodies in
memory and at receiving them in both modes. The JVM build sends 5 MiB bodies to
durable queues a little faster.

**Latency at fixed rates.** All three are close from 200 to 5,000 msg/s, with medians
of 1 to 3 ms. ElasticMQ native has the widest tail at 5,000 msg/s, at a p99 of 11.5 ms.
RusticMQ has the narrowest durable tail, a p99.9 of 5.5 to 6.5 ms against 10 to 20 ms.
In memory at 1,000 msg/s its p99 is the widest, at 3.7 ms against 2.5 ms, and one round
reached 12 ms.

**Memory.** RusticMQ starts at 0.17 MiB. 1,000 waiting long polls take 4 MiB, against
676 MiB for native and 1,242 MiB for the JVM build. Each held message takes about 1,080
bytes in either mode. A JVM's use follows its heap limit: 152 MiB idle under the 4 GiB
limit and 104 MiB under 1 GiB.

**CPU cost.** RusticMQ is the cheapest in both modes. In memory it uses 0.07 CPU
seconds per 1,000 sends and 0.02 per 1,000 receives and deletes, against 0.15 and 0.06
for native and 0.11 and 0.05 for the JVM. On durable queues it uses 0.09 and 0.04,
against 0.22 and 0.18 for native and 0.16 and 0.13 for the JVM.

## Setup

- **Machine:** Apple M1 Pro, Docker Desktop VM with 10 CPUs and 23.4 GiB. No emulation.
- **Images, all `linux/arm64`:** `rusticmq:0.7.0` (`c3611ce`, musl's allocator),
  `softwaremill/elasticmq-native:1.7.1` and `softwaremill/elasticmq:1.7.1` (Java 11).
- **Limits:** each server on CPUs 0-3 with 4 GiB, the load generator on CPUs 4-7, on
  a private Docker network with no published ports.
- **Configuration:** `sqs-limits = relaxed` and a 64 MiB body limit for all, so that
  5 MiB bodies are accepted. Memory mode is RusticMQ's default, with no `--data-dir`.
  Durable mode is `--data-dir /data` for RusticMQ and the documented
  `messages-storage` H2 file for ElasticMQ, on a named Docker volume.
- **Load generator:** `benchmark/loadgen`, raw unsigned SQS JSON over one keep-alive
  connection per worker. It peaked at 2.6 of 4 cores, so it was not the limit.
- **Rounds:** three, alternating servers, about 85 minutes in all. The other containers
  on the VM kept running. A phase started only when other work was below 15% of the
  VM's CPU, and any phase pushed above 25% was excluded. None of 410 was excluded.

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
- RusticMQ 0.7.0 follows ElasticMQ 2.0.0-rc1, but the comparison uses 1.7.1, the
  latest stable release, as the earlier runs did. The 2.0.0-rc1 images were not
  measured.
- Receive throughput varies between rounds: RusticMQ's 64-connection in-memory
  receives ranged from 72,293 to 82,741 msg/s.
- Memory is the cgroup's anonymous memory. A JVM's is mostly heap it has not yet handed
  back, not live data, so memory per message says little about it.
- Disk figures include what earlier phases left behind in each database.
- The phases run in a fixed order on one server process. Large bodies run last because
  their memory stays with the allocator or heap.

## Run it

```sh
python3 benchmark/compare.py run
python3 benchmark/compare.py report > benchmark/results/tables.md
```

This needs Docker with `linux/arm64` and the images above, and Python 3. It takes
about 90 minutes, and `--quick` shortens the phases to check the setup. An interrupted
run resumes at the first unfinished round. To start again, delete
`benchmark/results/raw.jsonl`. To include RusticMQ with `--data-dir :memory:`, pass
`--servers rusticmq,rusticmq-sqlite-memory,elasticmq-native,elasticmq-jvm`.
