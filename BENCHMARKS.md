# RusticMQ and ElasticMQ benchmarks

A comparison of RusticMQ 0.5.0 with ElasticMQ 1.7.1, run on 2026-11-08. It gives
ratios for this machine and setup, not capacity figures. Differences under about
15% should be read as a tie.

## Summary

On durable queues RusticMQ ties the ElasticMQ JVM build on concurrent sends and is
1.7 times as fast as native. It receives 2.5 times as fast as the JVM build and 4.5
times as fast as native, and uses the least CPU, memory and disk. It is also the only
one that lost no acknowledged message when killed. In memory mode RusticMQ ties
ElasticMQ native under concurrent load, and the JVM build leads.

| | RusticMQ | ElasticMQ native | ElasticMQ JVM |
| --- | --- | --- | --- |
| Send, 64 connections, durable (msg/s) | **28,662** | 16,739 | **30,816** |
| Receive and delete, 64 connections, durable (msg/s) | **45,674** | 10,077 | 18,091 |
| Send, 1 connection, durable (msg/s) | 2,677 | 2,424 | 2,757 |
| Send, 64 connections, in memory (msg/s) | 25,765 | 27,948 | **53,488** |
| Receive and delete, 64 connections, in memory (msg/s) | 46,301 | 49,914 | **74,700** |
| Send, 1 connection, in memory (msg/s) | **6,151** | 2,893 | 3,721 |
| FIFO receive and delete, 8 connections, durable (msg/s) | **18,133** | 4,905 | 6,280 |
| Send, 5 MiB bodies, in memory (msg/s) | **123** | 37 | 65 |
| Receive, 256 KiB bodies, durable (msg/s) | **1,834** | 844 | 927 |
| Median send latency at 5,000 msg/s, in memory (ms) | 1.34 | 1.20 | 1.14 |
| Server CPU seconds per 1,000 durable sends, 8 connections | **0.09** | 0.23 | 0.16 |
| Startup to first healthy response, in memory (ms) | **61** | 97 | 1,048 |
| Idle process memory, in memory (MiB) | **0.3** | 14.8 | 150 |
| Peak process memory, 64-connection send, durable (MiB) | **181** | 337 | 1,236 |
| Disk holding about 100,000 messages (MiB) | **130** | 157 | 719 |
| Acknowledged sends lost after `SIGKILL`, three rounds | **0, 0, 0** | 5,230, 2,058, 2,300 | 2,854, 2,645, 2,686 |
| Failed receiving 5 MiB bodies | No | No | **Yes, 5 of 6 rounds** |
| Image size, `linux/arm64` (MB) | **11.0** | 128 | 466 |

Numbers are medians of three rounds. All tables, with the lowest and highest round,
are in [benchmark/results/tables.md](benchmark/results/tables.md), and every
measurement is in [benchmark/results/raw.jsonl](benchmark/results/raw.jsonl).

## Changes since 0.4.0

The 0.4.0 run on 2026-11-06 is kept in
[benchmark/results/raw-0.4.0.jsonl](benchmark/results/raw-0.4.0.jsonl). ElasticMQ's
results in both runs agree within a few percent, so the two runs can be compared.

| Durable, RusticMQ (msg/s) | 0.4.0 | 0.5.0 |
| --- | --- | --- |
| Send, 64 connections | 16,277 | 28,662 |
| Receive and delete, 64 connections | 20,221 | 45,674 |
| FIFO receive and delete, 8 connections | 10,987 | 18,133 |
| Fill 100,000 messages in batches of ten | 20,033 | 43,858 |
| Receive 256 KiB bodies | 564 | 1,834 |
| Receive 5 MiB bodies | 34 | 78 |

Durable throughput is limited by the time SQLite spends writing each group commit
to disk, so these changes reduce what each commit writes:

- **Time-ordered message IDs** (0.4.1). Random IDs placed each new message on a
  different page of the queue index. SQLite logs whole 4 KiB pages, so every message
  wrote its own index page. UUIDv7 IDs add to the end of the index, and a commit now
  writes about a quarter as much.
- **Receive state apart from the message** (0.5.0). Receives and visibility changes
  update a small state row instead of rewriting the stored message with its body.
  That explains most of the gain for large messages.
- **One engine lock per receive** (0.5.0), instead of two.

The cost is memory: peak durable memory at 64 connections went from 94 to 181 MiB,
because more requests are in flight at once. Each held message also takes about 110
bytes more (1,079 instead of 971). Restart with 95,000 stored messages took 561 ms
instead of 826.

Disk use at the backlog step went from 90 to 130 MiB, but not because messages
take more space. Filling 100,000 messages into a new database takes 73 to 74 MiB
with either version. The faster earlier phases store more messages, and SQLite keeps the
space they used for later writes rather than shrinking the file.

The 0.3.0 run on 2026-11-04 measured 3,533 durable sends and 8,395 durable receives
per second at 64 connections. 0.4.0 added group commit: a writer thread commits the
changes of many requests in one SQLite transaction and answers each request only
after its commit is on disk. It also moved work out of the engine lock and switched
to `opt-level = 3`.

## Findings

**Concurrency.** RusticMQ's durable sends rise from 2,677 msg/s on one connection to
28,662 at 64, using 2.4 of its 4 cores. ElasticMQ uses 3 to 4. In memory mode RusticMQ
ties ElasticMQ native at 64 connections, and the JVM build sends about twice as fast.
At 8 connections in memory both ElasticMQ builds receive faster. Ten queues do not help
RusticMQ: one lock covers all queues.

**Durability.** ElasticMQ loses acknowledged messages when the process is killed.
RusticMQ commits to SQLite with `synchronous=FULL` before answering, so its durable
figures carry a stronger guarantee. The test kills the process only, so it says
nothing about operating system or power failure.

**Large messages.** The JVM server failed to deliver 40 messages of 5 MiB through four
connections in five of six rounds. It answered 503 or stopped accepting connections, in
line with its 1 GiB default heap under the 4 GiB limit. A larger heap would probably
avoid this; that was not tested. RusticMQ is the fastest at sending 5 MiB bodies and at
receiving large bodies from durable queues.

**Latency at fixed rates.** All three are close from 200 to 5,000 msg/s, with medians
of 1 to 3 ms. RusticMQ has the narrowest durable tail: p99.9 of 6 to 8 ms against 10 to
54 ms. In memory at 200 msg/s its p99 is the widest, at 7.2 ms against 5 to 6 ms.

**Memory.** RusticMQ starts at 0.3 MiB, and its durable mode peaks at 181 MiB under the
heaviest send. In memory mode it peaks at 401 MiB, because `:memory:` still keeps every
message in an in-memory SQLite database. A JVM's use follows its heap limit: 150 MiB
idle under the 4 GiB limit and 105 MiB under 1 GiB.

**CPU cost.** In durable mode RusticMQ is the cheapest: 0.09 CPU seconds per 1,000
sends and 0.05 per 1,000 receives and deletes, against 0.23 and 0.19 for native and
0.16 and 0.13 for the JVM. In memory mode it is about even: 0.12 and 0.07 against
0.16 and 0.06 for native and 0.11 and 0.05 for the JVM.

## Setup

- **Machine:** Apple M1 Pro, Docker Desktop VM with 10 CPUs and 23.4 GiB. No emulation.
- **Images, all `linux/arm64`:** `rusticmq:0.5.0` (`60dd354`, musl's allocator),
  `softwaremill/elasticmq-native:1.7.1` and `softwaremill/elasticmq:1.7.1` (Java 11).
- **Limits:** each server on CPUs 0-3 with 4 GiB, the load generator on CPUs 4-7, on
  a private Docker network with no published ports.
- **Configuration:** `sqs-limits = relaxed` and a 64 MiB body limit for all, so that
  5 MiB bodies are accepted. Durable mode is `--data-dir /data` for RusticMQ and the
  documented `messages-storage` H2 file for ElasticMQ, on a named Docker volume.
- **Load generator:** `benchmark/loadgen`, raw unsigned SQS JSON over one keep-alive
  connection per worker. It peaked at 2.6 of 4 cores, so it was not the limit.
- **Rounds:** three, alternating servers, about 86 minutes in all. The other containers
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
python3 benchmark/compare.py report > benchmark/results/tables.md
```

This needs Docker with `linux/arm64` and the images above, and Python 3. It takes
about 90 minutes, and `--quick` shortens the phases to check the setup. An interrupted
run resumes at the first unfinished round. To start again, delete
`benchmark/results/raw.jsonl`.
