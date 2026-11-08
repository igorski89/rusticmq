#!/usr/bin/env python3
"""Compare RusticMQ and ElasticMQ as linux/arm64 containers under the same limits.

python3 benchmark/compare.py run [--rounds 3] [--quick] [--output results/raw.jsonl]
python3 benchmark/compare.py report [--output results/raw.jsonl]

Every server runs on a private Docker network with no published ports, so nothing
else on the machine is touched. Servers alternate between rounds, so background load
hits all of them. An interrupted run resumes at the first unfinished round.
"""
import argparse
import http.client
import json
import os
import shutil
import socket
import statistics
import subprocess
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONF = ROOT / "conf"
PLATFORM = "linux/arm64"
LOADGEN = "rusticmq-loadgen:bench"
NETWORK = "rmqbench"
SERVER = "rmqbench-server"
GENERATOR = "rmqbench-loadgen"
VOLUME = "rmqbench-data"
# A fixed address: name lookups before the server exists would delay the health
# check by seconds and hide real startup times.
SUBNET = "172.30.250.0/24"
SERVER_IP = "172.30.250.10"
HOST = f"{SERVER_IP}:9324"
SERVER_CPUS = "0-3"
GENERATOR_CPUS = "4-7"
MEMORY = "4g"
SMALL_MEMORY = "1g"
SOCKET = os.path.expanduser("~/.docker/run/docker.sock")
VM_CPUS = 10
# Other work on the VM, in percent of all its CPUs. Phases start below QUIET, and any
# phase that other work pushed above DISTURBED is left out of the report's medians.
QUIET = 15
DISTURBED = 25


def elastic(image):
    def options(storage):
        return ["--mount", f"type=bind,source={CONF}/elasticmq-{storage}.conf,target=/opt/elasticmq.conf,readonly"]
    return {"image": image, "options": options, "args": lambda storage: []}


SERVERS = {
    "rusticmq": {
        "image": "localhost:5555/igorski89/rusticmq:0.5.0",
        "options": lambda storage: ["--mount", f"type=bind,source={CONF}/rusticmq.conf,target=/opt/bench.conf,readonly"],
        "args": lambda storage: ["--config", "/opt/bench.conf"] + (["--data-dir", "/data"] if storage == "durable" else []),
    },
    "elasticmq-native": elastic("softwaremill/elasticmq-native:1.7.1"),
    "elasticmq-jvm": elastic("softwaremill/elasticmq:1.7.1"),
}
STORAGES = ("memory", "durable")


def docker(*args, check=True, timeout=600):
    result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"docker {' '.join(args)}: {result.stderr.strip()}")
    return result.stdout.strip()


def cleanup():
    for name in (SERVER, GENERATOR):
        docker("rm", "--force", name, check=False)
    docker("volume", "rm", "--force", VOLUME, check=False)


class Stats:
    """Reads a container's cgroup counters through the Docker socket."""

    @staticmethod
    def read(container):
        connection = http.client.HTTPConnection("docker")
        connection.sock = socket.socket(socket.AF_UNIX)
        connection.sock.connect(SOCKET)
        connection.request("GET", f"/containers/{container}/stats?stream=false&one-shot=true")
        data = json.loads(connection.getresponse().read())
        connection.close()
        memory = data.get("memory_stats", {})
        return {
            "cpu_seconds": data["cpu_stats"]["cpu_usage"]["total_usage"] / 1e9,
            # Anonymous memory is what the process itself holds; usage adds page cache.
            "anon": memory.get("stats", {}).get("anon", 0),
            "usage": memory.get("usage", 0),
        }


class Sampler(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.stopping = threading.Event()
        self.anon_peak = 0
        self.usage_peak = 0

    def run(self):
        while not self.stopping.wait(0.5):
            try:
                stats = Stats.read(SERVER)
            except (KeyError, OSError, ValueError):
                continue
            self.anon_peak = max(self.anon_peak, stats["anon"])
            self.usage_peak = max(self.usage_peak, stats["usage"])


class Round:
    def __init__(self, server, storage, number, sink, scale):
        self.server, self.storage, self.number = server, storage, number
        self.sink, self.scale = sink, scale
        self.spec = SERVERS[server]

    def record(self, phase, **fields):
        row = dict(server=self.server, storage=self.storage, round=self.number, phase=phase, **fields)
        self.sink(row)
        summary = fields.get("messages_per_second") or fields.get("achieved_per_second") or fields.get("startup_ms")
        print(f"    {phase}: {round(summary) if summary else ''} errors={fields.get('errors', 0)}", flush=True)

    def loadgen(self, command, *args, **options):
        pairs = [str(item) for key, value in options.items() for item in (f"--{key.replace('_', '-')}", value)]
        output = docker("exec", GENERATOR, "/loadgen", command, "--host", HOST, *pairs, *args)
        return json.loads(output)

    def start(self, memory=MEMORY, fresh=True):
        if fresh:
            options = ["--platform", PLATFORM, "--name", SERVER, "--network", NETWORK, "--ip", SERVER_IP,
                "--cpuset-cpus", SERVER_CPUS, "--memory", memory, "--memory-swap", memory,
                *self.spec["options"](self.storage)]
            if self.storage == "durable":
                options += ["--mount", f"type=volume,source={VOLUME},target=/data"]
            docker("create", *options, self.spec["image"], *self.spec["args"](self.storage))
            architecture = docker("image", "inspect", self.spec["image"], "--format", "{{.Architecture}}")
            assert architecture == "arm64", architecture
        waiting = subprocess.Popen(["docker", "exec", GENERATOR, "/loadgen", "health", "--host", HOST],
            stdout=subprocess.PIPE, text=True)
        time.sleep(0.5)
        docker("start", SERVER)
        healthy = json.loads(waiting.communicate(timeout=180)[0])
        if not healthy["healthy"]:
            raise RuntimeError(f"{self.server} did not become healthy: {docker('logs', SERVER)[-800:]}")
        started = docker("inspect", SERVER, "--format", "{{.State.StartedAt}}")
        started = datetime.fromisoformat(started[:26].rstrip("Z") + "+00:00").timestamp()
        return healthy["epoch_ms"] - started * 1000

    def alive(self):
        return docker("inspect", SERVER, "--format", "{{.State.Running}}") == "true"

    def wait_for_quiet(self):
        """Hold the phase while something else keeps the shared VM busy."""
        began = time.monotonic()
        while time.monotonic() - began < 300:
            server = Stats.read(SERVER)["cpu_seconds"]
            busy = self.loadgen("idle", duration_ms=2000)["vm_cpu_busy_percent"]
            other = busy - 100 * (Stats.read(SERVER)["cpu_seconds"] - server) / (2 * VM_CPUS)
            if other < QUIET:
                break
        return time.monotonic() - began

    def measure(self, phase, command, *args, **options):
        waited = self.wait_for_quiet()
        before = Stats.read(SERVER)
        generator_before = Stats.read(GENERATOR)
        sampler = Sampler()
        sampler.start()
        began = time.monotonic()
        try:
            result = self.loadgen(command, *args, **options)
        except RuntimeError as error:
            result = {"errors": 1, "first_error": str(error)[:400]}
        wall = time.monotonic() - began
        sampler.stopping.set()
        sampler.join()
        if not self.alive():
            state = docker("inspect", SERVER, "--format", "{{.State.OOMKilled}} {{.State.ExitCode}}")
            logs = subprocess.run(["docker", "logs", "--tail", "400", SERVER], capture_output=True, text=True)
            fatal = [line[:200] for line in (logs.stdout + logs.stderr).splitlines() if "OutOfMemory" in line or "panicked" in line]
            self.record(phase, server_died=state, fatal=fatal[:2], **result)
            raise ServerDied(f"{phase}: OOMKilled/exit {state}")
        after = Stats.read(SERVER)
        generator_after = Stats.read(GENERATOR)
        server_cores = (after["cpu_seconds"] - before["cpu_seconds"]) / wall
        generator_cores = (generator_after["cpu_seconds"] - generator_before["cpu_seconds"]) / wall
        self.record(phase, wall_seconds=wall, waited_for_quiet=waited, server_cpu_seconds=after["cpu_seconds"] - before["cpu_seconds"],
            server_cpu_cores=server_cores, generator_cpu_cores=generator_cores,
            other_vm_cpu_percent=result.get("vm_cpu_busy_percent", 0) - 100 * (server_cores + generator_cores) / VM_CPUS,
            anon_peak=max(sampler.anon_peak, after["anon"]), anon_end=after["anon"],
            usage_peak=max(sampler.usage_peak, after["usage"]), **result)
        return result

    def queue(self, label, fifo=False):
        name = f"{label}-{uuid.uuid4().hex[:8]}" + (".fifo" if fifo else "")
        reply = self.loadgen("create", name=name, fifo=str(fifo).lower())
        if "QueueUrl" not in reply:
            raise RuntimeError(f"create {name}: {reply}")
        return reply["QueueUrl"]

    def drop(self, *queues):
        for queue in queues:
            self.loadgen("delete", queue=queue)

    def pair(self, label, count=None, seconds=6, conns=8, queues=1, fifo=False, body_bytes=256, cap_seconds=60):
        """Send for a fixed time or count, then receive and delete everything sent."""
        urls = [self.queue(label, fifo) for _ in range(queues)]
        limit = {"count": count} if count else {}
        sent = self.measure(f"{label}/send", "send", queues=",".join(urls), conns=conns, duration_ms=int(seconds * self.scale * 1000),
            body_bytes=body_bytes, fifo=str(fifo).lower(), **limit)
        expected = sent.get("sent", 0)
        if expected:
            self.measure(f"{label}/consume", "consume", queues=",".join(urls), conns=conns, expect=expected,
                max_ms=int(cap_seconds * 1000))
        self.drop(*urls)

    def rate(self, rate):
        queue = self.queue(f"rate-{rate}")
        self.measure(f"rate-{rate}", "rate", queues=queue, rate=rate, duration_ms=int(10_000 * self.scale))
        self.drop(queue)

    def scenarios(self):
        """Large bodies run last: their memory stays with the allocator or heap and
        would otherwise colour every later phase."""
        steps = [lambda: self.pair("warmup", seconds=15)]
        steps += [lambda conns=conns: self.pair(f"standard-{conns}conn", conns=conns) for conns in (1, 8, 64)]
        steps += [lambda: self.pair("fifo-8conn", fifo=True), lambda: self.pair("10queues-64conn", conns=64, queues=10)]
        rates = (200, 1000, 5000) if self.storage == "memory" else (200, 1000)
        steps += [lambda rate=rate: self.rate(rate) for rate in rates]
        steps += [self.long_polls, self.backlog,
            lambda: self.pair("body-256KiB", count=800, seconds=30, body_bytes=256 * 1024),
            lambda: self.pair("body-5MiB", count=40, seconds=30, conns=4, body_bytes=5 * 1024 * 1024)]
        steps += [self.crash] if self.storage == "durable" else [self.constrained]
        for step in steps:
            try:
                step()
            except ServerDied as error:
                # A crash is a result, not the end of the round.
                print(f"    SERVER DIED {error}", flush=True)
                docker("rm", "--force", SERVER)
                self.record("restart-after-death", startup_ms=self.start())

    def long_polls(self):
        queue = self.queue("long-polls")
        before = Stats.read(SERVER)["anon"]
        self.measure("long-polls-1000", "polls", queues=queue, conns=1000, duration_ms=10_000)
        self.record("long-polls-1000/before", anon_end=before)
        self.drop(queue)

    def backlog(self):
        queue = self.queue("backlog")
        before = Stats.read(SERVER)["anon"]
        count = int(100_000 * self.scale)
        filled = self.measure("backlog/fill", "send", queues=queue, conns=8, batch=10, count=count, duration_ms=180_000)
        time.sleep(3)
        held = filled.get("sent", 0)
        stats = Stats.read(SERVER)
        disk = None
        if self.storage == "durable":
            disk = docker("run", "--rm", "--platform", PLATFORM, "--mount", f"type=volume,source={VOLUME},target=/d",
                "alpine:3.20", "du", "-sb", "/d").split()[0]
        self.record("backlog/held", messages=held, anon_before=before, anon_end=stats["anon"], usage_end=stats["usage"],
            disk_bytes=int(disk) if disk else None)
        remaining = self.measure("backlog/consume-5000", "consume", queues=queue, conns=8, expect=min(5000, held), max_ms=60_000)
        if self.storage == "durable":
            stopped = time.monotonic()
            docker("stop", "--time", "60", SERVER)
            stop_seconds = time.monotonic() - stopped
            startup = self.start(fresh=False)
            expected = held - remaining.get("received", 0)
            restored, restore_seconds = self.wait_restored(queue, expected)
            self.record("backlog/restart", startup_ms=startup, stop_seconds=stop_seconds, expected=expected, restored=restored,
                restore_seconds=restore_seconds, anon_end=Stats.read(SERVER)["anon"])
        self.drop(queue)

    def wait_restored(self, queue, expected=None):
        """Restores may finish after the health check: poll until the depth settles."""
        began = time.monotonic()
        last, steady = None, 0
        while time.monotonic() - began < 120:
            attributes = self.loadgen("depth", queue=queue).get("Attributes", {})
            depth = int(attributes.get("ApproximateNumberOfMessages", -1)) + int(attributes.get("ApproximateNumberOfMessagesNotVisible", 0))
            if expected is not None and depth == expected:
                return depth, time.monotonic() - began
            steady = steady + 1 if depth == last else 0
            last = depth
            # An empty queue may still be restoring, so it gets longer to change.
            if steady >= (20 if depth <= 0 else 5):
                break
            time.sleep(0.25)
        return last, time.monotonic() - began

    def crash(self):
        """SIGKILL during sends: which acknowledged messages survive the restart."""
        queue = self.queue("crash")
        result = {}

        def sending():
            result.update(self.loadgen("send", queues=queue, conns=8, duration_ms=6000, record="true"))

        sender = threading.Thread(target=sending)
        sender.start()
        time.sleep(3)
        docker("kill", "--signal", "KILL", SERVER)
        sender.join()
        time.sleep(1)
        self.start(fresh=False)
        self.wait_restored(queue)
        drained = self.loadgen("consume", queues=queue, conns=4, idle_ms=4000, max_ms=120_000, record="true")
        acked = set(result.get("acked", []))
        seen = set(drained.get("seen", []))
        self.record("crash-sigkill", acked=len(acked), survived=len(seen), lost_acked=len(acked - seen),
            unacked_survivors=len(seen - acked), restore_error=drained.get("first_error"))

    def constrained(self):
        """Fresh server under a smaller memory limit, where a JVM sizes a smaller heap."""
        docker("rm", "--force", SERVER)
        self.record("small-memory/startup", startup_ms=self.start(memory=SMALL_MEMORY))
        time.sleep(5)
        self.record("small-memory/idle", anon_end=Stats.read(SERVER)["anon"])
        self.pair("small-memory-warm", seconds=10)
        queue = self.queue("small-fill")
        self.measure("small-memory/fill-50000", "send", queues=queue, conns=8, batch=10, count=50_000, duration_ms=120_000)

    def run(self):
        cleanup()
        docker("run", "--detach", "--platform", PLATFORM, "--name", GENERATOR, "--network", NETWORK,
            "--cpuset-cpus", GENERATOR_CPUS, LOADGEN)
        if self.storage == "durable":
            docker("volume", "create", VOLUME)
            if self.server == "elasticmq-jvm":
                # The JVM image runs as UID 999 but leaves /data owned by root.
                docker("run", "--rm", "--platform", PLATFORM, "--mount", f"type=volume,source={VOLUME},target=/d",
                    "alpine:3.20", "chown", "999:999", "/d")
        started = time.monotonic()
        try:
            self.record("startup", startup_ms=self.start())
            time.sleep(5)
            self.record("idle", anon_end=Stats.read(SERVER)["anon"], usage_end=Stats.read(SERVER)["usage"])
            self.scenarios()
        except ServerDied as error:
            print(f"    SERVER DIED {error}", flush=True)
        except RuntimeError as error:
            self.record("failed", first_error=str(error)[:600], errors=1)
        finally:
            logs = subprocess.run(["docker", "logs", "--tail", "5", SERVER], capture_output=True, text=True)
            self.record("done", seconds=time.monotonic() - started, log_tail=(logs.stdout + logs.stderr)[-500:])
            cleanup()


class ServerDied(Exception):
    pass


def load(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def run(options):
    path = Path(options.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    done = {(r["server"], r["storage"], r["round"]) for r in load(path) if r["phase"] == "done"}
    existing = docker("network", "ls", "--filter", f"name=^{NETWORK}$", "--format", "{{.Name}}")
    if not existing:
        docker("network", "create", "--internal", "--subnet", SUBNET, NETWORK)
    docker("build", "--quiet", "--platform", PLATFORM, "--tag", LOADGEN, str(ROOT / "loadgen"), timeout=1200)
    scale = 0.2 if options.quick else 1.0
    servers = options.servers.split(",")
    storages = options.storages.split(",")
    with path.open("a") as output:
        def sink(row):
            output.write(json.dumps(row) + "\n")
            output.flush()

        if not load(path):
            sink(dict(server="", storage="", round=0, phase="environment", platform=PLATFORM, memory=MEMORY,
                server_cpus=SERVER_CPUS, generator_cpus=GENERATOR_CPUS,
                images={name: docker("image", "inspect", spec["image"], "--format", "{{.Id}} {{.Architecture}} {{.Size}}")
                    for name, spec in SERVERS.items()},
                docker_cpus=docker("info", "--format", "{{.NCPU}}"), docker_memory=docker("info", "--format", "{{.MemTotal}}"),
                started=datetime.now().isoformat()))
        for number in range(1, options.rounds + 1):
            for storage in storages:
                for server in servers:
                    if (server, storage, number) in done:
                        continue
                    print(f"round {number} {storage} {server}", flush=True)
                    Round(server, storage, number, sink, scale).run()
    docker("network", "rm", NETWORK, check=False)


def spread(values):
    """Median with the lowest and highest round, for a metric across rounds."""
    values = [v for v in values if v is not None]
    if not values:
        return None
    return statistics.median(values), min(values), max(values)


def number(value):
    if value >= 1000:
        return f"{value:,.0f}"
    if value >= 100:
        return f"{value:.0f}"
    if value >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}"


def cell(values, scale=1.0):
    result = spread(values)
    if result is None:
        return "–"
    middle, low, high = (v * scale for v in result)
    if len(values) == 1 or high == low:
        return number(middle)
    return f"{number(middle)} ({number(low)}–{number(high)})"


def report(options):
    rows = load(Path(options.output))
    disturbed = [r for r in rows if r.get("other_vm_cpu_percent", 0) > DISTURBED]
    by = defaultdict(list)
    for row in rows:
        if row not in disturbed:
            by[row["server"], row["storage"], row["phase"]].append(row)
    servers = [s for s in SERVERS if any(k[0] == s for k in by)]

    def table(title, storage, metrics):
        print(f"\n### {title}\n")
        print("| Metric | " + " | ".join(servers) + " |")
        print("| --- | " + " | ".join("---" for _ in servers) + " |")
        for label, phase, read, scale in metrics:
            values = [cell([read(r) for r in by.get((s, storage, phase), []) if read(r) is not None], scale) for s in servers]
            print(f"| {label} | " + " | ".join(values) + " |")

    def field(*path):
        def read(row):
            value = row
            for key in path:
                value = value.get(key) if isinstance(value, dict) else None
            return value
        return read

    rate = field("messages_per_second")
    mib = 1 / (1024 * 1024)
    per_thousand = lambda count: lambda r: 1000 * r["server_cpu_seconds"] / r[count] if r.get(count) else None
    for storage in STORAGES:
        if not any(k[1] == storage for k in by):
            continue
        phases = ["standard-1conn", "standard-8conn", "standard-64conn", "fifo-8conn", "10queues-64conn", "body-256KiB", "body-5MiB"]
        table(f"{storage}: throughput, messages/s, median (lowest–highest round)", storage,
            [(f"{p} {kind}", f"{p}/{kind}", rate, 1) for p in ["warmup", *phases] for kind in ("send", "consume")]
            + [("100k backlog fill, batches of 10", "backlog/fill", rate, 1), ("Consume from 100k backlog", "backlog/consume-5000", rate, 1)])
        rates = [p[2] for p in by if p[1] == storage and p[2].startswith("rate-")]
        rates = sorted(set(rates), key=lambda p: int(p.split("-")[1]))
        rows_ = []
        for p in rates:
            rows_ += [(f"{p} achieved msg/s", p, field("achieved_per_second"), 1), (f"{p} send p50 ms", p, field("send_latency", "p50_ms"), 1),
                (f"{p} send p99 ms", p, field("send_latency", "p99_ms"), 1), (f"{p} send p99.9 ms", p, field("send_latency", "p999_ms"), 1),
                (f"{p} end-to-end p50 ms", p, field("end_to_end_latency", "p50_ms"), 1), (f"{p} end-to-end p99 ms", p, field("end_to_end_latency", "p99_ms"), 1)]
        table(f"{storage}: latency at fixed rates", storage, rows_)
        table(f"{storage}: resources", storage, [
            ("Startup to first healthy response, ms", "startup", field("startup_ms"), 1),
            ("Idle process memory (anon), MiB", "idle", field("anon_end"), mib),
            ("Idle cgroup usage (with page cache), MiB", "idle", field("usage_end"), mib),
            ("Peak memory, standard-64conn send, MiB", "standard-64conn/send", field("anon_peak"), mib),
            ("Server CPU seconds per 1,000 sends, 8 conns", "standard-8conn/send", per_thousand("sent"), 1),
            ("Server CPU seconds per 1,000 receive+delete, 8 conns", "standard-8conn/consume", per_thousand("received"), 1),
            ("Server cores used, 64-conn send", "standard-64conn/send", field("server_cpu_cores"), 1),
            ("Load generator cores used, 64-conn send (limit 4)", "standard-64conn/send", field("generator_cpu_cores"), 1),
            ("Memory with 1,000 long polls, MiB", "long-polls-1000", field("anon_end"), mib),
            ("Server cores with 1,000 long polls", "long-polls-1000", field("server_cpu_cores"), 1),
            ("Memory holding 100k backlog, MiB", "backlog/held", field("anon_end"), mib),
            ("Memory growth per backlog message, bytes", "backlog/held", lambda r: (r["anon_end"] - r["anon_before"]) / r["messages"] if r.get("messages") else None, 1),
            ("Disk with 100k backlog, MiB", "backlog/held", field("disk_bytes"), mib),
            ("Restart with backlog: startup ms", "backlog/restart", field("startup_ms"), 1),
            ("Restart with backlog: restored", "backlog/restart", field("restored"), 1),
            ("Restart with backlog: expected", "backlog/restart", field("expected"), 1),
            ("SIGKILL: acknowledged sends", "crash-sigkill", field("acked"), 1),
            ("SIGKILL: acknowledged sends lost", "crash-sigkill", field("lost_acked"), 1),
            ("1 GiB limit: idle memory, MiB", "small-memory/idle", field("anon_end"), mib),
            ("1 GiB limit: send, msg/s", "small-memory-warm/send", rate, 1),
            ("1 GiB limit: fill 50k, msg/s", "small-memory/fill-50000", rate, 1),
            ("1 GiB limit: memory after 50k, MiB", "small-memory/fill-50000", field("anon_end"), mib),
        ])
    print("\n### Exactly-once and error checks\n")
    problems = 0
    for row in rows:
        issues = {k: row[k] for k in ("errors", "duplicates", "server_died", "first_error", "failed") if row.get(k)}
        if row.get("complete") is False:
            issues["incomplete"] = f"{row.get('received')}/{row.get('expect', '?')}"
        if issues:
            problems += 1
            print(f"- {row['server']} {row['storage']} round {row['round']} {row['phase']}: {json.dumps(issues)[:300]}")
    print(f"\n{problems} phases reported a problem.")
    print(f"{len(disturbed)} of {sum('other_vm_cpu_percent' in r for r in rows)} measured phases left out for other VM work above {DISTURBED}%.")
    for row in disturbed:
        print(f"- {row['server']} {row['storage']} round {row['round']} {row['phase']}: {row['other_vm_cpu_percent']:.0f}%")
    other = [r["other_vm_cpu_percent"] for r in rows if "other_vm_cpu_percent" in r]
    if other:
        print(f"Other VM work during phases: median {statistics.median(other):.0f}%, highest {max(other):.0f}%.")
    generator = [r["generator_cpu_cores"] for r in rows if r.get("generator_cpu_cores") is not None]
    if generator:
        print(f"Load generator: highest {max(generator):.2f} of 4 cores.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["run", "report"])
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--quick", action="store_true", help="Short phases, to check the harness")
    parser.add_argument("--servers", default=",".join(SERVERS))
    parser.add_argument("--storages", default=",".join(STORAGES))
    parser.add_argument("--output", default=str(ROOT / "results" / "raw.jsonl"))
    options = parser.parse_args()
    if shutil.which("docker") is None:
        raise SystemExit("docker is required")
    {"run": run, "report": report}[options.command](options)


if __name__ == "__main__":
    main()
