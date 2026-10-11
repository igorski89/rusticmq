"""Build and verify all backend suites for Linux amd64 and arm64 Docker images.

Requires Docker/Buildx, boto3 in this Python environment, JDK/sbt, Node.js/npm,
and the pinned ElasticMQ source checkout. See BUILDING.md for invocation.
"""
import argparse
import os
import re
import json
import socket
import subprocess
import sys
import time
from http.client import HTTPException
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_TESTS = {"rust": 69, "aws_sdk": 14, "query": 3, "restart": 5, "container": 1,
    "upstream_java": 150, "upstream_node": 5}


def check_test_count(log_path, suite):
    output = log_path.read_text()
    if suite == "rust":
        results = re.findall(r"test result: ok\. (\d+) passed; 0 failed; 0 ignored; 0 measured; 0 filtered out", output)
        count = sum(map(int, results))
    elif suite == "upstream_java":
        results = re.findall(r"Tests: succeeded (\d+), failed 0, canceled 0, ignored 0, pending 0", output)
        count = sum(map(int, results))
    elif suite == "upstream_node":
        results = re.findall(r"Tests:\s+(\d+) passed, \d+ total", output)
        count = sum(map(int, results))
    else:
        results = re.findall(r"Ran (\d+) tests? in", output)
        count = sum(map(int, results))
    expected = EXPECTED_TESTS[suite]
    if count != expected:
        raise RuntimeError(f"{suite}: expected {expected} executed tests, found {count} in {log_path}")
    return count


def run(command, log_path, env=None):
    print(f"Running: {' '.join(map(str, command))}", flush=True)
    with log_path.open("w") as log:
        result = subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    if result.returncode:
        print(log_path.read_text(), file=sys.stderr)
        raise SystemExit(result.returncode)
    print(f"Passed: {log_path.name}", flush=True)


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def check_runtime(image, platform, report_dir):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"
    container = docker("run", "--detach", "--platform", platform,
        "--publish", f"127.0.0.1:{port}:9324", "--env", f"RUSTICMQ_BASE_URL={endpoint}", image)
    try:
        deadline = time.monotonic() + 30
        while True:
            try:
                with urlopen(endpoint + "/health", timeout=0.3) as response:
                    if response.read() != b"OK":
                        raise RuntimeError("Unexpected health response")
                    break
            except (URLError, TimeoutError, HTTPException, ConnectionError):
                if docker("inspect", "--format", "{{.State.Running}}", container) != "true" or time.monotonic() >= deadline:
                    raise RuntimeError(docker("logs", container))
                time.sleep(0.05)
        if docker("inspect", "--format", "{{.Config.User}}", container) != "10001:10001":
            raise RuntimeError("The runtime must use the non-root UID/GID")
        env = os.environ.copy()
        env["RUSTICMQ_ENDPOINT"] = endpoint
        for suite in ("aws_sdk", "query", "load"):
            command = [sys.executable, str(ROOT / "integration" / f"{suite}.py")]
            if suite == "load":
                command += ["1000"]
            run(command, report_dir / f"{suite}.log", env)
            if suite in EXPECTED_TESTS:
                check_test_count(report_dir / f"{suite}.log", suite)
        docker("stop", "--time", "10", container)
        if docker("inspect", "--format", "{{.State.ExitCode}}", container) != "0":
            raise RuntimeError("The runtime did not exit cleanly on SIGTERM")
    finally:
        docker("rm", "--force", container)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", action="append", choices=["linux/amd64", "linux/arm64"])
    parser.add_argument("--image-prefix", default="rusticmq:audit")
    parser.add_argument("--source", default=str(Path.home() / "Work/elasticmq"))
    parser.add_argument("--skip-build", action="store_true", help="use already built and tested images")
    args = parser.parse_args()
    for platform in args.platform or ["linux/amd64", "linux/arm64"]:
        architecture = platform.split("/")[1]
        image = f"{args.image_prefix}-{architecture}"
        report_dir = ROOT / "target/docker-audit" / architecture
        report_dir.mkdir(parents=True, exist_ok=True)
        (report_dir / "summary.json").unlink(missing_ok=True)
        if not args.skip_build:
            run(["docker", "buildx", "build", "--platform", platform, "--target", "test",
                "--no-cache-filter", "test", "--progress", "plain",
                "--output", "type=cacheonly", "."], report_dir / "rust-executed.log")
            check_test_count(report_dir / "rust-executed.log", "rust")
            run(["docker", "buildx", "build", "--platform", platform, "--load", "--tag", image, "."], report_dir / "build.log")
        if docker("image", "inspect", "--format", "{{.Architecture}}", image) != architecture:
            raise RuntimeError(f"{image} has the wrong architecture")
        check_runtime(image, platform, report_dir)
        env = os.environ.copy()
        env.update(RUSTICMQ_IMAGE=image, RUSTICMQ_PLATFORM=platform)
        for suite in ("restart", "container"):
            run([sys.executable, str(ROOT / "integration" / f"{suite}.py")], report_dir / f"{suite}.log", env)
            check_test_count(report_dir / f"{suite}.log", suite)
        run([sys.executable, str(ROOT / "integration/upstream/run.py"), args.source,
            "--nodejs", "--image", image, "--platform", platform], report_dir / "upstream.log")
        for suite in ("upstream_java", "upstream_node"):
            check_test_count(report_dir / "upstream.log", suite)
        summary = {"platform": platform, "image": image,
            "rust_tests_executed_this_run": not args.skip_build,
            "test_counts": {suite: count for suite, count in EXPECTED_TESTS.items()
                if suite != "rust" or not args.skip_build}}
        (report_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(f"All backend suites passed for {platform}: {image}", flush=True)


if __name__ == "__main__":
    main()
