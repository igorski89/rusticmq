"""Build and verify all backend suites for Linux amd64 and arm64 Docker images.

Requires Docker/Buildx, boto3 in this Python environment, JDK/sbt, Node.js/npm,
and the pinned ElasticMQ source checkout. See BUILDING.md for invocation.
"""
import argparse
import os
import socket
import subprocess
import sys
import time
from http.client import HTTPException
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]


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
        if not args.skip_build:
            run(["docker", "buildx", "build", "--platform", platform, "--target", "test",
                "--output", "type=cacheonly", "."], report_dir / "rust.log")
            run(["docker", "buildx", "build", "--platform", platform, "--load", "--tag", image, "."], report_dir / "build.log")
        if docker("image", "inspect", "--format", "{{.Architecture}}", image) != architecture:
            raise RuntimeError(f"{image} has the wrong architecture")
        check_runtime(image, platform, report_dir)
        env = os.environ.copy()
        env.update(RUSTICMQ_IMAGE=image, RUSTICMQ_PLATFORM=platform)
        for suite in ("restart", "container"):
            run([sys.executable, str(ROOT / "integration" / f"{suite}.py")], report_dir / f"{suite}.log", env)
        run([sys.executable, str(ROOT / "integration/upstream/run.py"), args.source,
            "--nodejs", "--image", image, "--platform", platform], report_dir / "upstream.log")
        print(f"All backend suites passed for {platform}: {image}", flush=True)


if __name__ == "__main__":
    main()
