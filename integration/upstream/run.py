"""Run ElasticMQ's unchanged Java SDK scenario traits against RusticMQ.

Requires a JDK, sbt, git, and a built target/debug/rusticmq binary.
Set RUSTICMQ_SBT_LAUNCHER to use a standalone sbt-launch.jar instead of sbt.
python3 integration/upstream/run.py [path/to/elasticmq]
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from http.client import HTTPException
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

REFERENCE = "b2abbc62954851b5244211fba75c3ac83fb6e0b2"
root = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("source", nargs="?", default=str(Path.home() / "Work/elasticmq"))
parser.add_argument("--nodejs", action="store_true", help="also run the original Node.js AWS SDK tests")
parser.add_argument("--image", help="test a Docker runtime image instead of the native executable")
parser.add_argument("--platform", help="Docker platform, e.g. linux/amd64 or linux/arm64")
args = parser.parse_args()
source = Path(args.source).expanduser().resolve()
checkout = root / "target/upstream-audit"
marker = checkout / ".source-revision"
if marker.exists():
    if marker.read_text().strip() != REFERENCE:
        raise SystemExit("Audit checkout belongs to another revision; remove target/upstream-audit before running")
else:
    checkout.mkdir(parents=True, exist_ok=True)
    archive = subprocess.Popen(["git", "-C", str(source), "archive", REFERENCE], stdout=subprocess.PIPE)
    unpack = subprocess.run(["tar", "-x", "-C", str(checkout)], stdin=archive.stdout)
    archive.stdout.close()
    if archive.wait() or unpack.returncode:
        raise SystemExit("Could not create the isolated upstream checkout")
    marker.write_text(REFERENCE + "\n")
settings = f'LocalProject("restSqsTestingAmazonJavaSdk") / Test / unmanagedSourceDirectories += file({json.dumps(str(root / "integration/upstream"))})\n'
(checkout / "rusticmq-audit.sbt").write_text(settings)
env = os.environ.copy()
if not env.get("JAVA_HOME"):
    prefix = subprocess.check_output(["brew", "--prefix", "openjdk"], text=True).strip()
    env["JAVA_HOME"] = str(Path(prefix) / "libexec/openjdk.jdk/Contents/Home")
server_env = {key: value for key, value in os.environ.items() if not key.startswith("RUSTICMQ_")}
server_env.update(RUSTICMQ_BIND="127.0.0.1:9321", RUSTICMQ_BASE_URL="http://localhost:9321",
    RUSTICMQ_ACCOUNT_ID="123456789012", RUSTICMQ_REGION="elasticmq")
with (checkout / "rusticmq-server.log").open("w") as server_log:
    container = "rusticmq-upstream-" + uuid.uuid4().hex if args.image else None
    if args.image:
        launch = ["docker", "run", "--rm", "--name", container, "--publish", "127.0.0.1:9321:9324"]
        if args.platform:
            launch += ["--platform", args.platform]
        for key, value in {**server_env, "RUSTICMQ_BIND": "0.0.0.0:9324"}.items():
            if key.startswith("RUSTICMQ_"):
                launch += ["--env", f"{key}={value}"]
        launch += [args.image]
    else:
        launch = [str(root / "target/debug/rusticmq")]
    server = subprocess.Popen(launch, env=server_env, stdout=server_log, stderr=server_log)
    try:
        deadline = time.monotonic() + 30
        while True:
            if server.poll() is not None:
                raise RuntimeError((checkout / "rusticmq-server.log").read_text())
            try:
                with urlopen("http://localhost:9321/health", timeout=0.3):
                    break
            except (URLError, TimeoutError, HTTPException, ConnectionError):
                if time.monotonic() >= deadline:
                    raise RuntimeError("RusticMQ did not become healthy")
                time.sleep(0.02)
        command = "restSqsTestingAmazonJavaSdk / Test / testOnly org.elasticmq.rest.sqs.integration.multisdk.RusticMqSdkV1TestSuite org.elasticmq.rest.sqs.integration.multisdk.RusticMqSdkV2TestSuite"
        launcher = os.environ.get("RUSTICMQ_SBT_LAUNCHER")
        sbt = [str(Path(env["JAVA_HOME"]) / "bin/java"), "-jar", str(Path(launcher).resolve())] if launcher else ["sbt", "-batch"]
        result = subprocess.run([*sbt, command], cwd=checkout, env=env)
        if result.returncode == 0 and args.nodejs:
            node_dir = checkout / "integration-tests/nodejs"
            result = subprocess.run(["npm", "ci", "--no-audit", "--no-fund"], cwd=node_dir, env=env)
            if result.returncode == 0:
                env.update(USE_LOCAL_SERVER="true", LOCAL_ENDPOINT="http://localhost:9321")
                result = subprocess.run(["npm", "test", "--", "--runInBand"], cwd=node_dir, env=env)
    finally:
        if container:
            subprocess.run(["docker", "stop", container], check=True, stdout=subprocess.DEVNULL)
        else:
            server.send_signal(signal.SIGTERM)
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
            raise RuntimeError("Server did not shut down gracefully")
        if server.returncode != 0:
            raise RuntimeError(f"Server exited with code {server.returncode}")
sys.exit(result.returncode)
