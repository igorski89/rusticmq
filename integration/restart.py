"""Verify persistence across real server process restarts. Requires boto3.

Build with cargo build, then run this file using the SDK test virtual environment.
"""
import os
import signal
import socket
import subprocess
import tempfile
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPException
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class RestartTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.binary = Path(os.environ.get("RUSTICMQ_BINARY", "target/debug/rusticmq")).resolve()
        self.image = os.environ.get("RUSTICMQ_IMAGE")
        self.platform = os.environ.get("RUSTICMQ_PLATFORM")
        self.container = None
        if self.image:
            # The runtime keeps its non-root UID; the temporary bind mount must
            # allow that UID to create database and queue-definition files.
            self.root.chmod(0o777)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.process = None
        self.addCleanup(self.stop)

    def start(self, args=(), queues_file=None, context=""):
        endpoint = f"http://127.0.0.1:{self.port}"
        env = {k: v for k, v in os.environ.items() if not k.startswith("RUSTICMQ_")}
        env.update(RUSTICMQ_BIND=f"127.0.0.1:{self.port}", RUSTICMQ_BASE_URL=endpoint + context)
        if queues_file:
            env["RUSTICMQ_QUEUES_STORAGE"] = str(queues_file)
        if self.image:
            self.container = "rusticmq-restart-" + uuid.uuid4().hex
            command = ["docker", "run", "--rm", "--name", self.container,
                "--publish", f"127.0.0.1:{self.port}:9324",
                "--mount", f"type=bind,source={self.root},target={self.root}"]
            if self.platform:
                command += ["--platform", self.platform]
            for key, value in {**env, "RUSTICMQ_BIND": "0.0.0.0:9324"}.items():
                if key.startswith("RUSTICMQ_"):
                    command += ["--env", f"{key}={value}"]
            command += [self.image, *map(str, args)]
        else:
            command = [str(self.binary), *map(str, args)]
        self.process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                stderr = self.process.communicate()[1].decode()
                self.process = None
                self.fail(stderr)
            try:
                with urlopen(endpoint + "/health", timeout=0.2) as response:
                    if response.status == 200:
                        break
            except (URLError, TimeoutError, HTTPException, ConnectionError):
                time.sleep(0.02)
        else:
            self.fail("Server did not become healthy")
        self.client = boto3.client("sqs", endpoint_url=endpoint + context, region_name="elasticmq",
            aws_access_key_id="x", aws_secret_access_key="x",
            config=Config(retries={"max_attempts": 0}, read_timeout=3))

    def stop(self):
        if self.process is not None:
            if self.container:
                subprocess.run(["docker", "stop", self.container], check=True, stdout=subprocess.DEVNULL)
                self.container = None
            else:
                self.process.send_signal(signal.SIGTERM)
            try:
                _, stderr = self.process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.communicate()
                self.fail("Server did not shut down on SIGTERM")
            self.assertEqual(self.process.returncode, 0, stderr.decode())
            self.process = None

    def test_sqlite_file_restores_messages_and_valid_receipts(self):
        args = ["--database", self.root / "messages.sqlite"]
        self.start(args)
        url = self.client.create_queue(QueueName="durable", tags={"team": "rust"})["QueueUrl"]
        sent = self.client.send_message(QueueUrl=url, MessageBody="persisted")
        message = self.client.receive_message(QueueUrl=url)["Messages"][0]
        self.stop()
        self.start(args)
        self.assertEqual(self.client.get_queue_url(QueueName="durable")["QueueUrl"], url)
        self.assertEqual(self.client.list_queue_tags(QueueUrl=url)["Tags"], {"team": "rust"})
        self.assertFalse(self.client.receive_message(QueueUrl=url).get("Messages"))
        self.client.change_message_visibility(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"], VisibilityTimeout=0)
        again = self.client.receive_message(QueueUrl=url, MessageSystemAttributeNames=["All"])["Messages"][0]
        self.assertEqual(again["MessageId"], sent["MessageId"])
        self.assertEqual(again["Attributes"]["ApproximateReceiveCount"], "2")
        self.client.delete_message(QueueUrl=url, ReceiptHandle=again["ReceiptHandle"])
        self.stop()
        self.start(args)
        attributes = self.client.get_queue_attributes(QueueUrl=url, AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed"])["Attributes"]
        self.assertEqual(set(attributes.values()), {"0"})
        self.assertFalse(self.client.receive_message(QueueUrl=url).get("Messages"))

    def test_sigterm_finishes_an_active_long_poll(self):
        self.start()
        url = self.client.create_queue(QueueName="shutdown")["QueueUrl"]
        client = boto3.client("sqs", endpoint_url=f"http://127.0.0.1:{self.port}",
            region_name="elasticmq", aws_access_key_id="x", aws_secret_access_key="x",
            config=Config(retries={"max_attempts": 0}, read_timeout=25))
        with ThreadPoolExecutor(max_workers=1) as executor:
            waiting = executor.submit(client.receive_message, QueueUrl=url, WaitTimeSeconds=20)
            time.sleep(0.2)
            self.assertFalse(waiting.done())
            started = time.monotonic()
            self.stop()
            self.assertLess(time.monotonic() - started, 3, "Shutdown waited for the long-poll deadline")
            self.assertFalse(waiting.result(timeout=3).get("Messages"))

    def test_default_memory_database_is_ephemeral(self):
        self.start()
        self.client.create_queue(QueueName="ephemeral")
        self.stop()
        self.start()
        with self.assertRaises(ClientError):
            self.client.get_queue_url(QueueName="ephemeral")

    def test_definition_file_restores_queues_but_not_messages(self):
        definitions = self.root / "queues.conf"
        self.start(queues_file=definitions)
        url = self.client.create_queue(QueueName="definitions-only")["QueueUrl"]
        self.client.send_message(QueueUrl=url, MessageBody="in memory")
        self.stop()
        self.start(queues_file=definitions)
        self.assertEqual(self.client.get_queue_url(QueueName="definitions-only")["QueueUrl"], url)
        self.assertFalse(self.client.receive_message(QueueUrl=url).get("Messages"))

    def test_hocon_startup_queues_context_and_copy_route(self):
        config = self.root / "elasticmq.conf"
        config.write_text('''
            node-address { context-path = "sqs" }
            queues {
                a-source { copyTo = "z-target", defaultVisibilityTimeout = 2 seconds }
                z-target {}
            }
        ''')
        self.start(["--config", config], context="/sqs")
        source = self.client.get_queue_url(QueueName="a-source")["QueueUrl"]
        target = self.client.get_queue_url(QueueName="z-target")["QueueUrl"]
        self.assertIn("/sqs/", source)
        self.client.send_message(QueueUrl=source, MessageBody="copied")
        self.assertEqual(self.client.receive_message(QueueUrl=target)["Messages"][0]["Body"], "copied")


if __name__ == "__main__":
    unittest.main(verbosity=2)
