"""Verify the built image, non-root runtime and SQLite named-volume restarts.

docker build -t rusticmq:local .
target/sdk-venv/bin/python integration/container.py
"""
import os
import subprocess
import time
import unittest
import uuid
from http.client import HTTPException
from urllib.error import URLError
from urllib.request import urlopen

import boto3
from botocore.config import Config


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


class ContainerTests(unittest.TestCase):
    def test_non_root_sqlite_volume_survives_container_replacement(self):
        volume = "rusticmq-test-" + uuid.uuid4().hex
        image = os.environ.get("RUSTICMQ_IMAGE", "rusticmq:local")
        platform = os.environ.get("RUSTICMQ_PLATFORM")
        container = None
        docker("volume", "create", volume)
        try:
            for generation in range(3):
                options = ["--platform", platform] if platform else []
                container = docker("run", *options, "--detach", "--rm", "--publish", "127.0.0.1::9324",
                    "--mount", f"type=volume,source={volume},target=/data",
                    image, "--database", "/data/messages.sqlite")
                address = docker("port", container, "9324/tcp").splitlines()[0]
                endpoint = "http://" + address
                deadline = time.monotonic() + 10
                while True:
                    try:
                        with urlopen(endpoint + "/health", timeout=0.3):
                            break
                    except (URLError, TimeoutError, HTTPException, ConnectionError):
                        if time.monotonic() >= deadline:
                            self.fail(docker("logs", container))
                        time.sleep(0.02)
                # The scratch runtime deliberately has no shell or id executable.
                self.assertEqual(docker("inspect", "--format", "{{.Config.User}}", container), "10001:10001")
                client = boto3.client("sqs", endpoint_url=endpoint, region_name="elasticmq",
                    aws_access_key_id="x", aws_secret_access_key="x",
                    config=Config(retries={"max_attempts": 0}))
                if generation == 0:
                    url = client.create_queue(QueueName="durable", tags={"runtime": "container"})["QueueUrl"]
                    client.send_message(QueueUrl=url, MessageBody="on disk")
                    message = client.receive_message(QueueUrl=url)["Messages"][0]
                else:
                    self.assertEqual(client.get_queue_url(QueueName="durable")["QueueUrl"], url)
                    self.assertEqual(client.list_queue_tags(QueueUrl=url)["Tags"], {"runtime": "container"})
                    self.assertFalse(client.receive_message(QueueUrl=url).get("Messages"))
                    if generation == 2:
                        attributes = client.get_queue_attributes(QueueUrl=url, AttributeNames=[
                            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
                            "ApproximateNumberOfMessagesDelayed"])["Attributes"]
                        self.assertEqual(set(attributes.values()), {"0"})
                    if generation == 1:
                        client.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])
                docker("stop", container)
                container = None
        finally:
            if container:
                docker("stop", container)
            docker("volume", "rm", volume)


if __name__ == "__main__":
    unittest.main(verbosity=2)
