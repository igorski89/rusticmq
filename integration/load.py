"""Concurrent SDK load check for standard and FIFO queues.

RUSTICMQ_ENDPOINT=http://127.0.0.1:19324 python integration/load.py [messages]
Reports local throughput; asserts every message is delivered exactly once in
this successful send/receive/delete workload. Requires boto3.
"""
import os
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config


def run(fifo, count):
    endpoint = os.environ.get("RUSTICMQ_ENDPOINT", "http://127.0.0.1:19324")
    client = boto3.client("sqs", endpoint_url=endpoint, region_name="elasticmq",
        aws_access_key_id="x", aws_secret_access_key="x",
        config=Config(retries={"max_attempts": 0}, read_timeout=10, max_pool_connections=16))
    name = "load-" + uuid.uuid4().hex + (".fifo" if fifo else "")
    attributes = {"FifoQueue":"true"} if fifo else {}
    url = client.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]
    received = set()
    lock = threading.Lock()
    started = time.monotonic()
    try:
        for offset in range(0, count, 10):
            entries = []
            for number in range(offset, min(offset + 10, count)):
                entry = {"Id":str(number), "MessageBody":str(number)}
                if fifo:
                    entry.update(MessageGroupId=str(number % 8), MessageDeduplicationId=str(number))
                entries.append(entry)
            result = client.send_message_batch(QueueUrl=url, Entries=entries)
            assert not result.get("Failed"), result
        sent_at = time.monotonic()

        def consume():
            while True:
                messages = client.receive_message(QueueUrl=url, MaxNumberOfMessages=10).get("Messages", [])
                if not messages:
                    return
                with lock:
                    for message in messages:
                        value = int(message["Body"])
                        assert value not in received, f"Duplicate delivery: {value}"
                        received.add(value)
                entries = [{"Id":str(index),"ReceiptHandle":message["ReceiptHandle"]} for index, message in enumerate(messages)]
                result = client.delete_message_batch(QueueUrl=url, Entries=entries)
                assert not result.get("Failed"), result

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(consume) for _ in range(8)]
            for future in futures:
                future.result()
        finished = time.monotonic()
        assert received == set(range(count)), f"Missing {count - len(received)} messages"
        assert not client.receive_message(QueueUrl=url).get("Messages")
        print(f"{'FIFO' if fifo else 'standard'}: {count} messages, send {count / (sent_at - started):.0f}/s, receive+delete {count / (finished - sent_at):.0f}/s")
    finally:
        client.delete_queue(QueueUrl=url)


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    if count < 1:
        raise SystemExit("Message count must be positive")
    run(False, count)
    run(True, count)
