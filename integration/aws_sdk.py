"""Exercise a running RusticMQ server with the real AWS Python SDK.

RUSTICMQ_ENDPOINT=http://127.0.0.1:19324 python integration/aws_sdk.py
Requires boto3. Creates isolated queues and deletes them after each test.
"""
import hashlib
import json
import os
import time
import unittest
import uuid
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class SqsCompatibility(unittest.TestCase):
    def setUp(self):
        self.endpoint = os.environ.get("RUSTICMQ_ENDPOINT", "http://127.0.0.1:19324")
        self.client = boto3.client(
            "sqs", endpoint_url=self.endpoint, region_name="elasticmq",
            aws_access_key_id="x", aws_secret_access_key="x",
            config=Config(retries={"max_attempts": 0}, read_timeout=5),
        )
        self.queues = []

    def tearDown(self):
        for url in reversed(self.queues):
            self.client.delete_queue(QueueUrl=url)

    def create(self, fifo=False, **kwargs):
        name = "sdk-" + uuid.uuid4().hex + (".fifo" if fifo else "")
        attributes = kwargs.pop("Attributes", {})
        if fifo:
            attributes.update(FifoQueue="true", ContentBasedDeduplication="true")
        result = self.client.create_queue(QueueName=name, Attributes=attributes, **kwargs)
        url = result["QueueUrl"]
        self.queues.append(url)
        return url

    def test_standard_attributes_binary_and_delete(self):
        url = self.create(tags={"owner": "rust"})
        attributes = {
            "binary": {"DataType": "Binary", "BinaryValue": bytes([0, 1, 255])},
            "number": {"DataType": "Number.custom", "StringValue": "1.234567890123456789"},
            "text": {"DataType": "String", "StringValue": "雪"},
        }
        sent = self.client.send_message(QueueUrl=url, MessageBody="hello\r\n雪", MessageAttributes=attributes)
        self.assertEqual(sent["MD5OfMessageBody"], hashlib.md5("hello\r\n雪".encode()).hexdigest())
        message = self.client.receive_message(QueueUrl=url, MessageAttributeNames=["All"], MessageSystemAttributeNames=["All"])["Messages"][0]
        self.assertEqual(message["MessageId"], sent["MessageId"])
        self.assertEqual(message["MessageAttributes"], attributes)
        self.assertEqual(message["MD5OfMessageAttributes"], sent["MD5OfMessageAttributes"])
        self.client.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])
        self.assertFalse(self.client.receive_message(QueueUrl=url).get("Messages"))
        self.assertEqual(self.client.list_queue_tags(QueueUrl=url)["Tags"], {"owner": "rust"})

    def test_queue_lifecycle_attributes_permissions_and_idempotent_creation(self):
        url = self.create(Attributes={"VisibilityTimeout": "2", "DelaySeconds": "0"})
        name = url.rsplit("/", 1)[1]
        self.assertEqual(self.client.create_queue(QueueName=name)["QueueUrl"], url)
        with self.assertRaises(ClientError):
            self.client.create_queue(QueueName=name, Attributes={"VisibilityTimeout": "3"})
        self.assertIn(url, self.client.list_queues(QueueNamePrefix=name)["QueueUrls"])
        self.client.set_queue_attributes(QueueUrl=url, Attributes={"VisibilityTimeout": "5"})
        self.assertEqual(self.client.get_queue_attributes(QueueUrl=url, AttributeNames=["VisibilityTimeout"])["Attributes"], {"VisibilityTimeout": "5"})
        self.client.tag_queue(QueueUrl=url, Tags={"team": "rust", "remove": "yes"})
        self.client.untag_queue(QueueUrl=url, TagKeys=["remove"])
        self.assertEqual(self.client.list_queue_tags(QueueUrl=url)["Tags"], {"team": "rust"})
        self.client.add_permission(QueueUrl=url, Label="test", AWSAccountIds=["123456789012"], Actions=["SendMessage"])
        self.client.remove_permission(QueueUrl=url, Label="test")

    def test_message_attribute_selection_and_trace_header_precedence(self):
        url = self.create(Attributes={"VisibilityTimeout": "0"})
        attributes = {name: {"DataType": "String", "StringValue": name} for name in ("one", "two", "prefix.extra")}
        self.client.send_message(QueueUrl=url, MessageBody="hello", MessageAttributes=attributes,
            MessageSystemAttributes={"AWSTraceHeader": {"DataType": "String", "StringValue": "system"}})
        message = self.client.receive_message(QueueUrl=url)["Messages"][0]
        self.assertNotIn("MessageAttributes", message)
        message = self.client.receive_message(QueueUrl=url, MessageAttributeNames=["one", "prefix.*"],
            MessageSystemAttributeNames=["AWSTraceHeader"])["Messages"][0]
        self.assertEqual(set(message["MessageAttributes"]), {"one", "prefix.extra"})
        self.assertEqual(message["Attributes"], {"AWSTraceHeader": "system"})
        self.client.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])
        request = Request(self.endpoint, data=json.dumps({"QueueUrl":url,"MessageBody":"header",
            "MessageSystemAttributes":{"AWSTraceHeader":{"DataType":"String","StringValue":"system"}}}).encode(),
            headers={"Content-Type":"application/x-amz-json-1.0","X-Amz-Target":"AmazonSQS.SendMessage","X-Amzn-Trace-Id":"header"})
        with urlopen(request) as response:
            self.assertEqual(response.status, 200)
        message = self.client.receive_message(QueueUrl=url, MessageSystemAttributeNames=["AWSTraceHeader"])["Messages"][0]
        self.assertEqual(message["Attributes"]["AWSTraceHeader"], "system")

    def test_message_size_boundaries_and_purge(self):
        url = self.create()
        body = "x" * (1024 * 1024)
        sent = self.client.send_message(QueueUrl=url, MessageBody=body)
        message = self.client.receive_message(QueueUrl=url)["Messages"][0]
        self.assertEqual(message["Body"], body)
        self.assertEqual(message["MessageId"], sent["MessageId"])
        with self.assertRaises(ClientError):
            self.client.send_message(QueueUrl=url, MessageBody=body + "x")
        self.client.purge_queue(QueueUrl=url)
        self.assertFalse(self.client.receive_message(QueueUrl=url).get("Messages"))

    def test_invalid_attributes_fail_without_enqueuing_a_message(self):
        url = self.create()
        for attribute in [
            {"DataType":"String","StringValue":""},
            {"DataType":"Number","StringValue":"not-a-number"},
            {"DataType":"Number","StringValue":"1e127"},
            {"DataType":"Unknown","StringValue":"text"},
        ]:
            with self.subTest(attribute=attribute):
                with self.assertRaises(ClientError):
                    self.client.send_message(QueueUrl=url, MessageBody="hello", MessageAttributes={"invalid":attribute})
        self.assertFalse(self.client.receive_message(QueueUrl=url).get("Messages"))

    def test_fifo_delay_zero_uses_the_queue_delay_and_deduplication_survives_delete(self):
        url = self.create(fifo=True)
        first = self.client.send_message(QueueUrl=url, MessageBody="once", MessageGroupId="group", DelaySeconds=0)
        message = self.client.receive_message(QueueUrl=url)["Messages"][0]
        self.client.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])
        duplicate = self.client.send_message(QueueUrl=url, MessageBody="once", MessageGroupId="group")
        self.assertEqual(duplicate["MessageId"], first["MessageId"])
        self.assertFalse(self.client.receive_message(QueueUrl=url).get("Messages"))

    def test_batch_partial_failure(self):
        url = self.create()
        batch = self.client.send_message_batch(QueueUrl=url, Entries=[
            {"Id": "valid", "MessageBody": "hello"},
            {"Id": "invalid", "MessageBody": ""},
        ])
        self.assertEqual(batch["Successful"][0]["Id"], "valid")
        self.assertEqual(batch["Failed"][0]["Id"], "invalid")
        self.assertTrue(batch["Failed"][0]["SenderFault"])

    def test_fifo_deduplication_and_group_blocking(self):
        url = self.create(fifo=True)
        request = dict(QueueUrl=url, MessageBody="first", MessageGroupId="g")
        first = self.client.send_message(**request)
        self.assertEqual(first["MessageId"], self.client.send_message(**request)["MessageId"])
        self.client.send_message(QueueUrl=url, MessageBody="second", MessageGroupId="g")
        message = self.client.receive_message(QueueUrl=url)["Messages"][0]
        self.assertEqual(message["Body"], "first")
        self.assertFalse(self.client.receive_message(QueueUrl=url).get("Messages"))
        self.client.delete_message(QueueUrl=url, ReceiptHandle=message["ReceiptHandle"])
        self.assertEqual(self.client.receive_message(QueueUrl=url)["Messages"][0]["Body"], "second")

    def test_dead_letter_source_and_visibility_batch(self):
        dlq = self.create()
        arn = self.client.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        url = self.create(Attributes={"RedrivePolicy": json.dumps({"deadLetterTargetArn": arn, "maxReceiveCount": 1})})
        self.client.send_message(QueueUrl=url, MessageBody="poison")
        first = self.client.receive_message(QueueUrl=url)["Messages"][0]
        self.client.change_message_visibility_batch(QueueUrl=url, Entries=[
            {"Id": "one", "ReceiptHandle": first["ReceiptHandle"], "VisibilityTimeout": 0},
        ])
        self.assertFalse(self.client.receive_message(QueueUrl=url).get("Messages"))
        message = self.client.receive_message(QueueUrl=dlq, MessageSystemAttributeNames=["All"])["Messages"][0]
        self.assertEqual(message["MessageId"], first["MessageId"])
        self.assertIn(url, self.client.list_dead_letter_source_queues(QueueUrl=dlq)["queueUrls"])

    def test_long_poll_wakes_on_send(self):
        url = self.create()
        with ThreadPoolExecutor(max_workers=1) as executor:
            waiting = executor.submit(self.client.receive_message, QueueUrl=url, WaitTimeSeconds=2)
            self.client.send_message(QueueUrl=url, MessageBody="wake")
            self.assertEqual(waiting.result(timeout=3)["Messages"][0]["Body"], "wake")

    def test_error_is_deserialized_by_the_sdk(self):
        with self.assertRaises(self.client.exceptions.QueueDoesNotExist) as context:
            self.client.get_queue_url(QueueName="absent-" + uuid.uuid4().hex)
        self.assertEqual(context.exception.response["Error"]["Code"], "QueueDoesNotExist")

    def test_message_move_task_completion(self):
        source = self.create()
        target = self.create()
        def arn(url):
            return self.client.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        source_arn = arn(source)
        for index in range(3):
            self.client.send_message(QueueUrl=source, MessageBody=str(index))
        handle = self.client.start_message_move_task(SourceArn=source_arn, DestinationArn=arn(target))["TaskHandle"]
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            task = self.client.list_message_move_tasks(SourceArn=source_arn)["Results"][0]
            if task["Status"] == "COMPLETED":
                break
            time.sleep(0.01)
        self.assertEqual(task["Status"], "COMPLETED")
        self.assertEqual(task["TaskHandle"], handle)
        self.assertEqual(task["ApproximateNumberOfMessagesMoved"], 3)
        self.assertEqual(len(self.client.receive_message(QueueUrl=target, MaxNumberOfMessages=10)["Messages"]), 3)

    def test_rate_limited_move_task_cancellation(self):
        source = self.create()
        target = self.create()
        def arn(url):
            return self.client.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        for index in range(3):
            self.client.send_message(QueueUrl=source, MessageBody=str(index))
        handle = self.client.start_message_move_task(SourceArn=arn(source), DestinationArn=arn(target), MaxNumberOfMessagesPerSecond=1)["TaskHandle"]
        time.sleep(0.05)
        moved = self.client.cancel_message_move_task(TaskHandle=handle)["ApproximateNumberOfMessagesMoved"]
        self.assertEqual(moved, 1)
        time.sleep(1.05)
        self.assertEqual(len(self.client.receive_message(QueueUrl=source, MaxNumberOfMessages=10)["Messages"]), 2)

    def test_legacy_query_protocol_round_trip(self):
        url = self.create()
        def query(action, **kwargs):
            data = urlencode(dict(Action=action, Version="2012-11-05", **kwargs)).encode()
            request = Request(self.endpoint, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
            with urlopen(request, timeout=5) as response:
                return ET.fromstring(response.read())
        query("SendMessage", QueueUrl=url, MessageBody="<&\r\n雪")
        result = query("ReceiveMessage", QueueUrl=url)
        body = next(element.text for element in result.iter() if element.tag.endswith("}Body"))
        self.assertEqual(body, "<&\r\n雪")


if __name__ == "__main__":
    unittest.main(verbosity=2)
