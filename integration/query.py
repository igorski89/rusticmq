"""Exercise all 23 SQS actions through the Query/XML protocol.

RUSTICMQ_ENDPOINT=http://127.0.0.1:19324 python integration/query.py
Uses the Python standard library; creates and removes isolated queues.
"""
import hashlib
import os
import time
import unittest
import uuid
import xml.etree.ElementTree as ET
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class QueryCompatibility(unittest.TestCase):
    def setUp(self):
        self.endpoint = os.environ.get("RUSTICMQ_ENDPOINT", "http://127.0.0.1:19324")
        self.queues = []

    def tearDown(self):
        for url in reversed(self.queues):
            self.call("DeleteQueue", QueueUrl=url)

    def call(self, action, error=False, **params):
        body = urlencode({"Action":action, "Version":"2012-11-05", **params}).encode()
        request = Request(self.endpoint, data=body, headers={"Content-Type":"application/x-www-form-urlencoded"})
        try:
            with urlopen(request, timeout=5) as response:
                payload = response.read()
                self.assertFalse(error, "Expected a Query protocol error")
        except HTTPError as response:
            with response:
                payload = response.read()
            self.assertTrue(error, payload.decode())
        document = ET.fromstring(payload)
        for element in document.iter():
            element.tag = element.tag.rsplit("}", 1)[-1]
        return document

    def create(self, attributes=None, tags=None):
        name = "query-" + uuid.uuid4().hex
        if attributes and attributes.get("FifoQueue") == "true":
            name += ".fifo"
        params = {"QueueName":name}
        for index, (key, value) in enumerate((attributes or {}).items(), 1):
            params.update({f"Attribute.{index}.Name":key, f"Attribute.{index}.Value":value})
        for index, (key, value) in enumerate((tags or {}).items(), 1):
            params.update({f"Tag.{index}.Key":key, f"Tag.{index}.Value":value})
        url = self.call("CreateQueue", **params).findtext(".//QueueUrl")
        self.queues.append(url)
        return url

    def test_queue_lifecycle_attributes_tags_permissions_and_listing(self):
        url = self.create(tags={"owner":"rust"})
        name = url.rsplit("/", 1)[1]
        self.assertEqual(self.call("GetQueueUrl", QueueName=name).findtext(".//QueueUrl"), url)
        urls = self.call("ListQueues", QueueNamePrefix=name, MaxResults=1, NextToken="ignored")
        self.assertEqual([element.text for element in urls.findall(".//QueueUrl")], [url])
        self.call("SetQueueAttributes", **{"QueueUrl":url,"Attribute.1.Name":"VisibilityTimeout","Attribute.1.Value":"2"})
        attributes = self.call("GetQueueAttributes", **{"QueueUrl":url,"AttributeName.1":"VisibilityTimeout"})
        self.assertEqual(attributes.findtext(".//Attribute/Value"), "2")
        self.call("TagQueue", **{"QueueUrl":url,"Tag.1.Key":"team","Tag.1.Value":"rust"})
        self.call("UntagQueue", **{"QueueUrl":url,"TagKey.1":"owner"})
        tags = self.call("ListQueueTags", QueueUrl=url)
        self.assertEqual([(tag.findtext("Key"),tag.findtext("Value")) for tag in tags.findall(".//Tag")], [("team","rust")])
        self.call("AddPermission", **{"QueueUrl":url,"Label":"test","AWSAccountId.1":"123456789012","ActionName.1":"SendMessage"})
        self.call("RemovePermission", QueueUrl=url, Label="test")
        self.call("DeleteQueue", QueueUrl=url)
        self.queues.remove(url)
        error = self.call("GetQueueUrl", error=True, QueueName=name)
        self.assertEqual(error.findtext(".//Code"), "AWS.SimpleQueueService.NonExistentQueue")

    def test_message_attributes_batches_visibility_receipts_and_purge(self):
        url = self.create()
        body = "<escaped>&\r\n雪"
        sent = self.call("SendMessage", **{"QueueUrl":url,"MessageBody":body,
            "MessageAttribute.1.Name":"binary", "MessageAttribute.1.Value.DataType":"Binary.raw",
            "MessageAttribute.1.Value.BinaryValue":"AP8="})
        self.assertEqual(sent.findtext(".//MD5OfMessageBody"), hashlib.md5(body.encode()).hexdigest())
        received = self.call("ReceiveMessage", **{"QueueUrl":url,"MessageAttributeName.1":"All"})
        self.assertEqual(received.findtext(".//Message/Body"), body)
        self.assertEqual(received.findtext(".//MessageAttribute/Value/BinaryValue"), "AP8=")
        receipt = received.findtext(".//ReceiptHandle")
        self.call("ChangeMessageVisibility", QueueUrl=url, ReceiptHandle=receipt, VisibilityTimeout=0)
        again = self.call("ReceiveMessage", QueueUrl=url)
        self.call("DeleteMessage", QueueUrl=url, ReceiptHandle=again.findtext(".//ReceiptHandle"))
        result = self.call("SendMessageBatch", **{"QueueUrl":url,
            "SendMessageBatchRequestEntry.1.Id":"good", "SendMessageBatchRequestEntry.1.MessageBody":"hello",
            "SendMessageBatchRequestEntry.2.Id":"bad", "SendMessageBatchRequestEntry.2.MessageBody":""})
        self.assertEqual(result.findtext(".//SendMessageBatchResultEntry/Id"), "good")
        self.assertEqual(result.findtext(".//BatchResultErrorEntry/Id"), "bad")
        received = self.call("ReceiveMessage", QueueUrl=url)
        receipt = received.findtext(".//ReceiptHandle")
        result = self.call("ChangeMessageVisibilityBatch", **{"QueueUrl":url,
            "ChangeMessageVisibilityBatchRequestEntry.1.Id":"good",
            "ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle":receipt,
            "ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout":0})
        self.assertEqual(result.findtext(".//ChangeMessageVisibilityBatchResultEntry/Id"), "good")
        received = self.call("ReceiveMessage", QueueUrl=url)
        result = self.call("DeleteMessageBatch", **{"QueueUrl":url,
            "DeleteMessageBatchRequestEntry.1.Id":"good",
            "DeleteMessageBatchRequestEntry.1.ReceiptHandle":received.findtext(".//ReceiptHandle")})
        self.assertEqual(result.findtext(".//DeleteMessageBatchResultEntry/Id"), "good")
        self.call("SendMessage", QueueUrl=url, MessageBody="purged")
        self.call("PurgeQueue", QueueUrl=url)
        self.assertIsNone(self.call("ReceiveMessage", QueueUrl=url).find(".//Message"))

    def test_dead_letter_sources_and_move_task_lifecycle(self):
        import json
        dlq = self.create()
        arn = self.call("GetQueueAttributes", **{"QueueUrl":dlq,"AttributeName.1":"QueueArn"}).findtext(".//Attribute/Value")
        source = self.create({"VisibilityTimeout":"0", "RedrivePolicy":json.dumps({"deadLetterTargetArn":arn,"maxReceiveCount":1})})
        self.assertEqual(self.call("ListDeadLetterSourceQueues", QueueUrl=dlq).findtext(".//QueueUrl"), source)
        self.call("SendMessage", QueueUrl=source, MessageBody="redrive")
        self.call("ReceiveMessage", QueueUrl=source)
        self.assertIsNone(self.call("ReceiveMessage", QueueUrl=source).find(".//Message"))
        task = self.call("StartMessageMoveTask", SourceArn=arn).findtext(".//TaskHandle")
        deadline = time.monotonic() + 3
        while True:
            tasks = self.call("ListMessageMoveTasks", SourceArn=arn)
            if tasks.findtext(".//Status") == "COMPLETED":
                break
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.02)
        self.assertEqual(tasks.findtext(".//TaskHandle"), task)
        self.assertEqual(tasks.findtext(".//ApproximateNumberOfMessagesMoved"), "1")
        for number in range(3):
            self.call("SendMessage", QueueUrl=dlq, MessageBody=str(number))
        task = self.call("StartMessageMoveTask", SourceArn=arn, MaxNumberOfMessagesPerSecond=1).findtext(".//TaskHandle")
        cancelled = self.call("CancelMessageMoveTask", TaskHandle=task)
        self.assertIn(int(cancelled.findtext(".//ApproximateNumberOfMessagesMoved")), (0,1))


if __name__ == "__main__":
    unittest.main(verbosity=2)
