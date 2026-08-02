import unittest

from data_processing_framework.connection_pool import ConnectionPool
from data_processing_framework.job_management import DynamoDbJobRunManager
from data_processing_framework.message_parser import MessageParser
from data_processing_framework.models import DataEvent, ProcessingContext
from data_processing_framework.pipeline import ProcessingPipeline


class MemoryStore:
    def __init__(self):
        self.keys = set()

    def acquire(self, key):
        if key in self.keys:
            return False
        self.keys.add(key)
        return True


class ExampleProcessor:
    name = "example"

    def validate(self, event):
        if "value" not in event.payload:
            raise ValueError("value is required")

    def process(self, event, context):
        return {"value": event.payload["value"]}


class PipelineTests(unittest.TestCase):
    def test_duplicate_event_is_ignored(self):
        event = DataEvent.from_message({"event_id": "e-1", "source_system": "test", "payload": {"value": 1}})
        pipeline = ProcessingPipeline(ExampleProcessor(), MemoryStore())
        self.assertFalse(pipeline.execute(event, ProcessingContext("job", "trace")).duplicate)
        self.assertTrue(pipeline.execute(event, ProcessingContext("job", "trace")).duplicate)

    def test_parses_sns_wrapped_sqs_message(self):
        parser = MessageParser()
        record = {
            "messageId": "sqs-1",
            "body": '{"Type":"Notification","Message":"{\\"event_id\\":\\"e-1\\",\\"source_system\\":\\"pos\\",\\"payload\\":{}}"}',
        }
        parsed = parser.parse_sqs_record(record)
        self.assertEqual("sqs-1", parsed.message_id)
        self.assertEqual("e-1", parsed.data_event.event_id)

    def test_connection_is_reused_after_successful_use(self):
        connections = []

        class Connection:
            def close(self):
                pass

        def factory():
            connection = Connection()
            connections.append(connection)
            return connection

        pool = ConnectionPool(factory, max_size=1)
        with pool.connection() as first:
            pass
        with pool.connection() as second:
            pass
        self.assertIs(first, second)
        self.assertEqual(1, len(connections))

    def test_job_start_and_completion_are_persisted(self):
        conditional_exception = type("ConditionalCheckFailedException", (Exception,), {})

        class Client:
            class exceptions:
                ConditionalCheckFailedException = conditional_exception

            def __init__(self):
                self.put_calls = []
                self.update_calls = []

            def put_item(self, **kwargs):
                self.put_calls.append(kwargs)

            def update_item(self, **kwargs):
                self.update_calls.append(kwargs)

        client = Client()
        manager = DynamoDbJobRunManager("jobs", client=client)
        self.assertTrue(manager.start("job-1", event_id="event-1", source_system="pos", trace_id="trace-1"))
        manager.complete("job-1", metrics={"processed_records": 1})
        self.assertEqual("RUNNING", client.put_calls[0]["Item"]["status"]["S"])
        self.assertEqual("SUCCEEDED", client.update_calls[0]["ExpressionAttributeValues"][":status"]["S"])


if __name__ == "__main__":
    unittest.main()
