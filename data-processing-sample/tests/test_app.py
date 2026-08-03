import os
import sys
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[2] / "data-processing-layer" / "layer" / "python"))
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

import app
from transaction_processor import TransactionProcessor
from data_processing_framework import DataEvent, ProcessingContext


class FakeRepository:
    def __init__(self, inserted=True):
        self.items = []
        self.inserted = inserted

    def insert(self, item):
        self.items.append(item)
        return self.inserted


class FakeJobRuns:
    def __init__(self):
        self.calls = []

    def start(self, job_id, *, event_id, source_system, trace_id=None):
        self.calls.append(("start", job_id, event_id, source_system, trace_id))
        return True

    def complete(self, job_id, *, metrics=None):
        self.calls.append(("complete", job_id, metrics))

    def fail(self, job_id, *, reason):
        self.calls.append(("fail", job_id, reason))


class SampleTests(unittest.TestCase):
    def test_rejects_missing_required_transaction_field(self):
        processor = TransactionProcessor(repository=FakeRepository())
        event = DataEvent.from_message({"event_id": "1", "source_system": "pos", "payload": {}})
        with self.assertRaisesRegex(ValueError, "transaction_id"):
            processor.validate(event)

    def test_rejects_non_positive_quantity(self):
        processor = TransactionProcessor(repository=FakeRepository())
        event = DataEvent.from_message({
            "event_id": "1", "source_system": "pos",
            "payload": {"transaction_id": "t", "line_id": "1", "store_id": "s", "quantity": 0, "unit_price": "2", "currency_code": "jpy"},
        })
        with self.assertRaisesRegex(ValueError, "quantity"):
            processor.validate(event)

    def test_process_writes_normalized_item_to_repository(self):
        repository = FakeRepository()
        processor = TransactionProcessor(repository=repository)
        event = DataEvent.from_message({
            "event_id": "evt-1",
            "source_system": "pos",
            "payload": {
                "transaction_id": "t-1",
                "line_id": "1",
                "store_id": "s-1",
                "quantity": 2,
                "unit_price": "3.50",
                "currency_code": "jpy",
            },
        })
        context = ProcessingContext(job_id="job-1", trace_id="trace-1")

        result = processor.process(event, context)

        self.assertTrue(result["inserted"])
        self.assertEqual(len(repository.items), 1)
        item = repository.items[0]
        self.assertEqual(item["event_id"], "evt-1")
        self.assertEqual(item["currency_code"], "JPY")
        self.assertEqual(item["gross_amount"], Decimal("7.00"))
        self.assertEqual(item["source_system"], "pos")
        self.assertEqual(item["trace_id"], "trace-1")

    def test_process_treats_duplicate_event_as_success(self):
        repository = FakeRepository(inserted=False)
        processor = TransactionProcessor(repository=repository)
        event = DataEvent.from_message({
            "event_id": "evt-2",
            "source_system": "pos",
            "payload": {
                "transaction_id": "t-2",
                "line_id": "2",
                "store_id": "s-2",
                "quantity": 1,
                "unit_price": "4.00",
                "currency_code": "jpy",
            },
        })

        result = processor.process(event, ProcessingContext(job_id="job-2", trace_id="trace-2"))

        self.assertFalse(result["inserted"])
        self.assertEqual(len(repository.items), 1)

    def test_lambda_handler_processes_successful_record(self):
        parsed = SimpleNamespace(
            message_id="msg-1",
            data_event=DataEvent.from_message({
                "event_id": "evt-1",
                "source_system": "pos",
                "payload": {
                    "transaction_id": "t-1",
                    "line_id": "1",
                    "store_id": "s-1",
                    "quantity": 2,
                    "unit_price": "3.50",
                    "currency_code": "jpy",
                },
            }),
            raw_message={"job_id": "job-1", "trace_id": "trace-1", "attempt": 2},
        )
        fake_job_runs = FakeJobRuns()
        fake_pipeline = SimpleNamespace(execute=lambda event, context: SimpleNamespace(status="SUCCEEDED", duplicate=False))

        with patch.object(app.MESSAGE_PARSER, "parse_sqs_record", return_value=parsed), \
            patch.object(app, "build_pipeline", return_value=fake_pipeline), \
            patch.object(app, "build_job_run_manager", return_value=fake_job_runs):
            result = app.lambda_handler({"Records": [{"messageId": "msg-1", "body": "{}"}]}, None)

        self.assertEqual(result, {"batchItemFailures": []})
        self.assertEqual(
            fake_job_runs.calls,
            [
                ("start", "job-1", "evt-1", "pos", "trace-1"),
                ("complete", "job-1", {"processed_records": 1, "duplicate": False}),
            ],
        )

    def test_lambda_handler_marks_failure_for_processing_error(self):
        parsed = SimpleNamespace(
            message_id="msg-2",
            data_event=DataEvent.from_message({
                "event_id": "evt-2",
                "source_system": "pos",
                "payload": {
                    "transaction_id": "t-2",
                    "line_id": "2",
                    "store_id": "s-2",
                    "quantity": 1,
                    "unit_price": "4.00",
                    "currency_code": "jpy",
                },
            }),
            raw_message={"job_id": "job-2", "trace_id": "trace-2"},
        )
        fake_job_runs = FakeJobRuns()

        class FailingPipeline:
            def execute(self, event, context):
                raise RuntimeError("boom")

        with patch.object(app.MESSAGE_PARSER, "parse_sqs_record", return_value=parsed), \
            patch.object(app, "build_pipeline", return_value=FailingPipeline()), \
            patch.object(app, "build_job_run_manager", return_value=fake_job_runs):
            result = app.lambda_handler({"Records": [{"messageId": "msg-2", "body": "{}"}]}, None)

        self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "msg-2"}]})
        self.assertEqual(
            fake_job_runs.calls,
            [
                ("start", "job-2", "evt-2", "pos", "trace-2"),
                ("fail", "job-2", "See Lambda log for failure details"),
            ],
        )

    def test_lambda_handler_reports_parse_failure(self):
        fake_job_runs = FakeJobRuns()

        with patch.object(app.MESSAGE_PARSER, "parse_sqs_record", side_effect=ValueError("bad body")), \
            patch.object(app, "build_pipeline", return_value=SimpleNamespace(execute=lambda *args, **kwargs: None)), \
            patch.object(app, "build_job_run_manager", return_value=fake_job_runs):
            result = app.lambda_handler({"Records": [{"messageId": "msg-3", "body": "broken"}]}, None)

        self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "msg-3"}]})
        self.assertEqual(fake_job_runs.calls, [])


if __name__ == "__main__":
    unittest.main()
