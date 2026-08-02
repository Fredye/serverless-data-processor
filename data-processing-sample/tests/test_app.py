import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2] / "data-processing-layer" / "layer" / "python"))
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
os.environ.setdefault("TRANSACTIONS_TABLE", "unused")

from app import TransactionProcessor
from data_processing_framework import DataEvent, ProcessingContext


class SampleTests(unittest.TestCase):
    def test_rejects_missing_required_transaction_field(self):
        processor = TransactionProcessor(table=object())
        event = DataEvent.from_message({"event_id": "1", "source_system": "pos", "payload": {}})
        with self.assertRaisesRegex(ValueError, "transaction_id"):
            processor.validate(event)

    def test_rejects_non_positive_quantity(self):
        processor = TransactionProcessor(table=object())
        event = DataEvent.from_message({
            "event_id": "1", "source_system": "pos",
            "payload": {"transaction_id": "t", "line_id": "1", "store_id": "s", "quantity": 0, "unit_price": "2", "currency_code": "jpy"},
        })
        with self.assertRaisesRegex(ValueError, "quantity"):
            processor.validate(event)


if __name__ == "__main__":
    unittest.main()
