import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[2] / "data-processing-layer" / "layer" / "python"))
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

import transaction_repository
from data_processing_framework.data_access import DatabaseRepository
from transaction_repository import PostgresTransactionRepository


class PostgresTransactionRepositoryTests(unittest.TestCase):
    def tearDown(self):
        PostgresTransactionRepository.clear_default()

    def test_insert_uses_framework_database_repository(self):
        with patch.object(DatabaseRepository, "execute", return_value=1) as execute:
            repository = PostgresTransactionRepository(pool=object())
            inserted = repository.insert({"event_id": "e-1", "currency_code": "JPY"})

        self.assertTrue(inserted)
        sql, parameters = execute.call_args.args
        self.assertIn("INSERT INTO processed_transactions", sql)
        self.assertIn("ON CONFLICT (event_id) DO NOTHING", sql)
        self.assertEqual(parameters["event_id"], "e-1")

    def test_insert_reports_duplicate_when_no_row_written(self):
        with patch.object(DatabaseRepository, "execute", return_value=0):
            repository = PostgresTransactionRepository(pool=object())
            inserted = repository.insert({"event_id": "e-2"})

        self.assertFalse(inserted)

    def test_get_repository_reuses_module_scope_pool(self):
        pools = []

        def fake_build_pool():
            pool = object()
            pools.append(pool)
            return pool

        with patch.object(PostgresTransactionRepository, "build_pool", side_effect=fake_build_pool):
            first = transaction_repository.get_repository()
            second = transaction_repository.get_repository()

        self.assertIs(first, second)
        self.assertEqual(len(pools), 1)


if __name__ == "__main__":
    unittest.main()
