import os
import sys
import types
import unittest
from unittest.mock import Mock, patch

from data_processing_framework import BaseRepository
from data_processing_framework.connection_pool import ConnectionPool
from data_processing_framework.data_access import DatabaseRepository


class FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class TestRepository(BaseRepository):
    created = []

    @staticmethod
    def connection_factory():
        connection = FakeConnection()
        TestRepository.created.append(connection)
        return connection


class BaseRepositoryTests(unittest.TestCase):
    def setUp(self):
        TestRepository.clear_default()
        TestRepository.created.clear()

    def tearDown(self):
        TestRepository.clear_default()
        TestRepository.created.clear()

    def test_build_pool_reads_environment_settings(self):
        with patch.dict(
            os.environ,
            {"DB_POOL_MAX_SIZE": "2", "DB_POOL_TIMEOUT_SECONDS": "3.5"},
            clear=False,
        ):
            pool = TestRepository.build_pool()
        self.assertIsInstance(pool, ConnectionPool)
        self.assertEqual(pool._max_size, 2)
        self.assertEqual(pool._timeout_seconds, 3.5)

    def test_pool_uses_subclass_connection_factory_and_reuses_connections(self):
        pool = TestRepository.build_pool()
        with pool.connection() as first:
            pass
        with pool.connection() as second:
            pass
        self.assertIs(first, second)
        self.assertEqual(1, len(TestRepository.created))

    def test_default_returns_process_wide_singleton(self):
        first = TestRepository.default()
        second = TestRepository.default()
        self.assertIs(first, second)

    def test_execute_delegates_to_database_repository(self):
        with patch.object(DatabaseRepository, "execute", return_value=3) as execute:
            repository = TestRepository(pool=object())
            rowcount = repository.execute("SELECT 1", [])
        self.assertEqual(3, rowcount)
        self.assertEqual(("SELECT 1", []), execute.call_args.args)

    def test_connection_factory_builds_psycopg_connection_from_environment(self):
        fake_psycopg = types.ModuleType("psycopg")
        fake_psycopg.connect = Mock(return_value=FakeConnection())
        env = {
            "DB_HOST": "db.example.com",
            "DB_PORT": "5433",
            "DB_NAME": "sales",
            "DB_USER": "app",
            "DB_PASSWORD": "secret",
            "DB_CONNECT_TIMEOUT_SECONDS": "7",
        }

        with patch.dict(sys.modules, {"psycopg": fake_psycopg}), patch.dict(os.environ, env):
            connection = BaseRepository.connection_factory()

        self.assertIsInstance(connection, FakeConnection)
        fake_psycopg.connect.assert_called_once_with(
            host="db.example.com",
            port=5433,
            dbname="sales",
            user="app",
            password="secret",
            connect_timeout=7,
        )

    def test_connection_factory_applies_default_port_and_timeout(self):
        fake_psycopg = types.ModuleType("psycopg")
        fake_psycopg.connect = Mock(return_value=FakeConnection())
        env = {"DB_HOST": "db.example.com", "DB_NAME": "sales", "DB_USER": "app", "DB_PASSWORD": "secret"}

        with patch.dict(sys.modules, {"psycopg": fake_psycopg}), patch.dict(os.environ, env):
            BaseRepository.connection_factory()

        kwargs = fake_psycopg.connect.call_args.kwargs
        self.assertEqual(5432, kwargs["port"])
        self.assertEqual(5, kwargs["connect_timeout"])

    def test_subclass_can_override_connection_factory(self):
        with patch.dict(os.environ, {}, clear=True):
            repository = TestRepository()
            with repository._pool.connection():
                pass
        self.assertEqual(1, len(TestRepository.created))


if __name__ == "__main__":
    unittest.main()
