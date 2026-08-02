"""Common DB-API data-access helpers with commit/rollback handling."""

from contextlib import contextmanager
from typing import Any, Iterator, Mapping, Sequence

from .connection_pool import ConnectionPool


class DatabaseRepository:
    """Base repository for relational stores accessed from a Lambda function."""

    def __init__(self, pool: ConnectionPool):
        self._pool = pool

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        with self._pool.connection() as connection:
            try:
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> int:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, parameters)
                return cursor.rowcount

    def fetch_all(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> list[Any]:
        with self._pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, parameters)
                return list(cursor.fetchall())
