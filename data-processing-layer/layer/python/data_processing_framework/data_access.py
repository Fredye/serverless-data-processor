"""Common DB-API data-access helpers with commit/rollback handling."""

import os
from contextlib import contextmanager
from typing import Any, Callable, ClassVar, Iterator, Mapping, Sequence

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


class BaseRepository(DatabaseRepository):
    """Repository base that owns a pooled DB-API connection.

    Provides a default psycopg ``connection_factory`` driven by ``DB_HOST`` /
    ``DB_PORT`` / ``DB_NAME`` / ``DB_USER`` / ``DB_PASSWORD``; subclasses can
    override it (or the ``*_env`` class attributes) for other drivers. The pool
    is built from ``DB_POOL_MAX_SIZE`` and ``DB_POOL_TIMEOUT_SECONDS``, and kept
    as a process-wide singleton via :meth:`default` so warm Lambda invocations
    reuse connections.
    """

    host_env = "DB_HOST"
    port_env = "DB_PORT"
    dbname_env = "DB_NAME"
    user_env = "DB_USER"
    password_env = "DB_PASSWORD"
    connect_timeout_env = "DB_CONNECT_TIMEOUT_SECONDS"

    default_port = 5432
    default_connect_timeout = 5
    pool_max_size_env = "DB_POOL_MAX_SIZE"
    pool_timeout_env = "DB_POOL_TIMEOUT_SECONDS"

    _defaults: ClassVar[dict[type["BaseRepository"], "BaseRepository"]] = {}

    def __init__(self, pool: ConnectionPool | None = None):
        super().__init__(pool if pool is not None else self.build_pool())

    @classmethod
    def connection_factory(cls) -> Any:
        """Default psycopg connection factory configured by DB_* env vars."""
        # psycopg is bundled in the Layer; import lazily so local unit tests
        # that never open a real database can run without installing the driver.
        import psycopg

        return psycopg.connect(
            host=os.environ[cls.host_env],
            port=int(os.environ.get(cls.port_env, cls.default_port)),
            dbname=os.environ[cls.dbname_env],
            user=os.environ[cls.user_env],
            password=os.environ[cls.password_env],
            connect_timeout=int(os.environ.get(cls.connect_timeout_env, cls.default_connect_timeout)),
        )

    @classmethod
    def build_pool(cls, connection_factory: Callable[[], Any] | None = None) -> ConnectionPool:
        factory = connection_factory if connection_factory is not None else cls.connection_factory
        return ConnectionPool(
            factory=factory,
            max_size=int(os.environ.get(cls.pool_max_size_env, "4")),
            timeout_seconds=float(os.environ.get(cls.pool_timeout_env, "5")),
        )

    @classmethod
    def default(cls) -> "BaseRepository":
        """Return the process-wide singleton so warm invocations reuse connections."""
        instance = cls._defaults.get(cls)
        if instance is None:
            instance = cls()
            cls._defaults[cls] = instance
        return instance

    @classmethod
    def clear_default(cls) -> None:
        cls._defaults.pop(cls, None)

    def close_all(self) -> None:
        """Close every pooled connection (e.g. before shutting down)."""
        self._pool.close_all()
