"""Small DB-API compatible pool that is safely reused by warm Lambda invocations.

The Layer deliberately does not bundle a database driver. Applications supply a
factory (for example a psycopg connection factory) and keep the pool at module
scope so a warm Lambda environment can reuse connections.
"""

from contextlib import contextmanager
from queue import Empty, LifoQueue
from threading import Lock
from typing import Callable, Iterator, Protocol, TypeVar


class ClosableConnection(Protocol):
    def close(self) -> None: ...


TConnection = TypeVar("TConnection", bound=ClosableConnection)


class ConnectionPool:
    def __init__(self, factory: Callable[[], TConnection], max_size: int = 4, timeout_seconds: float = 5):
        if max_size < 1:
            raise ValueError("max_size must be at least 1")
        self._factory = factory
        self._max_size = max_size
        self._timeout_seconds = timeout_seconds
        self._available: LifoQueue[TConnection] = LifoQueue(maxsize=max_size)
        self._created = 0
        self._lock = Lock()

    @contextmanager
    def connection(self) -> Iterator[TConnection]:
        connection = self._acquire()
        try:
            yield connection
        except Exception:
            self._discard(connection)
            raise
        else:
            self._available.put(connection)

    def close_all(self) -> None:
        while True:
            try:
                self._discard(self._available.get_nowait())
            except Empty:
                return

    def _acquire(self) -> TConnection:
        try:
            return self._available.get_nowait()
        except Empty:
            pass
        with self._lock:
            if self._created < self._max_size:
                self._created += 1
                try:
                    return self._factory()
                except Exception:
                    self._created -= 1
                    raise
        try:
            return self._available.get(timeout=self._timeout_seconds)
        except Empty as exc:
            raise TimeoutError("Timed out waiting for a database connection") from exc

    def _discard(self, connection: TConnection) -> None:
        try:
            connection.close()
        finally:
            with self._lock:
                self._created -= 1
