"""PostgreSQL persistence for processed transaction lines.

The connection lifecycle (pool sizing, warm-Lambda singleton, discard on error)
lives in the Layer's ``BaseRepository``, including the psycopg connection
factory driven by ``DB_*`` environment variables. This module only adds the
domain-specific insert statement.
"""

from typing import Any

from data_processing_framework import BaseRepository


INSERT_TRANSACTION_SQL = """
INSERT INTO processed_transactions (
    event_id,
    transaction_id,
    line_id,
    store_id,
    quantity,
    unit_price,
    gross_amount,
    currency_code,
    source_system,
    trace_id
)
VALUES (
    %(event_id)s,
    %(transaction_id)s,
    %(line_id)s,
    %(store_id)s,
    %(quantity)s,
    %(unit_price)s,
    %(gross_amount)s,
    %(currency_code)s,
    %(source_system)s,
    %(trace_id)s
)
ON CONFLICT (event_id) DO NOTHING
"""


class PostgresTransactionRepository(BaseRepository):
    """PostgreSQL repository; connection settings come from the environment."""

    def insert(self, item: dict[str, Any]) -> bool:
        """Insert one line; return False when event_id already exists."""
        return self.execute(INSERT_TRANSACTION_SQL, item) > 0


def get_repository() -> PostgresTransactionRepository:
    """Return the module-scope singleton so warm invocations reuse connections."""
    return PostgresTransactionRepository.default()
