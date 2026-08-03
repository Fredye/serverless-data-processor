"""POS transaction line processor: validation, normalization and persistence."""

from decimal import Decimal, InvalidOperation

from data_processing_framework import DataEvent, ProcessingContext

from transaction_repository import PostgresTransactionRepository, get_repository


class TransactionProcessor:
    name = "transaction-normalization"

    def __init__(self, repository: PostgresTransactionRepository | None = None):
        self._repository = repository if repository is not None else get_repository()

    def validate(self, event: DataEvent) -> None:
        payload = event.payload
        required = ("transaction_id", "line_id", "store_id", "quantity", "unit_price", "currency_code")
        missing = [field for field in required if payload.get(field) in (None, "")]
        if missing:
            raise ValueError(f"Transaction payload missing: {', '.join(missing)}")
        if int(payload["quantity"]) <= 0:
            raise ValueError("quantity must be positive")
        try:
            Decimal(str(payload["unit_price"]))
        except InvalidOperation as exc:
            raise ValueError("unit_price must be numeric") from exc

    def process(self, event: DataEvent, context: ProcessingContext) -> dict:
        payload = event.payload
        quantity = Decimal(str(payload["quantity"]))
        unit_price = Decimal(str(payload["unit_price"]))
        item = {
            "event_id": event.event_id,
            "transaction_id": str(payload["transaction_id"]),
            "line_id": str(payload["line_id"]),
            "store_id": str(payload["store_id"]),
            "quantity": quantity,
            "unit_price": unit_price,
            "gross_amount": quantity * unit_price,
            "currency_code": str(payload["currency_code"]).upper(),
            "source_system": event.source_system,
            "trace_id": context.trace_id,
        }
        # ON CONFLICT DO NOTHING makes the business write itself idempotent as well.
        inserted = self._repository.insert(item)
        return {
            "event_id": event.event_id,
            "gross_amount": str(item["gross_amount"]),
            "inserted": inserted,
        }
