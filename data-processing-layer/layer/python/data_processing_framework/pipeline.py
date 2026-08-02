"""Processor contract and orchestration with validation before side effects."""

from typing import Protocol

from .idempotency import IdempotencyStore
from .models import DataEvent, ProcessingContext, ProcessingResult


class DataProcessor(Protocol):
    name: str

    def validate(self, event: DataEvent) -> None: ...

    def process(self, event: DataEvent, context: ProcessingContext) -> dict: ...


class ProcessingPipeline:
    def __init__(self, processor: DataProcessor, idempotency_store: IdempotencyStore):
        self._processor = processor
        self._idempotency_store = idempotency_store

    def execute(self, event: DataEvent, context: ProcessingContext) -> ProcessingResult:
        # Keep malformed messages out of the ledger. They can be quarantined or
        # corrected and replayed without being mistaken for completed work.
        self._processor.validate(event)
        # The processor name prevents key collisions between independent stages.
        key = f"{self._processor.name}#{event.event_id}"
        if not self._idempotency_store.acquire(key):
            return ProcessingResult(event_id=event.event_id, status="SUCCEEDED", duplicate=True)

        output = self._processor.process(event, context)
        return ProcessingResult(event_id=event.event_id, status="SUCCEEDED", output=output)
