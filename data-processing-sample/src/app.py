"""Sample POS transaction processor invoked from SQS."""

import os
from decimal import Decimal, InvalidOperation

from data_processing_framework import (
    DataEvent,
    DynamoDbIdempotencyStore,
    DynamoDbJobRunManager,
    MessageParser,
    ProcessingContext,
    ProcessingPipeline,
    get_logger,
    log_context,
)

LOGGER = get_logger(__name__)
MESSAGE_PARSER = MessageParser()


class TransactionProcessor:
    name = "transaction-normalization"

    def __init__(self, table=None):
        self._table = table or _transactions_table()

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
        # This condition makes the business write itself idempotent as well.
        try:
            self._table.put_item(Item=item, ConditionExpression="attribute_not_exists(event_id)")
        except self._table.meta.client.exceptions.ConditionalCheckFailedException:
            pass
        return {"event_id": event.event_id, "gross_amount": str(item["gross_amount"])}


def build_pipeline() -> ProcessingPipeline:
    return ProcessingPipeline(
        TransactionProcessor(),
        DynamoDbIdempotencyStore(os.environ["IDEMPOTENCY_TABLE"]),
    )


def build_job_run_manager() -> DynamoDbJobRunManager:
    return DynamoDbJobRunManager(os.environ["JOB_RUNS_TABLE"])


def _transactions_table():
    # boto3 is bundled by the Lambda Python runtime. Keeping this import lazy
    # lets validation-focused unit tests run without AWS SDK installation.
    import boto3

    return boto3.resource("dynamodb").Table(os.environ["TRANSACTIONS_TABLE"])


def lambda_handler(event, _context):
    """Retry only failed SQS records; successful and duplicate records are acknowledged."""
    pipeline = build_pipeline()
    job_runs = build_job_run_manager()
    failures = []
    for record in event.get("Records", []):
        job_id = None
        try:
            parsed = MESSAGE_PARSER.parse_sqs_record(record)
            data_event = parsed.data_event
            with log_context(event_id=data_event.event_id, trace_id=data_event.trace_id, message_id=parsed.message_id):
                job_id = str(parsed.raw_message.get("job_id", data_event.event_id))
                context = ProcessingContext(
                    job_id=job_id,
                    trace_id=str(parsed.raw_message.get("trace_id", data_event.event_id)),
                    attempt=int(parsed.raw_message.get("attempt", 1)),
                )
                created = job_runs.start(
                    job_id,
                    event_id=data_event.event_id,
                    source_system=data_event.source_system,
                    trace_id=context.trace_id,
                )
                LOGGER.info("Job started", extra={"job_id": job_id, "created": created})
                result = pipeline.execute(data_event, context)
                job_runs.complete(
                    job_id,
                    metrics={"processed_records": 1, "duplicate": result.duplicate},
                )
                LOGGER.info("Job completed", extra={"job_id": job_id, "status": "SUCCEEDED"})
                LOGGER.info("Transaction processed", extra={"status": result.status, "duplicate": result.duplicate})
        except Exception:
            if job_id is not None:
                try:
                    job_runs.fail(job_id, reason="See Lambda log for failure details")
                except Exception:
                    LOGGER.exception("Unable to mark job as failed", extra={"job_id": job_id})
            LOGGER.exception("Failed processing SQS record", extra={"message_id": record.get("messageId")})
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
