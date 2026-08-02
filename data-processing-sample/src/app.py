"""Sample POS transaction processor invoked from SQS."""

import os

from data_processing_framework import (
    DynamoDbIdempotencyStore,
    DynamoDbJobRunManager,
    MessageParser,
    ProcessingContext,
    ProcessingPipeline,
    get_logger,
    log_context,
)
from transaction_processor import TransactionProcessor

LOGGER = get_logger(__name__)
MESSAGE_PARSER = MessageParser()


def build_pipeline() -> ProcessingPipeline:
    return ProcessingPipeline(
        TransactionProcessor(),
        DynamoDbIdempotencyStore(os.environ["IDEMPOTENCY_TABLE"]),
    )


def build_job_run_manager() -> DynamoDbJobRunManager:
    return DynamoDbJobRunManager(os.environ["JOB_RUNS_TABLE"])


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
