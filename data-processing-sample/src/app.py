"""Sample POS transaction processor invoked from SQS."""

import os
from collections.abc import Mapping

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
DEFAULT_ATTEMPT = 1
FALLBACK_MESSAGE_ID = "unknown"
JOB_FAILURE_REASON = "See Lambda log for failure details"


def build_pipeline() -> ProcessingPipeline:
    return ProcessingPipeline(
        TransactionProcessor(),
        DynamoDbIdempotencyStore(os.environ["IDEMPOTENCY_TABLE"]),
    )


def build_job_run_manager() -> DynamoDbJobRunManager:
    return DynamoDbJobRunManager(os.environ["JOB_RUNS_TABLE"])


def _message_value(message: Mapping[str, object], key: str, default: object) -> object:
    value = message.get(key, default)
    return default if value is None else value


def _build_processing_context(parsed_message) -> ProcessingContext:
    data_event = parsed_message.data_event
    raw_message = parsed_message.raw_message
    job_id = str(_message_value(raw_message, "job_id", data_event.event_id))
    trace_id = str(_message_value(raw_message, "trace_id", data_event.event_id))
    attempt = int(_message_value(raw_message, "attempt", DEFAULT_ATTEMPT))
    return ProcessingContext(job_id=job_id, trace_id=trace_id, attempt=attempt)


def _mark_job_failed(job_runs: DynamoDbJobRunManager, job_id: str) -> None:
    try:
        job_runs.fail(job_id, reason=JOB_FAILURE_REASON)
    except Exception:
        LOGGER.exception("Unable to mark job as failed", extra={"job_id": job_id})


def _process_parsed_message(parsed, pipeline: ProcessingPipeline, job_runs: DynamoDbJobRunManager) -> None:
    data_event = parsed.data_event
    context = _build_processing_context(parsed)
    with log_context(event_id=data_event.event_id, trace_id=data_event.trace_id, message_id=parsed.message_id):
        created = job_runs.start(
            context.job_id,
            event_id=data_event.event_id,
            source_system=data_event.source_system,
            trace_id=context.trace_id,
        )
        LOGGER.info("Job started", extra={"job_id": context.job_id, "job_created": created})
        result = pipeline.execute(data_event, context)
        job_runs.complete(
            context.job_id,
            metrics={"processed_records": 1, "duplicate": result.duplicate},
        )
        LOGGER.info("Job completed", extra={"job_id": context.job_id, "status": "SUCCEEDED"})
        LOGGER.info("Transaction processed", extra={"status": result.status, "duplicate": result.duplicate})


def lambda_handler(event, _context):
    """Retry only failed SQS records; successful and duplicate records are acknowledged."""
    pipeline = build_pipeline()
    job_runs = build_job_run_manager()
    failures = []
    for record in event.get("Records", []):
        message_id = str(record.get("messageId", FALLBACK_MESSAGE_ID))
        job_id = None
        try:
            parsed = MESSAGE_PARSER.parse_sqs_record(record)
            job_id = str(_message_value(parsed.raw_message, "job_id", parsed.data_event.event_id))
            _process_parsed_message(parsed, pipeline, job_runs)
        except Exception:
            if job_id is not None:
                _mark_job_failed(job_runs, job_id)
            LOGGER.exception("Failed processing SQS record", extra={"message_id": message_id})
            failures.append({"itemIdentifier": message_id})
    return {"batchItemFailures": failures}
