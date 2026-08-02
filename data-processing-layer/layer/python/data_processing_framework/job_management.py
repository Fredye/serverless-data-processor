"""Durable lifecycle management for individual data-processing jobs."""

from datetime import UTC, datetime
from typing import Any, Protocol


def _now() -> str:
    return datetime.now(UTC).isoformat()


class JobRunManager(Protocol):
    def start(self, job_id: str, *, event_id: str, source_system: str, trace_id: str | None = None) -> bool: ...

    def complete(self, job_id: str, *, metrics: dict[str, Any] | None = None) -> None: ...

    def fail(self, job_id: str, *, reason: str) -> None: ...


class DynamoDbJobRunManager:
    """Persists job state in DynamoDB using conditional creation for retries."""

    def __init__(self, table_name: str, client=None):
        if client is None:
            import boto3

            client = boto3.client("dynamodb")
        self._client = client
        self._table_name = table_name

    def start(self, job_id: str, *, event_id: str, source_system: str, trace_id: str | None = None) -> bool:
        item = {
            "job_id": {"S": job_id},
            "status": {"S": "RUNNING"},
            "event_id": {"S": event_id},
            "source_system": {"S": source_system},
            "started_at": {"S": _now()},
        }
        if trace_id:
            item["trace_id"] = {"S": trace_id}
        try:
            self._client.put_item(
                TableName=self._table_name,
                Item=item,
                ConditionExpression="attribute_not_exists(job_id)",
            )
            return True
        except self._client.exceptions.ConditionalCheckFailedException:
            # A retry keeps the original start timestamp and continues the same job.
            return False

    def complete(self, job_id: str, *, metrics: dict[str, Any] | None = None) -> None:
        names = {"#status": "status"}
        values = {
            ":status": {"S": "SUCCEEDED"},
            ":completed_at": {"S": _now()},
        }
        expression = "SET #status = :status, completed_at = :completed_at"
        if metrics is not None:
            names["#metrics"] = "metrics"
            values[":metrics"] = {"S": str(metrics)}
            expression += ", #metrics = :metrics"
        self._client.update_item(
            TableName=self._table_name,
            Key={"job_id": {"S": job_id}},
            UpdateExpression=expression,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
            ConditionExpression="attribute_exists(job_id)",
        )

    def fail(self, job_id: str, *, reason: str) -> None:
        self._client.update_item(
            TableName=self._table_name,
            Key={"job_id": {"S": job_id}},
            UpdateExpression="SET #status = :status, failed_at = :failed_at, failure_reason = :reason",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": {"S": "FAILED"},
                ":failed_at": {"S": _now()},
                ":reason": {"S": reason[:1000]},
            },
            ConditionExpression="attribute_exists(job_id)",
        )
