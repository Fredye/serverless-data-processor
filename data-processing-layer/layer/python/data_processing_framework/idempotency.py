"""DynamoDB-backed idempotency guard for at-least-once event delivery."""

from typing import Protocol


class IdempotencyStore(Protocol):
    def acquire(self, key: str) -> bool:
        """Return True only for the first successful acquisition of a key."""


class DynamoDbIdempotencyStore:
    def __init__(self, table_name: str, client=None):
        if client is None:
            import boto3

            client = boto3.client("dynamodb")
        self._client = client
        self._table_name = table_name

    def acquire(self, key: str) -> bool:
        try:
            self._client.put_item(
                TableName=self._table_name,
                Item={"idempotency_key": {"S": key}},
                ConditionExpression="attribute_not_exists(idempotency_key)",
            )
            return True
        except self._client.exceptions.ConditionalCheckFailedException:
            return False
