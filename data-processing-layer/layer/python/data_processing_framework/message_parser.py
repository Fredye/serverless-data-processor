"""Normalize Lambda event envelopes into application messages."""

import json
from dataclasses import dataclass
from typing import Any

from .models import DataEvent


@dataclass(frozen=True)
class ParsedMessage:
    message_id: str
    data_event: DataEvent
    raw_message: dict[str, Any]


class MessageParser:
    """Parses direct, EventBridge, SQS, and SNS-wrapped SQS messages."""

    def parse_records(self, event: dict[str, Any]) -> list[ParsedMessage]:
        records = event.get("Records")
        if records is not None:
            return [self.parse_sqs_record(record) for record in records]
        return [self.parse_direct_event(event)]

    def parse_sqs_record(self, record: dict[str, Any]) -> ParsedMessage:
        message_id = str(record.get("messageId", "unknown"))
        try:
            message = self._decode_json(record["body"])
            message = self._unwrap_sns(message)
            return ParsedMessage(message_id, self._to_data_event(message), message)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(f"Unable to parse SQS message {message_id}: {exc}") from exc

    def parse_direct_event(self, event: dict[str, Any]) -> ParsedMessage:
        message = event.get("detail", event)
        data_event = self._to_data_event(message)
        return ParsedMessage(str(event.get("id", data_event.event_id)), data_event, message)

    @staticmethod
    def _decode_json(value: str | dict[str, Any]) -> dict[str, Any]:
        message = json.loads(value) if isinstance(value, str) else value
        if not isinstance(message, dict):
            raise ValueError("message must be a JSON object")
        return message

    def _unwrap_sns(self, message: dict[str, Any]) -> dict[str, Any]:
        if message.get("Type") == "Notification" and "Message" in message:
            return self._decode_json(message["Message"])
        return message

    @staticmethod
    def _to_data_event(message: dict[str, Any]) -> DataEvent:
        return DataEvent.from_message(message)
