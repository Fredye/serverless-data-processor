"""Stable internal data contract shared by all pipeline stages."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class DataEvent:
    event_id: str
    source_system: str
    payload: dict[str, Any]
    event_type: str = "data.received"
    event_version: str = "1.0"
    trace_id: str | None = None
    occurred_at: str | None = None
    object_uri: str | None = None

    @classmethod
    def from_message(cls, message: dict[str, Any]) -> "DataEvent":
        required = ("event_id", "source_system", "payload")
        missing = [
            name
            for name in required
            if name not in message or message[name] is None or (name != "payload" and message[name] == "")
        ]
        if missing:
            raise ValueError(f"Message is missing required fields: {', '.join(missing)}")
        if not isinstance(message["payload"], dict):
            raise ValueError("payload must be a JSON object")
        return cls(
            event_id=str(message["event_id"]),
            source_system=str(message["source_system"]),
            payload=message["payload"],
            event_type=str(message.get("event_type", "data.received")),
            event_version=str(message.get("event_version", "1.0")),
            trace_id=message.get("trace_id"),
            occurred_at=message.get("occurred_at"),
            object_uri=message.get("object_uri"),
        )


@dataclass
class ProcessingContext:
    job_id: str
    trace_id: str
    attempt: int = 1
    started_at: str = field(default_factory=utc_now)


@dataclass
class ProcessingResult:
    event_id: str
    status: str
    output: dict[str, Any] | None = None
    duplicate: bool = False
