"""JSON logging with request/job/event correlation fields."""

import json
import logging
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Generator

_context: ContextVar[dict[str, str]] = ContextVar("data_processing_log_context", default={})


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            **_context.get(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        standard_fields = logging.makeLogRecord({}).__dict__
        payload.update(
            {
                key: value
                for key, value in record.__dict__.items()
                if key not in standard_fields and not key.startswith("_")
            }
        )
        return json.dumps(payload, ensure_ascii=False, default=str)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not any(getattr(handler, "_data_processing_json", False) for handler in logger.handlers):
        handler = logging.StreamHandler()
        handler._data_processing_json = True
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.propagate = False
    logger.setLevel(logging.INFO)
    return logger


@contextmanager
def log_context(**fields: str) -> Generator[None, None, None]:
    token = _context.set({**_context.get(), **{key: str(value) for key, value in fields.items() if value is not None}})
    try:
        yield
    finally:
        _context.reset(token)


def bind_log_context(**fields: str) -> None:
    _context.set({**_context.get(), **{key: str(value) for key, value in fields.items() if value is not None}})
