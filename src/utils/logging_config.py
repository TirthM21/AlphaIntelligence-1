"""Centralized logging bootstrap for executable entrypoints."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional


class JsonFormatter(logging.Formatter):
    """Simple JSON formatter for structured logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


def _normalize_level(level: Optional[str]) -> int:
    selected = (level or os.getenv("LOG_LEVEL") or "INFO").upper()
    return getattr(logging, selected, logging.INFO)


def configure_logging(level: Optional[str] = None, json_logs: bool = False) -> None:
    """Configure root logging once for scripts and dashboards.

    Env vars:
    - LOG_LEVEL: DEBUG/INFO/WARNING/ERROR/CRITICAL
    - LOG_FORMAT: json|text
    """

    requested_json = json_logs or os.getenv("LOG_FORMAT", "text").lower() == "json"
    log_level = _normalize_level(level)

    handler = logging.StreamHandler()
    if requested_json:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )

    logging.basicConfig(level=log_level, handlers=[handler], force=True)
