"""Structured logging.

Every log line is a structured event so that incident evidence, recovery
attempts, and batch outcomes stay machine-readable. Exceptions are never
swallowed silently: helpers here always attach the traceback.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

from datatrust.config import LogFormat, Settings, get_settings

_configured = False


def configure_logging(settings: Settings | None = None) -> None:
    """Install the structlog pipeline. Idempotent."""
    global _configured
    if _configured:
        return

    settings = settings or get_settings()
    level = getattr(logging, settings.log_level.upper(), logging.INFO)

    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)

    shared: list[structlog.typing.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    renderer: structlog.typing.Processor = (
        structlog.processors.JSONRenderer()
        if settings.log_format is LogFormat.JSON
        else structlog.dev.ConsoleRenderer(colors=True)
    )

    structlog.configure(
        processors=[*shared, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _configured = True


def get_logger(name: str, **initial: Any) -> structlog.stdlib.BoundLogger:
    """Return a bound logger, configuring the pipeline on first use."""
    configure_logging()
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    return logger.bind(**initial) if initial else logger
