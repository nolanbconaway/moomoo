"""A shared logger."""

import sys

import structlog

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
    logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
)

logger: structlog.stdlib.BoundLogger = structlog.get_logger(_app="moomoo")
