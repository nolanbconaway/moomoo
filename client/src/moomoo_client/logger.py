"""A shared logger."""
import structlog

logger: structlog.stdlib.BoundLogger = structlog.get_logger(_app="moomoo")
