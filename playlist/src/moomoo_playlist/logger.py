import structlog


def get_logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger()
