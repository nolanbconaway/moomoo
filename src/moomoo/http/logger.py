"""Logging module for the HTTP server."""
import logging


def get_logger(name: str) -> logging.Logger:
    """Get a logger for the HTTP server."""
    logger = logging.getLogger(name)

    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
