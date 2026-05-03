"""Shared logger for moomoo navidrome integrations."""

import sys

from loguru import logger as _logger


def _configure_logger() -> None:
    is_tty = sys.stderr.isatty()
    _logger.remove()
    _logger.add(
        sys.stderr,
        level="INFO",
        colorize=is_tty,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{function}:{line} | {message}",
    )


_configure_logger()

# Import this symbol across modules: from moomoo_navidrome.logger import logger
logger = _logger.bind(app="moomoo-navidrome")