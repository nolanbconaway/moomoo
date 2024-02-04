"""Connectivity utils for the database."""

import os
from logging import WARNING

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_message,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
)

from .logger import get_logger

logger = get_logger().bind(module=__name__)


def get_engine() -> Engine:
    """Get a sqlalchemy engine for the db."""
    return create_engine(os.environ["MOOMOO_POSTGRES_URI"])


def get_session() -> Session:
    """Get a sqlalchemy session for the db."""
    return Session(bind=get_engine())


def execute_sql_fetchall(
    sql: str, params: list[dict] | dict | None = None, session: Session | None = None
) -> list[dict]:
    """Execute a SQL statement and return all results via dict cursors."""

    def f(s: Session):
        res = s.execute(text(sql), params)
        if res.returns_rows:
            return list(map(dict, res.mappings()))
        return []

    if session is None:
        with get_session() as session:
            return f(session)

    return f(session)


# A retry that waits 15s if a required table is missing. this is useful to manage
# runs that happen at the same time as dbt is refreshing the database. This usually
# completes in a few seconds, so just one retry should be enough.
db_retry = retry(
    wait=wait_fixed(5),
    stop=stop_after_attempt(3),
    retry=(
        retry_if_exception_type(ProgrammingError)
        | retry_if_exception_message(match="psycopg.errors.UndefinedTable")
    ),
    reraise=True,
    before_sleep=before_sleep_log(logger, log_level=WARNING, exc_info=True),
)
