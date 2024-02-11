"""Connectivity utils for the database."""

import atexit
import datetime
import os
from contextlib import suppress
from logging import WARNING
from typing import Optional
from uuid import uuid4

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
    """Get a sqlalchemy session for the db.

    Automatically registers a close_session function to be called at exit.
    """
    session = Session(bind=get_engine())

    def close_session():
        with suppress(Exception):
            session.close()

    atexit.register(close_session)

    return session


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


def make_tmp_name() -> str:
    """Make a temporary table name.

    Follows the pattern tmp_{datetime}_{uuid}.
    """
    uuid = str(uuid4())[-12:].replace("-", "")
    dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    return f"tmp_{dt}_{uuid}"


def make_temp_table(
    types: dict[str, str], data: list[dict], session: Session, pk: Optional[str] = None
) -> str:
    """Make a temporary table from data, returning the table name.

    Args:
        types: Dictionary of column names to types.
        data: List of dictionaries to insert into the table.
        session: Sqlalchemy session.
        pk: Primary key column name.

    Returns:
        The name of the temporary table.
    """
    tmp_name = make_tmp_name()
    columns = []
    for k, v in types.items():
        if k == pk:
            columns.append(f"{k} {v} primary key")
        else:
            columns.append(f"{k} {v}")

    session.execute(text(f"""create temp table {tmp_name} ({", ".join(columns)})"""))

    if data:
        cols = ", ".join(types.keys())
        values = ", ".join([f":{k}" for k in types])
        sql = f"insert into {tmp_name} ({cols}) values ({values})"
        session.execute(text(sql), data)

    return tmp_name


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
