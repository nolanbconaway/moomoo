"""Connectivity utils for the database."""
import os

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session


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
